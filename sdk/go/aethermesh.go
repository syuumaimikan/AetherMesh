// Package aethermesh is a client for AetherMesh: publish data once, run tasks
// and WebAssembly modules across a mesh of machines.
//
// The wire format is four bytes of big-endian length followed by one JSON
// object, in both directions, so this package needs nothing outside the
// standard library.
//
//	mesh, err := aethermesh.Connect(aethermesh.Options{Port: 7100})
//	if err != nil { log.Fatal(err) }
//	defer mesh.Close()
//
//	data, err := mesh.Publish(payload)
//	result, err := mesh.Run("hash", []byte("seed"), []string{data.DataID})
package aethermesh

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

// MaxFrameBytes bounds how much one response may allocate.
const MaxFrameBytes = 256 * 1024 * 1024

// Options describes where the controller is and how to authenticate to it.
type Options struct {
	Host string // defaults to 127.0.0.1
	Port int    // defaults to 7100
	// Token is the shared secret, when the controller requires one.
	Token string
	// TLSCAPath is the controller's certificate. Setting it switches to TLS.
	TLSCAPath string
	// TLSServerName defaults to Host.
	TLSServerName string
	// Timeout applies to each request. Defaults to two minutes.
	Timeout time.Duration
}

// Published is a dataset the controller now holds.
type Published struct {
	DataID    string
	SizeBytes int64
}

// TaskResult is what a task produced. A task that ran and failed has Success
// false and an Error; only transport and protocol problems return an error.
type TaskResult struct {
	TaskID     string
	NodeID     string
	Success    bool
	Output     []byte
	DurationMs float64
	Error      string
}

// NodeSummary is one node in the mesh.
type NodeSummary struct {
	NodeID      string
	Hostname    string
	CPUCores    int
	CPUUsage    float64
	MemoryUsage float64
}

// Mesh is a connection to an AetherMesh controller. It is not safe for
// concurrent use: requests and responses are matched by order.
type Mesh struct {
	conn    net.Conn
	timeout time.Duration
}

type frame map[string]any

// Connect opens a connection and completes the handshake.
func Connect(options Options) (*Mesh, error) {
	host := options.Host
	if host == "" {
		host = "127.0.0.1"
	}
	port := options.Port
	if port == 0 {
		port = 7100
	}
	timeout := options.Timeout
	if timeout == 0 {
		timeout = 2 * time.Minute
	}
	address := net.JoinHostPort(host, fmt.Sprint(port))

	var conn net.Conn
	var err error
	if options.TLSCAPath == "" {
		conn, err = net.DialTimeout("tcp", address, timeout)
	} else {
		var pem []byte
		pem, err = os.ReadFile(options.TLSCAPath)
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", options.TLSCAPath, err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%s contains no certificates", options.TLSCAPath)
		}
		serverName := options.TLSServerName
		if serverName == "" {
			serverName = host
		}
		conn, err = tls.Dial("tcp", address, &tls.Config{RootCAs: roots, ServerName: serverName})
	}
	if err != nil {
		return nil, err
	}

	mesh := &Mesh{conn: conn, timeout: timeout}
	var token any
	if options.Token != "" {
		token = options.Token
	}
	welcome, err := mesh.request(frame{"type": "hello", "token": token})
	if err != nil {
		mesh.Close()
		return nil, err
	}
	if welcome["type"] != "welcome" {
		mesh.Close()
		return nil, fmt.Errorf("handshake refused: %v", welcome["message"])
	}
	return mesh, nil
}

// Close closes the connection.
func (m *Mesh) Close() error { return m.conn.Close() }

// Publish stores data on the controller. Identical bytes yield the same id.
func (m *Mesh) Publish(data []byte) (Published, error) {
	response, err := m.request(frame{
		"type": "publish",
		"data": base64.StdEncoding.EncodeToString(data),
	})
	if err != nil {
		return Published{}, err
	}
	if err := expect(response, "published"); err != nil {
		return Published{}, err
	}
	return Published{
		DataID:    asString(response["data_id"]),
		SizeBytes: int64(asFloat(response["size_bytes"])),
	}, nil
}

// PublishFile publishes a file, e.g. a compiled .wasm module.
func (m *Mesh) PublishFile(path string) (Published, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Published{}, err
	}
	return m.Publish(data)
}

// Run runs a built-in task: echo, hash, or cpu. Inputs are ids from Publish;
// the mesh moves them to the chosen node only if it does not already hold them.
func (m *Mesh) Run(kind string, payload []byte, inputs []string) (TaskResult, error) {
	return m.submit(kind, payload, inputs, nil)
}

// RunWasm runs a WebAssembly module previously published.
func (m *Mesh) RunWasm(moduleID string, payload []byte, inputs []string) (TaskResult, error) {
	return m.submit("wasm", payload, inputs, moduleID)
}

// Nodes lists the nodes currently in the mesh.
func (m *Mesh) Nodes() ([]NodeSummary, error) {
	response, err := m.request(frame{"type": "nodes"})
	if err != nil {
		return nil, err
	}
	if err := expect(response, "nodes"); err != nil {
		return nil, err
	}

	raw, _ := response["nodes"].([]any)
	nodes := make([]NodeSummary, 0, len(raw))
	for _, entry := range raw {
		node, _ := entry.(map[string]any)
		nodes = append(nodes, NodeSummary{
			NodeID:      asString(node["node_id"]),
			Hostname:    asString(node["hostname"]),
			CPUCores:    int(asFloat(node["cpu_cores"])),
			CPUUsage:    asFloat(node["cpu_usage"]),
			MemoryUsage: asFloat(node["memory_usage"]),
		})
	}
	return nodes, nil
}

func (m *Mesh) submit(kind string, payload []byte, inputs []string, module any) (TaskResult, error) {
	if inputs == nil {
		inputs = []string{}
	}
	response, err := m.request(frame{
		"type":    "submit",
		"kind":    kind,
		"payload": base64.StdEncoding.EncodeToString(payload),
		"inputs":  inputs,
		"module":  module,
	})
	if err != nil {
		return TaskResult{}, err
	}
	if err := expect(response, "result"); err != nil {
		return TaskResult{}, err
	}

	output, err := base64.StdEncoding.DecodeString(asString(response["output"]))
	if err != nil {
		return TaskResult{}, fmt.Errorf("controller sent invalid base64: %w", err)
	}
	return TaskResult{
		TaskID:     asString(response["task_id"]),
		NodeID:     asString(response["node_id"]),
		Success:    response["success"] == true,
		Output:     output,
		DurationMs: asFloat(response["duration_ms"]),
		Error:      asString(response["error"]),
	}, nil
}

func (m *Mesh) request(request frame) (frame, error) {
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	deadline := time.Now().Add(m.timeout)
	if err := m.conn.SetDeadline(deadline); err != nil {
		return nil, err
	}

	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))
	if _, err := m.conn.Write(append(header, payload...)); err != nil {
		return nil, err
	}

	if _, err := io.ReadFull(m.conn, header); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header)
	if length > MaxFrameBytes {
		return nil, fmt.Errorf("controller announced a %d byte frame", length)
	}

	body := make([]byte, length)
	if _, err := io.ReadFull(m.conn, body); err != nil {
		return nil, err
	}

	var response frame
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("controller sent invalid JSON: %w", err)
	}
	return response, nil
}

func expect(response frame, kind string) error {
	if response["type"] == kind {
		return nil
	}
	if message := asString(response["message"]); message != "" {
		return fmt.Errorf("%s", message)
	}
	return fmt.Errorf("expected %s, got %v", kind, response["type"])
}

func asString(value any) string {
	text, _ := value.(string)
	return text
}

func asFloat(value any) float64 {
	number, _ := value.(float64)
	return number
}

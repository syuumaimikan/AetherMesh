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
	// Labels is what the node claims to be: {"gpu": "true", "region": "eu-west"}.
	Labels  map[string]string
	Address string
	// DatasetsHeld is what this node already has. Work reading those costs no
	// transfer, which is the decision the scheduler makes on every task.
	DatasetsHeld int
	BytesHeld    int64
	// Connected is not the same as registered: a node keeps its registration
	// until its heartbeat times out, because one late heartbeat is not a death.
	Connected bool
}

// Priority decides who waits once more work has arrived than there are nodes:
// "critical", "high", "normal", "low", "background". It changes nothing on an
// idle mesh, where there is no queue to reorder.
type Priority = string

// Step is one step of a workflow. DependsOn holds indices into the list of
// steps; every dependency's output becomes an input of the step waiting for
// it, so a step reads what the steps before it produced without moving it.
type Step struct {
	Kind        string
	Payload     []byte
	DependsOn   []int
	Inputs      []string
	Constraints []string
	Module      string
}

// StepOutcome is what one step of a workflow did. Step is its index in the
// submitted workflow, not its position in the reply — those differ as soon as
// any step is skipped or resumed.
type StepOutcome struct {
	Step       int
	NodeID     string
	Success    bool
	Output     []byte
	DurationMs float64
	Error      string
}

// WorkflowResult is what a workflow produced.
type WorkflowResult struct {
	Steps   []StepOutcome
	Skipped []int
	// Resumed lists steps an earlier run of the same name already finished.
	Resumed []int
	Success bool
}

// FinishedTask is one task that finished anywhere in the mesh.
type FinishedTask struct {
	TaskID     string
	Kind       string
	NodeID     string
	Success    bool
	DurationMs float64
	// OutputBytes is the size of the whole output, of which Preview is the front.
	OutputBytes int64
	Preview     string
	SecondsAgo  float64
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
//
// Constraints say where the task may run at all: "gpu=true", "region!=us-east",
// or "nvme" for a label that only has to be present. A task no node satisfies
// returns an error rather than running somewhere it was not allowed.
func (m *Mesh) Run(kind string, payload []byte, inputs []string, constraints ...string) (TaskResult, error) {
	return m.submit(kind, payload, inputs, constraints, nil, "")
}

// RunWasm runs a WebAssembly module previously published.
func (m *Mesh) RunWasm(moduleID string, payload []byte, inputs []string, constraints ...string) (TaskResult, error) {
	return m.submit("wasm", payload, inputs, constraints, moduleID, "")
}

// RunWithPriority is Run, saying how urgently this wants a node once a backlog
// has formed.
func (m *Mesh) RunWithPriority(kind string, payload []byte, priority Priority, inputs []string, constraints ...string) (TaskResult, error) {
	return m.submit(kind, payload, inputs, constraints, nil, priority)
}

// Workflow runs several tasks, each after the ones it depends on.
//
// A non-empty run names the run, so submitting the same workflow again resumes
// it rather than repeating it: steps that already finished are skipped when
// their output is still on a node. It needs a controller started with
// checkpoint_path. Reusing a name for a different workflow returns an error
// rather than resuming — skipping step 3 because another graph's step 3
// finished is the one failure here that answers confidently and wrongly.
func (m *Mesh) Workflow(steps []Step, run string) (WorkflowResult, error) {
	encoded := make([]frame, 0, len(steps))
	for _, step := range steps {
		dependsOn := step.DependsOn
		if dependsOn == nil {
			dependsOn = []int{}
		}
		inputs := step.Inputs
		if inputs == nil {
			inputs = []string{}
		}
		constraints := step.Constraints
		if constraints == nil {
			constraints = []string{}
		}
		var module any
		if step.Module != "" {
			module = step.Module
		}
		encoded = append(encoded, frame{
			"kind":        step.Kind,
			"payload":     base64.StdEncoding.EncodeToString(step.Payload),
			"depends_on":  dependsOn,
			"inputs":      inputs,
			"constraints": constraints,
			"module":      module,
		})
	}

	request := frame{"type": "workflow", "steps": encoded}
	if run != "" {
		request["run"] = run
	}

	response, err := m.request(request)
	if err != nil {
		return WorkflowResult{}, err
	}
	if err := expect(response, "workflow"); err != nil {
		return WorkflowResult{}, err
	}

	raw, _ := response["steps"].([]any)
	outcomes := make([]StepOutcome, 0, len(raw))
	for _, entry := range raw {
		step, _ := entry.(map[string]any)
		output, err := base64.StdEncoding.DecodeString(asString(step["output"]))
		if err != nil {
			return WorkflowResult{}, fmt.Errorf("controller sent invalid base64: %w", err)
		}
		outcomes = append(outcomes, StepOutcome{
			Step:       int(asFloat(step["step"])),
			NodeID:     asString(step["node_id"]),
			Success:    step["success"] == true,
			Output:     output,
			DurationMs: asFloat(step["duration_ms"]),
			Error:      asString(step["error"]),
		})
	}

	return WorkflowResult{
		Steps:   outcomes,
		Skipped: asInts(response["skipped"]),
		Resumed: asInts(response["resumed"]),
		Success: response["success"] == true,
	}, nil
}

// Recent lists the last few tasks that finished anywhere in the mesh — not
// only the ones this connection submitted, since a task somebody else ran is
// exactly the interesting case. Preview is the front of the output, not the
// output: results stay on the node that produced them.
func (m *Mesh) Recent(limit int) ([]FinishedTask, error) {
	response, err := m.request(frame{"type": "recent", "limit": limit})
	if err != nil {
		return nil, err
	}
	if err := expect(response, "recent"); err != nil {
		return nil, err
	}

	raw, _ := response["tasks"].([]any)
	tasks := make([]FinishedTask, 0, len(raw))
	for _, entry := range raw {
		task, _ := entry.(map[string]any)
		tasks = append(tasks, FinishedTask{
			TaskID:      asString(task["task_id"]),
			Kind:        asString(task["kind"]),
			NodeID:      asString(task["node_id"]),
			Success:     task["success"] == true,
			DurationMs:  asFloat(task["duration_ms"]),
			OutputBytes: int64(asFloat(task["output_bytes"])),
			Preview:     asString(task["preview"]),
			SecondsAgo:  asFloat(task["seconds_ago"]),
		})
	}
	return tasks, nil
}

// Stats is what the mesh has moved, saved, run and queued, as the controller
// sent it. Returned raw rather than as a struct: it is a dashboard feed that
// grows fields, and a client receiving one it does not know about should see
// it rather than lose it.
func (m *Mesh) Stats() (map[string]any, error) {
	response, err := m.request(frame{"type": "stats"})
	if err != nil {
		return nil, err
	}
	if err := expect(response, "stats"); err != nil {
		return nil, err
	}
	delete(response, "type")
	return response, nil
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
			NodeID:       asString(node["node_id"]),
			Hostname:     asString(node["hostname"]),
			CPUCores:     int(asFloat(node["cpu_cores"])),
			CPUUsage:     asFloat(node["cpu_usage"]),
			MemoryUsage:  asFloat(node["memory_usage"]),
			Labels:       asLabels(node["labels"]),
			Address:      asString(node["address"]),
			DatasetsHeld: int(asFloat(node["datasets_held"])),
			BytesHeld:    int64(asFloat(node["bytes_held"])),
			Connected:    node["connected"] == true,
		})
	}
	return nodes, nil
}

func (m *Mesh) submit(kind string, payload []byte, inputs, constraints []string, module any, priority Priority) (TaskResult, error) {
	if inputs == nil {
		inputs = []string{}
	}
	if constraints == nil {
		constraints = []string{}
	}
	response, err := m.request(frame{
		"type":        "submit",
		"kind":        kind,
		"payload":     base64.StdEncoding.EncodeToString(payload),
		"inputs":      inputs,
		"constraints": constraints,
		"module":      module,
		"priority":    priorityOrNil(priority),
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

// priorityOrNil sends null rather than "" for an unset priority, so the
// controller applies its own default instead of failing to parse an empty one.
func priorityOrNil(priority Priority) any {
	if priority == "" {
		return nil
	}
	return priority
}

// asInts reads a JSON array of numbers, which is how step indices arrive.
func asInts(value any) []int {
	raw, _ := value.([]any)
	out := make([]int, 0, len(raw))
	for _, entry := range raw {
		out = append(out, int(asFloat(entry)))
	}
	return out
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

func asLabels(value any) map[string]string {
	raw, _ := value.(map[string]any)
	labels := make(map[string]string, len(raw))
	for key, entry := range raw {
		labels[key] = asString(entry)
	}
	return labels
}

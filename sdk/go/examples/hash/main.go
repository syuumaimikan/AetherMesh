// Publishes a dataset once and hashes it from three tasks.
//
// Run a controller and at least one agent first, then:
//
//	go run ./examples/hash
package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strconv"

	aethermesh "github.com/syuumaimikan/aethermesh/sdk/go"
)

func main() {
	port := 7100
	if value := os.Getenv("AETHERMESH_PORT"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			log.Fatalf("AETHERMESH_PORT: %v", err)
		}
		port = parsed
	}

	host := os.Getenv("AETHERMESH_HOST")
	if host == "" {
		host = "127.0.0.1"
	}

	mesh, err := aethermesh.Connect(aethermesh.Options{
		Host:  host,
		Port:  port,
		Token: os.Getenv("AETHERMESH_TOKEN"),
	})
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer mesh.Close()

	nodes, err := mesh.Nodes()
	if err != nil {
		log.Fatalf("nodes: %v", err)
	}
	names := make([]string, 0, len(nodes))
	for _, node := range nodes {
		names = append(names, node.Hostname)
	}
	fmt.Println("nodes:", names)

	// 4 MiB of repetitive data: published once, transferred once.
	published, err := mesh.Publish(bytes.Repeat([]byte{0xab}, 4*1024*1024))
	if err != nil {
		log.Fatalf("publish: %v", err)
	}
	fmt.Printf("published %d bytes as %s…\n", published.SizeBytes, published.DataID[:16])

	for index := 0; index < 3; index++ {
		result, err := mesh.Run("hash", []byte("seed"), []string{published.DataID})
		if err != nil {
			log.Fatalf("run: %v", err)
		}
		if !result.Success {
			log.Fatalf("task failed: %s", result.Error)
		}
		fmt.Printf(
			"task %d: %s… on %s in %.1f ms\n",
			index,
			hex.EncodeToString(result.Output)[:16],
			result.NodeID[:8],
			result.DurationMs,
		)
	}
}

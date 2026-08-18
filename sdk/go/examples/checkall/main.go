// Exercises everything the Go SDK can now ask a controller for.
package main

import (
	"fmt"
	"log"

	"github.com/syuumaimikan/aethermesh/sdk/go"
)

func main() {
	mesh, err := aethermesh.Connect(aethermesh.Options{Port: 7100})
	if err != nil {
		log.Fatal(err)
	}
	defer mesh.Close()

	nodes, err := mesh.Nodes()
	if err != nil {
		log.Fatal(err)
	}
	node := nodes[0]
	fmt.Printf("node fields: held=%d bytes=%d connected=%v address=%q\n",
		node.DatasetsHeld, node.BytesHeld, node.Connected, node.Address)

	steps := []aethermesh.Step{
		{Kind: "echo", Payload: []byte("seed")},
		{Kind: "hash", DependsOn: []int{0}},
		{Kind: "no-such-kind", DependsOn: []int{1}},
	}

	first, err := mesh.Workflow(steps, "go-check")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print("run 1: ran=")
	printSteps(first)
	second, err := mesh.Workflow(steps, "go-check")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print("run 2: ran=")
	printSteps(second)

	if _, err := mesh.Workflow([]aethermesh.Step{{Kind: "echo"}}, "go-check"); err != nil {
		fmt.Printf("wrong workflow refused: %.60s…\n", err)
	} else {
		fmt.Println("FAIL: a different workflow was accepted under the same name")
	}

	result, err := mesh.RunWithPriority("echo", []byte("urgent"), "critical", nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("priority run: %q on %.8s\n", result.Output, result.NodeID)

	stats, err := mesh.Stats()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("stats keys: %d\n", len(stats))

	tasks, err := mesh.Recent(3)
	if err != nil {
		log.Fatal(err)
	}
	for _, task := range tasks {
		fmt.Printf("  recent: %-6s %5.1f ms %.0fs ago %q\n",
			task.Kind, task.DurationMs, task.SecondsAgo, task.Preview)
	}
}

func printSteps(result aethermesh.WorkflowResult) {
	indices := []int{}
	for _, step := range result.Steps {
		indices = append(indices, step.Step)
	}
	fmt.Printf("%v resumed=%v\n", indices, result.Resumed)
}

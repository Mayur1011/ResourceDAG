package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/mayur/scheduler/internal/dag"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	wideTasks := generateWideDAG(100)
	saveToFile(wideTasks, "examples/bench_wide.json")

	deepTasks := generateDeepDAG(50)
	saveToFile(deepTasks, "examples/bench_deep.json")

	fmt.Println("✅ Benchmark DAGs generated successfully in examples/ directory.")
}

func generateWideDAG(count int) []dag.Task {
	var tasks []dag.Task
	for i := 1; i <= count; i++ {
		tasks = append(tasks, dag.Task{
			ID:           fmt.Sprintf("WideTask_%d", i),
			Command:      "sleep 100ms",
			Dependencies: []string{},
			CPUCost:      rand.Intn(10) + 1,
			MemCost:      rand.Intn(128) + 16,
		})
	}
	return tasks
}

func generateDeepDAG(depth int) []dag.Task {
	var tasks []dag.Task
	for i := 1; i <= depth; i++ {
		task := dag.Task{
			ID:           fmt.Sprintf("DeepTask_%d", i),
			Command:      "sleep 100ms",
			Dependencies: []string{},
			CPUCost:      rand.Intn(20) + 5,
			MemCost:      128,
		}
		if i > 1 {
			task.Dependencies = append(task.Dependencies, fmt.Sprintf("DeepTask_%d", i-1))
		}
		tasks = append(tasks, task)
	}
	return tasks
}

func saveToFile(tasks []dag.Task, filename string) {
	data, _ := json.MarshalIndent(tasks, "", "  ")
	os.WriteFile(filename, data, 0644)
}
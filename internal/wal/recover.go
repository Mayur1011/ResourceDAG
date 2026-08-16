package wal

import (
	"bufio"
	"encoding/json"
	"os"

	"github.com/mayur/scheduler/internal/dag"
)

func Replay(filename string, graph *dag.DAG) (int, error) {
	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil // No previous crash, start fresh
		}
		return 0, err
	}
	defer file.Close()

	finalStates := make(map[string]dag.TaskState)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err == nil {
			finalStates[entry.TaskID] = entry.State
		}
	}

	recoveredCount := 0
	for taskID, state := range finalStates {
		task := graph.Tasks[taskID]

		if state == dag.StateCompleted || state == dag.StateSkipped {
			task.State = state
			recoveredCount++

			// Unblock downstream dependencies
			for _, childID := range graph.AdjacencyList[taskID] {
				graph.InDegree[childID]--
			}
		} else if state == dag.StateFailed {
			task.State = state
			recoveredCount++
		}
		// Notice: We ignore dag.StateRunning!
		// If a task crashed while running, it will remain in dag.StatePending
		// and its InDegree is already correct. It will just be retried.
	}
	return recoveredCount, nil
}
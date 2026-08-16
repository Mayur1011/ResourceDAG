package dag

import (
	"encoding/json"
	"fmt"
)

// ParseJSON parses a JSON array of tasks and builds the internal DAG structures.
func ParseJSON(payload []byte) (*DAG, error) {
	var taskList []Task
	if err := json.Unmarshal(payload, &taskList); err != nil {
		return nil, fmt.Errorf("failed to parse JSON payload: %w", err)
	}

	graph := NewDAG()

	// 1st Pass: Register all tasks and initialize their In-Degrees
	for i := range taskList {
		task := &taskList[i]
		task.State = StatePending // Default state

		if _, exists := graph.Tasks[task.ID]; exists {
			return nil, fmt.Errorf("duplicate task ID found: %s", task.ID)
		}

		graph.Tasks[task.ID] = task
		graph.InDegree[task.ID] = len(task.Dependencies)
	}

	// 2nd Pass: Build the Adjacency List and validate dependencies
	for _, task := range graph.Tasks {
		for _, depID := range task.Dependencies {
			// Validate that the parent dependency actually exists in our graph
			if _, exists := graph.Tasks[depID]; !exists {
				return nil, fmt.Errorf("task %s depends on unknown task %s", task.ID, depID)
			}

			// Add the current task as a child of its dependency
			// (Parent -> Child relationship)
			graph.AdjacencyList[depID] = append(graph.AdjacencyList[depID], task.ID)
		}
	}

	if err := graph.Validate(); err != nil {
		return nil, fmt.Errorf("invalid DAG: %w", err)
	}

	return graph, nil
}



/*
 [
   {
     "id": "Task_A",
     "command": "echo 'Running A'",
     "dependencies": [],
     "cpu_cost": 20,
     "mem_cost": 512
   },
   {
     "id": "Task_B",
     "command": "echo 'Running B'",
     "dependencies": [],
     "cpu_cost": 10,
     "mem_cost": 256
   },
   {
     "id": "Task_C",
     "command": "echo 'Running C (Needs A and B)'",
     "dependencies": ["Task_A", "Task_B"],
     "cpu_cost": 50,
     "mem_cost": 1024
   }
 ]
 */
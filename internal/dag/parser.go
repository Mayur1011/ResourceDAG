package dag

import (
	"encoding/json"
	"fmt"
	"log"
)

type DAGPayload struct {
	ID    string `json:"id"`
	Tasks []Task `json:"tasks"`
}

// ParseJSON parses a JSON array of tasks and builds the internal DAG structures.
func ParseJSON(payload []byte) (*DAG, error) {
	log.Printf("Parsing DAG: %s\n", payload)
	var req DAGPayload
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("failed to parse JSON payload: %w", err)
	}
	log.Printf("Parsed DAG: %v\n", req)
	if req.ID == "" {
		return nil, fmt.Errorf("id field is required")
	}

	graph := NewDAG()
	graph.ID = req.ID

	log.Printf("Parsing DAG: %s\n", graph.ID)

	for i := range req.Tasks {
		task := &req.Tasks[i]
		task.State = StatePending // Default state
		if _, exists := graph.Tasks[task.ID]; exists {
			return nil, fmt.Errorf("duplicate task ID found: %s", task.ID)
		}
		graph.Tasks[task.ID] = task
		graph.InDegree[task.ID] = len(task.Dependencies)
	}

	for _, task := range graph.Tasks {
		for _, depID := range task.Dependencies {
			if _, exists := graph.Tasks[depID]; !exists {
				return nil, fmt.Errorf("task %s depends on unknown task %s", task.ID, depID)
			}
			graph.AdjacencyList[depID] = append(graph.AdjacencyList[depID], task.ID)
		}
	}

	if err := graph.Validate(); err != nil {
		return nil, fmt.Errorf("invalid DAG: %w", err)
	}
	return graph, nil
}



/*
{
  "id": "dag_production_run_01",
  "tasks": [
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
}
 */

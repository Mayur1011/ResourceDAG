package dag

import (
	"fmt"
	"strings"
)

func (d *DAG) Validate() error {
	// 0 = unvisited (default map value)
	// 1 = visiting (in the current execution path)
	// 2 = visited (fully processed and safe)
	states := make(map[string]int)

	var dfs func(nodeID string, path []string) error
	dfs = func(nodeID string, path []string) error {
		states[nodeID] = 1 // Mark as "Visiting"
		path = append(path, nodeID)

		for _, childID := range d.AdjacencyList[nodeID] {
			if states[childID] == 1 {
				// Cycle detected! The child is already in our current path.
				cycle := append(path, childID)
				return fmt.Errorf("invalid DAG - cycle detected: %s", strings.Join(cycle, " -> "))
			}

			if states[childID] == 0 { // Unvisited
				if err := dfs(childID, path); err != nil {
					return err
				}
			}
		}

		states[nodeID] = 2
		return nil
	}

	for taskID := range d.Tasks {
		if states[taskID] == 0 {
			if err := dfs(taskID, nil); err != nil {
				return err
			}
		}
	}

	return nil
}

func (d *DAG) ComputeCriticalPaths() {
	computed := make(map[string]bool)

	var dfs func(nodeID string) int
	dfs = func(nodeID string) int {
		if computed[nodeID] {
			return d.Tasks[nodeID].CriticalPath
		}

		maxChildPath := 0
		for _, childID := range d.AdjacencyList[nodeID] {
			childPath := dfs(childID)
			if childPath > maxChildPath {
				maxChildPath = childPath
			}
		}

		d.Tasks[nodeID].CriticalPath = maxChildPath + 1
		computed[nodeID] = true
		return d.Tasks[nodeID].CriticalPath
	}

	for id := range d.Tasks {
		dfs(id)
	}
}
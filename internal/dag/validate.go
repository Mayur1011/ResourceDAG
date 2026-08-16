package dag

import (
	"fmt"
	"strings"
)

// Validate traverses the graph to ensure there are no circular dependencies.
func (d *DAG) Validate() error {
	// Track visitation states:
	// 0 = unvisited (default map value)
	// 1 = visiting (in the current execution path)
	// 2 = visited (fully processed and safe)
	states := make(map[string]int)

	// dfs is a recursive helper function to explore the graph
	var dfs func(nodeID string, path []string) error
	dfs = func(nodeID string, path []string) error {
		states[nodeID] = 1 // Mark as "Visiting"
		path = append(path, nodeID)

		// Check all children of the current node
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

		// All children explored successfully. Mark as "Visited".
		states[nodeID] = 2
		return nil
	}

	// We must iterate over all tasks because the graph might have
	// multiple disconnected components (e.g., Task A and Task Z have no relation).
	for taskID := range d.Tasks {
		if states[taskID] == 0 {
			if err := dfs(taskID, nil); err != nil {
				return err
			}
		}
	}

	return nil
}

// ComputeCriticalPaths calculates the maximum depth of descendants for every task.
// A leaf node (no children) has a Critical Path of 1.
func (d *DAG) ComputeCriticalPaths() {
	// Memoization map to avoid recalculating paths
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

		// Critical path = my cost (1) + longest child's path
		d.Tasks[nodeID].CriticalPath = maxChildPath + 1
		computed[nodeID] = true
		return d.Tasks[nodeID].CriticalPath
	}

	for id := range d.Tasks {
		dfs(id)
	}
}
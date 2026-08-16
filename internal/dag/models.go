package dag

import "time"

// TaskState represents the current lifecycle phase of a task.
type TaskState string

const (
	StatePending   TaskState = "PENDING"   // Waiting for dependencies to finish
	StateReady     TaskState = "READY"     // Dependencies met, waiting for resources
	StateRunning   TaskState = "RUNNING"   // Currently executing in a worker
	StateCompleted TaskState = "COMPLETED" // Successfully finished
	StateFailed    TaskState = "FAILED"    // Panicked or returned an error
	StateSkipped   TaskState = "SKIPPED"   // Cancelled because a parent failed
)

// Task represents a single unit of work in the DAG.
// internal/dag/models.go

type Task struct {
	ID           string   `json:"id"`
	Command      string   `json:"command"`
	Dependencies []string `json:"dependencies"`
	CPUCost      int      `json:"cpu_cost"`
	MemCost      int      `json:"mem_cost"`

	State        TaskState `json:"-"`
	CriticalPath int       `json:"-"`
	EnqueuedAt   time.Time `json:"-"` // NEW: Tracks queue entry time
}

// DAG represents the entire executable graph.
type DAG struct {
	Tasks         map[string]*Task    // Quick lookup of tasks by ID
	AdjacencyList map[string][]string // Maps a Parent ID -> List of Child IDs
	InDegree      map[string]int      // Tracks how many uncompleted parents a task has
}

// NewDAG initializes an empty DAG structure.
func NewDAG() *DAG {
	return &DAG{
		Tasks:         make(map[string]*Task),
		AdjacencyList: make(map[string][]string),
		InDegree:      make(map[string]int),
	}
}
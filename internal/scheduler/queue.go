package scheduler

import (
	"container/heap"
	"time"

	"github.com/mayur/scheduler/internal/dag"
)

type Policy string

const (
	PolicySJF          Policy = "SJF"
	PolicyCriticalPath Policy = "CRITICAL_PATH"
)

type Item struct {
	Task     *dag.Task
	Priority int
	Index    int
}

type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

// Max-Heap: We want the item with the HIGHEST priority to pop first.
func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Priority > pq[j].Priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // Avoid memory leak
	item.Index = -1 // For safety
	*pq = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) Enqueue(task *dag.Task, policy Policy) {
	task.EnqueuedAt = time.Now()

	priority := 0
	if policy == PolicySJF {
		priority = -task.CPUCost
	} else if policy == PolicyCriticalPath {
		priority = task.CriticalPath
	}

	heap.Push(pq, &Item{
		Task:     task,
		Priority: priority,
	})
}
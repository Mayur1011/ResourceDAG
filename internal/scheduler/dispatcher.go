package scheduler

import (
	"container/heap"
	"context"
	"fmt"
	"time"

	"github.com/mayur/scheduler/internal/dag"
	"github.com/mayur/scheduler/internal/resource"
	"github.com/mayur/scheduler/internal/worker"
)

type Dispatcher struct {
	graph               *dag.DAG
	workerCount         int
	resMgr              resource.Manager
	policy              Policy
	processedCount      int
	starvationThreshold time.Duration // NEW
}

func NewDispatcher(graph *dag.DAG, workerCount int, resMgr resource.Manager, policy Policy) *Dispatcher {
	return &Dispatcher{
		graph:               graph,
		workerCount:         workerCount,
		resMgr:              resMgr,
		policy:              policy,
		starvationThreshold: 2 * time.Second, // After 2 seconds, trigger anti-starvation
	}
}

func (d *Dispatcher) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tasksChan := make(chan *dag.Task)
	resultsChan := make(chan worker.Result)

	worker.StartPool(ctx, d.workerCount, tasksChan, resultsChan)

	pq := make(PriorityQueue, 0)
	heap.Init(&pq)

	for taskID, degree := range d.graph.InDegree {
		if degree == 0 {
			pq.Enqueue(d.graph.Tasks[taskID], d.policy)
		}
	}

	totalTasks := len(d.graph.Tasks)

	for d.processedCount < totalTasks {
		var activeTaskChan chan<- *dag.Task
		var candidate *dag.Task
		candidateIdx := -1

		if pq.Len() > 0 {
			headItem := pq[0]
			isStarving := time.Since(headItem.Task.EnqueuedAt) > d.starvationThreshold

			if isStarving {
				if d.resMgr.HasCapacity(headItem.Task.CPUCost, headItem.Task.MemCost) {
					candidate = headItem.Task
					candidateIdx = 0
				} else {
					fmt.Printf("⚠️  [Starvation Mode] Halting backfill. Waiting on resources for %s\n", headItem.Task.ID)
				}
			} else {
				for i, item := range pq {
					if d.resMgr.HasCapacity(item.Task.CPUCost, item.Task.MemCost) {
						candidate = item.Task
						candidateIdx = i
						break // Found a job that fits!
					}
				}
			}

			if candidate != nil {
				activeTaskChan = tasksChan
			}
		}

		select {
		case activeTaskChan <- candidate:
			candidate.State = dag.StateRunning
			d.resMgr.TryAcquire(candidate.CPUCost, candidate.MemCost)
			candidate.State = dag.StateRunning
			heap.Remove(&pq, candidateIdx)

		case result := <-resultsChan:
			task := d.graph.Tasks[result.TaskID]
			d.resMgr.Release(task.CPUCost, task.MemCost)
			d.processedCount++

			if result.Err != nil {
				task.State = dag.StateFailed
				d.skipChildren(task.ID)
			} else {
				task.State = dag.StateCompleted
				for _, childID := range d.graph.AdjacencyList[result.TaskID] {
					d.graph.InDegree[childID]--
					if d.graph.InDegree[childID] == 0 && d.graph.Tasks[childID].State != dag.StateSkipped {
						d.graph.Tasks[childID].State = dag.StateReady
						pq.Enqueue(d.graph.Tasks[childID], d.policy)
					}
				}
			}
		}
	}

	close(tasksChan)
	fmt.Println("🎉 Engine shutdown. Queue processed successfully.")
}

func (d *Dispatcher) skipChildren(failedTaskID string) {
	var queue []string
	queue = append(queue, d.graph.AdjacencyList[failedTaskID]...)

	for len(queue) > 0 {
		currID := queue[0]
		queue = queue[1:]

		childTask := d.graph.Tasks[currID]

		if childTask.State != dag.StateSkipped {
			fmt.Printf("⏭️  Skipping %s due to upstream failure.\n", currID)
			childTask.State = dag.StateSkipped
			d.processedCount++

			queue = append(queue, d.graph.AdjacencyList[currID]...)
		}
	}
}
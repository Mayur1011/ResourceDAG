package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/mayur/scheduler/internal/dag"
)

type Result struct {
	TaskID string
	Err    error
}

func StartPool(ctx context.Context, workerCount int, tasksChan <-chan *dag.Task, resultsChan chan<- Result) {
	for i := 1; i <= workerCount; i++ {
		go func(workerID int) {
			for {
				select {
				case <-ctx.Done():
					return // Graceful shutdown signal received
				case task, ok := <-tasksChan:
					if !ok {
						return
					}
					executeTask(workerID, task, resultsChan)
				}
			}
		}(i)
	}
}

func executeTask(workerID int, task *dag.Task, resultsChan chan<- Result) {
	var err error

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("worker panic: %v", r)
			resultsChan <- Result{TaskID: task.ID, Err: err}
		}
	}()

	fmt.Printf("👷 Worker %d starting task: %s\n", workerID, task.ID)

	if task.Command == "panic" {
		panic("simulated catastrophic crash")
	} else if task.Command == "fail" {
		err = fmt.Errorf("simulated standard error")
	} else {
		time.Sleep(500 * time.Millisecond)
		fmt.Printf("✅ Worker %d finished task: %s\n", workerID, task.ID)
	}

	resultsChan <- Result{TaskID: task.ID, Err: err}
}
package metrics

import (
	"sync"
	"time"
)

// Metrics holds system health and performance data.
type Metrics struct {
	mu sync.RWMutex

	TotalDAGsCompleted  int     `json:"total_dags_completed"`
	TotalTasksProcessed int     `json:"total_tasks_processed"`
	TotalWaitTimeMs     int64   `json:"-"` // Internal accumulator
	AverageWaitTimeMs   float64 `json:"average_queue_wait_ms"`
	TotalMakespanMs     int64   `json:"-"` // Internal accumulator
	AverageMakespanMs   float64 `json:"average_makespan_ms"`

	// Tracked via the Resource Manager
	CPUUtilizationPct float64 `json:"cpu_utilization_pct"`
	MemUtilizationPct float64 `json:"mem_utilization_pct"`
}

var GlobalMetrics = &Metrics{}

type Snapshot struct {
	TotalDAGsCompleted  int     `json:"total_dags_completed"`
	TotalTasksProcessed int     `json:"total_tasks_processed"`
	AverageWaitTimeMs   float64 `json:"average_queue_wait_ms"`
	AverageMakespanMs   float64 `json:"average_makespan_ms"`
	CPUUtilizationPct   float64 `json:"cpu_utilization_pct"`
	MemUtilizationPct   float64 `json:"mem_utilization_pct"`
}

// Snapshot returns a thread-safe read-only view of the current metrics.
func (m *Metrics) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return Snapshot{
		TotalDAGsCompleted:  m.TotalDAGsCompleted,
		TotalTasksProcessed: m.TotalTasksProcessed,
		AverageWaitTimeMs:   m.AverageWaitTimeMs,
		AverageMakespanMs:   m.AverageMakespanMs,
		CPUUtilizationPct:   m.CPUUtilizationPct,
		MemUtilizationPct:   m.MemUtilizationPct,
	}
}

// RecordTaskWait logs how long a single task sat in the Priority Queue.
func (m *Metrics) RecordTaskWait(enqueuedAt time.Time) {
	waitMs := time.Since(enqueuedAt).Milliseconds()
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TotalTasksProcessed++
	m.TotalWaitTimeMs += waitMs
	m.AverageWaitTimeMs = float64(m.TotalWaitTimeMs) / float64(m.TotalTasksProcessed)
}

// RecordDAGMakespan logs the total time from DAG submission to final task completion.
func (m *Metrics) RecordDAGMakespan(start time.Time) {
	makespan := time.Since(start).Milliseconds()
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TotalDAGsCompleted++
	m.TotalMakespanMs += makespan
	m.AverageMakespanMs = float64(m.TotalMakespanMs) / float64(m.TotalDAGsCompleted)
}
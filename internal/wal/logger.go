package wal

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/mayur/scheduler/internal/dag"
)

type LogEntry struct {
	Timestamp time.Time     `json:"timestamp"`
	TaskID    string        `json:"task_id"`
	State     dag.TaskState `json:"state"`
}

type Logger struct {
	file *os.File
	mu   sync.Mutex
}

func NewLogger(filename string) (*Logger, error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &Logger{file: file}, nil
}

func (l *Logger) Append(taskID string, state dag.TaskState) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry := LogEntry{
		Timestamp: time.Now(),
		TaskID:    taskID,
		State:     state,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	data = append(data, '\n')
	if _, err := l.file.Write(data); err != nil {
		return err
	}

	return l.file.Sync() // forcfully pushing the data to disk (as we dont want to lost the data from RAM)
}

func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}
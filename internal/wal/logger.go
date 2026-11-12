package wal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mayur/scheduler/internal/dag"
)

const walDir = "walLogs"

type LogEntry struct {
	Timestamp time.Time     `json:"timestamp"`
	DAGID     string        `json:"dag_id"`
	TaskID    string        `json:"task_id"`
	State     dag.TaskState `json:"state"`
}

type Logger struct {
	dagID string
	file  *os.File
	mu    sync.Mutex
}

func init() {
	_ = os.MkdirAll(walDir, 0755)
}

func getPath(dagID string) string {
	return filepath.Join(walDir, fmt.Sprintf("wal_%s.log", dagID))
}

func NewLogger(dagID string) (*Logger, error) {
	file, err := os.OpenFile(getPath(dagID), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &Logger{dagID: dagID, file: file}, nil
}

func (l *Logger) Append(taskID string, state dag.TaskState) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	fmt.Printf("Appending log entry: task_id=%s, state=%s\n", taskID, state)

	entry := LogEntry{
		Timestamp: time.Now(),
		DAGID:     l.dagID,
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
	return l.file.Sync()
}

func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}
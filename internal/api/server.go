package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/mayur/scheduler/internal/dag"
	"github.com/mayur/scheduler/internal/metrics"
	"github.com/mayur/scheduler/internal/resource"
	"github.com/mayur/scheduler/internal/scheduler"
	"github.com/mayur/scheduler/internal/wal"
)

type Server struct {
	httpServer *http.Server
	resMgr     resource.Manager
	wg         sync.WaitGroup
	isShuttingDown bool
	mu         sync.RWMutex
}

func NewServer(addr string, resMgr resource.Manager, walLog *wal.Logger) *Server {
	s := &Server{
		resMgr: resMgr,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/submit", s.handleSubmit)
	mux.HandleFunc("/metrics", s.handleMetrics)

	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return s
}

func (s *Server) Start() error {
	return s.httpServer.ListenAndServe()
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
    s.mu.RLock()
    if s.isShuttingDown {
        s.mu.RUnlock()
        http.Error(w, "Server is shutting down", http.StatusServiceUnavailable)
        return
    }
    s.mu.RUnlock()

    body, err := io.ReadAll(r.Body)
    if err != nil {
        http.Error(w, "Failed to read body", http.StatusBadRequest)
        return
    }
    // log.Printf("Received DAG submission: %s\n", body)

    graph, err := dag.ParseJSON(body)
    if err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }
    log.Printf("Parsed DAG: %s\n", graph.ID)

    walLogger, err := wal.NewLogger(graph.ID)
    if err != nil {
        http.Error(w, "Failed to initialize WAL", http.StatusInternalServerError)
        return
    }

    s.wg.Add(1)

    // running DAG in the background
    go func() {
        defer s.wg.Done()
        defer walLogger.Close() // NEW: Safely close the file when the DAG finishes

        startTime := time.Now()

        if recovered, _ := wal.Replay(graph.ID, graph); recovered > 0 {
            fmt.Printf("[DAG %s] Recovered %d tasks from WAL\n", graph.ID, recovered)
        }

        sched := scheduler.NewDispatcher(graph, 4, s.resMgr, walLogger, scheduler.PolicyCriticalPath)
        sched.Run(context.Background())

        metrics.GlobalMetrics.RecordDAGMakespan(startTime)
    }()

    w.WriteHeader(http.StatusAccepted)
    w.Write([]byte(`{"status": "DAG accepted and queued for execution"}`))
}

// handleMetrics returns the current state of the engine.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics.GlobalMetrics.Snapshot())
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.isShuttingDown = true
	s.mu.Unlock()

	if err := s.httpServer.Shutdown(ctx); err != nil {
		return err
	}

	c := make(chan struct{})
	go func() {
		defer close(c)
		s.wg.Wait()
	}()

	select {
	case <-c:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
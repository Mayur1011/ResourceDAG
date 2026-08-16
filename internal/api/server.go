package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/mayur/scheduler/internal/dag"
	"github.com/mayur/scheduler/internal/metrics"
	"github.com/mayur/scheduler/internal/resource"
	"github.com/mayur/scheduler/internal/scheduler"
)

type Server struct {
	httpServer *http.Server
	resMgr     resource.Manager
	wg         sync.WaitGroup
	isShuttingDown bool
	mu         sync.RWMutex
}

func NewServer(addr string, resMgr resource.Manager) *Server {
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

	graph, err := dag.ParseJSON(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.wg.Add(1)

	//  running DAG in the background
	go func() {
		defer s.wg.Done()
		startTime := time.Now()

		sched := scheduler.NewDispatcher(graph, 4, s.resMgr, scheduler.PolicyCriticalPath)
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
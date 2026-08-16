package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mayur/scheduler/internal/api"
	"github.com/mayur/scheduler/internal/resource"
)

func main() {
	resMgr := resource.NewMutexManager(100, 1024)
	srv := api.NewServer(":6969", resMgr)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		fmt.Println("🚀 DAG Engine API starting on :6969...")
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()
	<-ctx.Done()
	fmt.Println("\n🛑 Shutdown signal received. Draining queues...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("⚠️ Forced shutdown (timeout or error): %v", err)
	}
	fmt.Println("✅ All active jobs finished. Engine shut down cleanly.")
}
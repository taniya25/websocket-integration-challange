package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Cryptovate-India/websocket-service/internal/handlers"
)

func main() {
	// Create WebSocket handler
	wsHandler := handlers.NewWebSocketHandler()

	// Start metrics reset goroutine
	wsHandler.resetMetrics()

	// Create HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", wsHandler.HandleWebsocket)
	mux.HandleFunc("/metrics", wsHandler.HandleMetrics)

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// Start server in a goroutine
	go func() {
		log.Printf("Server starting on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Error starting server: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// Graceful shutdown
	log.Println("Shutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited properly")
}

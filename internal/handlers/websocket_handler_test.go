package handlers

import (
	"context"
	"testing"

	"github.com/Cryptovate-India/websocket-service/internal/config"
)

func setupTestHandler(t *testing.T, opts ...func(*WebsocketHandler)) (*WebsocketHandler, func()) {
	cfg := &config.Config{
		ServiceName: "test",
		Environment: "test",
		LogLevel:    "debug",
		HTTPPort:    8080,
		GRPCPort:    9090,
	}
	cfg.Websocket.ReadBufferSize = 1024
	cfg.Websocket.WriteBufferSize = 1024
	cfg.Websocket.CheckOrigin = false

	ctx, cancel := context.WithCancel(context.Background())
	handler := NewWebsocketHandler(ctx, cfg)

	// Apply options
	for _, opt := range opts {
		opt(handler)
	}

	cleanup := func() {
		cancel()
		handler.Close()
	}

	return handler, cleanup
}

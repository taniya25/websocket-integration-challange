package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Cryptovate-India/websocket-service/internal/config"
	"github.com/Cryptovate-India/websocket-service/internal/telemetry"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestHandler(t *testing.T) (*WebsocketHandler, func()) {
	ctx := context.Background()
	cfg := &config.Config{
		Websocket: config.WebsocketConfig{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin:     false,
			AllowedOrigins:  []string{"*"},
		},
	}
	logger := telemetry.NewLogger()
	handler := NewWebsocketHandler(ctx, cfg)
	handler.logger = logger
	return handler, func() {
		// Cleanup if needed
	}
}

func TestWebsocketHandler_ErrorScenarios(t *testing.T) {
	handler, cleanup := setupTestHandler(t)
	defer cleanup()
	server := httptest.NewServer(http.HandlerFunc(handler.HandleWebsocket))
	defer server.Close()

	// Test invalid message format
	t.Run("Invalid Message Format", func(t *testing.T) {
		conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
		require.NoError(t, err)
		defer conn.Close()

		// Send invalid JSON
		err = conn.WriteMessage(websocket.TextMessage, []byte("invalid json"))
		require.NoError(t, err)

		// Should receive error message
		_, message, err := conn.ReadMessage()
		require.NoError(t, err)

		var response map[string]interface{}
		err = json.Unmarshal(message, &response)
		require.NoError(t, err)
		assert.Equal(t, "error", response["type"])
		assert.Equal(t, "message_parse_error", response["error"])

		// Close connection gracefully
		err = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		require.NoError(t, err)
	})

	// Test invalid message type
	t.Run("Invalid Message Type", func(t *testing.T) {
		conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
		require.NoError(t, err)
		defer conn.Close()

		// Send message with invalid type
		msg := map[string]interface{}{
			"type": "invalid_type",
		}
		msgBytes, _ := json.Marshal(msg)
		err = conn.WriteMessage(websocket.TextMessage, msgBytes)
		require.NoError(t, err)

		// Should receive error message
		_, message, err := conn.ReadMessage()
		require.NoError(t, err)

		var response map[string]interface{}
		err = json.Unmarshal(message, &response)
		require.NoError(t, err)
		assert.Equal(t, "error", response["type"])
		assert.Equal(t, "invalid_message_type", response["error"])

		// Close connection gracefully
		err = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		require.NoError(t, err)
	})

	// Test duplicate subscription
	t.Run("Duplicate Subscription", func(t *testing.T) {
		conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
		require.NoError(t, err)
		defer conn.Close()

		// Subscribe to channel
		subscribeMsg := map[string]interface{}{
			"type":    "subscribe",
			"channel": "test",
		}
		msgBytes, _ := json.Marshal(subscribeMsg)
		err = conn.WriteMessage(websocket.TextMessage, msgBytes)
		require.NoError(t, err)

		// Read subscription confirmation
		_, _, err = conn.ReadMessage()
		require.NoError(t, err)

		// Try to subscribe again
		err = conn.WriteMessage(websocket.TextMessage, msgBytes)
		require.NoError(t, err)

		// Should receive error message
		_, message, err := conn.ReadMessage()
		require.NoError(t, err)

		var response map[string]interface{}
		err = json.Unmarshal(message, &response)
		require.NoError(t, err)
		assert.Equal(t, "error", response["type"])
		assert.Equal(t, "subscribe_error", response["error"])

		// Close connection gracefully
		err = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		require.NoError(t, err)
	})

	// Test unsubscribe without subscription
	t.Run("Unsubscribe Without Subscription", func(t *testing.T) {
		conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
		require.NoError(t, err)
		defer conn.Close()

		// Try to unsubscribe from channel
		unsubscribeMsg := map[string]interface{}{
			"type":    "unsubscribe",
			"channel": "test",
		}
		msgBytes, _ := json.Marshal(unsubscribeMsg)
		err = conn.WriteMessage(websocket.TextMessage, msgBytes)
		require.NoError(t, err)

		// Should receive error message
		_, message, err := conn.ReadMessage()
		require.NoError(t, err)

		var response map[string]interface{}
		err = json.Unmarshal(message, &response)
		require.NoError(t, err)
		assert.Equal(t, "error", response["type"])
		assert.Equal(t, "unsubscribe_error", response["error"])

		// Close connection gracefully
		err = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		require.NoError(t, err)
	})
}

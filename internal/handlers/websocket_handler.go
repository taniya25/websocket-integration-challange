package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Cryptovate-India/websocket-service/internal/clients"
	"github.com/Cryptovate-India/websocket-service/internal/config"
	"github.com/Cryptovate-India/websocket-service/internal/telemetry"
	"github.com/gorilla/websocket"
	"golang.org/x/time/rate"
)

const (
	// Time allowed to write a message to the peer
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer
	pongWait = 60 * time.Second

	// Send pings to peer with this period
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 512 * 1024 // 512KB

	// Maximum number of messages in the send channel
	maxSendChannelSize = 256

	// Maximum number of messages that can be queued for a client
	maxMessageQueue = 1000

	// Rate limiting constants
	requestsPerSecond = 10
	burstSize         = 20
)

// Client is a middleman between the websocket connection and the hub.
type Client struct {
	hub *WebsocketHandler

	// The websocket connection.
	conn *websocket.Conn

	// Buffered channel of outbound messages.
	send chan []byte

	// Subscribed channels
	subscribedChannels map[string]bool

	// Mutex for subscribedChannels
	subMutex sync.RWMutex

	productFilters map[string][]string
	mu             sync.RWMutex
	id             string
	connectedAt    time.Time
	lastActivity   time.Time
	reconnectCount int
}

// WebsocketHandler handles websocket connections
type WebsocketHandler struct {
	upgrader         websocket.Upgrader
	clients          map[*Client]bool
	clientsMu        sync.RWMutex
	broadcast        chan []byte
	register         chan *Client
	unregister       chan *Client
	subscriptions    map[string]map[*Client]bool
	subscriptionsMu  sync.RWMutex
	config           *config.Config
	deltaClient      *clients.DeltaWebsocketClient
	ctx              context.Context
	cancel           context.CancelFunc
	messagesSent     int64
	messagesReceived int64
	logger           *telemetry.Logger
	CleanupInterval  time.Duration
	MaxMessageQueue  int
	// Rate limiter
	limiter *rate.Limiter
	// Metrics
	metrics *Metrics
}

type Metrics struct {
	mu                sync.RWMutex
	TotalConnections  int64
	ActiveConnections int64
	TotalMessages     int64
	MessagesPerSecond int64
	ErrorCount        int64
	LastMinuteErrors  []time.Time
}

// NewWebsocketHandler creates a new websocket handler
func NewWebsocketHandler(ctx context.Context, cfg *config.Config) *WebsocketHandler {
	handlerCtx, cancel := context.WithCancel(ctx)

	// Create the websocket handler
	handler := &WebsocketHandler{
		upgrader: websocket.Upgrader{
			ReadBufferSize:  cfg.Websocket.ReadBufferSize,
			WriteBufferSize: cfg.Websocket.WriteBufferSize,
			CheckOrigin: func(r *http.Request) bool {
				if !cfg.Websocket.CheckOrigin {
					return true
				}
				origin := r.Header.Get("Origin")
				for _, allowedOrigin := range cfg.GetCORSAllowedOrigins() {
					if allowedOrigin == "*" || allowedOrigin == origin {
						return true
					}
				}
				return false
			},
		},
		clients:         make(map[*Client]bool),
		broadcast:       make(chan []byte, 1000),
		register:        make(chan *Client, 100),
		unregister:      make(chan *Client, 100),
		subscriptions:   make(map[string]map[*Client]bool),
		config:          cfg,
		ctx:             handlerCtx,
		cancel:          cancel,
		logger:          telemetry.NewLogger(),
		CleanupInterval: time.Minute,
		MaxMessageQueue: 1000,
		limiter:         rate.NewLimiter(rate.Limit(requestsPerSecond), burstSize),
		metrics: &Metrics{
			LastMinuteErrors: make([]time.Time, 0),
		},
	}

	// Create the Delta Exchange client if enabled
	if cfg.Delta.Enabled {
		handler.deltaClient = clients.NewDeltaWebsocketClient(handlerCtx, &cfg.Delta)
		if err := handler.deltaClient.Connect(); err != nil {
			handler.logger.LogError("delta_connection", err, nil)
		}
	}

	// Start the handler
	go handler.run()
	go handler.cleanupStaleConnections()

	return handler
}

// HandleWebsocket handles a websocket connection
func (h *WebsocketHandler) HandleWebsocket(w http.ResponseWriter, r *http.Request) {
	// Check rate limit
	if !h.limiter.Allow() {
		http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
		h.recordError()
		return
	}

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Error upgrading connection: %v", err)
		h.recordError()
		return
	}

	h.clientsMu.Lock()
	h.metrics.TotalConnections++
	h.metrics.ActiveConnections++
	h.clientsMu.Unlock()

	client := &Client{
		conn:               conn,
		send:               make(chan []byte, h.MaxMessageQueue),
		subscribedChannels: make(map[string]bool),
		id:                 fmt.Sprintf("%d", time.Now().UnixNano()),
		connectedAt:        time.Now(),
		lastActivity:       time.Now(),
	}

	h.logger.LogConnection(client.id, map[string]interface{}{
		"remote_addr": r.RemoteAddr,
		"user_agent":  r.UserAgent(),
	})

	h.register <- client

	go h.readPump(client)
	go h.writePump(client)
}

// BroadcastToChannel broadcasts a message to all clients subscribed to a channel
func (h *WebsocketHandler) BroadcastToChannel(channel string, message []byte, productID string) {
	startTime := time.Now()

	h.subscriptionsMu.RLock()
	clients, ok := h.subscriptions[channel]
	h.subscriptionsMu.RUnlock()

	if !ok {
		return
	}

	for client := range clients {
		client.mu.RLock()
		clientProductIDs, hasFilter := client.productFilters[channel]
		client.mu.RUnlock()

		if hasFilter && len(clientProductIDs) > 0 {
			match := false
			for _, clientProductID := range clientProductIDs {
				if productID == clientProductID {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}

		select {
		case client.send <- message:
			h.logger.LogInfo("broadcast_send_success", map[string]interface{}{"client_id": client.id})
			h.logger.LogMessage(client.id, channel, productID, len(message), time.Since(startTime))
			atomic.AddInt64(&h.messagesSent, 1)
		default:
			h.logger.LogInfo("broadcast_send_queue_full", map[string]interface{}{"client_id": client.id})
			h.logger.LogError("message_queue_full", fmt.Errorf("client message queue full"), map[string]interface{}{
				"client_id": client.id,
				"channel":   channel,
			})
			h.logger.LogInfo("broadcast_unreg_start", map[string]interface{}{"client_id": client.id})
			h.unregister <- client
			h.logger.LogInfo("broadcast_unreg_sent", map[string]interface{}{"client_id": client.id})
		}
	}
}

// GetDeltaConnectionStatus gets the connection status of the Delta Exchange client
func (h *WebsocketHandler) GetDeltaConnectionStatus() map[string]interface{} {
	if h.deltaClient != nil {
		return h.deltaClient.GetConnectionStatus()
	}
	return map[string]interface{}{
		"connected": false,
	}
}

// GetStatistics gets statistics about the websocket handler
func (h *WebsocketHandler) GetStatistics() map[string]interface{} {
	// Get the number of active connections
	h.clientsMu.RLock()
	activeConnections := len(h.clients)
	h.clientsMu.RUnlock()

	// Get the number of active subscriptions
	h.subscriptionsMu.RLock()
	activeSubscriptions := 0
	subscriptionsByChannel := make(map[string]int)
	for channel, clients := range h.subscriptions {
		subscriptionsByChannel[channel] = len(clients)
		activeSubscriptions += len(clients)
	}
	h.subscriptionsMu.RUnlock()

	// Get the external sources
	externalSources := make(map[string]bool)
	if h.deltaClient != nil {
		externalSources["delta"] = h.deltaClient.IsConnected()
	}

	// Create the statistics
	stats := map[string]interface{}{
		"active_connections":       activeConnections,
		"active_subscriptions":     activeSubscriptions,
		"messages_sent":            atomic.LoadInt64(&h.messagesSent),
		"messages_received":        atomic.LoadInt64(&h.messagesReceived),
		"subscriptions_by_channel": subscriptionsByChannel,
		"external_sources":         externalSources,
	}

	return stats
}

// Close closes the websocket handler
func (h *WebsocketHandler) Close() {
	h.cancel()
}

// registerDeltaHandler registers a handler for a Delta Exchange channel
func (h *WebsocketHandler) registerDeltaHandler(channel string) {
	h.deltaClient.RegisterHandler(channel, func(message []byte, msgProductID string) {
		// Broadcast the message to all clients subscribed to the channel
		h.BroadcastToChannel(channel, message, msgProductID)
	})
}

// run runs the websocket handler
func (h *WebsocketHandler) run() {
	defer func() {
		// Close all clients
		h.clientsMu.Lock()
		for client := range h.clients {
			client.conn.Close()
		}
		h.clientsMu.Unlock()

		// Close the Delta Exchange client
		if h.deltaClient != nil {
			h.deltaClient.Close()
		}
	}()

	for {
		select {
		case <-h.ctx.Done():
			return
		case client := <-h.register:
			h.clientsMu.Lock()
			h.clients[client] = true
			h.logger.LogInfo("client_registered", map[string]interface{}{"client_id": client.id})
			h.clientsMu.Unlock()
		case client := <-h.unregister:
			h.clientsMu.Lock()
			h.logger.LogInfo("unregistering_client", map[string]interface{}{"client_id": client.id})
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
				h.logger.LogInfo("client_removed_from_map", map[string]interface{}{"client_id": client.id})
			}
			h.clientsMu.Unlock()

			// Remove the client from all subscriptions
			h.subscriptionsMu.Lock()
			for channel, clients := range h.subscriptions {
				if _, ok := clients[client]; ok {
					delete(clients, client)
					if len(clients) == 0 {
						delete(h.subscriptions, channel)
					}
				}
			}
			h.subscriptionsMu.Unlock()
		case message := <-h.broadcast:
			// Broadcast the message to all clients
			h.clientsMu.RLock()
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, client)
				}
			}
			h.clientsMu.RUnlock()
		}
	}
}

// sendError sends an error message to the client
func (h *WebsocketHandler) sendError(client *Client, errorType string, err error, metadata map[string]interface{}) {
	response := map[string]interface{}{
		"type":    "error",
		"error":   errorType,
		"message": err.Error(),
	}
	if metadata != nil {
		response["metadata"] = metadata
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		h.logger.LogError("error_marshal_failed", err, map[string]interface{}{
			"client_id":  client.id,
			"error_type": errorType,
		})
		return
	}

	select {
	case client.send <- responseBytes:
		h.logger.LogError(errorType, err, metadata)
	default:
		h.unregister <- client
	}
}

// handleSubscribe handles a subscribe message
func (h *WebsocketHandler) handleSubscribe(client *Client, channel string) {
	// Check if already subscribed
	client.subMutex.RLock()
	if client.subscribedChannels[channel] {
		client.subMutex.RUnlock()
		h.sendError(client, "subscribe_error", fmt.Errorf("already subscribed to channel"), map[string]interface{}{
			"client_id": client.id,
			"channel":   channel,
		})
		return
	}
	client.subMutex.RUnlock()

	h.logger.LogInfo("subscribe_processing", map[string]interface{}{
		"client_id": client.id,
		"channel":   channel,
	})

	h.subscribeClient(client, channel)
	h.logger.LogSubscription(client.id, channel, []string{})

	// Send success response
	response := map[string]interface{}{
		"type":    "subscribe_success",
		"channel": channel,
	}
	responseBytes, _ := json.Marshal(response)
	client.send <- responseBytes

	h.logger.LogInfo("subscribe_complete", map[string]interface{}{
		"client_id": client.id,
		"channel":   channel,
	})
}

// subscribeClient subscribes a client to a channel
func (h *WebsocketHandler) subscribeClient(client *Client, channel string) {
	h.subscriptionsMu.Lock()
	defer h.subscriptionsMu.Unlock()

	// Create the channel if it doesn't exist
	if _, ok := h.subscriptions[channel]; !ok {
		h.subscriptions[channel] = make(map[*Client]bool)
	}

	// Add the client to the channel
	h.subscriptions[channel][client] = true

	// Update the client's subscriptions
	client.mu.Lock()
	client.subscribedChannels[channel] = true
	client.mu.Unlock()
}

// handleUnsubscribe handles an unsubscribe message
func (h *WebsocketHandler) handleUnsubscribe(client *Client, channel string) {
	h.logger.LogInfo("unsubscribe_request", map[string]interface{}{
		"client_id": client.id,
		"channel":   channel,
	})

	// Check if subscribed
	client.mu.RLock()
	if !client.subscribedChannels[channel] {
		client.mu.RUnlock()
		h.sendError(client, "unsubscribe_error", fmt.Errorf("not subscribed to channel"), map[string]interface{}{
			"client_id": client.id,
			"channel":   channel,
		})
		return
	}
	client.mu.RUnlock()

	h.logger.LogInfo("unsubscribe_processing", map[string]interface{}{
		"client_id": client.id,
		"channel":   channel,
	})

	h.unsubscribeClient(client, channel)

	// Send success response
	response := map[string]interface{}{
		"type":    "unsubscribed",
		"channel": channel,
	}
	responseBytes, _ := json.Marshal(response)
	client.send <- responseBytes

	h.logger.LogInfo("unsubscribe_complete", map[string]interface{}{
		"client_id": client.id,
		"channel":   channel,
	})
}

// unsubscribeClient unsubscribes a client from a channel
func (h *WebsocketHandler) unsubscribeClient(client *Client, channel string) {
	h.logger.LogInfo("unsubscribe_client_start", map[string]interface{}{
		"client_id": client.id,
		"channel":   channel,
	})

	client.mu.Lock()
	delete(client.subscribedChannels, channel)
	delete(client.productFilters, channel)
	client.mu.Unlock()

	h.subscriptionsMu.Lock()
	if clients, ok := h.subscriptions[channel]; ok {
		delete(clients, client)
		if len(clients) == 0 {
			delete(h.subscriptions, channel)
		}
	}
	h.subscriptionsMu.Unlock()

	h.logger.LogInfo("unsubscribe_client_complete", map[string]interface{}{
		"client_id": client.id,
		"channel":   channel,
	})
}

// handlePing handles a ping message
func (h *WebsocketHandler) handlePing(client *Client) {
	client.mu.Lock()
	client.lastActivity = time.Now()
	client.mu.Unlock()

	// Send pong response
	response := map[string]interface{}{
		"type": "pong",
		"time": time.Now().UnixMilli(),
	}
	responseBytes, err := json.Marshal(response)
	if err != nil {
		h.logger.LogError("ping_error", err, map[string]interface{}{
			"client_id": client.id,
		})
		return
	}

	select {
	case client.send <- responseBytes:
	default:
		h.unregister <- client
	}
}

// readPump pumps messages from the websocket connection to the hub
func (h *WebsocketHandler) readPump(client *Client) {
	defer func() {
		h.unregister <- client
		client.conn.Close()
	}()

	client.conn.SetReadLimit(maxMessageSize)
	client.conn.SetReadDeadline(time.Now().Add(pongWait))
	client.conn.SetPongHandler(func(string) error {
		client.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, message, err := client.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				h.logger.LogError("websocket read error", err, nil)
			}
			break
		}

		var msg map[string]interface{}
		if err := json.Unmarshal(message, &msg); err != nil {
			h.sendError(client, "message_parse_error", err, nil)
			continue
		}

		msgType, ok := msg["type"].(string)
		if !ok {
			h.sendError(client, "invalid_message_type", fmt.Errorf("type field missing or invalid"), nil)
			continue
		}

		switch msgType {
		case "subscribe":
			channel, ok := msg["channel"].(string)
			if !ok {
				h.sendError(client, "subscribe_error", fmt.Errorf("channel field missing or invalid"), nil)
				continue
			}
			h.handleSubscribe(client, channel)
		case "unsubscribe":
			channel, ok := msg["channel"].(string)
			if !ok {
				h.sendError(client, "unsubscribe_error", fmt.Errorf("channel field missing or invalid"), nil)
				continue
			}
			h.handleUnsubscribe(client, channel)
		default:
			h.sendError(client, "invalid_message_type", fmt.Errorf("unknown message type: %s", msgType), nil)
		}
	}
}

// writePump pumps messages from the hub to the websocket connection
func (h *WebsocketHandler) writePump(client *Client) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		client.conn.Close()
	}()

	for {
		select {
		case message, ok := <-client.send:
			client.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// The hub closed the channel.
				client.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := client.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// Add queued messages to the current websocket message.
			n := len(client.send)
			for i := 0; i < n; i++ {
				w.Write([]byte{'\n'})
				w.Write(<-client.send)
			}

			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			client.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := client.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// cleanupStaleConnections periodically cleans up stale connections
func (h *WebsocketHandler) cleanupStaleConnections() {
	ticker := time.NewTicker(h.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			h.clientsMu.Lock()
			for client := range h.clients {
				if time.Since(client.lastActivity) > pongWait*2 {
					h.logger.LogDisconnection(client.id, "stale_connection")
					h.unregister <- client
				}
			}
			h.clientsMu.Unlock()
		case <-h.ctx.Done():
			return
		}
	}
}

func (h *WebsocketHandler) recordError() {
	h.metrics.mu.Lock()
	defer h.metrics.mu.Unlock()

	h.metrics.ErrorCount++
	now := time.Now()
	h.metrics.LastMinuteErrors = append(h.metrics.LastMinuteErrors, now)

	// Clean up old errors
	cutoff := now.Add(-time.Minute)
	for i, t := range h.metrics.LastMinuteErrors {
		if t.After(cutoff) {
			h.metrics.LastMinuteErrors = h.metrics.LastMinuteErrors[i:]
			break
		}
	}
}

func (h *WebsocketHandler) recordMessage() {
	h.metrics.mu.Lock()
	defer h.metrics.mu.Unlock()

	h.metrics.TotalMessages++
	h.metrics.MessagesPerSecond++
}

func (h *WebsocketHandler) HandleMetrics(w http.ResponseWriter, r *http.Request) {
	h.metrics.mu.RLock()
	defer h.metrics.mu.RUnlock()

	// Calculate errors per minute
	errorsPerMinute := len(h.metrics.LastMinuteErrors)

	metrics := map[string]interface{}{
		"total_connections":   h.metrics.TotalConnections,
		"active_connections":  h.metrics.ActiveConnections,
		"total_messages":      h.metrics.TotalMessages,
		"messages_per_second": h.metrics.MessagesPerSecond,
		"total_errors":        h.metrics.ErrorCount,
		"errors_per_minute":   errorsPerMinute,
		"rate_limit": map[string]interface{}{
			"requests_per_second": requestsPerSecond,
			"burst_size":          burstSize,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

// Reset metrics periodically
func (h *WebsocketHandler) resetMetrics() {
	ticker := time.NewTicker(time.Second)
	go func() {
		for range ticker.C {
			h.metrics.mu.Lock()
			h.metrics.MessagesPerSecond = 0
			h.metrics.mu.Unlock()
		}
	}()
}

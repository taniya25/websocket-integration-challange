package telemetry

import (
	"encoding/json"
	"log"
	"os"
	"time"
)

// Event represents a telemetry event
type Event struct {
	Timestamp   time.Time              `json:"timestamp"`
	EventType   string                 `json:"event_type"`
	ClientID    string                 `json:"client_id,omitempty"`
	Channel     string                 `json:"channel,omitempty"`
	ProductID   string                 `json:"product_id,omitempty"`
	MessageSize int                    `json:"message_size,omitempty"`
	Latency     time.Duration          `json:"latency,omitempty"`
	Error       string                 `json:"error,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// Logger handles structured logging
type Logger struct {
	logger *log.Logger
}

// NewLogger creates a new logger instance
func NewLogger() *Logger {
	return &Logger{
		logger: log.New(os.Stdout, "", 0),
	}
}

// LogEvent logs a telemetry event
func (l *Logger) LogEvent(event Event) {
	event.Timestamp = time.Now()
	eventJSON, err := json.Marshal(event)
	if err != nil {
		l.logger.Printf("Failed to marshal event: %v", err)
		return
	}
	l.logger.Printf("%s", string(eventJSON))
}

// LogConnection logs a connection event
func (l *Logger) LogConnection(clientID string, metadata map[string]interface{}) {
	l.LogEvent(Event{
		EventType: "connection",
		ClientID:  clientID,
		Metadata:  metadata,
	})
}

// LogDisconnection logs a disconnection event
func (l *Logger) LogDisconnection(clientID string, reason string) {
	l.LogEvent(Event{
		EventType: "disconnection",
		ClientID:  clientID,
		Error:     reason,
	})
}

// LogMessage logs a message event
func (l *Logger) LogMessage(clientID, channel, productID string, messageSize int, latency time.Duration) {
	l.LogEvent(Event{
		EventType:   "message",
		ClientID:    clientID,
		Channel:     channel,
		ProductID:   productID,
		MessageSize: messageSize,
		Latency:     latency,
	})
}

// LogSubscription logs a subscription event
func (l *Logger) LogSubscription(clientID, channel string, productIDs []string) {
	l.LogEvent(Event{
		EventType: "subscription",
		ClientID:  clientID,
		Channel:   channel,
		Metadata: map[string]interface{}{
			"product_ids": productIDs,
		},
	})
}

// LogError logs an error event
func (l *Logger) LogError(eventType string, err error, metadata map[string]interface{}) {
	var errMsg string
	if err != nil {
		errMsg = err.Error()
	} else {
		errMsg = ""
	}
	l.LogEvent(Event{
		EventType: eventType,
		Error:     errMsg,
		Metadata:  metadata,
	})
}

// LogInfo logs an informational event
func (l *Logger) LogInfo(eventType string, metadata map[string]interface{}) {
	l.LogEvent(Event{
		EventType: eventType,
		Metadata:  metadata,
	})
}

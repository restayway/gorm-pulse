// Package nats provides a NATS dispatcher for gormpulse.
// It publishes database change events to NATS subjects for
// real-time event streaming.
package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/restayway/gorm-pulse/pulse"
)

// Config holds the configuration for the NATS dispatcher.
type Config struct {
	// URL is the NATS server URL (e.g., "nats://localhost:4222").
	URL string

	// Conn is an existing NATS connection to use.
	// If provided, URL is ignored and the connection is not managed by the dispatcher.
	Conn *nats.Conn

	// SubjectPrefix is the prefix for NATS subjects.
	// Events are published to: {prefix}.{table}.{event_type}
	// Default: "pulse"
	SubjectPrefix string

	// SubjectMapper allows custom subject name mapping.
	// By default, subjects follow pattern: {prefix}.{table}.{event_type}
	SubjectMapper func(event pulse.PulseEvent) string

	// ConnectTimeout is the connection timeout.
	// Default: 5 seconds
	ConnectTimeout time.Duration

	// Options are additional NATS connection options.
	Options []nats.Option

	// OnError is called when an error occurs.
	OnError func(event pulse.PulseEvent, err error)

	// IncludeFullModel determines if the full model data is included in messages.
	// Default: false (only PulseDocument data is sent)
	IncludeFullModel bool
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		SubjectPrefix:  "pulse",
		ConnectTimeout: 5 * time.Second,
	}
}

// Dispatcher implements pulse.Dispatcher for NATS.
// It publishes events as JSON messages to NATS subjects.
type Dispatcher struct {
	config    *Config
	conn      *nats.Conn
	ownedConn bool // true if we created the connection
}

// NewDispatcher creates a new NATS dispatcher.
func NewDispatcher(config *Config) (*Dispatcher, error) {
	if config == nil {
		config = DefaultConfig()
	}

	// Set defaults
	if config.SubjectPrefix == "" {
		config.SubjectPrefix = "pulse"
	}
	if config.ConnectTimeout == 0 {
		config.ConnectTimeout = 5 * time.Second
	}

	d := &Dispatcher{
		config: config,
	}

	// Use existing connection or create new one
	if config.Conn != nil {
		d.conn = config.Conn
		d.ownedConn = false
	} else {
		// Build connection options
		opts := append([]nats.Option{
			nats.Timeout(config.ConnectTimeout),
			nats.Name("gormpulse"),
		}, config.Options...)

		conn, err := nats.Connect(config.URL, opts...)
		if err != nil {
			return nil, fmt.Errorf("nats: failed to connect: %w", err)
		}
		d.conn = conn
		d.ownedConn = true
	}

	return d, nil
}

// Dispatch implements pulse.Dispatcher.
// It publishes the event to the appropriate NATS subject.
func (d *Dispatcher) Dispatch(ctx context.Context, event pulse.PulseEvent) error {
	// Build the subject
	subject := d.buildSubject(event)

	// Build the message
	msg, err := d.buildMessage(event)
	if err != nil {
		if d.config.OnError != nil {
			d.config.OnError(event, err)
		}
		return err
	}

	// Publish the message
	if err := d.conn.Publish(subject, msg); err != nil {
		if d.config.OnError != nil {
			d.config.OnError(event, err)
		}
		return fmt.Errorf("nats: failed to publish: %w", err)
	}

	return nil
}

// buildSubject constructs the NATS subject for an event.
func (d *Dispatcher) buildSubject(event pulse.PulseEvent) string {
	if d.config.SubjectMapper != nil {
		return d.config.SubjectMapper(event)
	}

	// Default pattern: {prefix}.{table}.{event_type}
	// Replace any dots in table name with underscores to avoid subject hierarchy issues
	table := strings.ReplaceAll(event.Table, ".", "_")
	return fmt.Sprintf("%s.%s.%s", d.config.SubjectPrefix, table, event.Type)
}

// Message is the structure published to NATS.
type Message struct {
	Type       pulse.PulseEventType `json:"type"`
	Table      string               `json:"table"`
	Model      string               `json:"model"`
	PrimaryKey any                  `json:"primary_key"`
	Document   *pulse.PulseDocument `json:"document,omitempty"`
	Data       any                  `json:"data,omitempty"`
	OccurredAt time.Time            `json:"occurred_at"`
}

// buildMessage creates the JSON message payload.
func (d *Dispatcher) buildMessage(event pulse.PulseEvent) ([]byte, error) {
	msg := Message{
		Type:       event.Type,
		Table:      event.Table,
		Model:      event.Model,
		PrimaryKey: event.PrimaryKey,
		Document:   event.Document,
		OccurredAt: event.OccurredAt,
	}

	// Optionally include full model data
	if d.config.IncludeFullModel {
		if event.After != nil {
			msg.Data = event.After
		} else if event.Before != nil {
			msg.Data = event.Before
		}
	}

	return json.Marshal(msg)
}

// Close closes the NATS connection if it was created by the dispatcher.
func (d *Dispatcher) Close() error {
	if d.ownedConn && d.conn != nil {
		d.conn.Close()
	}
	return nil
}

// Flush ensures all published messages have been sent.
func (d *Dispatcher) Flush() error {
	return d.conn.Flush()
}

// FlushTimeout flushes with a timeout.
func (d *Dispatcher) FlushTimeout(timeout time.Duration) error {
	return d.conn.FlushTimeout(timeout)
}

// Connection returns the underlying NATS connection.
func (d *Dispatcher) Connection() *nats.Conn {
	return d.conn
}

// Verify Dispatcher implements pulse.Dispatcher
var _ pulse.Dispatcher = (*Dispatcher)(nil)

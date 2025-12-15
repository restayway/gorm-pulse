// Package jetstream provides a NATS JetStream dispatcher for gormpulse.
// It publishes database change events to JetStream streams for
// persistent, reliable event streaming with at-least-once delivery.
package jetstream

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/restayway/gorm-pulse/pulse"
)

// Config holds the configuration for the JetStream dispatcher.
type Config struct {
	// URL is the NATS server URL (e.g., "nats://localhost:4222").
	URL string

	// Conn is an existing NATS connection to use.
	// If provided, URL is ignored and the connection is not managed by the dispatcher.
	Conn *nats.Conn

	// StreamName is the name of the JetStream stream.
	// Default: "PULSE"
	StreamName string

	// StreamConfig allows custom stream configuration.
	// If nil, a default stream configuration is used.
	StreamConfig *jetstream.StreamConfig

	// SubjectPrefix is the prefix for JetStream subjects.
	// Events are published to: {prefix}.{table}.{event_type}
	// Default: "pulse"
	SubjectPrefix string

	// SubjectMapper allows custom subject name mapping.
	// By default, subjects follow pattern: {prefix}.{table}.{event_type}
	SubjectMapper func(event pulse.PulseEvent) string

	// ConnectTimeout is the connection timeout.
	// Default: 5 seconds
	ConnectTimeout time.Duration

	// PublishTimeout is the timeout for publish operations.
	// Default: 5 seconds
	PublishTimeout time.Duration

	// Options are additional NATS connection options.
	Options []nats.Option

	// CreateStream determines if the stream should be created if it doesn't exist.
	// Default: true
	CreateStream bool

	// OnError is called when an error occurs.
	OnError func(event pulse.PulseEvent, err error)

	// OnPublishAck is called when a message is acknowledged by JetStream.
	OnPublishAck func(event pulse.PulseEvent, ack *jetstream.PubAck)

	// IncludeFullModel determines if the full model data is included in messages.
	// Default: false (only PulseDocument data is sent)
	IncludeFullModel bool

	// AsyncPublish enables asynchronous publishing.
	// When true, publishes don't wait for acknowledgment.
	// Default: false (synchronous publish with ack)
	AsyncPublish bool
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		StreamName:     "PULSE",
		SubjectPrefix:  "pulse",
		ConnectTimeout: 5 * time.Second,
		PublishTimeout: 5 * time.Second,
		CreateStream:   true,
	}
}

// Dispatcher implements pulse.Dispatcher for NATS JetStream.
// It publishes events as JSON messages to JetStream streams,
// providing persistent, reliable message delivery.
type Dispatcher struct {
	config    *Config
	conn      *nats.Conn
	js        jetstream.JetStream
	ownedConn bool
}

// NewDispatcher creates a new JetStream dispatcher.
func NewDispatcher(config *Config) (*Dispatcher, error) {
	if config == nil {
		config = DefaultConfig()
	}

	// Set defaults
	if config.StreamName == "" {
		config.StreamName = "PULSE"
	}
	if config.SubjectPrefix == "" {
		config.SubjectPrefix = "pulse"
	}
	if config.ConnectTimeout == 0 {
		config.ConnectTimeout = 5 * time.Second
	}
	if config.PublishTimeout == 0 {
		config.PublishTimeout = 5 * time.Second
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
			nats.Name("gormpulse-jetstream"),
		}, config.Options...)

		conn, err := nats.Connect(config.URL, opts...)
		if err != nil {
			return nil, fmt.Errorf("jetstream: failed to connect: %w", err)
		}
		d.conn = conn
		d.ownedConn = true
	}

	// Create JetStream context
	js, err := jetstream.New(d.conn)
	if err != nil {
		if d.ownedConn {
			d.conn.Close()
		}
		return nil, fmt.Errorf("jetstream: failed to create context: %w", err)
	}
	d.js = js

	// Create or update stream if configured
	if config.CreateStream {
		if err := d.ensureStream(); err != nil {
			if d.ownedConn {
				d.conn.Close()
			}
			return nil, err
		}
	}

	return d, nil
}

// ensureStream creates or updates the JetStream stream.
func (d *Dispatcher) ensureStream() error {
	ctx, cancel := context.WithTimeout(context.Background(), d.config.ConnectTimeout)
	defer cancel()

	streamConfig := d.config.StreamConfig
	if streamConfig == nil {
		// Default stream configuration
		streamConfig = &jetstream.StreamConfig{
			Name:        d.config.StreamName,
			Description: "gormpulse events stream",
			Subjects:    []string{d.config.SubjectPrefix + ".>"},
			Retention:   jetstream.LimitsPolicy,
			MaxAge:      24 * time.Hour * 7, // 7 days retention
			MaxBytes:    1024 * 1024 * 1024, // 1GB max
			MaxMsgs:     -1,                 // unlimited messages
			Discard:     jetstream.DiscardOld,
			Storage:     jetstream.FileStorage,
			Replicas:    1,
		}
	}

	// Try to get existing stream first
	_, err := d.js.Stream(ctx, d.config.StreamName)
	if err != nil {
		// Stream doesn't exist, create it
		_, err = d.js.CreateStream(ctx, *streamConfig)
		if err != nil {
			return fmt.Errorf("jetstream: failed to create stream: %w", err)
		}
	}

	return nil
}

// Dispatch implements pulse.Dispatcher.
// It publishes the event to the JetStream stream.
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

	// Create publish context with timeout
	pubCtx, cancel := context.WithTimeout(ctx, d.config.PublishTimeout)
	defer cancel()

	// Publish to JetStream
	if d.config.AsyncPublish {
		// Async publish (fire and forget with JetStream guarantees)
		_, err = d.js.PublishAsync(subject, msg)
		if err != nil {
			if d.config.OnError != nil {
				d.config.OnError(event, err)
			}
			return fmt.Errorf("jetstream: failed to publish async: %w", err)
		}
	} else {
		// Sync publish with acknowledgment
		ack, err := d.js.Publish(pubCtx, subject, msg)
		if err != nil {
			if d.config.OnError != nil {
				d.config.OnError(event, err)
			}
			return fmt.Errorf("jetstream: failed to publish: %w", err)
		}
		if d.config.OnPublishAck != nil {
			d.config.OnPublishAck(event, ack)
		}
	}

	return nil
}

// buildSubject constructs the JetStream subject for an event.
func (d *Dispatcher) buildSubject(event pulse.PulseEvent) string {
	if d.config.SubjectMapper != nil {
		return d.config.SubjectMapper(event)
	}

	// Default pattern: {prefix}.{table}.{event_type}
	// Replace any dots in table name with underscores to avoid subject hierarchy issues
	table := strings.ReplaceAll(event.Table, ".", "_")
	return fmt.Sprintf("%s.%s.%s", d.config.SubjectPrefix, table, event.Type)
}

// Message is the structure published to JetStream.
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

// JetStream returns the JetStream context.
func (d *Dispatcher) JetStream() jetstream.JetStream {
	return d.js
}

// StreamInfo returns information about the stream.
func (d *Dispatcher) StreamInfo(ctx context.Context) (*jetstream.StreamInfo, error) {
	stream, err := d.js.Stream(ctx, d.config.StreamName)
	if err != nil {
		return nil, err
	}
	return stream.Info(ctx)
}

// Verify Dispatcher implements pulse.Dispatcher
var _ pulse.Dispatcher = (*Dispatcher)(nil)

// Package redis provides a Redis Pub/Sub dispatcher for gormpulse.
// It publishes database change events to Redis channels for
// lightweight real-time event streaming.
package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/restayway/gorm-pulse/pulse"
)

// Config holds the configuration for the Redis dispatcher.
type Config struct {
	// Addr is the Redis server address (e.g., "localhost:6379").
	Addr string

	// Password is the Redis password (optional).
	Password string

	// DB is the Redis database number.
	// Default: 0
	DB int

	// Client is an existing Redis client to use.
	// If provided, Addr/Password/DB are ignored and the client is not managed.
	Client *redis.Client

	// ChannelPrefix is the prefix for Redis channels.
	// Events are published to: {prefix}:{table}:{event_type}
	// Default: "pulse"
	ChannelPrefix string

	// ChannelMapper allows custom channel name mapping.
	// By default, channels follow pattern: {prefix}:{table}:{event_type}
	ChannelMapper func(event pulse.PulseEvent) string

	// UseStreams enables Redis Streams instead of Pub/Sub.
	// Streams provide persistence and consumer groups.
	// Default: false
	UseStreams bool

	// StreamMaxLen is the maximum stream length when using Streams.
	// Default: 10000 (approximately, uses MAXLEN ~)
	StreamMaxLen int64

	// OnError is called when an error occurs.
	OnError func(event pulse.PulseEvent, err error)

	// IncludeFullModel determines if the full model data is included in messages.
	// Default: false
	IncludeFullModel bool
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		ChannelPrefix: "pulse",
		StreamMaxLen:  10000,
	}
}

// Dispatcher implements pulse.Dispatcher for Redis.
// It publishes events as JSON messages to Redis channels or streams.
type Dispatcher struct {
	config      *Config
	client      *redis.Client
	ownedClient bool
}

// NewDispatcher creates a new Redis dispatcher.
func NewDispatcher(config *Config) (*Dispatcher, error) {
	if config == nil {
		config = DefaultConfig()
	}

	// Set defaults
	if config.ChannelPrefix == "" {
		config.ChannelPrefix = "pulse"
	}
	if config.StreamMaxLen == 0 {
		config.StreamMaxLen = 10000
	}

	d := &Dispatcher{
		config: config,
	}

	// Use existing client or create new one
	if config.Client != nil {
		d.client = config.Client
		d.ownedClient = false
	} else {
		d.client = redis.NewClient(&redis.Options{
			Addr:     config.Addr,
			Password: config.Password,
			DB:       config.DB,
		})
		d.ownedClient = true

		// Test connection
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := d.client.Ping(ctx).Err(); err != nil {
			d.client.Close()
			return nil, fmt.Errorf("redis: failed to connect: %w", err)
		}
	}

	return d, nil
}

// Dispatch implements pulse.Dispatcher.
// It publishes the event to the appropriate Redis channel or stream.
func (d *Dispatcher) Dispatch(ctx context.Context, event pulse.PulseEvent) error {
	var err error

	if d.config.UseStreams {
		err = d.publishToStream(ctx, event)
	} else {
		err = d.publishToPubSub(ctx, event)
	}

	if err != nil && d.config.OnError != nil {
		d.config.OnError(event, err)
	}

	return err
}

// publishToPubSub publishes an event to Redis Pub/Sub.
func (d *Dispatcher) publishToPubSub(ctx context.Context, event pulse.PulseEvent) error {
	channel := d.buildChannel(event)

	msg, err := d.buildMessage(event)
	if err != nil {
		return err
	}

	return d.client.Publish(ctx, channel, msg).Err()
}

// publishToStream publishes an event to Redis Streams.
func (d *Dispatcher) publishToStream(ctx context.Context, event pulse.PulseEvent) error {
	stream := d.buildChannel(event)

	msg, err := d.buildMessage(event)
	if err != nil {
		return err
	}

	// Build stream entry values
	values := map[string]interface{}{
		"type":        string(event.Type),
		"table":       event.Table,
		"model":       event.Model,
		"primary_key": fmt.Sprintf("%v", event.PrimaryKey),
		"payload":     string(msg),
		"occurred_at": event.OccurredAt.Format(time.RFC3339Nano),
	}

	args := &redis.XAddArgs{
		Stream: stream,
		MaxLen: d.config.StreamMaxLen,
		Approx: true, // Use ~ for efficiency
		Values: values,
	}

	return d.client.XAdd(ctx, args).Err()
}

// buildChannel constructs the Redis channel/stream name for an event.
func (d *Dispatcher) buildChannel(event pulse.PulseEvent) string {
	if d.config.ChannelMapper != nil {
		return d.config.ChannelMapper(event)
	}

	// Default pattern: {prefix}:{table}:{event_type}
	// Replace dots with underscores for Redis-friendly names
	table := strings.ReplaceAll(event.Table, ".", "_")
	return fmt.Sprintf("%s:%s:%s", d.config.ChannelPrefix, table, event.Type)
}

// Message is the structure published to Redis.
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

// Close closes the Redis client if it was created by the dispatcher.
func (d *Dispatcher) Close() error {
	if d.ownedClient && d.client != nil {
		return d.client.Close()
	}
	return nil
}

// Client returns the underlying Redis client.
func (d *Dispatcher) Client() *redis.Client {
	return d.client
}

// Verify Dispatcher implements pulse.Dispatcher
var _ pulse.Dispatcher = (*Dispatcher)(nil)

// Package kafka provides a Kafka dispatcher for gormpulse.
// It publishes database change events to Kafka topics for
// reliable, scalable event streaming.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/restayway/gorm-pulse/pulse"
)

// Config holds the configuration for the Kafka dispatcher.
type Config struct {
	// Brokers is the list of Kafka broker addresses.
	Brokers []string

	// TopicPrefix is the prefix for Kafka topics.
	// Events are published to: {prefix}.{table}.{event_type}
	// Default: "pulse"
	TopicPrefix string

	// TopicMapper allows custom topic name mapping.
	// By default, topics follow pattern: {prefix}.{table}.{event_type}
	TopicMapper func(event pulse.PulseEvent) string

	// KeyMapper allows custom message key mapping.
	// By default, the primary key is used as the message key.
	KeyMapper func(event pulse.PulseEvent) string

	// Producer is an existing Kafka producer to use.
	// If provided, Brokers is ignored and the producer is not managed.
	Producer sarama.SyncProducer

	// ProducerConfig is the Sarama producer configuration.
	// Default: sarama.NewConfig() with appropriate settings
	ProducerConfig *sarama.Config

	// OnError is called when an error occurs.
	OnError func(event pulse.PulseEvent, err error)

	// OnSuccess is called when a message is successfully published.
	OnSuccess func(event pulse.PulseEvent, partition int32, offset int64)

	// IncludeFullModel determines if the full model data is included in messages.
	// Default: false
	IncludeFullModel bool
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		TopicPrefix: "pulse",
	}
}

// Dispatcher implements pulse.Dispatcher for Kafka.
// It publishes events as JSON messages to Kafka topics.
type Dispatcher struct {
	config        *Config
	producer      sarama.SyncProducer
	ownedProducer bool
}

// NewDispatcher creates a new Kafka dispatcher.
func NewDispatcher(config *Config) (*Dispatcher, error) {
	if config == nil {
		config = DefaultConfig()
	}

	// Set defaults
	if config.TopicPrefix == "" {
		config.TopicPrefix = "pulse"
	}

	d := &Dispatcher{
		config: config,
	}

	// Use existing producer or create new one
	if config.Producer != nil {
		d.producer = config.Producer
		d.ownedProducer = false
	} else {
		// Create Sarama config
		saramaConfig := config.ProducerConfig
		if saramaConfig == nil {
			saramaConfig = sarama.NewConfig()
			saramaConfig.Producer.Return.Successes = true
			saramaConfig.Producer.Return.Errors = true
			saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
			saramaConfig.Producer.Retry.Max = 3
			saramaConfig.Producer.Compression = sarama.CompressionSnappy
		}

		producer, err := sarama.NewSyncProducer(config.Brokers, saramaConfig)
		if err != nil {
			return nil, fmt.Errorf("kafka: failed to create producer: %w", err)
		}
		d.producer = producer
		d.ownedProducer = true
	}

	return d, nil
}

// Dispatch implements pulse.Dispatcher.
// It publishes the event to the appropriate Kafka topic.
func (d *Dispatcher) Dispatch(ctx context.Context, event pulse.PulseEvent) error {
	// Build the topic
	topic := d.buildTopic(event)

	// Build the message key
	key := d.buildKey(event)

	// Build the message value
	value, err := d.buildMessage(event)
	if err != nil {
		if d.config.OnError != nil {
			d.config.OnError(event, err)
		}
		return err
	}

	// Create the Kafka message
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
		Headers: []sarama.RecordHeader{
			{Key: []byte("event_type"), Value: []byte(event.Type)},
			{Key: []byte("table"), Value: []byte(event.Table)},
			{Key: []byte("model"), Value: []byte(event.Model)},
		},
		Timestamp: event.OccurredAt,
	}

	// Send the message
	partition, offset, err := d.producer.SendMessage(msg)
	if err != nil {
		if d.config.OnError != nil {
			d.config.OnError(event, err)
		}
		return fmt.Errorf("kafka: failed to send message: %w", err)
	}

	// Call success callback
	if d.config.OnSuccess != nil {
		d.config.OnSuccess(event, partition, offset)
	}

	return nil
}

// buildTopic constructs the Kafka topic for an event.
func (d *Dispatcher) buildTopic(event pulse.PulseEvent) string {
	if d.config.TopicMapper != nil {
		return d.config.TopicMapper(event)
	}

	// Default pattern: {prefix}.{table}.{event_type}
	// Replace dots with underscores for Kafka-friendly names
	table := strings.ReplaceAll(event.Table, ".", "_")
	return fmt.Sprintf("%s.%s.%s", d.config.TopicPrefix, table, event.Type)
}

// buildKey constructs the message key for an event.
func (d *Dispatcher) buildKey(event pulse.PulseEvent) string {
	if d.config.KeyMapper != nil {
		return d.config.KeyMapper(event)
	}

	// Default: use primary key as message key for consistent partitioning
	if event.PrimaryKey == nil {
		return ""
	}

	switch pk := event.PrimaryKey.(type) {
	case string:
		return pk
	case int, int32, int64, uint, uint32, uint64:
		return fmt.Sprintf("%v", pk)
	default:
		// For composite keys, serialize as JSON
		data, _ := json.Marshal(pk)
		return string(data)
	}
}

// Message is the structure published to Kafka.
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

// Close closes the Kafka producer if it was created by the dispatcher.
func (d *Dispatcher) Close() error {
	if d.ownedProducer && d.producer != nil {
		return d.producer.Close()
	}
	return nil
}

// Verify Dispatcher implements pulse.Dispatcher
var _ pulse.Dispatcher = (*Dispatcher)(nil)

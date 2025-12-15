# gormpulse

[![Go Reference](https://pkg.go.dev/badge/github.com/restayway/gorm-pulse.svg)](https://pkg.go.dev/github.com/restayway/gorm-pulse)
[![Go Report Card](https://goreportcard.com/badge/github.com/restayway/gorm-pulse)](https://goreportcard.com/report/github.com/restayway/gorm-pulse)

A production-ready GORM plugin that emits domain events on database changes, enabling external systems (search engines, message brokers, caches) to stay in sync with your data.

## 🎯 What Problem Does gormpulse Solve?

When building applications with search functionality (Typesense, Elasticsearch) or event-driven architectures (NATS, Kafka, Redis), you need to keep external systems synchronized with your database. Common approaches have significant drawbacks:

| Approach | Problem |
|----------|---------|
| **Manual sync** | Scattered, error-prone code throughout your application |
| **Debezium/CDC** | Complex infrastructure, requires Kafka, database-specific |
| **Triggers** | Database-specific, limited to SQL, no application context |
| **Polling** | Wasteful, high latency, doesn't scale |

**gormpulse** provides a clean, Go-native solution:

- ✅ **Declarative**: Models opt-in via a simple interface
- ✅ **Transaction-safe**: Events only fire after successful commits
- ✅ **Pluggable**: Swap dispatchers without changing application code
- ✅ **Lightweight**: No external dependencies for core functionality
- ✅ **Framework-agnostic**: Works with any Go application using GORM

## 📦 Installation

```bash
go get github.com/restayway/gorm-pulse
```

For extensions:

```bash
# Typesense support
go get github.com/restayway/gorm-pulse/extensions/typesense

# NATS support
go get github.com/restayway/gorm-pulse/extensions/nats

# NATS JetStream support (persistent messaging)
go get github.com/restayway/gorm-pulse/extensions/jetstream

# Kafka support  
go get github.com/restayway/gorm-pulse/extensions/kafka

# Redis support
go get github.com/restayway/gorm-pulse/extensions/redis
```

## 🚀 Quick Start

### 1. Define Your Model

Only models implementing `PulseModel` will emit events:

```go
type Hotel struct {
    ID          string `gorm:"primaryKey"`
    Name        string
    City        string
    StarRating  int
    Description string
}

// Pulse implements pulse.PulseModel
// This defines how the model is represented in external systems
func (h Hotel) Pulse() pulse.PulseDocument {
    return pulse.PulseDocument{
        ID:         h.ID,
        Collection: "hotels", // Typesense collection / NATS subject / etc.
        Document: map[string]any{
            "id":          h.ID,
            "name":        h.Name,
            "city":        h.City,
            "star_rating": h.StarRating,
            "description": h.Description,
        },
    }
}
```

### 2. Configure the Plugin

```go
import (
    "github.com/restayway/gorm-pulse/pulse"
    "github.com/restayway/gorm-pulse/extensions/typesense"
)

func main() {
    // Create your dispatcher (Typesense example)
    dispatcher := typesense.NewDispatcher(&typesense.Config{
        Host:   "http://localhost:8108",
        APIKey: "your-api-key",
    })

    // Create and configure the plugin
    plugin := pulse.New(&pulse.Config{
        Dispatcher: dispatcher,
        SyncMode:   pulse.SyncModeAsync, // Non-blocking (default)
    })

    // Register with GORM
    db, _ := gorm.Open(sqlite.Open("app.db"), &gorm.Config{})
    db.Use(plugin)

    // That's it! Events are now emitted automatically
    db.Create(&Hotel{ID: "h1", Name: "Grand Hotel"}) // → Typesense create
    db.Save(&hotel)                                    // → Typesense update
    db.Delete(&hotel)                                  // → Typesense delete
}
```

## 🔒 Transaction Safety

gormpulse ensures events are only dispatched for successfully committed transactions. This prevents ghost events from rolled-back operations.

### The Problem

```go
// Without transaction safety:
db.Transaction(func(tx *gorm.DB) error {
    tx.Create(&hotel)     // Event dispatched immediately 😱
    return errors.New("oops")  // Transaction rolls back
})
// Hotel doesn't exist, but Typesense has it! Data inconsistency!
```

### Solution 1: In-Memory Hooks (Default)

Events are queued during the transaction and dispatched after commit:

```go
plugin := pulse.New(&pulse.Config{
    Dispatcher: dispatcher,
    // UseOutbox: false (default)
})
```

**Limitation**: If the application crashes between commit and dispatch, events may be lost.

### Solution 2: Outbox Pattern (Recommended for Production)

Events are written to an `events_outbox` table within the same transaction, guaranteeing atomicity:

```go
plugin := pulse.New(&pulse.Config{
    Dispatcher:           dispatcher,
    UseOutbox:            true,
    OutboxTableName:      "events_outbox",
    OutboxWorkerInterval: time.Second,
    OutboxBatchSize:      100,
    OutboxRetryLimit:     3,
})

// Create the outbox table
plugin.AutoMigrate(db)

// Start the background worker
plugin.StartOutboxWorker(db)
defer plugin.StopOutboxWorker()
```

The outbox pattern ensures:
- ✅ Events survive application restarts
- ✅ Automatic retries for failed dispatches
- ✅ No data inconsistency possible
- ✅ Works correctly in multi-instance deployments

## 📖 API Reference

### Event Structure

```go
type PulseEvent struct {
    Type       PulseEventType // "create", "update", "delete"
    Table      string         // Database table name
    Model      string         // Go struct name
    PrimaryKey any            // Record's primary key
    Before     any            // State before (updates only)
    After      any            // State after (create/update)
    Document   *PulseDocument // From Pulse() method
    OccurredAt time.Time
}
```

### Configuration Options

```go
type Config struct {
    // Required: Handler for events
    Dispatcher Dispatcher

    // Async (default) or sync dispatch
    SyncMode SyncMode

    // Enable outbox pattern
    UseOutbox            bool
    OutboxTableName      string        // Default: "pulse_outbox"
    OutboxWorkerInterval time.Duration // Default: 1s
    OutboxBatchSize      int           // Default: 100
    OutboxRetryLimit     int           // Default: 3

    // Error handling
    ErrorHandler func(event PulseEvent, err error)

    // Filter which events to dispatch
    EventFilter func(event PulseEvent) bool
}
```

### Creating Custom Dispatchers

Implement the `Dispatcher` interface:

```go
type Dispatcher interface {
    Dispatch(ctx context.Context, event PulseEvent) error
}

// Example: Webhook dispatcher
type WebhookDispatcher struct {
    URL string
}

func (d *WebhookDispatcher) Dispatch(ctx context.Context, event pulse.PulseEvent) error {
    body, _ := json.Marshal(event)
    req, _ := http.NewRequestWithContext(ctx, "POST", d.URL, bytes.NewReader(body))
    req.Header.Set("Content-Type", "application/json")
    
    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    
    if resp.StatusCode >= 400 {
        return fmt.Errorf("webhook returned %d", resp.StatusCode)
    }
    return nil
}
```

### Multi-Dispatcher

Send events to multiple systems:

```go
multiDispatcher := pulse.NewMultiDispatcher(
    typesenseDispatcher,
    natsDispatcher,
    webhookDispatcher,
)

plugin := pulse.New(&pulse.Config{
    Dispatcher: multiDispatcher,
})
```

## 🔌 Extensions

### Typesense

```go
import "github.com/restayway/gorm-pulse/extensions/typesense"

dispatcher := typesense.NewDispatcher(&typesense.Config{
    Host:       "http://localhost:8108",
    APIKey:     "your-api-key",
    RetryCount: 3,
    
    // Optional: Transform documents before indexing
    DocumentTransformer: func(event pulse.PulseEvent, doc map[string]any) map[string]any {
        doc["indexed_at"] = time.Now().Unix()
        return doc
    },
})
```

### NATS

```go
import natsext "github.com/restayway/gorm-pulse/extensions/nats"

dispatcher, _ := natsext.NewDispatcher(&natsext.Config{
    URL:           "nats://localhost:4222",
    SubjectPrefix: "myapp.events", // → myapp.events.hotels.create
})
defer dispatcher.Close()
```

Subscribe to events:
```bash
nats sub "myapp.events.>"
```

### NATS JetStream

JetStream provides persistent, reliable messaging with at-least-once delivery guarantees:

```go
import jsext "github.com/restayway/gorm-pulse/extensions/jetstream"

dispatcher, _ := jsext.NewDispatcher(&jsext.Config{
    URL:           "nats://localhost:4222",
    StreamName:    "MYAPP_EVENTS",
    SubjectPrefix: "myapp.events",
    CreateStream:  true, // Auto-create stream
    
    // Optional: Custom stream configuration
    StreamConfig: &jetstream.StreamConfig{
        Name:      "MYAPP_EVENTS",
        Subjects:  []string{"myapp.events.>"},
        Retention: jetstream.LimitsPolicy,
        MaxAge:    24 * time.Hour * 30, // 30 days
        Storage:   jetstream.FileStorage,
    },
    
    // Callbacks
    OnPublishAck: func(event pulse.PulseEvent, ack *jetstream.PubAck) {
        log.Printf("Persisted: seq=%d", ack.Sequence)
    },
})
defer dispatcher.Close()
```

Create a durable consumer:
```bash
nats consumer add MYAPP_EVENTS my-consumer --filter 'myapp.events.>' --ack explicit
nats consumer next MYAPP_EVENTS my-consumer --count 10
```

### Kafka

```go
import "github.com/restayway/gorm-pulse/extensions/kafka"

dispatcher, _ := kafka.NewDispatcher(&kafka.Config{
    Brokers:     []string{"localhost:9092"},
    TopicPrefix: "myapp", // → myapp.hotels.create
})
defer dispatcher.Close()
```

### Redis

```go
import redisext "github.com/restayway/gorm-pulse/extensions/redis"

dispatcher, _ := redisext.NewDispatcher(&redisext.Config{
    Addr:          "localhost:6379",
    ChannelPrefix: "myapp", // → myapp:hotels:create
    
    // Optional: Use Redis Streams instead of Pub/Sub
    UseStreams:   true,
    StreamMaxLen: 10000,
})
defer dispatcher.Close()
```

## 🔧 Advanced Usage

### Event Filtering

Only dispatch certain events:

```go
plugin := pulse.New(&pulse.Config{
    Dispatcher: dispatcher,
    EventFilter: func(event pulse.PulseEvent) bool {
        // Only dispatch for specific tables
        return event.Table == "hotels" || event.Table == "bookings"
    },
})
```

### Custom Error Handling

```go
plugin := pulse.New(&pulse.Config{
    Dispatcher: dispatcher,
    ErrorHandler: func(event pulse.PulseEvent, err error) {
        // Log to your monitoring system
        sentry.CaptureException(err)
        
        // Or write to dead letter queue
        dlq.Write(event, err)
    },
})
```

### Composite Primary Keys

```go
type OrderItem struct {
    OrderID   string `gorm:"primaryKey"`
    ProductID string `gorm:"primaryKey"`
    Quantity  int
}

func (o OrderItem) Pulse() pulse.PulseDocument {
    return pulse.PulseDocument{
        ID:         fmt.Sprintf("%s_%s", o.OrderID, o.ProductID),
        Collection: "order_items",
        Document: map[string]any{
            "order_id":   o.OrderID,
            "product_id": o.ProductID,
            "quantity":   o.Quantity,
        },
    }
}

// Implement for explicit primary key control
func (o OrderItem) PulsePrimaryKey() any {
    return map[string]string{
        "order_id":   o.OrderID,
        "product_id": o.ProductID,
    }
}
```

## 🆚 Why Not Debezium?

| Feature | gormpulse | Debezium |
|---------|-----------|----------|
| **Setup complexity** | Go library | Kafka Connect, ZooKeeper, Kafka |
| **Resource usage** | Minimal | Heavy |
| **Application context** | Full access | SQL-level only |
| **Multi-database** | Via GORM | Separate connectors |
| **Typesense integration** | Built-in | Requires custom consumer |
| **Transaction safety** | Native Go | Database-level |

**Choose gormpulse when**:
- You're already using GORM
- You want simplicity over infrastructure
- You need application-level context in events
- You're syncing to search engines like Typesense

**Choose Debezium when**:
- You need database-level CDC
- You're not using Go
- You already have Kafka infrastructure
- You need to capture changes from multiple sources

## 📁 Project Structure

```
github.com/restayway/gorm-pulse/
├── pulse/
│   ├── plugin.go      # Main plugin implementation
│   ├── callbacks.go   # GORM lifecycle callbacks
│   ├── event.go       # Event types and interfaces
│   ├── dispatcher.go  # Dispatcher interface and utilities
│   ├── outbox.go      # Outbox pattern implementation
│   ├── config.go      # Configuration
│   └── errors.go      # Error definitions
├── extensions/
│   ├── typesense/     # Typesense dispatcher
│   ├── nats/          # NATS Core dispatcher
│   ├── jetstream/     # NATS JetStream dispatcher (persistent)
│   ├── kafka/         # Kafka dispatcher
│   └── redis/         # Redis dispatcher
└── examples/
    ├── basic/         # Simple logging example
    ├── typesense/     # Typesense integration
    ├── nats/          # NATS integration
    ├── jetstream/     # NATS JetStream integration
    └── outbox/        # Outbox pattern demo
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

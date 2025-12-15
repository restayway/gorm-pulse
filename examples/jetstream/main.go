// Example: gormpulse with NATS JetStream integration
// JetStream provides persistent, reliable messaging with at-least-once delivery.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	jsext "github.com/restayway/gorm-pulse/extensions/jetstream"
	"github.com/restayway/gorm-pulse/pulse"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// Hotel model
type Hotel struct {
	ID         string `gorm:"primaryKey"`
	Name       string
	City       string
	StarRating int
}

func (h Hotel) Pulse() pulse.PulseDocument {
	return pulse.PulseDocument{
		ID:         h.ID,
		Collection: "hotels",
		Document: map[string]any{
			"id":          h.ID,
			"name":        h.Name,
			"city":        h.City,
			"star_rating": h.StarRating,
		},
	}
}

func main() {
	// NATS configuration
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	// Create JetStream dispatcher
	jsDispatcher, err := jsext.NewDispatcher(&jsext.Config{
		URL:           natsURL,
		StreamName:    "GORMPULSE_EVENTS", // Custom stream name
		SubjectPrefix: "events",
		CreateStream:  true, // Auto-create stream if not exists

		// Optional: Custom stream configuration
		StreamConfig: &jetstream.StreamConfig{
			Name:        "GORMPULSE_EVENTS",
			Description: "Database change events from gormpulse",
			Subjects:    []string{"events.>"},
			Retention:   jetstream.LimitsPolicy,
			MaxAge:      24 * time.Hour * 30, // 30 days retention
			MaxBytes:    1024 * 1024 * 512,   // 512MB
			Storage:     jetstream.FileStorage,
			Replicas:    1,
		},

		// Callbacks
		OnError: func(event pulse.PulseEvent, err error) {
			log.Printf("JetStream error: %v", err)
		},
		OnPublishAck: func(event pulse.PulseEvent, ack *jetstream.PubAck) {
			fmt.Printf("✓ Ack: stream=%s, seq=%d\n", ack.Stream, ack.Sequence)
		},

		IncludeFullModel: true,
	})
	if err != nil {
		log.Fatalf("Failed to create JetStream dispatcher: %v", err)
	}
	defer jsDispatcher.Close()

	fmt.Println("✅ Connected to NATS JetStream")

	// Print stream info
	ctx := context.Background()
	info, err := jsDispatcher.StreamInfo(ctx)
	if err != nil {
		log.Printf("Warning: could not get stream info: %v", err)
	} else {
		fmt.Printf("📊 Stream: %s (messages: %d, bytes: %d)\n",
			info.Config.Name, info.State.Msgs, info.State.Bytes)
	}

	// Create the Pulse plugin
	plugin := pulse.New(&pulse.Config{
		Dispatcher: jsDispatcher,
		SyncMode:   pulse.SyncModeSync, // Sync to see acks immediately
	})

	// Connect to SQLite
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Register plugin and migrate
	if err := db.Use(plugin); err != nil {
		log.Fatalf("Failed to register plugin: %v", err)
	}
	if err := db.AutoMigrate(&Hotel{}); err != nil {
		log.Fatalf("Failed to migrate: %v", err)
	}

	// Handle shutdown
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	// Create some hotels
	fmt.Println("\n=== Creating hotels (events persisted to JetStream) ===")
	hotels := []Hotel{
		{ID: "h1", Name: "JetStream Hotel 1", City: "NYC", StarRating: 5},
		{ID: "h2", Name: "JetStream Hotel 2", City: "LA", StarRating: 4},
		{ID: "h3", Name: "JetStream Hotel 3", City: "Chicago", StarRating: 3},
	}
	for _, h := range hotels {
		if err := db.Create(&h).Error; err != nil {
			log.Printf("Failed to create hotel: %v", err)
			continue
		}
		fmt.Printf("Created: %s → events.hotels.create\n", h.Name)
	}

	// Update
	fmt.Println("\n=== Updating hotel ===")
	db.Model(&Hotel{}).Where("id = ?", "h1").Update("StarRating", 4)
	fmt.Println("Updated h1 → events.hotels.update")

	// Delete
	fmt.Println("\n=== Deleting hotel ===")
	db.Where("id = ?", "h3").Delete(&Hotel{})
	fmt.Println("Deleted h3 → events.hotels.delete")

	// Flush to ensure all messages are sent
	jsDispatcher.FlushTimeout(time.Second)

	// Show stream stats after operations
	info, _ = jsDispatcher.StreamInfo(ctx)
	if info != nil {
		fmt.Printf("\n📊 Stream after operations: messages=%d, bytes=%d\n",
			info.State.Msgs, info.State.Bytes)
	}

	fmt.Println("\n=== JetStream Consumer Examples ===")
	fmt.Println("Create a durable consumer:")
	fmt.Println("  nats consumer add GORMPULSE_EVENTS my-consumer --filter 'events.>' --ack explicit --deliver all")
	fmt.Println("\nConsume messages:")
	fmt.Println("  nats consumer next GORMPULSE_EVENTS my-consumer --count 10")
	fmt.Println("\nOr subscribe ephemerally:")
	fmt.Println("  nats sub 'events.>'")

	fmt.Println("\nPress Ctrl+C to exit")
	<-shutdown

	fmt.Println("\n👋 Goodbye!")
}

// Example: gormpulse with outbox pattern for transactional safety
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

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

// SimpleDispatcher logs events (replace with actual dispatcher in production)
type SimpleDispatcher struct{}

func (d *SimpleDispatcher) Dispatch(ctx context.Context, event pulse.PulseEvent) error {
	fmt.Printf("🚀 Dispatched: %s on %s (pk: %v)\n", event.Type, event.Table, event.PrimaryKey)
	return nil
}

func main() {
	logger := log.New(os.Stdout, "[gormpulse] ", log.LstdFlags)

	// Create the Pulse plugin with outbox pattern enabled
	plugin := pulse.New(&pulse.Config{
		Dispatcher:           &SimpleDispatcher{},
		UseOutbox:            true,                    // Enable outbox pattern
		OutboxTableName:      "events_outbox",         // Custom table name
		OutboxWorkerInterval: 500 * time.Millisecond,  // Check every 500ms
		OutboxBatchSize:      50,                      // Process 50 events per batch
		OutboxRetryLimit:     3,                       // Retry failed events 3 times
		Logger:               logger,
	})

	// Connect to SQLite
	db, err := gorm.Open(sqlite.Open("outbox_demo.db"), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Register the plugin
	if err := db.Use(plugin); err != nil {
		log.Fatalf("Failed to register plugin: %v", err)
	}

	// Create the outbox table
	if err := plugin.AutoMigrate(db); err != nil {
		log.Fatalf("Failed to create outbox table: %v", err)
	}

	// Auto-migrate models
	if err := db.AutoMigrate(&Hotel{}); err != nil {
		log.Fatalf("Failed to migrate: %v", err)
	}

	// Start the outbox worker
	if err := plugin.StartOutboxWorker(db); err != nil {
		log.Fatalf("Failed to start outbox worker: %v", err)
	}
	fmt.Println("✅ Outbox worker started")

	// Handle graceful shutdown
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	// Demo: Create hotels within a transaction
	fmt.Println("\n=== Creating hotels within transaction ===")
	err = db.Transaction(func(tx *gorm.DB) error {
		hotels := []Hotel{
			{ID: "h1", Name: "Hotel Alpha", City: "NYC", StarRating: 5},
			{ID: "h2", Name: "Hotel Beta", City: "LA", StarRating: 4},
		}
		for _, h := range hotels {
			if err := tx.Create(&h).Error; err != nil {
				return err // Rollback, events won't be dispatched!
			}
			fmt.Printf("   Created: %s\n", h.Name)
		}
		return nil // Commit, events will be dispatched
	})
	if err != nil {
		log.Printf("Transaction failed: %v", err)
	}

	// Demo: Transaction rollback
	fmt.Println("\n=== Transaction with rollback (events should NOT be dispatched) ===")
	err = db.Transaction(func(tx *gorm.DB) error {
		hotel := Hotel{ID: "h3", Name: "Hotel Gamma", City: "Chicago", StarRating: 3}
		if err := tx.Create(&hotel).Error; err != nil {
			return err
		}
		fmt.Printf("   Created (will rollback): %s\n", hotel.Name)
		return fmt.Errorf("simulated error") // Force rollback
	})
	fmt.Printf("   Transaction result: %v\n", err)

	// Wait for events to be processed
	fmt.Println("\n=== Waiting for outbox worker to process events ===")
	time.Sleep(2 * time.Second)

	// Check outbox status
	var stats struct {
		Total     int64
		Pending   int64
		Processed int64
		Failed    int64
	}
	db.Table("events_outbox").Count(&stats.Total)
	db.Table("events_outbox").Where("status = ?", "pending").Count(&stats.Pending)
	db.Table("events_outbox").Where("status = ?", "processed").Count(&stats.Processed)
	db.Table("events_outbox").Where("status = ?", "failed").Count(&stats.Failed)

	fmt.Printf("\n📊 Outbox Statistics:\n")
	fmt.Printf("   Total: %d, Pending: %d, Processed: %d, Failed: %d\n",
		stats.Total, stats.Pending, stats.Processed, stats.Failed)

	// Cleanup demo: Remove processed entries older than 1 hour
	deleted, err := pulse.CleanupProcessedEntries(db, "events_outbox", time.Hour)
	if err != nil {
		log.Printf("Cleanup error: %v", err)
	} else {
		fmt.Printf("   Cleaned up %d old entries\n", deleted)
	}

	fmt.Println("\n=== Press Ctrl+C to shutdown ===")
	<-shutdown

	// Graceful shutdown
	fmt.Println("\n⏳ Shutting down...")
	if err := plugin.StopOutboxWorker(); err != nil {
		log.Printf("Error stopping worker: %v", err)
	}
	fmt.Println("✅ Shutdown complete")

	// Cleanup demo database
	os.Remove("outbox_demo.db")
}

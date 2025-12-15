// Example: gormpulse with NATS integration
package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	natsext "github.com/restayway/gorm-pulse/extensions/nats"
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

	// Create NATS dispatcher
	natsDispatcher, err := natsext.NewDispatcher(&natsext.Config{
		URL:              natsURL,
		SubjectPrefix:    "gormpulse",
		IncludeFullModel: true, // Include full model data in messages
		OnError: func(event pulse.PulseEvent, err error) {
			log.Printf("NATS error: %v", err)
		},
	})
	if err != nil {
		log.Fatalf("Failed to create NATS dispatcher: %v", err)
	}
	defer natsDispatcher.Close()

	fmt.Println("✅ Connected to NATS")

	// Create the Pulse plugin
	plugin := pulse.New(&pulse.Config{
		Dispatcher: natsDispatcher,
		SyncMode:   pulse.SyncModeAsync,
	})

	// Connect to SQLite
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Register plugin and migrate
	db.Use(plugin)
	db.AutoMigrate(&Hotel{})

	// Handle shutdown
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	// Create some hotels
	fmt.Println("\n=== Creating hotels (events published to NATS) ===")
	hotels := []Hotel{
		{ID: "h1", Name: "NATS Hotel 1", City: "NYC", StarRating: 5},
		{ID: "h2", Name: "NATS Hotel 2", City: "LA", StarRating: 4},
	}
	for _, h := range hotels {
		db.Create(&h)
		fmt.Printf("Created: %s (published to: gormpulse.hotels.create)\n", h.Name)
	}

	// Update
	fmt.Println("\n=== Updating hotel ===")
	db.Model(&Hotel{}).Where("id = ?", "h1").Update("StarRating", 4)
	fmt.Println("Updated h1 (published to: gormpulse.hotels.update)")

	// Delete
	fmt.Println("\n=== Deleting hotel ===")
	db.Where("id = ?", "h2").Delete(&Hotel{})
	fmt.Println("Deleted h2 (published to: gormpulse.hotels.delete)")

	// Flush to ensure all messages are sent
	natsDispatcher.FlushTimeout(time.Second)

	fmt.Println("\n=== Subscribe to events with: ===")
	fmt.Println("nats sub 'gormpulse.>'")

	fmt.Println("Press Ctrl+C to exit")
	<-shutdown

	fmt.Println("\n👋 Goodbye!")
}

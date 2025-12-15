// Example: Basic usage of gormpulse with logging dispatcher
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/restayway/gorm-pulse/pulse"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// Hotel is an example model that implements PulseModel.
// Only models implementing this interface will emit events.
type Hotel struct {
	ID          string `gorm:"primaryKey"`
	Name        string
	City        string
	StarRating  int
	Description string
}

// Pulse implements pulse.PulseModel.
// This method defines how the model is represented in external systems.
func (h Hotel) Pulse() pulse.PulseDocument {
	return pulse.PulseDocument{
		ID:         h.ID,
		Collection: "hotels",
		Document: map[string]any{
			"id":          h.ID,
			"name":        h.Name,
			"city":        h.City,
			"star_rating": h.StarRating,
			"description": h.Description,
		},
	}
}

// Room is an example model that does NOT implement PulseModel.
// No events will be emitted for this model.
type Room struct {
	ID      uint `gorm:"primaryKey"`
	HotelID string
	Number  string
	Type    string
}

func main() {
	// Create logger
	logger := log.New(os.Stdout, "[gormpulse] ", log.LstdFlags)

	// Create a simple logging dispatcher
	dispatcher := &pulse.LoggingDispatcher{
		Logger: func(event pulse.PulseEvent) {
			fmt.Printf("📣 Event: %s on %s (pk: %v)\n", event.Type, event.Table, event.PrimaryKey)
			if event.Document != nil {
				fmt.Printf("   Collection: %s, Doc ID: %s\n", event.Document.Collection, event.Document.ID)
			}
		},
	}

	// Create the Pulse plugin
	plugin := pulse.New(&pulse.Config{
		Dispatcher: dispatcher,
		SyncMode:   pulse.SyncModeSync, // Use sync for demo to see output immediately
		Logger:     logger,
	})

	// Connect to SQLite (for demo purposes)
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Register the plugin
	if err := db.Use(plugin); err != nil {
		log.Fatalf("Failed to register plugin: %v", err)
	}

	// Auto-migrate models
	if err := db.AutoMigrate(&Hotel{}, &Room{}); err != nil {
		log.Fatalf("Failed to migrate: %v", err)
	}

	fmt.Println("\n=== Creating a Hotel (implements PulseModel) ===")
	hotel := Hotel{
		ID:          "hotel-001",
		Name:        "Grand Palace Hotel",
		City:        "New York",
		StarRating:  5,
		Description: "Luxury hotel in Manhattan",
	}
	db.Create(&hotel)

	fmt.Println("\n=== Creating a Room (does NOT implement PulseModel) ===")
	room := Room{
		HotelID: hotel.ID,
		Number:  "101",
		Type:    "Suite",
	}
	db.Create(&room) // No event emitted!
	fmt.Println("   (no event emitted for Room)")

	fmt.Println("\n=== Updating the Hotel ===")
	db.Model(&hotel).Update("StarRating", 4)

	fmt.Println("\n=== Deleting the Hotel ===")
	db.Delete(&hotel)

	fmt.Println("\n=== Demo Complete ===")
}

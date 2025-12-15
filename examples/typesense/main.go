// Example: gormpulse with Typesense integration
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/restayway/gorm-pulse/extensions/typesense"
	"github.com/restayway/gorm-pulse/pulse"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// Hotel model with Typesense-friendly document mapping
type Hotel struct {
	ID            string `gorm:"primaryKey"`
	Name          string
	City          string
	Country       string
	StarRating    int
	PricePerNight float64
	Amenities     string // Comma-separated for simplicity
	Description   string
}

// Pulse implements pulse.PulseModel for Typesense synchronization.
func (h Hotel) Pulse() pulse.PulseDocument {
	return pulse.PulseDocument{
		ID:         h.ID,
		Collection: "hotels",
		Document: map[string]any{
			"name":            h.Name,
			"city":            h.City,
			"country":         h.Country,
			"star_rating":     h.StarRating,
			"price_per_night": h.PricePerNight,
			"amenities":       h.Amenities,
			"description":     h.Description,
		},
	}
}

func main() {
	// Typesense configuration
	typesenseHost := os.Getenv("TYPESENSE_HOST")
	if typesenseHost == "" {
		typesenseHost = "http://localhost:8108"
	}
	typesenseAPIKey := os.Getenv("TYPESENSE_API_KEY")
	if typesenseAPIKey == "" {
		typesenseAPIKey = "xyz" // Default for local development
	}

	// Create Typesense dispatcher
	tsDispatcher := typesense.NewDispatcher(&typesense.Config{
		Host:       typesenseHost,
		APIKey:     typesenseAPIKey,
		RetryCount: 3,
		OnError: func(event pulse.PulseEvent, err error) {
			log.Printf("Typesense error for %s/%s: %v", event.Table, event.PrimaryKey, err)
		},
	})

	// Create the Pulse plugin with Typesense dispatcher
	plugin := pulse.New(&pulse.Config{
		Dispatcher: tsDispatcher,
		SyncMode:   pulse.SyncModeSync, // Blocking - see errors immediately
		ErrorHandler: func(event pulse.PulseEvent, err error) {
			log.Printf("Event dispatch failed: %v", err)
		},
	})

	// Connect to database
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Register the plugin
	if err := db.Use(plugin); err != nil {
		log.Fatalf("Failed to register plugin: %v", err)
	}

	// Auto-migrate
	if err := db.AutoMigrate(&Hotel{}); err != nil {
		log.Fatalf("Failed to migrate: %v", err)
	}

	fmt.Println("Creating hotels...")

	// Create some hotels
	hotels := []Hotel{
		{
			ID:            "hotel-001",
			Name:          "Grand Palace Hotel",
			City:          "New York",
			Country:       "USA",
			StarRating:    5,
			PricePerNight: 450.00,
			Amenities:     "Pool,Spa,Gym,Restaurant",
			Description:   "Luxury hotel in the heart of Manhattan",
		},
		{
			ID:            "hotel-002",
			Name:          "Seaside Resort",
			City:          "Miami",
			Country:       "USA",
			StarRating:    4,
			PricePerNight: 280.00,
			Amenities:     "Beach,Pool,Restaurant,Bar",
			Description:   "Beautiful beachfront resort",
		},
		{
			ID:            "hotel-003",
			Name:          "Mountain Lodge",
			City:          "Denver",
			Country:       "USA",
			StarRating:    3,
			PricePerNight: 150.00,
			Amenities:     "Fireplace,Hiking,Restaurant",
			Description:   "Cozy mountain retreat",
		},
	}

	for _, hotel := range hotels {
		if err := db.Create(&hotel).Error; err != nil {
			log.Printf("Failed to create hotel %s: %v", hotel.ID, err)
		} else {
			fmt.Printf("Created: %s (%s)\n", hotel.Name, hotel.ID)
		}
	}

	fmt.Println("\nUpdating a hotel...")

	// Update a hotel - load the model first, then save
	var hotelToUpdate Hotel
	db.First(&hotelToUpdate, "id = ?", "hotel-001")
	hotelToUpdate.StarRating = 4
	hotelToUpdate.PricePerNight = 380.00
	db.Save(&hotelToUpdate)

	fmt.Println("\nDeleting a hotel...")

	// Delete a hotel - load the model first so Pulse() can be called
	var hotelToDelete Hotel
	db.First(&hotelToDelete, "id = ?", "hotel-003")
	db.Delete(&hotelToDelete)

	// Wait a bit for async operations
	fmt.Println("\nWaiting for async operations...")
	// In production, you'd have proper shutdown handling

	fmt.Println("\nDone! Check your Typesense 'hotels' collection.")

	// Example Typesense query (for reference):
	// curl "http://localhost:8108/collections/hotels/documents/search?q=*&query_by=name" \
	//   -H "X-TYPESENSE-API-KEY: xyz"

	// Note: You need to create the Typesense collection schema first:
	/*
		curl "http://localhost:8108/collections" \
		  -X POST \
		  -H "Content-Type: application/json" \
		  -H "X-TYPESENSE-API-KEY: xyz" \
		  -d '{
		    "name": "hotels",
		    "fields": [
		      {"name": "id", "type": "string"},
		      {"name": "name", "type": "string"},
		      {"name": "city", "type": "string", "facet": true},
		      {"name": "country", "type": "string", "facet": true},
		      {"name": "star_rating", "type": "int32", "facet": true},
		      {"name": "price_per_night", "type": "float"},
		      {"name": "amenities", "type": "string"},
		      {"name": "description", "type": "string"}
		    ]
		  }'
	*/

	// For graceful shutdown in production:
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = ctx // Use for graceful shutdown
}

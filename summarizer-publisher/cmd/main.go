package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lexicon-bo/summarizer-publisher/config"
	"github.com/lexicon-bo/summarizer-publisher/internal/database"
	"github.com/lexicon-bo/summarizer-publisher/internal/logger"
	"github.com/lexicon-bo/summarizer-publisher/internal/nats"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		logger.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize logger
	logger.Init(cfg.LogLevel)
	logger.Infof("Starting Supreme Court Decision Summarizer Publisher with batch size %d and interval %d seconds",
		cfg.BatchSize, cfg.Interval)

	// Connect to database
	db, err := database.New(cfg)
	if err != nil {
		logger.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Connect to NATS
	publisher, err := nats.New(cfg)
	if err != nil {
		logger.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer publisher.Close()

	// Setup signal handling for graceful shutdown
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	// Run the publisher at regular intervals until signaled to stop
	ticker := time.NewTicker(time.Duration(cfg.Interval) * time.Second)
	defer ticker.Stop()

	logger.Infof("Publisher started, checking for extractions every %d seconds", cfg.Interval)

	// Process once immediately
	processExtractions(db, publisher, cfg.BatchSize)

	// Then process on a regular interval
	for {
		select {
		case <-ticker.C:
			processExtractions(db, publisher, cfg.BatchSize)
		case <-shutdown:
			logger.Info("Shutting down gracefully...")
			return
		}
	}
}

func processExtractions(db *database.Database, publisher *nats.Publisher, batchSize int) {
	logger.Debugf("Processing extractions with batch size: %d", batchSize)

	// Get extraction IDs
	extractions, err := db.GetExtractionIDs(batchSize)
	if err != nil {
		logger.Errorf("Error fetching extraction IDs: %v", err)
		return
	}

	if len(extractions) == 0 {
		logger.Info("No extractions found")
		return
	}

	logger.Infof("Found %d extractions", len(extractions))

	// Publish each extraction ID to NATS
	successCount := 0
	for _, extraction := range extractions {
		if err := publisher.PublishExtractionID(extraction.ID); err != nil {
			logger.Errorf("Error publishing extraction ID %s: %v", extraction.ID, err)
			continue
		}
		successCount++
	}

	logger.Infof("Successfully published %d of %d extraction IDs", successCount, len(extractions))
}

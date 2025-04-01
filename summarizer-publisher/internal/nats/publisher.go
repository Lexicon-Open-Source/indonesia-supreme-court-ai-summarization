package nats

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/lexicon-bo/summarizer-publisher/config"
	"github.com/lexicon-bo/summarizer-publisher/internal/logger"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Publisher represents a NATS publisher
type Publisher struct {
	nc         *nats.Conn
	js         jetstream.JetStream
	streamName string
	subject    string
}

// New creates a new NATS publisher
func New(cfg *config.Config) (*Publisher, error) {
	logger.Info("Connecting to NATS server...")

	// Connect to NATS
	nc, err := nats.Connect(cfg.NATS.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}
	logger.Infof("Connected to NATS server at %s", cfg.NATS.URL)

	// Create JetStream context
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}
	logger.Info("Created JetStream context")

	ctx := context.Background()

	// Ensure the stream exists
	_, err = js.Stream(ctx, cfg.NATS.StreamName)
	if err != nil {
		// If the stream doesn't exist, create it
		logger.Infof("Stream %s not found, creating...", cfg.NATS.StreamName)
		_, err = js.CreateStream(ctx, jetstream.StreamConfig{
			Name:     cfg.NATS.StreamName,
			Subjects: []string{cfg.NATS.StreamName + ".>"},
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create stream: %w", err)
		}
		logger.Infof("Created stream: %s", cfg.NATS.StreamName)
	} else {
		logger.Infof("Found existing stream: %s", cfg.NATS.StreamName)
	}

	logger.Info("Successfully configured NATS with JetStream")
	return &Publisher{
		nc:         nc,
		js:         js,
		streamName: cfg.NATS.StreamName,
		subject:    cfg.NATS.Subject,
	}, nil
}

// Close closes the NATS connection
func (p *Publisher) Close() {
	logger.Info("Closing NATS connection...")
	p.nc.Close()
	logger.Info("NATS connection closed")
}

// SummarizationRequest represents a request to summarize a court decision document
type SummarizationRequest struct {
	ExtractionID string `json:"extraction_id"`
}

// PublishExtractionID publishes an extraction ID to the NATS subject
func (p *Publisher) PublishExtractionID(extractionID string) error {
	logger.Debugf("Publishing extraction ID: %s", extractionID)

	req := SummarizationRequest{
		ExtractionID: extractionID,
	}

	data, err := json.Marshal(req)
	if err != nil {
		logger.Errorf("Failed to marshal request: %v", err)
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	ctx := context.Background()

	// Publish the message
	_, err = p.js.Publish(ctx, p.subject, data)
	if err != nil {
		logger.Errorf("Failed to publish message: %v", err)
		return fmt.Errorf("failed to publish message: %w", err)
	}

	logger.Infof("Published extraction ID %s to subject %s", extractionID, p.subject)
	return nil
}

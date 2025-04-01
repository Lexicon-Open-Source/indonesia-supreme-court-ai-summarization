package database

import (
	"database/sql"
	"fmt"

	"github.com/lexicon-bo/summarizer-publisher/config"
	"github.com/lexicon-bo/summarizer-publisher/internal/logger"
	_ "github.com/lib/pq"
)

// Database represents a connection to the database
type Database struct {
	db        *sql.DB
	crawlerDb *sql.DB
	summaryDb *sql.DB
}

// New creates new database connections
func New(cfg *config.Config) (*Database, error) {
	// Connect to crawler database
	crawlerConnStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.DB.Host, cfg.DB.Port, cfg.DB.User, cfg.DB.Password, "lexicon_bo_crawler", cfg.DB.SSLMode,
	)

	crawlerDb, err := sql.Open("postgres", crawlerConnStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to crawler database: %w", err)
	}

	if err := crawlerDb.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping crawler database: %w", err)
	}

	logger.Info("Successfully connected to crawler database")

	// Connect to summary database
	summaryConnStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.DB.Host, cfg.DB.Port, cfg.DB.User, cfg.DB.Password, "lexicon_bo", cfg.DB.SSLMode,
	)

	summaryDb, err := sql.Open("postgres", summaryConnStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to summary database: %w", err)
	}

	if err := summaryDb.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping summary database: %w", err)
	}

	logger.Info("Successfully connected to summary database")

	return &Database{
		crawlerDb: crawlerDb,
		summaryDb: summaryDb,
	}, nil
}

// Close closes the database connections
func (d *Database) Close() error {
	if err := d.crawlerDb.Close(); err != nil {
		logger.Errorf("Error closing crawler database connection: %v", err)
		return err
	}

	if err := d.summaryDb.Close(); err != nil {
		logger.Errorf("Error closing summary database connection: %v", err)
		return err
	}

	logger.Info("Successfully closed database connections")
	return nil
}

// ExtractionID represents an extraction ID from the database
type ExtractionID struct {
	ID string
}

// GetExtractionIDs fetches Supreme Court extraction IDs
func (d *Database) GetExtractionIDs(batchSize int) ([]ExtractionID, error) {
	logger.Debugf("Fetching extraction IDs with batch size: %d", batchSize)

	// Get all supreme court extractions regardless of processing status
	query := `
		SELECT e.id
		FROM extraction e
		WHERE e.raw_page_link LIKE 'https://putusan3.mahkamahagung.go.id%'
		LIMIT $1
	`

	rows, err := d.crawlerDb.Query(query, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	var extractions []ExtractionID
	for rows.Next() {
		var ex ExtractionID
		if err := rows.Scan(&ex.ID); err != nil {
			logger.Errorf("Failed to scan row: %v", err)
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		extractions = append(extractions, ex)
	}

	if err := rows.Err(); err != nil {
		logger.Errorf("Error iterating rows: %v", err)
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	logger.Debugf("Found %d extraction IDs", len(extractions))
	return extractions, nil
}

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

	// Get all decision numbers that already have summaries
	// Use prepared statement for better performance
	existingQuery := `
		SELECT decision_number
		FROM cases
		WHERE summary_en IS NOT NULL
	`

	// Create temporary table to hold decision numbers with English summaries
	_, err := d.crawlerDb.Exec(`CREATE TEMPORARY TABLE IF NOT EXISTS temp_processed_decisions (decision_number TEXT PRIMARY KEY)`)
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary table: %w", err)
	}

	// Clean previous data if any
	_, err = d.crawlerDb.Exec(`TRUNCATE TABLE temp_processed_decisions`)
	if err != nil {
		return nil, fmt.Errorf("failed to truncate temporary table: %w", err)
	}

	// Get decisions with summaries from summary database
	existingRows, err := d.summaryDb.Query(existingQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query cases with summaries: %w", err)
	}
	defer existingRows.Close()

	// Insert decision numbers to temporary table for efficient joining
	txn, err := d.crawlerDb.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	stmt, err := txn.Prepare(`INSERT INTO temp_processed_decisions (decision_number) VALUES ($1) ON CONFLICT DO NOTHING`)
	if err != nil {
		txn.Rollback()
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	decisionsCount := 0
	for existingRows.Next() {
		var decisionNumber string
		if err := existingRows.Scan(&decisionNumber); err != nil {
			txn.Rollback()
			return nil, fmt.Errorf("failed to scan existing decision number: %w", err)
		}

		if decisionNumber != "" {
			if _, err := stmt.Exec(decisionNumber); err != nil {
				txn.Rollback()
				return nil, fmt.Errorf("failed to insert decision number: %w", err)
			}
			decisionsCount++
		}
	}

	if err := existingRows.Err(); err != nil {
		txn.Rollback()
		return nil, fmt.Errorf("error iterating existing decision numbers: %w", err)
	}

	if err := txn.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	logger.Debugf("Found %d decision numbers with English summaries", decisionsCount)

	// Get extractions that don't have English summaries yet
	extractionsQuery := `
		SELECT e.id
		FROM extraction e
		WHERE e.raw_page_link LIKE 'https://putusan3.mahkamahagung.go.id%'
		AND NOT EXISTS (
			SELECT 1
			FROM temp_processed_decisions t
			WHERE t.decision_number = e.metadata->>'number'
		)
		LIMIT $1
	`

	rows, err := d.crawlerDb.Query(extractionsQuery, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to execute extraction query: %w", err)
	}
	defer rows.Close()

	var extractions []ExtractionID
	for rows.Next() {
		var ex ExtractionID
		if err := rows.Scan(&ex.ID); err != nil {
			return nil, fmt.Errorf("failed to scan extraction ID: %w", err)
		}
		extractions = append(extractions, ex)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating extraction rows: %w", err)
	}

	logger.Debugf("Found %d extraction IDs without English summaries", len(extractions))
	return extractions, nil
}

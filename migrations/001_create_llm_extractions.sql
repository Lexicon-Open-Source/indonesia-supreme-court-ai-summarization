-- Migration 001: Create llm_extractions table
--
-- This migration creates the base llm_extractions table for storing
-- LLM extraction results from Indonesian Supreme Court documents.
--
-- Prerequisites:
-- - PostgreSQL 12+

-- Step 1: Create llm_extractions table
CREATE TABLE IF NOT EXISTS llm_extractions (
    id VARCHAR(36) PRIMARY KEY,
    extraction_id VARCHAR(255) UNIQUE NOT NULL,
    extraction_result JSONB,
    summary_en TEXT,
    summary_id TEXT,
    extraction_confidence FLOAT8,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Step 2: Create indexes for common queries
CREATE INDEX IF NOT EXISTS ix_llm_extractions_extraction_id
ON llm_extractions (extraction_id);

CREATE INDEX IF NOT EXISTS ix_llm_extractions_status
ON llm_extractions (status);

CREATE INDEX IF NOT EXISTS ix_llm_extractions_created_at
ON llm_extractions (created_at DESC);

-- Step 3: Add comments
COMMENT ON TABLE llm_extractions IS
    'Stores LLM extraction results from Indonesian Supreme Court documents';
COMMENT ON COLUMN llm_extractions.id IS
    'Primary key UUID';
COMMENT ON COLUMN llm_extractions.extraction_id IS
    'Foreign key reference to extractions table';
COMMENT ON COLUMN llm_extractions.extraction_result IS
    'Structured extraction result as JSONB (50+ fields)';
COMMENT ON COLUMN llm_extractions.summary_en IS
    'English summary of the court decision';
COMMENT ON COLUMN llm_extractions.summary_id IS
    'Indonesian summary of the court decision';
COMMENT ON COLUMN llm_extractions.extraction_confidence IS
    'Confidence score of the extraction (0.0 to 1.0)';
COMMENT ON COLUMN llm_extractions.status IS
    'Extraction status: pending, processing, completed, failed';

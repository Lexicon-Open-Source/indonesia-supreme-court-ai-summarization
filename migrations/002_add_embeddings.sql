-- Migration 002: Add embedding support to llm_extractions table
--
-- This migration adds pgvector extension and embedding columns for semantic search.
-- Three embedding types for comprehensive search capabilities:
-- 1. content_embedding: Structured extraction data (crime, defendant, verdict, etc.)
-- 2. summary_embedding_id: Indonesian summary for narrative search
-- 3. summary_embedding_en: English summary for international queries
--
-- Prerequisites:
-- - PostgreSQL 12+ with pgvector extension installed
-- - Install pgvector: apt install postgresql-16-pgvector (Ubuntu)
-- - Migration 001 must be applied first

-- Step 1: Enable pgvector extension (requires superuser)
CREATE EXTENSION IF NOT EXISTS vector;

-- Step 2: Add embedding columns to llm_extractions
ALTER TABLE llm_extractions
ADD COLUMN IF NOT EXISTS content_embedding vector(768),
ADD COLUMN IF NOT EXISTS summary_embedding_id vector(768),
ADD COLUMN IF NOT EXISTS summary_embedding_en vector(768),
ADD COLUMN IF NOT EXISTS embedding_generated BOOLEAN DEFAULT FALSE;

-- Step 3: Create HNSW indexes for fast semantic search
-- HNSW provides O(log n) search time with high recall
-- m=16: connections per node (higher = better recall, more memory)
-- ef_construction=64: build quality (higher = slower build, better index)

-- Content embedding index (structured case data search)
CREATE INDEX IF NOT EXISTS ix_llm_extractions_content_hnsw
ON llm_extractions
USING hnsw (content_embedding vector_cosine_ops)
WITH (m = 16, ef_construction = 64);

-- Indonesian summary embedding index
CREATE INDEX IF NOT EXISTS ix_llm_extractions_summary_id_hnsw
ON llm_extractions
USING hnsw (summary_embedding_id vector_cosine_ops)
WITH (m = 16, ef_construction = 64);

-- English summary embedding index
CREATE INDEX IF NOT EXISTS ix_llm_extractions_summary_en_hnsw
ON llm_extractions
USING hnsw (summary_embedding_en vector_cosine_ops)
WITH (m = 16, ef_construction = 64);

-- Step 4: Create GIN index for JSONB queries (if not exists)
-- This enables fast filtering on extraction_result fields
CREATE INDEX IF NOT EXISTS ix_llm_extractions_result_gin
ON llm_extractions USING GIN (extraction_result);

-- Step 5: Add index on embedding_generated for backfill queries
CREATE INDEX IF NOT EXISTS ix_llm_extractions_embedding_generated
ON llm_extractions (embedding_generated)
WHERE embedding_generated = FALSE;

-- Comments
COMMENT ON COLUMN llm_extractions.content_embedding IS
    'Vector embedding of structured extraction content (768 dims, gemini-embedding-001)';
COMMENT ON COLUMN llm_extractions.summary_embedding_id IS
    'Vector embedding of Indonesian summary (768 dims, gemini-embedding-001)';
COMMENT ON COLUMN llm_extractions.summary_embedding_en IS
    'Vector embedding of English summary (768 dims, gemini-embedding-001)';
COMMENT ON COLUMN llm_extractions.embedding_generated IS
    'Flag indicating if embeddings have been generated for this extraction';

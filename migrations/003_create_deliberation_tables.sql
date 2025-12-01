-- Migration 003: Create deliberation tables for Virtual Judicial Council
-- Description: Creates tables for storing deliberation sessions and messages
-- Schema: council_v1
-- Created: 2025-12-01

-- =============================================================================
-- Create schema if not exists
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS council_v1;

-- =============================================================================
-- Table: council_v1.deliberation_sessions
-- =============================================================================
-- Stores session metadata and complex nested data as JSONB

CREATE TABLE IF NOT EXISTS council_v1.deliberation_sessions (
    id VARCHAR PRIMARY KEY,
    user_id VARCHAR,
    status VARCHAR NOT NULL DEFAULT 'active',

    -- Complex nested data stored as JSONB
    case_input JSONB NOT NULL,
    similar_cases JSONB NOT NULL DEFAULT '[]'::jsonb,
    legal_opinion JSONB,

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    concluded_at TIMESTAMP WITH TIME ZONE
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_deliberation_sessions_user_id
    ON council_v1.deliberation_sessions(user_id);

CREATE INDEX IF NOT EXISTS idx_deliberation_sessions_status
    ON council_v1.deliberation_sessions(status);

CREATE INDEX IF NOT EXISTS idx_deliberation_sessions_created_at
    ON council_v1.deliberation_sessions(created_at DESC);

-- Add comment to table
COMMENT ON TABLE council_v1.deliberation_sessions IS 'Stores deliberation sessions for the Virtual Judicial Council feature';
COMMENT ON COLUMN council_v1.deliberation_sessions.case_input IS 'JSONB containing: input_type, raw_input, parsed_case (case_type, summary, key_facts, charges, etc.)';
COMMENT ON COLUMN council_v1.deliberation_sessions.similar_cases IS 'JSONB array of similar cases found via semantic search';
COMMENT ON COLUMN council_v1.deliberation_sessions.legal_opinion IS 'JSONB containing the generated legal opinion draft';


-- =============================================================================
-- Table: council_v1.deliberation_messages
-- =============================================================================
-- Stores individual messages in the deliberation conversation

CREATE TABLE IF NOT EXISTS council_v1.deliberation_messages (
    id VARCHAR PRIMARY KEY,
    session_id VARCHAR NOT NULL REFERENCES council_v1.deliberation_sessions(id) ON DELETE CASCADE,

    -- Sender stored as JSONB to handle union type (user/agent/system)
    sender JSONB NOT NULL,
    content TEXT NOT NULL,
    intent VARCHAR,

    -- Citations stored as JSONB arrays
    cited_cases JSONB NOT NULL DEFAULT '[]'::jsonb,
    cited_laws JSONB NOT NULL DEFAULT '[]'::jsonb,

    -- Ordering and timestamp
    sequence_number INTEGER NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Index for efficient message retrieval by session
CREATE INDEX IF NOT EXISTS idx_deliberation_messages_session_id
    ON council_v1.deliberation_messages(session_id);

-- Index for ordering messages within a session
CREATE INDEX IF NOT EXISTS idx_deliberation_messages_session_sequence
    ON council_v1.deliberation_messages(session_id, sequence_number);

-- Add comments
COMMENT ON TABLE council_v1.deliberation_messages IS 'Stores messages exchanged during deliberation sessions';
COMMENT ON COLUMN council_v1.deliberation_messages.sender IS 'JSONB with type field: "user", "agent" (with agent_id), or "system"';
COMMENT ON COLUMN council_v1.deliberation_messages.sequence_number IS 'Used for ordering messages within a session (0-indexed)';


-- =============================================================================
-- Example sender JSONB formats:
-- =============================================================================
-- User sender:   {"type": "user"}
-- Agent sender:  {"type": "agent", "agent_id": "strict"}  -- strict, humanist, historian
-- System sender: {"type": "system"}


-- =============================================================================
-- Rollback script (if needed)
-- =============================================================================
-- DROP TABLE IF EXISTS council_v1.deliberation_messages;
-- DROP TABLE IF EXISTS council_v1.deliberation_sessions;
-- DROP SCHEMA IF EXISTS council_v1;

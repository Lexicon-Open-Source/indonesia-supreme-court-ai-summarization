# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Backend service for extracting structured data from Indonesian Supreme Court (Mahkamah Agung) decision documents using LLM (Gemini). The system processes PDF court decisions through an extraction pipeline that produces structured JSON data (50+ fields) and bilingual summaries (Indonesian & English).

## Commands

### Development

```bash
# Install dependencies
uv sync

# Run API server (development)
uv run uvicorn main:app --port 8004 --reload

# Run API server (production)
uv run uvicorn main:app --port 8004 --host 0.0.0.0

# Run single extraction via CLI
uv run python cli.py <extraction_id>

# Lint/format
uv run ruff check .
uv run ruff format .
```

### Docker

```bash
# Start all services (API + NATS)
docker compose up -d

# View logs
docker compose logs -f extraction-api

# Stop services
docker compose down
```

## Architecture

### Processing Flow

```
FastAPI Server → NATS Queue → Worker Consumer → Extraction Pipeline → PostgreSQL
                                                       ↓
                                              PDF Download/Extract
                                                       ↓
                                              Text Chunking (100 pages/chunk)
                                                       ↓
                                              LLM Extraction (Gemini)
                                                       ↓
                                              Summary Generation (ID + EN)
```

### Core Components

- **main.py**: FastAPI application with REST endpoints, NATS message processor, and lifespan management
- **nats_consumer.py**: NATS JetStream consumer configuration and message handling with durable consumers
- **contexts.py**: Application context manager holding database engines and NATS client connections
- **settings.py**: Pydantic settings with environment variable loading and GCP credentials handling
- **cli.py**: Typer-based CLI for running single extractions without NATS

### Source Modules (src/)

- **pipeline.py**: Orchestrates the complete extraction workflow (fetch → download → extract → save)
- **extraction.py**: LLM extraction logic using litellm with Gemini, Pydantic models for structured output (ExtractionResult with 50+ nested fields), and summary generation
- **io.py**: PDF reading with fallback chain (pdfminer → unstructured → OCR), GCS download support, database I/O

### Database Schemas

The service connects to PostgreSQL with two schemas:
- **bo_crawler_v1**: Contains `extractions` table (source data) and `llm_extractions` table (results)
- **bo_v1**: Contains `cases` table with decision metadata

### Key Models

- **ExtractionResult** (extraction.py): Deeply nested Pydantic model with defendant info, court info, indictment, prosecution demand, verdict, state loss, judicial considerations, etc.
- **LLMExtraction** (extraction.py): SQLModel for persisting extraction results with JSONB storage
- **Extraction/Cases** (io.py): SQLModel for reading source data from crawler schema

### LLM Configuration

- Model: `gemini/gemini-2.5-flash-lite` via litellm
- Chunk size: 100 pages per LLM call
- Structured output with JSON schema validation
- Retry logic with exponential backoff (5 attempts)

### NATS Configuration

- Stream: `SUPREME_COURT_SUMMARIZATION_EVENT`
- Subject: `SUPREME_COURT_SUMMARIZATION_EVENT.summarize`
- Durable consumer with max 3 concurrent workers
- Ack wait: 3600 seconds (1 hour per message)

## Environment Variables

Required:
- `GEMINI_API_KEY`: Gemini API key from Google AI Studio
- `DB_ADDR`: PostgreSQL address (host:port)
- `DB_USER`, `DB_PASS`: Database credentials
- `NATS__URL`: NATS server URL

Optional:
- `NATS__NUM_OF_SUMMARIZER_CONSUMER_INSTANCES`: Number of consumer instances (default: 3)
- `GCP_PROJECT_ID`, `GCP_CREDENTIALS_BASE64`: For GCS PDF access

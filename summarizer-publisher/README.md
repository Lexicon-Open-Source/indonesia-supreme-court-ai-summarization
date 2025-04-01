# Supreme Court Decision Summarizer Publisher

This Go application fetches extraction IDs from a database and publishes them to NATS for processing by the Indonesia Supreme Court AI Summarization service.

## Features

- Periodically queries the database for Supreme Court extraction IDs
- Publishes extraction IDs to NATS JetStream for asynchronous processing
- Configurable batch size and processing interval
- Structured, level-based logging for better observability
- Graceful shutdown on system signals

## Prerequisites

- Go 1.18 or higher
- PostgreSQL database with extraction data
- NATS server with JetStream enabled

## Configuration

The application can be configured using a YAML file or environment variables. Create a `config.yaml` file in the project root or in the `config` directory:

```yaml
db:
  host: localhost
  port: 5432
  user: lexicon
  password: password
  sslmode: disable

nats:
  url: nats://localhost:4222
  streamname: SUPREME_COURT_SUMMARIZATION_EVENT
  subject: SUPREME_COURT_SUMMARIZATION_EVENT.summarize

# Number of extraction IDs to process per batch
batchsize: 10

# Interval in seconds between batch processing (5 minutes)
interval: 300

# Logging level (debug, info, warn, error)
loglevel: debug
```

You can also use environment variables with the following naming pattern:
- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`
- `DB_SSLMODE`
- `NATS_URL`
- `NATS_STREAMNAME`
- `NATS_SUBJECT`
- `BATCHSIZE`
- `INTERVAL`
- `LOGLEVEL`

## Building and Running

### Building

```bash
go build -o summarizer-publisher ./cmd
```

### Running

```bash
./summarizer-publisher
```

## How It Works

1. The application connects to your database and NATS server
2. It queries the database for Supreme Court extraction IDs
3. For each extraction ID, it publishes a message to the NATS subject
4. The Indonesia Supreme Court AI Summarization service will consume these messages and generate summaries
5. The process repeats at the configured interval

## Database Schema

The application assumes the following schema:

- An `extraction` table with extraction IDs and metadata containing court decision information
- The query filters for extractions with URLs from the Supreme Court website

## Logging

The application uses structured logging with timestamps and log levels:

- `debug`: Detailed information useful for troubleshooting
- `info`: General operational messages about what the application is doing
- `warn`: Warning messages that don't prevent the application from working
- `error`: Error messages that might prevent some functionality

You can control the logging verbosity by setting the `loglevel` in the configuration.

## Monitoring

The application logs all operations, including:
- Connection status to database and NATS
- Number of extraction IDs found and processed
- Any errors that occur during processing

You can use standard system tools to monitor the logs.
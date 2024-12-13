# indonesia-supreme-court-ai-postprocess

# How to Deploy

1. Prepare the `.env` file, see `.env.example` for the configuration
2. run `docker compose up --build` ( to force rebuild, if no code change, omit the `--build`)
3. This will run the service in port `8080` if you want to change the port, just modify the exposed port and the build arg `SERVICE_PORT` in the docker compose file

# API Docs

After running the service, go tom `localhost:8080/docs`

# Process Flows

## Incoming Summarization Job Request

```mermaid
sequenceDiagram
    participant req as API Request
    participant service as Summarizer Service
    participant nats as NATS Message Broker

    req->>service: incoming request on POST `/court-decision/summary`
    service--)nats: submit job message
    nats--)service: job submission status
    service->>req: inform job submission status

```

## Background Processing

```mermaid
sequenceDiagram
    participant nats as NATS Message Broker
    participant service as Summarizer Service
    participant db as Database
    participant web as Web
    participant ai as OpenAI

    nats--)service: pull job message
    service-)db: get extraction and case metadata
    db-)service: return extraction and case metadata
    service->>service: validate metadata
    service-)web: download PDF document
    web-)service: PDF document
    service->>service: cleaning up
    service-)ai: prompt to generate summary
    ai-)service: generated summary
    service-)ai: prompt to generate translation
    ai-)service: generated translation
    service-)db: store summary in DB<br>`summary`,`summary_en`,<br>`summary_formatted`,`summary_formatted_en`
    service--)nats: acknowledged job done
```
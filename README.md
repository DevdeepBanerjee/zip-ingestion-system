
#ZIP Ingestion System with Live Progress

This project implements an event-driven document ingestion platform where users upload ZIP files containing multiple documents. The system incrementally unzips files and publishes real-time progress updates using Redpanda (Kafka-compatible).

#Architecture

- upload-service
  - Accepts ZIP uploads via REST API
  - Extracts files incrementally
  - Publishes progress events

- processor-service
  - Consumes progress events
  - Stores file metadata in PostgreSQL
  - Ensures idempotent inserts

- progress-ui
  - Displays live upload progress

- redpanda
  - Kafka-compatible message broker

- postgres
  - Stores file metadata

#How to Run

```bash
docker compose up --build

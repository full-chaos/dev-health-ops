# Product Telemetry Pipeline and Retention

This document describes the technical pipeline for product telemetry ingestion, processing, and retention within the Dev Health platform.

## Ingestion Pipeline

Product telemetry events are emitted by the `dev-health-web` frontend and ingested by the `dev-health-ops` backend.

### 1. Ingestion Endpoint
Events are sent in batches to the following endpoint:
`POST /api/v1/product-telemetry/events`

The backend validates the batch against the `ProductTelemetryBatch` schema, ensuring that event names and payload structures conform to the canonical event catalog.

### 2. Redis Stream
Accepted batches are written to a Redis Stream (or Valkey) for asynchronous processing. The stream naming convention is:
`product-telemetry:{orgHashOrAnonymous}:events`

This separation ensures that product telemetry ingestion does not interfere with core provider data ingestion.

### 3. Celery Consumer
A dedicated Celery task, `run_product_telemetry_consumer`, runs on the `ingest` queue. This consumer:
- Reads batches from the Redis Stream.
- Performs final validation.
- Flushes events to the ClickHouse analytics store.
- Acknowledges successful processing.

### 4. Dead Letter Queue (DLQ)
Events that repeatedly fail validation or persistence are moved to a Dead Letter Queue stream:
`product-telemetry:dlq`

## Persistence and Retention

Product telemetry is persisted in a dedicated ClickHouse table: `product_telemetry_events`.

### Table Schema
The table is optimized for analytical queries and includes the following fields:
- `org_id_hash`: Salted hash of the organization ID.
- `event_id`: Unique UUID for the event.
- `name`: Event name (LowCardinality).
- `schema_version`: Version of the event schema.
- `session_id`: Unique session identifier.
- `anonymous_user_id`: Stable anonymous user identifier.
- `route_pattern`: Normalized route pattern.
- `payload_json`: JSON string of the event payload.
- `occurred_at`: Timestamp of the event.
- `ingested_at`: Timestamp of ingestion.
- `source`: Source identifier (e.g., `dev-health-web`).

### Retention Policy
- **Raw Events:** Raw product telemetry events are retained for **180 days**.
- **TTL:** ClickHouse automatically deletes rows older than 180 days based on the `occurred_at` column.
- **Rollups:** Future phases may introduce aggregate rollups for long-term trend analysis beyond the 180-day raw event window.

## Separation of Concerns

Product telemetry is strictly separated from other telemetry types in the platform:
- **Instance Telemetry:** Voluntary telemetry about the health of the Dev Health instance itself (`/api/v1/telemetry/*`).
- **Feature Flag / Release Impact:** Telemetry used to measure the impact of specific releases or feature flags (`/api/v1/ingest/telemetry`).

## Implementation Reference
For further technical details, refer to the following Pull Requests:
- [dev-health-ops #785](https://github.com/chrisgeo/dev-health-ops/pull/785) — Backend ingestion foundation (CHAOS-1789)
- [dev-health-ops #786](https://github.com/chrisgeo/dev-health-ops/pull/786) — ClickHouse persistence and TTL (CHAOS-450)

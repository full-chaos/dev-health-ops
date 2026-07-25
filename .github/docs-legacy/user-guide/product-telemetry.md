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

## Product Telemetry Dashboard

The first-party dashboard is available in the admin UI at `/admin/product-telemetry`. It reads the persisted ClickHouse `product_telemetry_events` table through the GraphQL API and does not call external analytics providers.

The default dashboard window is the last 30 days, represented as a half-open date range: `start_date` is inclusive and `end_date` is exclusive. Dashboard sections are intentionally limited to the early product-usage questions supported by the sanitized event contract:

- Daily active anonymous users.
- Top route patterns by page-view events, sessions, and anonymous users.
- Stable feature IDs viewed by surface.
- Filter changes by view and filter key, including average selected value count.
- Chart interactions by chart type, action, and surface.
- Client errors by route pattern, boundary, and error class.
- Session duration percentiles plus average pages viewed and interactions.

Organization scoping uses the same one-way organization hash emitted by `dev-health-web` product telemetry. The dashboard resolver hashes the authenticated organization ID before querying `org_id_hash`, so raw organization IDs are not stored in or queried from the product telemetry table.

The dashboard is a read-only analytics surface. It renders persisted aggregates only; it does not recompute telemetry classifications in the UI and does not write dashboard outputs back to storage.

## Separation of Concerns

Product telemetry is strictly separated from other telemetry types in the platform:
- **Instance Telemetry:** Voluntary telemetry about the health of the Dev Health instance itself (`/api/v1/telemetry/*`).
- **Feature Flag / Release Impact:** Telemetry used to measure the impact of specific releases or feature flags (`/api/v1/ingest/telemetry`).

## Implementation Reference
For further technical details, refer to the following Pull Requests:
- [dev-health-ops #785](https://github.com/chrisgeo/dev-health-ops/pull/785) — Backend ingestion foundation (CHAOS-1789)
- [dev-health-ops #786](https://github.com/chrisgeo/dev-health-ops/pull/786) — ClickHouse persistence and TTL (CHAOS-450)

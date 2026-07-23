---
page_id: int-provider
summary: Implement a provider under the canonical provider package boundary with explicit transport, discovery, normalization, batching, rate limits, and tests.
content_type: task-guide
owner: engineering
source_of_truth:
  - src/dev_health_ops/providers/base.py
  - src/dev_health_ops/providers/_base.py
  - src/dev_health_ops/providers/registry.py
  - docs/architecture/adr-001-canonical-provider-pattern.md
applicability: current
lifecycle: active
---

# Build a provider integration

New provider code belongs under `src/dev_health_ops/providers/<provider>/`. The provider package is the boundary between an external system and Dev Health's canonical records: it owns authentication, transport, discovery, pagination, rate behavior, and normalization before data reaches processors or storage.
{: .fc-page-lede }

Do not add a new implementation to the legacy `src/dev_health_ops/connectors/` package. The GitHub and GitLab connector modules there remain compatibility surfaces for existing ingestion paths; new provider work uses the canonical provider contracts.

## Understand the provider boundary

A provider integration has three responsibilities:

1. **Fetch source records** through a provider-specific client or adapter.
2. **Normalize them** into canonical domain models or typed rows.
3. **Return bounded batches and progress state** that orchestration can persist, retry, and observe.

Processors should not need to understand raw provider payloads. Storage should not receive undocumented provider-native shapes. A provider-native label or field must not silently become a canonical product concept without an approved normalization rule.

## Choose the base pattern

The repository currently supports two related provider seams:

- `Provider` and `ProviderWithClient` in `src/dev_health_ops/providers/base.py` for provider packages that return typed domain batches and benefit from an injectable client.
- `BasePipelineAdapter` and related TestOps contracts in `src/dev_health_ops/providers/_base.py` for async, REST-heavy pipeline and job APIs.

Choose the seam that matches the data family and existing neighboring providers. Do not introduce a third base pattern merely because an external API has different naming.

## Recommended package layout

```text
src/dev_health_ops/providers/<provider>/
├── __init__.py
├── client.py            # authentication, requests, pagination, provider errors
├── provider.py          # capabilities, discovery, batching, orchestration boundary
├── normalize.py         # raw provider payload → canonical models or rows
└── testops_pipeline.py  # only when the provider exposes pipeline/job telemetry
```

Small providers can combine files, but the transport and normalization responsibilities should remain separable and testable.

## 1. Define the supported contract

Before writing requests, document:

- supported product and host variants;
- authentication schemes;
- minimum permissions;
- discoverable organizations, groups, projects, repositories, or pipelines;
- record families the provider will emit;
- incremental cursor or watermark behavior;
- historical backfill boundary;
- deletion and tombstone behavior;
- rate-limit and retry expectations;
- provider capabilities exposed to the registry and callers.

Unsupported variants should fail clearly rather than partially imitating support.

## 2. Implement an injectable transport client

Keep provider authentication and HTTP or SDK behavior inside the provider package. The client should:

- create authenticated requests without logging credentials;
- parse provider-specific errors into a small typed error surface;
- expose pagination or cursor iteration explicitly;
- surface rate-limit information when the provider supplies it;
- accept an injectable SDK client or `httpx` transport so tests do not require the network;
- preserve provider identifiers and source timestamps for reconciliation.

Use existing SDKs where they materially reduce risk, but avoid leaking SDK objects beyond the client boundary.

## 3. Discover source scope deterministically

Discovery determines which source entities are eligible for synchronization. It must be reproducible for the same credential and configuration.

Return stable identifiers, not display names alone. Record enough context to distinguish similarly named organizations, projects, repositories, pipelines, or environments. Apply allowlists and configured scopes before scheduling expensive backfills.

When discovery is partial because of permissions or rate limits, return an observable partial state instead of silently presenting the result as complete.

## 4. Normalize into canonical contracts

Place substantial mapping logic in `normalize.py`. Normalization should:

- map provider identifiers into canonical identity fields;
- convert timestamps to the platform's expected timezone and precision;
- preserve raw source identifiers needed for updates and reconciliation;
- translate enums only through an explicit mapping;
- retain unknown provider values safely when the canonical contract allows it;
- separate provider metadata from product concepts such as Investment themes;
- validate required fields before a record enters a batch.

A normalization function should be testable with a plain payload and expected canonical output. It should not make network calls or write to storage.

## 5. Implement bounded synchronization

A synchronization batch should make progress visible and repeatable. Implement:

- pagination or cursor advancement;
- incremental watermarks based on a documented source field;
- bounded historical backfill;
- stable idempotency keys or upsert identifiers;
- retry behavior for transient failures;
- explicit handling for authentication, permission, validation, and rate-limit failures;
- deletion or tombstone behavior where the source provides it;
- checkpoint state only after the corresponding records are safe to persist.

Do not advance a watermark past failed or unpersisted records. A retry must not duplicate canonical facts.

## 6. Respect rate and cost budgets

Use the shared rate-limit helpers where they fit the provider. The integration should distinguish:

- retryable server or network failures;
- provider throttling with a reset or retry interval;
- non-retryable authentication or permission failures;
- bounded backfill work versus continuous incremental synchronization.

Emit enough telemetry to explain whether a sync is progressing, waiting on a provider, partially covered, or failed. Do not log tokens, private payload bodies, or tenant-sensitive source text.

## 7. Register the provider

Add the provider through the lazy registry in `src/dev_health_ops/providers/registry.py` and expose its capabilities without importing optional provider dependencies eagerly. Keep compatibility aliases separate from the canonical implementation.

Document the configuration and permission requirements under the appropriate administration or integration page so an operator does not need to read provider code to connect it.

## 8. Test the integration

Use layered tests:

| Layer | What to prove |
| --- | --- |
| Normalization unit tests | Representative payloads map to exact canonical records, enums, identifiers, and timestamps. |
| Client tests | Authentication headers, pagination, provider errors, and rate-limit handling work through an injected fake or `httpx.MockTransport`. |
| Provider batch tests | Discovery, batching, cursor advancement, partial failure, and idempotent replay behave as documented. |
| Registry tests | The provider loads lazily and reports the correct capabilities. |
| Live-like tests | A restricted test credential exercises permissions, rate limits, pagination, replay, and deletion without production data. |

Include fixtures for empty pages, duplicate records, out-of-order timestamps, permission gaps, throttling, malformed payloads, and a retry after partial progress.

## Review checklist

Before requesting review, confirm:

- the code lives under `providers/<provider>/`;
- raw payloads do not escape the provider boundary;
- normalization has exact unit tests;
- pagination, backfill, and cursor ownership are documented;
- retries cannot advance progress incorrectly or duplicate records;
- provider permissions and coverage limitations are visible to administrators;
- logs and metrics are tenant-safe;
- a provider-native field has not become a canonical product category implicitly;
- the narrow tests and the repository aggregate checks pass.

## Continue

- [Canonical data contracts](data-contracts.md)
- [Test an integration](testing.md)
- [Provider limits](../../reference/limits-and-compatibility/provider-limits.md)
- [Provider connection troubleshooting](../../admin/troubleshooting/provider-connections.md)

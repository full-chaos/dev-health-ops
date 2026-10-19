---
page_id: con-auth-cp-architecture
summary: The Go Auth Control Plane as built in Wave 1 - the two binaries, the package layout, what runs and what is deliberately dormant - with the guardrail G-75 checklist that links every Wave 1 documentation surface to its page.
content_type: architecture
owner: engineering
source_of_truth:
  - cmd/auth-service/main.go
  - cmd/auth-migrate/main.go
  - internal/auth/authruntime/runtime.go
  - internal/audit/audit.go
  - internal/storage/postgres/authschema/authschema.go
  - contracts/auth/v1/README.md
applicability: current
lifecycle: active
---

# Auth Control Plane architecture and package layout

The Auth Control Plane (ACP) is a separately deployable Go service that will
become the single issuer and validator authority for platform credentials. In
Wave 1 it is **built and deployed dormant**: the binaries, the owned database
schema, the contracts, the audit and outbox machinery and the release gates
exist, and nothing in production calls the service.
{: .fc-page-lede }

!!! info "What dormant means here"
    The auth API listener mounts **no route** (`authruntime.Routes()` returns
    an empty set), so every path answers the service's own 404 envelope. No
    production token is issued. Auth.js and the Ops login path are unchanged,
    and no resource API depends on this process. Only the operator listener
    (`/healthz`, `/readyz`, `/metrics`) does real work.

## The two binaries

| Binary | Source | Job |
| --- | --- | --- |
| `auth-service` | `cmd/auth-service/main.go`, a thin entrypoint over `internal/auth/authruntime` | The HTTP service: two listeners, live readiness, graceful shutdown. |
| `auth-migrate` | `cmd/auth-migrate/main.go` | The **only** thing that issues DDL against the auth schema. A one-shot job; `auth-service` never migrates. |

Both are built from `docker/go-worker.Dockerfile` as distroless, non-root
images (targets `auth-service` and `auth-migrate`) and published with the other
Go images. See [deployment](auth-control-plane-deployment.md).

## Package layout

| Path | Responsibility |
| --- | --- |
| `internal/auth/authconfig` | Strict configuration: flags, environment, `_FILE` secrets, bounds. See the [configuration reference](../../reference/auth/configuration.md). |
| `internal/auth/authruntime` | Composes the process: store, outbox loop, the two listeners, the readiness gate; `--healthcheck`. |
| `internal/auth/authstore` | The PostgreSQL pool and the readiness probe (reachable, auth schema visible). |
| `internal/auth/keystore` | Signing-key custody: the file contract and the public half only. |
| `internal/auth/httpapi` | The middleware stack and server shared by the API listener: request ids, body bound, rate limits, panic recovery, error envelope. |
| `internal/auth/contracts` | Go validation of the wire contracts under `contracts/auth/v1/`. |
| `internal/auth/edgetoken`, `signedtoken`, `passwordhash`, `passwordpolicy`, `ratelimitvalkey` | Behaviour-parity ports of the existing Python auth helpers. |
| `internal/audit` | The same-transaction audit and outbox write, the outbox publisher loop and the reaper. See [audit and outbox operations](auth-control-plane-audit-outbox.md). |
| `internal/storage/postgres/authschema` | The auth-owned schema: its own migration lineage, the runtime grant manifest, posture checks. See [database, roles and migrations](auth-control-plane-database.md). |
| `contracts/auth/v1/` | The language-neutral wire contracts and golden fixtures. See the [contract and SDK guide](auth-control-plane-contracts.md). |

HTTP handlers do not select database drivers or reach around the domain
packages, and signing-key access is isolated behind the key-custody interface
(`internal/auth/keystore`).

## Two listeners, on purpose

The API listener (default `:8095`) carries requests. The operator listener
(default `:8096`) serves `/healthz`, `/readyz` and `/metrics`. Probes and
scrapes never pass through request middleware, and the operator surface is never
on the API port. Details are in the
[readiness and SLO reference](../../reference/auth/readiness-and-slo.md).

## Guardrail G-75 checklist

G-75 requires every Wave 1 documentation surface to exist and be linked from
this page.

| Surface | Page |
| --- | --- |
| Architecture and repository/package layout | This page |
| Dependency and cryptography selection | [Dependencies and cryptography](auth-control-plane-dependencies.md) |
| Local Compose and Kubernetes deployment | [Deployment](auth-control-plane-deployment.md) |
| Configuration and secret-file reference | [Configuration reference](../../reference/auth/configuration.md) |
| Database role, migration and backup ownership | [Database, roles and migrations](auth-control-plane-database.md) |
| Contract and SDK contributor guide | [Contracts and SDK](auth-control-plane-contracts.md) |
| Audit and outbox operations and troubleshooting | [Audit and outbox operations](auth-control-plane-audit-outbox.md) |
| Go build, release and security gate reference | [Go gates](auth-control-plane-go-gates.md) |
| Initial service SLO and readiness reference | [Readiness and SLO reference](../../reference/auth/readiness-and-slo.md) |

The decisions behind all of it are in the [ADRs](auth-control-plane-adrs.md),
the [decision sheet](auth-control-plane-decision-sheet.md) and the
[threat model](auth-control-plane-threat-model.md).

## Verified against

- `cmd/auth-service/main.go:1`, `cmd/auth-migrate/main.go:1`: the two entrypoints.
- `internal/auth/authruntime/runtime.go` (`Routes`, `configure`): empty route set, component order.
- `docker/go-worker.Dockerfile`: targets `auth-service` and `auth-migrate`.
- Package list: `go list ./internal/auth/... ./internal/audit/...` on the commit that merged this page.

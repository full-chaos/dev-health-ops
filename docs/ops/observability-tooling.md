# Observability Tooling Analysis

Open-source observability stack evaluation for replacing Sentry SaaS while pre-revenue.

!!! info "Status: Approved for implementation"
    This analysis was completed 2026-03-03. Implementation is deferred until prioritized.

---

## Quick Links

| Section | Description |
|---------|-------------|
| [Current State](#current-state) | What's instrumented today |
| [Tool Comparison](#tool-comparison) | Side-by-side matrix of evaluated tools |
| [Recommendation](#recommendation) | SigNoz + GlitchTip |
| [Architecture](#architecture) | Phase 1 (Compose) and Phase 2 (K8s) diagrams |
| [Migration Steps](#migration-steps) | Step-by-step implementation guide |
| [Cost Analysis](#cost-analysis) | SaaS vs self-hosted cost comparison |
| [Risks](#risk-assessment) | Known risks and mitigations |

---

## Current State

### Backend (FastAPI/Python)

| Layer | Tool | Config |
|-------|------|--------|
| Errors | `sentry-sdk[fastapi,celery]>=2.0.0` | DSN-based, 10% traces, 0% profiles |
| Tracing | `opentelemetry-*>=1.24.0` | OTLP gRPC to `localhost:4317`; instruments FastAPI, HTTPX, SQLAlchemy, Celery |
| Metrics | `prometheus-fastapi-instrumentator` | `/metrics` endpoint with custom counters/histograms for Celery, ClickHouse, LLM, GitHub |
| Logs | `python-json-logger` | JSON structured logging with correlation IDs via `X-Request-ID` |
| Alerting | Prometheus rules | `alerts/rules.yml` — API availability, ClickHouse, Celery, LLM, GitHub rate limits |

Key files:

- `src/dev_health_ops/sentry.py` — Sentry init with FastAPI, Starlette, Celery, Logging integrations
- `src/dev_health_ops/tracing.py` — OTEL setup with OTLP gRPC exporter
- `src/dev_health_ops/metrics/prometheus.py` — Custom Prometheus metrics
- `src/dev_health_ops/logging_config.py` — JSON log configuration
- `src/dev_health_ops/api/middleware/correlation_id.py` — Request ID propagation

### Frontend (Next.js) — dev-health-web

| Layer | Tool | Config |
|-------|------|--------|
| Errors | `@sentry/nextjs@^10.40.0` | Server + client + edge configs, 10% traces (prod) |
| Session Replay | Sentry Replay | 10% session sampling, 100% on error |
| Web Vitals | Custom `src/lib/webVitals.ts` | LCP, INP, CLS, FCP, TTFB to optional `/api/v1/rum` |
| Telemetry | Custom `src/lib/telemetry.ts` | Events via `sendBeacon()` to `/api/v1/telemetry` |

### Key Observation

The backend already has **production-grade OTEL instrumentation** exporting OTLP gRPC. The only vendor lock-in is **Sentry** for error tracking and session replay. This makes migration straightforward — any OTEL-native backend receives traces with zero code changes.

---

## Tool Comparison

### Evaluated Tools

| Tool | Category | Links |
|------|----------|-------|
| SigNoz | Full observability platform | [signoz.io](https://signoz.io/) |
| GlitchTip | Sentry-compatible error tracking | [glitchtip.com](https://glitchtip.com/) |
| Highlight.io | Full-stack monitoring + session replay | [highlight.io](https://www.highlight.io/) |
| BugSink | Minimal Sentry-compatible error tracking | [bugsink.com](https://www.bugsink.com/) |
| OpenReplay | Session replay specialist | [github.com/openreplay](https://github.com/openreplay/openreplay) |
| Self-hosted Sentry | Full Sentry, self-hosted | [develop.sentry.dev](https://develop.sentry.dev/self-hosted/) |
| Groundcover | eBPF Kubernetes APM | [groundcover.com](https://www.groundcover.com/) |

### Feature Matrix

| Tool | Sentry SDK Compat | OTEL Native | Resources | Errors | APM | Metrics | Logs | Session Replay | K8s |
|------|-------------------|-------------|-----------|--------|-----|---------|------|----------------|-----|
| **SigNoz** | No | Yes | 8GB+ RAM | Yes | Yes | Yes (PromQL) | Yes | No | Helm |
| **GlitchTip** | **Yes (drop-in)** | No | 2 CPU / 2GB RAM | Yes | Basic | No | No | No | Possible |
| **Highlight.io** | No | Yes | Moderate | Yes | Yes | Yes | Yes | **Yes** | Possible |
| **BugSink** | **Yes (drop-in)** | No | Minimal | Yes | No | No | No | No | No |
| **OpenReplay** | No | No | Moderate | Basic | No | No | No | **Yes** | Yes |
| **Self-hosted Sentry** | **Yes (identical)** | Partial | **16-32GB RAM** | Yes | Yes | No | No | Yes | Difficult |
| **Groundcover** | No | Yes (exports OTEL) | Low (eBPF) | Basic | Yes | Yes | Yes | No | **Yes** |

### Tool Deep Dives

??? note "SigNoz — Platform Analytics + APM"
    **What**: Open-source Datadog/New Relic alternative built natively on OpenTelemetry with ClickHouse storage.

    **Strengths**:

    - Backend already exports OTLP — SigNoz consumes traces with zero code changes
    - Replaces separate Prometheus + Grafana stack with built-in PromQL
    - Exception monitoring can partially replace Sentry for backend errors
    - Single pane for logs + metrics + traces with signal correlation
    - Helm chart for Kubernetes; single binary for Docker

    **Requirements**: 4 CPU, 8GB RAM minimum. ClickHouse for storage.

    **Limitations**: No session replay. No Sentry SDK compatibility. ClickHouse resource growth with data volume.

??? note "GlitchTip — Sentry Drop-in Replacement"
    **What**: Lightweight open-source error tracker that speaks the Sentry SDK protocol.

    **Strengths**:

    - Zero code changes — same Sentry SDKs, just swap the DSN
    - 2 CPU / 2GB RAM (vs self-hosted Sentry's 16-32GB)
    - v6 "All-in-One" mode: single container with web + worker
    - Includes basic APM (performance transactions) and uptime monitoring

    **Requirements**: Single container + PostgreSQL + Redis, or all-in-one mode.

    **Limitations**: No session replay. No log management. No metrics/dashboards. Basic APM only.

??? note "Highlight.io — Full-Stack with Session Replay"
    **What**: Open-source monitoring platform with error tracking, logging, tracing, and session replay.

    **Strengths**: Only OSS tool with session replay + errors + traces + logs. Built on OTEL + ClickHouse. Next.js SDK available.

    **Risks**: Smaller community. Self-hosted docs less mature. Requires full SDK swap (not Sentry-compatible).

??? note "BugSink — Minimal Error Tracking"
    **What**: Ultra-lightweight Sentry-compatible error tracker.

    **Strengths**: Sentry SDK compatible drop-in. Single Docker container. Minimal resources.

    **Limitations**: Error tracking only — no APM, traces, metrics, logs, or replay. Very young project.

??? note "OpenReplay — Session Replay Specialist"
    **What**: Open-source session replay platform (LogRocket/FullStory alternative).

    **Strengths**: Best OSS session replay. Kubernetes-native. Integrates with Sentry/Datadog as complement. Privacy controls.

    **Limitations**: Replay is its primary feature — not a full observability platform. Adds maintenance surface.

??? note "Self-Hosted Sentry — Full Feature Parity"
    **What**: The actual Sentry product, self-hosted via Docker Compose.

    **Why not now**: 16GB RAM minimum (32GB recommended). 12+ containers. Complex upgrades, I/O intensive. Missing AI features (Seer), spike protection. Maintenance burden acknowledged by Sentry.

??? note "Groundcover — eBPF Kubernetes APM"
    **What**: eBPF-based observability for Kubernetes with zero SDK instrumentation.

    **Strengths**: No code changes needed. Captures traces, metrics, logs at kernel level. Data stays in your infrastructure. Exports OTEL data to SigNoz.

    **Pricing**: Per-node (not per-event). Free tier available. Not fully open-source (commercial with free tier).

    **Limitations**: Requires Kubernetes. No error tracking or session replay.

---

## Recommendation

**SigNoz** (traces/metrics/logs/APM) + **BugSink** (error tracking via existing Sentry SDKs)

!!! note "Why BugSink over GlitchTip"
    BugSink was chosen over GlitchTip for the pre-revenue stage (<50 orgs) because:
    single container vs 3 services, reuses existing Postgres vs dedicated instance,
    handles 1.5M events/day on 4GB RAM, smart retention with no manual cleanup,
    and supports Sentry CLI + source map uploads for the Next.js frontend.
    GlitchTip remains a viable option if APM/uptime monitoring become needed later.

### Why This Combo

1. **Zero backend code changes** — OTLP already exports to `localhost:4317`, BugSink speaks Sentry protocol
2. **Minimal frontend changes** — swap `NEXT_PUBLIC_SENTRY_DSN`, replay already conditional
3. **Minimal resource footprint** — BugSink is a single container sharing existing Postgres
4. **Kubernetes-ready** — SigNoz has Helm chart, BugSink is a single container, add Groundcover eBPF later
5. **No vendor lock-in** — everything is OTEL standard
6. **$0 software cost** with near-zero incremental infrastructure cost

### What You Lose

- Session replay (defer to OpenReplay when needed)
- Sentry AI features (Seer)
- APM/performance dashboards in error tracker (SigNoz covers this)

### What You Gain

- Unlimited events with full data ownership
- Unified traces + metrics + logs in SigNoz with signal correlation
- Production-ready K8s path with eBPF auto-instrumentation
- Minimal operational surface for error tracking

### Decision Matrix

| Option | Score | Rationale |
|--------|-------|-----------|
| **SigNoz + BugSink** | **9/10** | OTEL-native platform + minimal-ops error tracking, ideal for <50 orgs |
| SigNoz + GlitchTip | 7/10 | More features but heavier (3 services, dedicated Postgres, sparse docs) |
| Highlight.io alone | 7/10 | Has replay, but requires full SDK swap |
| Self-hosted Sentry | 5/10 | Feature parity but 16-32GB RAM is wasteful pre-revenue |
| SigNoz alone | 6/10 | Good APM but error tracking less polished |

---

## Architecture

### Phase 1: Docker Compose (Pre-Revenue)

Replace Sentry SaaS with free alternatives at minimal infrastructure cost.

```
┌─────────────────────────────────────────────────────┐
│                   Docker Compose                     │
│                                                      │
│  ┌──────────┐  OTLP gRPC   ┌──────────────────┐    │
│  │ FastAPI   │─────────────▶│ SigNoz           │    │
│  │ + Celery  │              │ (OTEL Collector + │    │
│  │           │──/metrics──▶ │  ClickHouse +     │    │
│  └──────────┘              │  Query Service)   │    │
│       │                     └──────────────────┘    │
│       │ Sentry SDK (DSN swap)        ▲              │
│       ▼                              │              │
│  ┌──────────┐                        │              │
│  │ GlitchTip│              ┌─────────┘              │
│  │ (errors) │              │ OTLP export            │
│  └──────────┘              │                        │
│       ▲                    │                        │
│       │ Sentry SDK         │                        │
│  ┌──────────┐              │                        │
│  │ Next.js  │──────────────┘                        │
│  │ frontend │                                       │
│  └──────────┘                                       │
└─────────────────────────────────────────────────────┘

Error Tracking:  BugSink (Sentry SDK compatible, single container)
APM/Traces:      SigNoz (consumes existing OTLP traces)
Metrics:         SigNoz (PromQL, replaces Prometheus+Grafana)
Logs:            SigNoz (JSON log ingestion)
Session Replay:  DEFERRED
Alerting:        SigNoz (migrate Prometheus alert rules)
```

### Phase 2: Kubernetes (Production)

Add eBPF-based auto-instrumentation and scale the observability stack.

```
┌─────────────────────────────────────────────────────┐
│                   Kubernetes Cluster                  │
│                                                      │
│  ┌───────────────────────────────────────────┐      │
│  │ Groundcover eBPF Sensor (DaemonSet)       │      │
│  │ Auto-captures: traces, metrics, logs      │      │
│  └─────────────────┬─────────────────────────┘      │
│                    │ OTLP export                     │
│                    ▼                                 │
│  ┌──────────────────────────────────────┐           │
│  │ SigNoz (Helm)                        │           │
│  │ - OTEL Collector                     │           │
│  │ - ClickHouse                         │           │
│  │ - Query Service + Frontend           │           │
│  └──────────────────────────────────────┘           │
│                                                      │
│  ┌──────────┐     ┌──────────┐    ┌──────────┐     │
│  │ FastAPI   │     │ Celery   │    │ Next.js  │     │
│  │ Pod       │     │ Workers  │    │ Pod      │     │
│  └──────────┘     └──────────┘    └──────────┘     │
│       │                                │             │
│       └──── Sentry SDK (DSN) ──────────┘             │
│                    ▼                                 │
│  ┌──────────────────┐                               │
│  │ BugSink (single) │                               │
│  └──────────────────┘                               │
└─────────────────────────────────────────────────────┘

eBPF Layer:      Groundcover (auto-instrumentation, free tier)
Error Tracking:  BugSink (same Sentry SDKs, single container)
APM/Traces:      SigNoz + Groundcover data
Metrics:         SigNoz (PromQL + eBPF metrics)
Logs:            SigNoz (centralized log aggregation)
Session Replay:  OpenReplay OR Highlight.io (evaluate at this stage)
```

---

## Migration Steps

### Step 1: Add SigNoz to Docker Compose

The backend already exports OTLP to `localhost:4317`. Add SigNoz as a compose service and traces flow immediately.

```yaml
# Add to compose.yml
signoz:
  image: signoz/signoz:latest
  ports:
    - "8080:8080"    # SigNoz UI
    - "4317:4317"    # OTLP gRPC (already targeted by tracing.py)
    - "4318:4318"    # OTLP HTTP
  volumes:
    - signoz-data:/var/lib/signoz
```

!!! success "Result"
    Traces, metrics, and logs visible in SigNoz UI at `http://localhost:8080` with zero code changes.

### Step 2: Add BugSink to Docker Compose

Single container, reuses the existing Postgres (with a `bugsink` database created via init script).

```yaml
bugsink:
  image: bugsink/bugsink:latest
  ports:
    - "8800:8000"
  environment:
    DATABASE_URL: "postgresql://postgres:postgres@postgres:5432/bugsink"
    SECRET_KEY: ${BUGSINK_SECRET_KEY:?BUGSINK_SECRET_KEY must be set}
    BASE_URL: http://localhost:8800
    PORT: "8000"
    CREATE_SUPERUSER: "admin@example.com:changeme"  # first run only
  depends_on:
    postgres:
      condition: service_healthy
```

### Step 3: Swap Sentry DSN

One environment variable change per service — no code changes.

=== "Backend (.env)"

    ```bash
    # Before
    SENTRY_DSN=https://xxx@o123.ingest.sentry.io/456

    # After
    SENTRY_DSN=https://key@localhost:8800/1
    ```

=== "Frontend (.env)"

    ```bash
    # Before
    NEXT_PUBLIC_SENTRY_DSN=https://xxx@o123.ingest.sentry.io/789

    # After
    NEXT_PUBLIC_SENTRY_DSN=https://key@localhost:8800/2
    ```

### Step 4: Migrate Prometheus Alert Rules

SigNoz supports PromQL. Migrate `alerts/rules.yml` into SigNoz's alerting configuration.

### Step 5: Remove Sentry-Specific Features

- **Session Replay**: Controlled via `NEXT_PUBLIC_SENTRY_REPLAY_ENABLED` env var (defaults to false for BugSink compatibility)
- **Profiles**: Already disabled (`SENTRY_PROFILES_RATE=0.0`)

### Step 6 (K8s): Deploy via Helm

```bash
# SigNoz
helm repo add signoz https://charts.signoz.io
helm install signoz signoz/signoz -n observability --create-namespace

# BugSink — single container, point DATABASE_URL at your cluster Postgres
kubectl run bugsink --image=bugsink/bugsink:latest \
  --env="DATABASE_URL=postgresql://..." \
  --env="SECRET_KEY=..." \
  --port=8000 -n observability
```

### Step 7 (K8s): Add Groundcover eBPF Sensor

```bash
helm repo add groundcover https://helm.groundcover.com
helm install groundcover groundcover/groundcover \
  --set global.clusterName=dev-health-prod \
  -n groundcover --create-namespace
```

---

## Cost Analysis

### Current: Sentry SaaS

| Plan | Limits | Monthly Cost |
|------|--------|-------------|
| Developer (free) | 5K errors, 10K transactions, 50 replays, 1 user | $0 |
| Team | 50K errors, 100K transactions | $26+ |
| Business | Scales with usage | $80+ |

### Proposed: Self-Hosted

| Tool | Infrastructure | Software | Events |
|------|---------------|----------|--------|
| SigNoz | ~$20-40/mo (2-4 CPU VPS or shared K8s) | $0 | Unlimited |
| BugSink | ~$0/mo (shares existing Postgres) | $0 | Unlimited |
| Groundcover (K8s) | In-cluster | $0 (free tier) | Per-node |
| **Total** | **~$20-40/mo** | **$0** | **Unlimited** |

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| BugSink missing Sentry SDK features | Medium | Low | Core error tracking works; unsupported features (replays, APM) silently ignored |
| SigNoz ClickHouse resource growth | Medium | Medium | Set retention policies; app already runs ClickHouse |
| Maintenance burden of self-hosting | Low | Low | BugSink is single container; SigNoz has Helm chart |
| No session replay | High | Low | Acceptable pre-revenue; add OpenReplay when needed |
| Groundcover free tier limits | Low | Low | Per-node pricing, generous for small clusters |

---

## References

- [SigNoz Documentation](https://signoz.io/docs/)
- [SigNoz Kubernetes Install](https://signoz.io/docs/install/kubernetes/)
- [BugSink Documentation](https://www.bugsink.com/docs/)
- [BugSink: GlitchTip vs Sentry vs BugSink](https://www.bugsink.com/blog/glitchtip-vs-sentry-vs-bugsink/)
- [GlitchTip](https://glitchtip.com/) (evaluated, deferred — heavier than BugSink for current scale)
- [Highlight.io](https://www.highlight.io/)
- [OpenReplay](https://github.com/openreplay/openreplay)
- [Self-Hosted Sentry](https://develop.sentry.dev/self-hosted/)
- [Groundcover](https://www.groundcover.com/)
- [Groundcover Pricing](https://www.groundcover.com/pricing)

ARG GO_BUILD_IMAGE="mirror.gcr.io/library/golang:1.27.0-alpine@sha256:4c9fe60190a2a3350ddc51de80d0224b8a6698d12bdfc999fee45ea9d6c46dbc"

# ── Stage 0: one-shot Go tooling builder ───────────────────────────────────────
# The Python migration command invokes this static binary only when the
# dedicated MIGRATION_DATABASE_URI contract is configured. Long-running
# processes never invoke it and never receive the migration role.
FROM --platform=$BUILDPLATFORM ${GO_BUILD_IMAGE} AS go-migrator-builder

ARG TARGETOS
ARG TARGETARCH
ARG SOURCE_DATE_EPOCH="0"

ENV CGO_ENABLED=0 \
  GOFLAGS="-mod=readonly" \
  SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH}

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY cmd/dev-health-worker-migrate ./cmd/dev-health-worker-migrate
COPY internal ./internal

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" go build \
      -buildvcs=false \
      -trimpath \
      -ldflags="-s -w -buildid=" \
      -o /out/dev-health-worker-migrate \
      ./cmd/dev-health-worker-migrate \
    && touch -d "@${SOURCE_DATE_EPOCH}" \
      /out/dev-health-worker-migrate

# ── Stage 1: builder ──────────────────────────────────────────────────────────
# Install build dependencies and compile wheels. This stage is NOT shipped.
FROM python:3.14-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
  PYTHONUNBUFFERED=1

WORKDIR /build

# Install build-time system deps (gcc, build-essential, git for setuptools-scm)
RUN apt-get update && apt-get install -y --no-install-recommends \
  build-essential \
  gcc \
  git \
  wget \
  && rm -rf /var/lib/apt/lists/*

# Receive the pretend version so setuptools-scm can produce a version string
# in environments without a git history (e.g. Docker build in CI).
ARG SETUPTOOLS_SCM_PRETEND_VERSION
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${SETUPTOOLS_SCM_PRETEND_VERSION}

COPY . .

# Build and install into a prefix that we can copy into the runtime image.
# Strip pip and setuptools (not needed at runtime).
RUN pip install --upgrade pip && \
  pip install --no-cache-dir --prefix=/install . && \
  rm -rf /install/lib/python3.14/site-packages/pip \
         /install/lib/python3.14/site-packages/pip-* \
         /install/lib/python3.14/site-packages/setuptools \
         /install/lib/python3.14/site-packages/setuptools-* \
  && find /install/lib/python3.14/site-packages/ -name '*.dist-info' -exec sh -c \
       'for d; do rm -f "$d/RECORD" "$d/SOURCES.txt" "$d/direct_url.json"; done' _ {} + \
  && find /install/ -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null; true


# ── Stage 2: runtime ──────────────────────────────────────────────────────────
# Minimal image — no build tools, only the installed package and its runtime deps.
FROM python:3.14-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
  PYTHONUNBUFFERED=1

# GitPython requires git; wget used by Docker/compose healthchecks;
# postgresql-client runs the River role provisioning below (CHAOS-3925).
RUN apt-get update && apt-get install -y --no-install-recommends git wget postgresql-client \
  && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder (no build tools, pip, or setuptools).
COPY --from=builder /install /usr/local
COPY --from=go-migrator-builder /out/dev-health-worker-migrate /usr/local/bin/dev-health-worker-migrate

# River runtime-role provisioning travels WITH the image that runs it. It used
# to be bind-mounted from a source checkout, which a pull-only host cannot
# produce: Docker answers a missing bind-mount source by creating an empty
# DIRECTORY, so the first Go bring-up failed as
# `psql: could not read from input file: Is a directory` -- a message that
# names psql rather than the missing artifact (CHAOS-3925).
#
# The SQL, not this image, remains the source of truth for the domain role's
# grants; only its delivery changes.
COPY scripts/worker/provision_river_roles.sql /usr/local/share/dev-health/provision_river_roles.sql

# Fail the image build if the installed distribution cannot load the exact job
# contract artifacts used by Python worker routing.
RUN python -c "from dev_health_ops.workers.job_contracts.registry import load_migration_jobs, load_registry; load_registry(); load_migration_jobs()"

RUN useradd --uid 10001 --create-home --shell /bin/bash appuser

WORKDIR /app

USER 10001


# ── API target ────────────────────────────────────────────────────────────────
FROM runtime AS api

EXPOSE 8000

# Readiness probe — fast check, no external dependency validation
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
  CMD wget -qO- http://localhost:8000/ready || exit 1

ENTRYPOINT ["dev-hops", "api"]
CMD ["--host", "0.0.0.0", "--port", "8000"]

# ── Generic runner target ──────────────────────────────────────────────────────
FROM runtime AS runner

ENTRYPOINT ["dev-hops"]
CMD ["--help"]

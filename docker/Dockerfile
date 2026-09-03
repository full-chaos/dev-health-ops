ARG GO_BUILD_IMAGE="mirror.gcr.io/library/golang:1.27.0-alpine@sha256:4c9fe60190a2a3350ddc51de80d0224b8a6698d12bdfc999fee45ea9d6c46dbc"
# CHAOS-4922: the Python base comes from our ghcr mirror, not Docker Hub.
#
# CI never authenticates to Docker Hub, so an unauthenticated Hub pull is rate
# limited per IP and shared with every other runner on that address -- a
# failure that arrives as a build error on an unrelated PR and cannot be
# reproduced locally.
#
# DIGEST-PINNED, and the digest is the UPSTREAM one on purpose:
# `docker buildx imagetools create` copies the manifest without rewriting it,
# so the mirrored ref carries the SAME digest as the Hub original. Pinning the
# upstream digest therefore names exactly one image whichever registry serves
# it, and the pin can be verified against Hub without trusting the mirror.
#
# Resolved 2026-09-03 from registry-1.docker.io for python:3.14-slim:
#   application/vnd.oci.image.index.v1+json, linux/amd64 + linux/arm64 present
ARG PYTHON_BASE_IMAGE="ghcr.io/full-chaos/python:3.14-slim@sha256:cad9a2c871761c413caa6fdd6441c783451e740a48aaeba60ae62a8b53525ef6"

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
FROM ${PYTHON_BASE_IMAGE} AS builder

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
FROM ${PYTHON_BASE_IMAGE} AS runtime

ARG VERSION="dev"
ARG COMMIT="unknown"
ARG BUILD_TIME="1970-01-01T00:00:00Z"

# CHAOS-4947: same mechanism docker/go-worker.Dockerfile's runtime stage
# already uses -- one LABEL block, not a second (docker/build-push-action's
# own `labels:` input would be a parallel path setting the same key). The
# fan-in job in .github/workflows/docker-images.yml reads
# org.opencontainers.image.revision off whatever is currently tagged
# `latest` to decide whether a new build may move that tag.
LABEL org.opencontainers.image.title="Dev Health ops runtime" \
      org.opencontainers.image.description="dev-health-ops API and runner images" \
      org.opencontainers.image.source="https://github.com/full-chaos/dev-health-ops" \
      org.opencontainers.image.version=${VERSION} \
      org.opencontainers.image.revision=${COMMIT} \
      org.opencontainers.image.created=${BUILD_TIME}

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

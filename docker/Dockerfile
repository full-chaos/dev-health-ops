ARG GO_BUILD_IMAGE="mirror.gcr.io/library/golang:1.25.9-alpine@sha256:5caaf1cca9dc351e13deafbc3879fd4754801acba8653fa9540cea125d01a71f"

# ── Stage 0: one-shot River migrator builder ──────────────────────────────────
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
    && touch -d "@${SOURCE_DATE_EPOCH}" /out/dev-health-worker-migrate

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

# GitPython requires git; wget used by Docker/compose healthchecks
RUN apt-get update && apt-get install -y --no-install-recommends git wget \
  && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder (no build tools, pip, or setuptools).
COPY --from=builder /install /usr/local
COPY --from=go-migrator-builder /out/dev-health-worker-migrate /usr/local/bin/dev-health-worker-migrate

RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

USER appuser


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

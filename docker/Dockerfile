# ---- builder stage: install deps with build tools ----
FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
  PYTHONUNBUFFERED=1

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
  build-essential git wget \
  && rm -rf /var/lib/apt/lists/*

COPY . .

ARG SETUPTOOLS_SCM_PRETEND_VERSION
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${SETUPTOOLS_SCM_PRETEND_VERSION}

RUN pip install --no-cache-dir --prefix=/install .

# ---- final stage: runtime only (no build tools) ----
FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
  PYTHONUNBUFFERED=1

# Copy installed packages and scripts from builder
COPY --from=builder /install/lib /usr/local/lib
COPY --from=builder /install/bin /usr/local/bin

WORKDIR /app

FROM base AS api
EXPOSE 8000

ENTRYPOINT ["dev-hops", "api"]
CMD ["--host", "0.0.0.0", "--port", "8000"]

FROM base AS runner
ENTRYPOINT ["dev-hops"]
CMD ["--help"]

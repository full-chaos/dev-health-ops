FROM python:3.14-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build

COPY . .
RUN apt update && apt install -y build-essential git

ARG SETUPTOOLS_SCM_PRETEND_VERSION
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${SETUPTOOLS_SCM_PRETEND_VERSION}

RUN pip install --no-cache-dir .
RUN rm -rf /build

WORKDIR /app
RUN mkdir -p /app
# Ensure config files are available if needed
# COPY --from=base /usr/local/lib/python3.14/site-packages/dev_health_ops/config /app/config

FROM base AS api
EXPOSE 8000

ENTRYPOINT ["dev-hops", "api"]
CMD ["--host", "0.0.0.0", "--port", "8000"]



FROM base AS runner
ENTRYPOINT ["dev-hops"]
CMD ["--help"]

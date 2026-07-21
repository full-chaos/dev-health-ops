# Keep both build and runtime images immutable. Updating either digest is an
# explicit dependency change reviewed alongside the Go toolchain pin.
ARG GO_BUILD_IMAGE="mirror.gcr.io/library/golang:1.25.9-alpine@sha256:5caaf1cca9dc351e13deafbc3879fd4754801acba8653fa9540cea125d01a71f"
ARG GO_RUNTIME_IMAGE="gcr.io/distroless/static-debian12:nonroot@sha256:f5b485ea962d9bd1186b2f6b3a061191539b905b82ec395de78cbfae51f20e35"

FROM --platform=$BUILDPLATFORM ${GO_BUILD_IMAGE} AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION="dev"
ARG COMMIT="unknown"
ARG BUILD_TIME="1970-01-01T00:00:00Z"
ARG SOURCE_DATE_EPOCH="0"

ENV CGO_ENABLED=0 \
    GOFLAGS="-mod=readonly" \
    SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH}

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY cmd ./cmd
COPY contracts ./contracts
COPY internal ./internal

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eu; \
    for command in \
        dev-health-worker \
        dev-health-scheduler \
        dev-health-reconciler \
        dev-health-stream-runner \
        worker-contractcheck; do \
      GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" go build \
        -buildvcs=false \
        -trimpath \
        -ldflags="-s -w -buildid= \
          -X github.com/full-chaos/dev-health-ops/internal/platform/version.Version=${VERSION} \
          -X github.com/full-chaos/dev-health-ops/internal/platform/version.Commit=${COMMIT} \
          -X github.com/full-chaos/dev-health-ops/internal/platform/version.BuildTime=${BUILD_TIME}" \
        -o "/out/${command}" \
        "./cmd/${command}"; \
    done; \
    mkdir -p \
      /runtime/worker/usr/local/bin \
      /runtime/scheduler/usr/local/bin \
      /runtime/reconciler/usr/local/bin \
      /runtime/stream-runner/usr/local/bin \
      /runtime/contractcheck/usr/local/bin \
      /runtime/contractcheck/app/contracts/jobs; \
    cp /out/dev-health-worker /runtime/worker/usr/local/bin/dev-health-worker; \
    cp /out/dev-health-scheduler /runtime/scheduler/usr/local/bin/dev-health-scheduler; \
    cp /out/dev-health-reconciler /runtime/reconciler/usr/local/bin/dev-health-reconciler; \
    cp /out/dev-health-stream-runner /runtime/stream-runner/usr/local/bin/dev-health-stream-runner; \
    cp /out/worker-contractcheck /runtime/contractcheck/usr/local/bin/worker-contractcheck; \
    cp -R /src/contracts/jobs/v1 /runtime/contractcheck/app/contracts/jobs/v1; \
    find /runtime -exec touch -d "@${SOURCE_DATE_EPOCH}" {} +

FROM ${GO_RUNTIME_IMAGE} AS runtime

ARG VERSION="dev"
ARG COMMIT="unknown"
ARG BUILD_TIME="1970-01-01T00:00:00Z"

LABEL org.opencontainers.image.title="Dev Health Go worker runtime" \
      org.opencontainers.image.description="Additive Go worker foundation for Dev Health" \
      org.opencontainers.image.source="https://github.com/full-chaos/dev-health-ops" \
      org.opencontainers.image.version=${VERSION} \
      org.opencontainers.image.revision=${COMMIT} \
      org.opencontainers.image.created=${BUILD_TIME}

USER 65532:65532
EXPOSE 8080

FROM runtime AS worker
COPY --from=build --chown=65532:65532 /runtime/worker/ /
ENTRYPOINT ["/usr/local/bin/dev-health-worker"]

FROM runtime AS scheduler
COPY --from=build --chown=65532:65532 /runtime/scheduler/ /
ENTRYPOINT ["/usr/local/bin/dev-health-scheduler"]

FROM runtime AS reconciler
COPY --from=build --chown=65532:65532 /runtime/reconciler/ /
ENTRYPOINT ["/usr/local/bin/dev-health-reconciler"]

FROM runtime AS stream-runner
COPY --from=build --chown=65532:65532 /runtime/stream-runner/ /
ENTRYPOINT ["/usr/local/bin/dev-health-stream-runner"]

FROM runtime AS contractcheck
COPY --from=build --chown=65532:65532 /runtime/contractcheck/ /
WORKDIR /app
ENTRYPOINT ["/usr/local/bin/worker-contractcheck"]
CMD ["validate"]

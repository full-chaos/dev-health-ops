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
COPY deploy/go-workers ./deploy/go-workers
COPY internal ./internal
COPY src/dev_health_ops/config/status_mapping.yaml ./src/dev_health_ops/config/status_mapping.yaml
COPY src/dev_health_ops/config/investment_areas.yaml ./src/dev_health_ops/config/investment_areas.yaml

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eu; \
    for command in \
        dev-health-worker \
        dev-health-scheduler \
        dev-health-reconciler \
        dev-health-stream-runner \
        dev-health-workerctl \
        worker-contractcheck \
        dev-health-worker-migrate; do \
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
      /runtime/worker/app/contracts/jobs \
      /runtime/worker/app/config \
      /runtime/worker/app/deploy/go-workers \
      /runtime/scheduler/usr/local/bin \
      /runtime/scheduler/app/contracts/jobs \
      /runtime/scheduler/app/deploy/go-workers \
      /runtime/reconciler/usr/local/bin \
      /runtime/reconciler/app/contracts/jobs \
      /runtime/reconciler/app/contracts/sync-dispatch \
      /runtime/stream-runner/usr/local/bin \
      /runtime/operator/usr/local/bin \
      /runtime/operator/app/contracts/jobs \
      /runtime/operator/app/contracts/sync-dispatch \
      /runtime/operator/app/deploy/go-workers \
      /runtime/contractcheck/usr/local/bin \
      /runtime/contractcheck/app/contracts/jobs \
      /runtime/contractcheck/app/deploy/go-workers \
      /runtime/migrate/usr/local/bin; \
    cp /out/dev-health-worker /runtime/worker/usr/local/bin/dev-health-worker; \
    cp /out/dev-health-scheduler /runtime/scheduler/usr/local/bin/dev-health-scheduler; \
    cp -R /src/contracts/jobs/v1 /runtime/scheduler/app/contracts/jobs/v1; \
    cp /src/deploy/go-workers/profiles.json /runtime/scheduler/app/deploy/go-workers/profiles.json; \
    cp /out/dev-health-reconciler /runtime/reconciler/usr/local/bin/dev-health-reconciler; \
    cp -R /src/contracts/jobs/v1 /runtime/reconciler/app/contracts/jobs/v1; \
    cp -R /src/contracts/sync-dispatch/v1 /runtime/reconciler/app/contracts/sync-dispatch/v1; \
    cp /out/dev-health-stream-runner /runtime/stream-runner/usr/local/bin/dev-health-stream-runner; \
    cp /out/dev-health-workerctl /runtime/operator/usr/local/bin/dev-health-workerctl; \
    cp /out/worker-contractcheck /runtime/contractcheck/usr/local/bin/worker-contractcheck; \
    cp -R /src/contracts/jobs/v1 /runtime/worker/app/contracts/jobs/v1; \
    cp /src/deploy/go-workers/profiles.json /runtime/worker/app/deploy/go-workers/profiles.json; \
    cp /src/src/dev_health_ops/config/status_mapping.yaml /runtime/worker/app/config/status_mapping.yaml; \
    cp /src/src/dev_health_ops/config/investment_areas.yaml /runtime/worker/app/config/investment_areas.yaml; \
    cp -R /src/contracts/jobs/v1 /runtime/operator/app/contracts/jobs/v1; \
    cp -R /src/contracts/sync-dispatch/v1 /runtime/operator/app/contracts/sync-dispatch/v1; \
    cp /src/deploy/go-workers/profiles.json /runtime/operator/app/deploy/go-workers/profiles.json; \
    cp -R /src/contracts/jobs/v1 /runtime/contractcheck/app/contracts/jobs/v1; \
    cp /src/deploy/go-workers/profiles.json /runtime/contractcheck/app/deploy/go-workers/profiles.json; \
    cp /out/dev-health-worker-migrate /runtime/migrate/usr/local/bin/dev-health-worker-migrate; \
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
WORKDIR /app
ENTRYPOINT ["/usr/local/bin/dev-health-worker"]

FROM runtime AS scheduler
COPY --from=build --chown=65532:65532 /runtime/scheduler/ /
WORKDIR /app
ENTRYPOINT ["/usr/local/bin/dev-health-scheduler"]

FROM runtime AS reconciler
COPY --from=build --chown=65532:65532 /runtime/reconciler/ /
WORKDIR /app
ENTRYPOINT ["/usr/local/bin/dev-health-reconciler"]

FROM runtime AS stream-runner
COPY --from=build --chown=65532:65532 /runtime/stream-runner/ /
ENTRYPOINT ["/usr/local/bin/dev-health-stream-runner"]

FROM runtime AS operator
COPY --from=build --chown=65532:65532 /runtime/operator/ /
WORKDIR /app
ENTRYPOINT ["/usr/local/bin/dev-health-workerctl"]

FROM runtime AS contractcheck
COPY --from=build --chown=65532:65532 /runtime/contractcheck/ /
WORKDIR /app
ENTRYPOINT ["/usr/local/bin/worker-contractcheck"]
CMD ["validate"]

# migrate is the one-shot River schema/grant migration
# (cmd/dev-health-worker-migrate). It reads no contract or deployment-profile
# files at runtime -- only its flags and the MIGRATION_DATABASE_URI /
# RIVER_*_ROLE environment -- so, unlike most targets above, its runtime layer
# stages nothing under /app.
#
# It therefore must NOT declare `WORKDIR /app`, and follows stream-runner (the
# only other target with no /app tree) in omitting it. The `runtime` base does
# not create /app, so the two cases are not equivalent:
#
#   - targets that stage an app/ tree receive /app from the build stage, where
#     `find /runtime -exec touch -d @$SOURCE_DATE_EPOCH` has already flattened
#     its mtime, and WORKDIR is then a no-op on an existing directory;
#   - a target with no app/ tree makes WORKDIR *create* the directory during
#     this stage, which is not covered by that normalisation.
#
# With `WORKDIR /app` here, `check_go_containers.sh reproducible` failed on two
# separate CI runs -- `migrate image is not reproducible` -- while all six
# other targets passed both times. Those seven results isolate this line as the
# only structural difference: five targets pair WORKDIR with a staged app/
# tree, stream-runner has neither, and migrate was the sole WORKDIR-without-app
# combination.
#
# Caveat for whoever revisits this: the mechanism above is inferred from that
# CI evidence, not confirmed locally. A platform-faithful local probe
# (--platform linux/amd64, SOURCE_DATE_EPOCH=0, --no-cache) found migrate
# reproducible both with and without the line, so the timestamp normalisation
# apparently differs between Docker Desktop's BuildKit and the CI runner's.
# Do not re-add WORKDIR on the strength of a local probe alone.
FROM runtime AS migrate
COPY --from=build --chown=65532:65532 /runtime/migrate/ /
ENTRYPOINT ["/usr/local/bin/dev-health-worker-migrate"]

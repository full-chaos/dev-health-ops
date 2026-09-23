# Keep both build and runtime images immutable. Updating either digest is an
# explicit dependency change reviewed alongside the Go toolchain pin.
ARG GO_BUILD_IMAGE="mirror.gcr.io/library/golang:1.27.0-alpine@sha256:4c9fe60190a2a3350ddc51de80d0224b8a6698d12bdfc999fee45ea9d6c46dbc"
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
# go mod download talks to proxy.golang.org over the network; a transient
# proxy-side error should not fail the whole image build, so retry a
# bounded number of times before giving up.
RUN --mount=type=cache,target=/go/pkg/mod \
    attempt=1; \
    until go mod download; do \
      status=$?; \
      if [ "$attempt" -ge 3 ]; then exit "$status"; fi; \
      attempt=$((attempt + 1)); \
      sleep 5; \
    done

COPY cmd ./cmd
COPY contracts ./contracts
COPY deploy/go-workers ./deploy/go-workers
COPY internal ./internal
COPY src/dev_health_ops/config/status_mapping.yaml ./src/dev_health_ops/config/status_mapping.yaml
COPY src/dev_health_ops/config/investment_areas.yaml ./src/dev_health_ops/config/investment_areas.yaml
COPY src/dev_health_ops/config/complexity.yaml ./src/dev_health_ops/config/complexity.yaml

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eu; \
    for command in \
        dev-health-worker \
        dev-health-worker-migrate \
        dho; do \
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
      /runtime/operator/usr/local/bin \
      /runtime/operator/app/contracts/jobs \
      /runtime/operator/app/contracts/sync-dispatch \
      /runtime/operator/app/deploy/go-workers \
      /runtime/contractcheck/usr/local/bin \
      /runtime/contractcheck/app/contracts/jobs \
      /runtime/contractcheck/app/deploy/go-workers \
      /runtime/migrate/usr/local/bin \
      /runtime/dho/usr/local/bin \
      /runtime/dho/app/contracts/jobs \
      /runtime/dho/app/contracts/sync-dispatch; \
    cp /out/dev-health-worker /runtime/worker/usr/local/bin/dev-health-worker; \
    cp /out/dho /runtime/worker/usr/local/bin/dho; \
    cp /out/dho /runtime/operator/usr/local/bin/dho; \
    cp /out/dho /runtime/contractcheck/usr/local/bin/dho; \
    cp -R /src/contracts/jobs/v1 /runtime/worker/app/contracts/jobs/v1; \
    cp /src/deploy/go-workers/deployment.json /runtime/worker/app/deploy/go-workers/deployment.json; \
    cp /src/src/dev_health_ops/config/status_mapping.yaml /runtime/worker/app/config/status_mapping.yaml; \
    cp /src/src/dev_health_ops/config/investment_areas.yaml /runtime/worker/app/config/investment_areas.yaml; \
    cp /src/src/dev_health_ops/config/complexity.yaml /runtime/worker/app/config/complexity.yaml; \
    cp -R /src/contracts/jobs/v1 /runtime/operator/app/contracts/jobs/v1; \
    cp -R /src/contracts/sync-dispatch/v1 /runtime/operator/app/contracts/sync-dispatch/v1; \
    cp /src/deploy/go-workers/deployment.json /runtime/operator/app/deploy/go-workers/deployment.json; \
    cp -R /src/contracts/jobs/v1 /runtime/contractcheck/app/contracts/jobs/v1; \
    cp /src/deploy/go-workers/deployment.json /runtime/contractcheck/app/deploy/go-workers/deployment.json; \
    cp /out/dev-health-worker-migrate /runtime/migrate/usr/local/bin/dev-health-worker-migrate; \
    cp /out/dho /runtime/dho/usr/local/bin/dho; \
    cp -R /src/contracts/jobs/v1 /runtime/dho/app/contracts/jobs/v1; \
    cp -R /src/contracts/sync-dispatch/v1 /runtime/dho/app/contracts/sync-dispatch/v1; \
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

# The operator image runs dho; route activation passes `workers routes apply
# ...` as args (spec S2 folded dev-health-workerctl into `dho workers`).
FROM runtime AS operator
COPY --from=build --chown=65532:65532 /runtime/operator/ /
WORKDIR /app
ENTRYPOINT ["/usr/local/bin/dho"]

# The contractcheck image runs dho; worker-contractcheck folded into
# `dho contracts`.
FROM runtime AS contractcheck
COPY --from=build --chown=65532:65532 /runtime/contractcheck/ /
WORKDIR /app
ENTRYPOINT ["/usr/local/bin/dho"]
CMD ["contracts", "validate"]

# dho is the operator binary (cmd/dho): one binary whose compiled-in
# verticals are selected by the first argument, so every Deployment of it
# passes its subcommand as args (`dho api`, `dho stream-runner
# --profile=...`, `dho reconciler`, `dho scheduler`). The api vertical's
# webhook-intake routes (CHAOS-6247), the reconciler and the scheduler load the
# checked-in contracts/jobs/v1 manifest (jobruntime.Load), and the reconciler
# also loads contracts/sync-dispatch/v1 -- so, like worker/operator/
# contractcheck, this target stages them under /app and declares a WORKDIR.
# The stream-runner verb reads no staged file.
FROM runtime AS dho
COPY --from=build --chown=65532:65532 /runtime/dho/ /
WORKDIR /app
ENTRYPOINT ["/usr/local/bin/dho"]

# migrate is the one-shot River schema/grant migration
# (cmd/dev-health-worker-migrate). It reads no contract or deployment-profile
# files at runtime -- only its flags and the MIGRATION_DATABASE_URI /
# RIVER_*_ROLE environment -- so, unlike most targets above, its runtime layer
# stages nothing under /app.
#
# It therefore must NOT declare `WORKDIR /app`, and follows dho (the only
# other target with no /app tree) in omitting it. The `runtime` base does
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


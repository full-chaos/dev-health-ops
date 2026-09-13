# CHAOS-5666: publishes the go-api-routing and go-api-prove operator
# binaries as one image, ghcr.io/full-chaos/dev-health-go-api-tools --
# CI had no publish job for either binary, so the package only ever
# carried hand-built tags (sha-44f758e8b, sha-b36d2dad35f3). Routing rows
# can only be re-enabled by a tools image built from the SAME commit as
# the running query-api (its posture manifest sha is derived from the
# same schemav1.SDL embed both binaries link -- see go-api-routing's own
# import of contracts/graphql/v1 below), so a hand-built, occasionally
# updated tag is not a substitute for a commit-addressed CI image.
#
# One runtime target, not a target-per-binary split like
# go-worker.Dockerfile: both binaries are read/write operator surfaces
# for the SAME rollout (repoint/enable/disable/status vs. prove), always
# invoked together by the same operator, so there is no deploy unit that
# wants one without the other.
#
# Same base-image pins and build discipline as query-api.Dockerfile /
# go-worker.Dockerfile: pin both build and runtime image digests so an
# update is an explicit, reviewed dependency change.
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
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Both binaries import internal/goapidigest and internal/goapiproof;
# go-api-routing also imports contracts/graphql/v1 (the schemav1.SDL
# embed the posture/schema-digest is computed from). Whole-tree COPY for
# both, same reasoning query-api.Dockerfile documents for its own
# COPY internal ./internal: an enumerated subpackage list is how the
# next import added to either binary goes uncopied and fails closed with
# a "-mod=readonly" error instead of a clear diff.
COPY cmd/go-api-routing ./cmd/go-api-routing
COPY cmd/go-api-prove ./cmd/go-api-prove
COPY contracts/graphql/v1 ./contracts/graphql/v1
COPY internal ./internal

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eu; \
    for command in \
        go-api-routing \
        go-api-prove; do \
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
    find /out -exec touch -d "@${SOURCE_DATE_EPOCH}" {} +

FROM ${GO_RUNTIME_IMAGE} AS runtime

ARG VERSION="dev"
ARG COMMIT="unknown"
ARG BUILD_TIME="1970-01-01T00:00:00Z"

LABEL org.opencontainers.image.title="Dev Health Go-API tools" \
      org.opencontainers.image.description="go-api-routing and go-api-prove operator binaries for the Go-API rollout" \
      org.opencontainers.image.source="https://github.com/full-chaos/dev-health-ops" \
      org.opencontainers.image.version=${VERSION} \
      org.opencontainers.image.revision=${COMMIT} \
      org.opencontainers.image.created=${BUILD_TIME}

USER 65532:65532

COPY --from=build /out/go-api-routing /usr/local/bin/go-api-routing
COPY --from=build /out/go-api-prove /usr/local/bin/go-api-prove

ENTRYPOINT ["/usr/local/bin/go-api-routing"]

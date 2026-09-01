# Dedicated Dockerfile for query-api (CHAOS-4366 Wave 0), kept separate from
# docker/go-worker.Dockerfile rather than folded into its multi-binary build:
# that file's `build` stage compiles and packages seven existing worker
# binaries into five different runtime targets in one RUN block, and
# query-api has no relationship to the worker fleet (it is a read-only
# GraphQL API service, not a job consumer) -- adding an eighth binary and a
# sixth runtime target there would only couple two unrelated deploy units'
# build cache and blast radius for no shared benefit.
#
# Same base-image pins and build discipline as go-worker.Dockerfile: pin
# both build and runtime image digests so an update is an explicit,
# reviewed dependency change.
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

COPY cmd/query-api ./cmd/query-api
# LANE-4460-L3: `COPY contracts/graphql` IS now required -- this comment
# used to say the opposite ("gqlgen bakes the schema into generated Go
# source ... no go:embed/ReadFile of the SDL exists there") and that build
# broke the moment it stopped being true. CHAOS-4696 PR2 added
# contracts/graphql/v1/sdl.go, a go:embed of schema.graphql that
# query_route.go's verifySchemaDigest imports as `schemav1.SDL` to check
# GO_API_SCHEMA_DIGEST against the SDL this binary was actually built with
# (see that file's doc comment) -- a real, verified runtime import this
# Dockerfile's build context did not carry, so `go build` failed closed
# with "cannot find module providing package
# .../contracts/graphql/v1: import lookup disabled by -mod=readonly"
# (confirmed by running this exact build). Copy only contracts/graphql/v1,
# not the whole tree: it is the one subpackage query-api's import graph
# reaches (verified: `grep -rn "dev-health-ops/internal\|dev-health-ops/contracts"
# cmd/query-api` finds no other cross-tree import).
COPY contracts/graphql/v1 ./contracts/graphql/v1
# Still NOT `COPY internal ./internal`: query-api imports nothing from the
# shared repo-root internal/ tree (distinct from cmd/query-api/internal/,
# which is already copied above with the rest of cmd/query-api) -- add it
# back only once a resolver actually imports from there or from the
# dev-health-go module.

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eu; \
    GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" go build \
      -buildvcs=false \
      -trimpath \
      -ldflags="-s -w -buildid= \
        -X github.com/full-chaos/dev-health-ops/internal/platform/version.Version=${VERSION} \
        -X github.com/full-chaos/dev-health-ops/internal/platform/version.Commit=${COMMIT} \
        -X github.com/full-chaos/dev-health-ops/internal/platform/version.BuildTime=${BUILD_TIME}" \
      -o /out/query-api \
      ./cmd/query-api; \
    find /out -exec touch -d "@${SOURCE_DATE_EPOCH}" {} +

FROM ${GO_RUNTIME_IMAGE} AS runtime

ARG VERSION="dev"
ARG COMMIT="unknown"
ARG BUILD_TIME="1970-01-01T00:00:00Z"

LABEL org.opencontainers.image.title="Dev Health query-api" \
      org.opencontainers.image.description="Wave 0: empty Go read-only GraphQL analytics service (no resolvers implemented, CHAOS-4366)" \
      org.opencontainers.image.source="https://github.com/full-chaos/dev-health-ops" \
      org.opencontainers.image.version=${VERSION} \
      org.opencontainers.image.revision=${COMMIT} \
      org.opencontainers.image.created=${BUILD_TIME}

USER 65532:65532
EXPOSE 8090

COPY --from=build /out/query-api /usr/local/bin/query-api

ENTRYPOINT ["/usr/local/bin/query-api"]

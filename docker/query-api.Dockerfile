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

COPY cmd/query-api ./cmd/query-api
# NOT `COPY contracts/graphql`: gqlgen bakes the schema into generated Go
# source at codegen time (cmd/query-api/internal/graph/generated.go), not
# read at runtime -- confirmed no go:embed/ReadFile of the SDL exists there.
# NOT `COPY internal ./internal`: query-api imports nothing from the shared
# internal/ tree yet (no resolver exists to need a store reader -- see
# main.go's package doc). Add both back once a resolver actually imports
# from internal/ or from the dev-health-go module (CHAOS-4377).

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

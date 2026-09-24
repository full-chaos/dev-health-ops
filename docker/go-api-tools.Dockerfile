# CHAOS-5666 published go-api-routing and go-api-prove as one image,
# ghcr.io/full-chaos/dev-health-go-api-tools. Spec S1 (CHAOS-6280) folds
# both of them, go-api-rest-prove, mint-envelope and mint-edge-token into
# the ONE dho operator binary (cmd/dho): this image now builds and ships
# that one binary instead of five, as `goapi routing`, `goapi prove`,
# `goapi rest-prove`, `mint envelope` and `mint edge-token`. Routing rows
# can only be re-enabled by a tools image built from the SAME commit as
# the running query-api (its posture manifest sha is derived from the
# same schemav1.SDL embed the binary links -- see internal/goapicli/routing's
# own import of contracts/graphql/v1 below), so a hand-built, occasionally
# updated tag is not a substitute for a commit-addressed CI image.
#
# One runtime target: every verb here is a read/write operator surface for
# the SAME rollout (repoint/enable/disable/status, prove, rest-prove, the
# two mint helpers), always invoked from the same tools pod, so there is
# no deploy unit that wants a subset.
#
# Same build-image pin and build discipline as go-worker.Dockerfile.
# The runtime base deliberately does NOT follow it into distroless: this image is also the operator's one-off
# corrective-verb pod (docs/operate/runbooks/query-api-bootstrap.md), and
# a distroless runtime has no shell or coreutils to stay up for
# `kubectl exec` or to read the baked-in documents dump / operation
# catalog off disk with a plain path. Both image digests stay pinned so
# an update is an explicit, reviewed dependency change.
ARG GO_BUILD_IMAGE="mirror.gcr.io/library/golang:1.27.0-alpine@sha256:4c9fe60190a2a3350ddc51de80d0224b8a6698d12bdfc999fee45ea9d6c46dbc"
ARG TOOLS_RUNTIME_IMAGE="mirror.gcr.io/library/debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171"

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

# cmd/dho pulls in every vertical under internal/ (including
# internal/goapicli, internal/mintcli, internal/goapidigest and
# internal/goapiproof) plus contracts/graphql/v1 (the schemav1.SDL embed
# the posture/schema-digest is computed from) and contracts/jobs/v1 (the job
# migration policy `dho migrate river` embeds). cmd/registrydump is needed
# too, with the query_route.go it reads (below, inside internal/). The
# whole internal/ tree is copied, not an enumerated subpackage list: an
# enumerated list is how the next import added to any of these goes
# uncopied and fails closed with a "-mod=readonly" error instead of a
# clear diff.
COPY cmd/dho ./cmd/dho
COPY cmd/registrydump ./cmd/registrydump
COPY contracts/graphql/v1 ./contracts/graphql/v1
COPY contracts/jobs/v1 ./contracts/jobs/v1
COPY internal ./internal

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" go build \
      -buildvcs=false \
      -trimpath \
      -ldflags="-s -w -buildid= \
        -X github.com/full-chaos/dev-health-ops/internal/platform/version.Version=${VERSION} \
        -X github.com/full-chaos/dev-health-ops/internal/platform/version.Commit=${COMMIT} \
        -X github.com/full-chaos/dev-health-ops/internal/platform/version.BuildTime=${BUILD_TIME}" \
      -o /out/dho \
      ./cmd/dho && \
    touch -d "@${SOURCE_DATE_EPOCH}" /out/dho

# `goapi prove`'s -documents flag needs registrydump's enumeration of
# query-api's registered GraphQL documents (internal/queryapi/server/query_route.go).
# Built and run here, at the SAME commit as dho above, so the baked-in
# dump can never drift from what this image's `dho goapi prove` actually
# verifies against -- a stale, hand-carried dump was exactly the gap the
# old hand-built tags left open.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -buildvcs=false -trimpath -o /out/registrydump ./cmd/registrydump && \
    /out/registrydump -file internal/queryapi/server/query_route.go > /out/documents.json && \
    rm /out/registrydump && \
    touch -d "@${SOURCE_DATE_EPOCH}" /out/documents.json

FROM ${TOOLS_RUNTIME_IMAGE} AS runtime

ARG VERSION="dev"
ARG COMMIT="unknown"
ARG BUILD_TIME="1970-01-01T00:00:00Z"

LABEL org.opencontainers.image.title="Dev Health Go-API tools" \
      org.opencontainers.image.description="the dho operator binary's goapi and mint verbs (routing, prove, rest-prove, mint envelope, mint edge-token), plus the documents dump and operation catalog they need, for the Go-API rollout's tools pod" \
      org.opencontainers.image.source="https://github.com/full-chaos/dev-health-ops" \
      org.opencontainers.image.version=${VERSION} \
      org.opencontainers.image.revision=${COMMIT} \
      org.opencontainers.image.created=${BUILD_TIME}

# Same non-root numeric-friendly convention as docker/Dockerfile's runtime
# stage: a real user, not root, but one a Kubernetes securityContext can
# still pin by uid.
RUN useradd --uid 10001 --create-home --shell /bin/bash toolsuser

WORKDIR /app/go-api

COPY --from=build /out/dho /usr/local/bin/dho

# The documents dump (freshly generated above, same commit as dho) and
# the checked-in operation catalog (never regenerated here -- see
# internal/goapiproof/routing_catalog.go's own comment on why it is a
# checked-in artifact, not a build output). The catalog lands at its
# DefaultCatalogPath *relative to WORKDIR*, so `dho goapi routing`'s
# `-catalog` flag needs no override from this image's default working
# directory.
COPY --from=build /out/documents.json /app/go-api/documents.json
COPY src/dev_health_ops/api/graphql/go_api_operations.json /app/go-api/src/dev_health_ops/api/graphql/go_api_operations.json

RUN chown -R toolsuser:toolsuser /app/go-api

USER toolsuser

# No long-lived process of its own -- this exists to be `kubectl exec`ed
# or `kubectl run ... -- <command>`ed into for one-off `dho goapi routing`
# / `dho goapi prove` / `dho goapi rest-prove` / `dho mint envelope` /
# `dho mint edge-token` runs, so no ENTRYPOINT is set: a caller-supplied
# command replaces CMD outright instead of trailing a fixed entrypoint
# binary. The default CMD only needs to keep the Pod alive when no
# command is given (docker/Dockerfile's `api` and `runner` targets exist
# because THEIR image runs continuously; this one never does).
CMD ["sleep", "infinity"]

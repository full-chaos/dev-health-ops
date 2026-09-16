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
# Same build-image pin and build discipline as query-api.Dockerfile /
# go-worker.Dockerfile. The runtime base deliberately does NOT follow
# those two into distroless: this image is also the operator's one-off
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
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Both binaries import internal/goapidigest and internal/goapiproof;
# go-api-routing also imports contracts/graphql/v1 (the schemav1.SDL
# embed the posture/schema-digest is computed from). cmd/query-api is
# needed too, whole-tree, for its tools/registrydump helper and the
# query_route.go it reads (below) -- same reasoning query-api.Dockerfile
# documents for its own COPY internal ./internal: an enumerated
# subpackage list is how the next import added to any of these goes
# uncopied and fails closed with a "-mod=readonly" error instead of a
# clear diff.
COPY cmd/go-api-routing ./cmd/go-api-routing
COPY cmd/go-api-prove ./cmd/go-api-prove
COPY cmd/go-api-rest-prove ./cmd/go-api-rest-prove
COPY cmd/mint-envelope ./cmd/mint-envelope
COPY cmd/mint-edge-token ./cmd/mint-edge-token
COPY cmd/query-api ./cmd/query-api
COPY contracts/graphql/v1 ./contracts/graphql/v1
COPY internal ./internal

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eu; \
    for command in \
        go-api-routing \
        go-api-prove \
        go-api-rest-prove \
        mint-envelope \
        mint-edge-token; do \
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

# go-api-prove's -documents flag needs registrydump's enumeration of
# query-api's registered GraphQL documents (cmd/query-api/query_route.go).
# Built and run here, at the SAME commit as the two binaries above, so the
# baked-in dump can never drift from what this image's go-api-prove
# actually verifies against -- a stale, hand-carried dump was exactly the
# gap the old hand-built tags left open.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -buildvcs=false -trimpath -o /out/registrydump ./cmd/query-api/tools/registrydump && \
    /out/registrydump -file cmd/query-api/query_route.go > /out/documents.json && \
    rm /out/registrydump && \
    touch -d "@${SOURCE_DATE_EPOCH}" /out/documents.json

FROM ${TOOLS_RUNTIME_IMAGE} AS runtime

ARG VERSION="dev"
ARG COMMIT="unknown"
ARG BUILD_TIME="1970-01-01T00:00:00Z"

LABEL org.opencontainers.image.title="Dev Health Go-API tools" \
      org.opencontainers.image.description="go-api-routing, go-api-prove and go-api-rest-prove operator binaries, mint-envelope and mint-edge-token (go-api-prove's/go-api-rest-prove's local envelope and edge-token signing helpers), plus the documents dump and operation catalog they need, for the Go-API rollout's tools pod" \
      org.opencontainers.image.source="https://github.com/full-chaos/dev-health-ops" \
      org.opencontainers.image.version=${VERSION} \
      org.opencontainers.image.revision=${COMMIT} \
      org.opencontainers.image.created=${BUILD_TIME}

# Same non-root numeric-friendly convention as docker/Dockerfile's runtime
# stage: a real user, not root, but one a Kubernetes securityContext can
# still pin by uid.
RUN useradd --uid 10001 --create-home --shell /bin/bash toolsuser

WORKDIR /app/go-api

COPY --from=build /out/go-api-routing /usr/local/bin/go-api-routing
COPY --from=build /out/go-api-prove /usr/local/bin/go-api-prove
# go-api-rest-prove is go-api-prove's REST sibling: it proves a ported
# /api/v1/* route by calling the Python api service and query-api
# directly, in cluster, using the SAME two minting helpers below --
# see internal/goapiproof/mintexec.go's own doc comment for why its
# -*-bearer-exec flags name one of these two binaries by NAME, never
# by a path this image (or an operator) could otherwise vary.
COPY --from=build /out/go-api-rest-prove /usr/local/bin/go-api-rest-prove
# The -proof-bearer-exec helper for go-api-prove (internal/envelopemint's
# package doc has the design): mints the envelope LOCALLY from the same
# Ed25519 signing key the api pod's environment holds
# (GO_API_ENVELOPE_PRIVATE_KEY), reached the same way -- a Secret mounted
# into THIS pod's environment via secretKeyRef, never a copied key file
# and never a call to a running pod.
COPY --from=build /out/mint-envelope /usr/local/bin/mint-envelope
# The -edge-bearer-exec helper (internal/edgetokenmint's package doc has the
# design): mints the edge access token for the dedicated proof service
# principal LOCALLY, from the same JWT_SECRET_KEY the api pod's environment
# holds, reached the same way -- a Secret key via secretKeyRef. It reads the
# principal row through POSTGRES_URI, which go-api-prove already needs.
COPY --from=build /out/mint-edge-token /usr/local/bin/mint-edge-token

# The documents dump (freshly generated above, same commit as the
# binaries) and the checked-in operation catalog (never regenerated here
# -- see internal/goapiproof/routing_catalog.go's own comment on why it
# is a checked-in artifact, not a build output). The catalog lands at its
# DefaultCatalogPath *relative to WORKDIR*, so go-api-routing's `-catalog`
# flag needs no override from this image's default working directory.
COPY --from=build /out/documents.json /app/go-api/documents.json
COPY src/dev_health_ops/api/graphql/go_api_operations.json /app/go-api/src/dev_health_ops/api/graphql/go_api_operations.json

RUN chown -R toolsuser:toolsuser /app/go-api

USER toolsuser

# No long-lived process of its own -- this exists to be `kubectl exec`ed
# or `kubectl run ... -- <command>`ed into for one-off go-api-routing /
# go-api-prove runs, so no ENTRYPOINT is set: a caller-supplied command
# replaces CMD outright instead of trailing a fixed entrypoint binary.
# The default CMD only needs to keep the Pod alive when no command is
# given (docker/Dockerfile's `api` and `runner` targets exist because
# THEIR image runs continuously; this one never does).
CMD ["sleep", "infinity"]

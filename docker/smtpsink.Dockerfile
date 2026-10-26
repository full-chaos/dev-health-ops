# Corpus-only mail sink (ci/auth_corpus_external.sh). Not a release image: no workflow builds or pushes it, and
# docker/go-worker.Dockerfile never copies this binary (internal/cigates/workflows checks both).
ARG GO_BUILD_IMAGE="mirror.gcr.io/library/golang:1.27.3-alpine@sha256:f2c872bcee949b7be9c490efd02b48a8a2fbf947cdf533f1c58525ec9d8170fd"
ARG GO_RUNTIME_IMAGE="gcr.io/distroless/static-debian12:nonroot@sha256:f5b485ea962d9bd1186b2f6b3a061191539b905b82ec395de78cbfae51f20e35"

FROM --platform=$BUILDPLATFORM ${GO_BUILD_IMAGE} AS build
ARG TARGETOS
ARG TARGETARCH
ENV CGO_ENABLED=0 GOFLAGS="-mod=readonly"
WORKDIR /src
COPY go.mod go.sum ./
COPY third_party/vendor/atlassian ./third_party/vendor/atlassian
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd ./cmd
COPY contracts ./contracts
COPY internal ./internal
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" go build -buildvcs=false -trimpath -ldflags="-s -w -buildid=" \
      -o /out/smtpsink ./internal/testsupport/smtpcapture/cmd/smtpsink

FROM ${GO_RUNTIME_IMAGE} AS smtpsink
COPY --from=build /out/smtpsink /usr/local/bin/smtpsink
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/smtpsink"]

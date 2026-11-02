# Test-only identity-provider stub (internal/testsupport/idpstub): the flip-back kit runs it. Not a release image: no
# workflow builds or pushes it, docker/go-worker.Dockerfile never copies this binary, and it holds no key or
# certificate (idpstub generates its signing keys at start; `idpstub certs` writes the TLS files outside the image).
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
      -o /out/idpstub ./internal/testsupport/idpstub/cmd/idpstub

FROM ${GO_RUNTIME_IMAGE} AS idpstub
COPY --from=build /out/idpstub /usr/local/bin/idpstub
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/idpstub"]

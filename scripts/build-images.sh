#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if ! command -v docker >/dev/null; then
  echo "docker is required to build the images" >&2
  exit 1
fi

IMAGE_REGISTRY="${IMAGE_REGISTRY:-ghcr.io/chrisgeo/dev-health-ops}"
VERSION="${VERSION:-latest}"
SETUPTOOLS_SCM_PRETEND_VERSION="${SETUPTOOLS_SCM_PRETEND_VERSION:-$VERSION}"

BUILD_ARGS=(
  --build-arg "SETUPTOOLS_SCM_PRETEND_VERSION=${SETUPTOOLS_SCM_PRETEND_VERSION}"
)

DOCKERFILE="${REPO_ROOT}/docker/Dockerfile"
API_IMAGE="${IMAGE_REGISTRY}/dev-hops-api:${VERSION}"
RUNNER_IMAGE="${IMAGE_REGISTRY}/dev-hops-runner:${VERSION}"

echo "Building ${RUNNER_IMAGE} (target=runner) ..."
docker build \
  "${BUILD_ARGS[@]}" \
  "$@" \
  -t "${RUNNER_IMAGE}" \
  --target runner \
  -f "${DOCKERFILE}" \
  "${REPO_ROOT}"

echo "Building ${API_IMAGE} (target=api) ..."
docker build \
  "${BUILD_ARGS[@]}" \
  "$@" \
  -t "${API_IMAGE}" \
  --target api \
  -f "${DOCKERFILE}" \
  "${REPO_ROOT}"

echo "Built images:"
echo "- ${RUNNER_IMAGE}"
echo "- ${API_IMAGE}"

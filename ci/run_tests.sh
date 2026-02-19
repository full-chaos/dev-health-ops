#!/usr/bin/env bash
set -u -o pipefail

EXIT_USAGE=2
EXIT_MISSING_DEP=3
EXIT_MISSING_TOKENS=4
EXIT_FAILURE=10

usage() {
  cat <<'EOF'
Usage: ci/run_tests.sh <tier>

Tiers:
  unit         Run unit test suite (excludes integration test files)
  integration  Run integration tests (skips when tokens are not set)
  e2e          Run end-to-end tests if present
  ci           Run blocking quality gates + coverage-gated unit tests + optional integration/e2e
EOF
}

if [ "$#" -ne 1 ]; then
  usage
  exit "$EXIT_USAGE"
fi

TIER="$1"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR" || exit "$EXIT_FAILURE"

export PYTHONUNBUFFERED="${PYTHONUNBUFFERED:-1}"
export PYTHONHASHSEED="${PYTHONHASHSEED:-0}"

HAS_POETRY=0
HAS_UV=0
if command -v poetry >/dev/null 2>&1; then
  HAS_POETRY=1
fi
if command -v uv >/dev/null 2>&1; then
  HAS_UV=1
  export UV_CACHE_DIR="${UV_CACHE_DIR:-${ROOT_DIR}/.uv-cache}"
  mkdir -p "${UV_CACHE_DIR}"
fi

run_cmd() {
  local cmd="$1"
  shift

  if command -v "${cmd}" >/dev/null 2>&1; then
    "${cmd}" "$@"
    return $?
  fi

  if [ "${HAS_POETRY}" -eq 1 ]; then
    poetry run "${cmd}" "$@"
    return $?
  fi

  if [ "${HAS_UV}" -eq 1 ]; then
    uv run "${cmd}" "$@"
    return $?
  fi

  return 127
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1 && [ "${HAS_POETRY}" -eq 0 ] && [ "${HAS_UV}" -eq 0 ]; then
    echo "ERROR: Required command '$cmd' is not available."
    exit "$EXIT_MISSING_DEP"
  fi
}

run_step() {
  local label="$1"
  shift
  echo "==> ${label}"
  run_cmd "$@"
  local rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "ERROR: ${label} failed (exit ${rc})"
    exit "$EXIT_FAILURE"
  fi
}

run_advisory_step() {
  local label="$1"
  shift
  echo "==> ${label} (advisory)"
  run_cmd "$@"
  local rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "WARN: ${label} failed (exit ${rc}); continuing because advisory checks are non-blocking."
  fi
}

unit_tests() {
  require_cmd pytest
  run_step "unit tests" \
    pytest tests -v --tb=short -m "not benchmark" \
    --ignore=tests/test_connectors_integration.py \
    --ignore=tests/test_private_repo_access.py
}

integration_tests() {
  require_cmd pytest

  if [ -n "${GH_TOKEN:-}" ] && [ -z "${GITHUB_TOKEN:-}" ]; then
    export GITHUB_TOKEN="${GH_TOKEN}"
  fi
  if [ -n "${GL_TOKEN:-}" ] && [ -z "${GITLAB_TOKEN:-}" ]; then
    export GITLAB_TOKEN="${GL_TOKEN}"
  fi

  if [ "${SKIP_INTEGRATION_TESTS:-0}" = "1" ]; then
    echo "Skipping integration tests because SKIP_INTEGRATION_TESTS=1."
    return 0
  fi

  if [ -z "${GITHUB_TOKEN:-}" ] && [ -z "${GITLAB_TOKEN:-}" ]; then
    if [ "${REQUIRE_INTEGRATION_TOKENS:-0}" = "1" ]; then
      echo "ERROR: Integration tokens are required but missing."
      echo "Set GITHUB_TOKEN and/or GITLAB_TOKEN, or unset REQUIRE_INTEGRATION_TOKENS."
      exit "$EXIT_MISSING_TOKENS"
    fi
    echo "Skipping integration tests because no integration tokens were provided."
    return 0
  fi

  run_step "integration tests" \
    pytest tests/test_connectors_integration.py tests/test_private_repo_access.py -q
}

e2e_tests() {
  require_cmd pytest

  local has_e2e=0
  if [ -d "tests/e2e" ]; then
    has_e2e=1
  fi
  if [ -n "$(find tests -type f -name '*e2e*.py' -print -quit 2>/dev/null)" ]; then
    has_e2e=1
  fi

  if [ "$has_e2e" -eq 0 ]; then
    echo "No e2e tests found. Skipping."
    return 0
  fi

  if [ -d "tests/e2e" ]; then
    run_step "e2e tests" pytest tests/e2e -v --tb=short
  else
    run_step "e2e tests" pytest tests -v --tb=short -k "e2e"
  fi
}

ci_tests() {
  require_cmd black
  require_cmd isort
  require_cmd flake8
  require_cmd mypy
  require_cmd pytest

  local coverage_threshold="${COVERAGE_THRESHOLD:-50}"
  local strict_quality_gates="${STRICT_QUALITY_GATES:-0}"

  if [ "${strict_quality_gates}" = "1" ]; then
    run_step "black (format check)" black --check .
    run_step "isort (import order check)" isort --check-only .
    run_step "mypy (type checking)" mypy --install-types --non-interactive .
  else
    run_advisory_step "black (format check)" black --check .
    run_advisory_step "isort (import order check)" isort --check-only .
    run_advisory_step "mypy (type checking)" mypy --install-types --non-interactive .
  fi

  run_step "flake8 (lint)" flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
  run_step "unit tests with coverage >= ${coverage_threshold}" \
    pytest tests -v --tb=short -m "not benchmark" \
    --ignore=tests/test_connectors_integration.py \
    --ignore=tests/test_private_repo_access.py \
    --cov=. --cov-report=xml --cov-report=term-missing --cov-fail-under="${coverage_threshold}"
  integration_tests
  e2e_tests
}

case "$TIER" in
  unit)
    unit_tests
    ;;
  integration)
    integration_tests
    ;;
  e2e)
    e2e_tests
    ;;
  ci)
    ci_tests
    ;;
  *)
    echo "ERROR: Unknown tier '$TIER'"
    usage
    exit "$EXIT_USAGE"
    ;;
esac

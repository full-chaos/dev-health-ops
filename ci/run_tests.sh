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
  live-e2e     Run live backend e2e harness against local services
  ci           Run blocking quality gates + coverage-gated unit tests + optional integration/e2e

Environment:
  PYTEST_SINGLE_RETRY=1  Retry a failing pytest tier once
  PYTEST_DURATIONS=25    Emit the slowest N test durations in output
  TEST_RESULTS_DIR=...   Base directory for junit outputs (default: ./test-results)
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

TEST_RESULTS_DIR="${TEST_RESULTS_DIR:-${ROOT_DIR}/test-results}"
JUNIT_RESULTS_DIR="${JUNIT_RESULTS_DIR:-${TEST_RESULTS_DIR}/junit}"
mkdir -p "${JUNIT_RESULTS_DIR}"

JUNIT_XML_UNIT="${JUNIT_XML_UNIT:-${JUNIT_RESULTS_DIR}/unit.xml}"
JUNIT_XML_INTEGRATION="${JUNIT_XML_INTEGRATION:-${JUNIT_RESULTS_DIR}/integration.xml}"
JUNIT_XML_E2E="${JUNIT_XML_E2E:-${JUNIT_RESULTS_DIR}/e2e.xml}"

PYTEST_SINGLE_RETRY="${PYTEST_SINGLE_RETRY:-0}"
PYTEST_DURATIONS="${PYTEST_DURATIONS:-25}"
PYTEST_DIAGNOSTIC_OPTS=(-ra "--durations=${PYTEST_DURATIONS}")

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

run_pytest_step() {
  local label="$1"
  local junit_xml="$2"
  shift 2

  local max_attempts=1
  if [ "${PYTEST_SINGLE_RETRY}" = "1" ]; then
    max_attempts=2
  fi

  local attempt=1
  local rc=0
  while [ "${attempt}" -le "${max_attempts}" ]; do
    echo "==> ${label} (attempt ${attempt}/${max_attempts})"
    run_cmd pytest "$@" "${PYTEST_DIAGNOSTIC_OPTS[@]}" --junitxml="${junit_xml}"
    rc=$?
    if [ "${rc}" -eq 0 ]; then
      echo "JUnit XML: ${junit_xml}"
      return 0
    fi

    if [ "${attempt}" -lt "${max_attempts}" ]; then
      echo "WARN: ${label} failed (exit ${rc}); retrying once because PYTEST_SINGLE_RETRY=1."
    fi
    attempt=$((attempt + 1))
  done

  echo "ERROR: ${label} failed (exit ${rc})"
  echo "JUnit XML (may be partial): ${junit_xml}"
  exit "$EXIT_FAILURE"
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

emit_junit_paths() {
  echo "JUnit XML paths:"
  echo "  unit: ${JUNIT_XML_UNIT}"
  echo "  integration: ${JUNIT_XML_INTEGRATION}"
  echo "  e2e: ${JUNIT_XML_E2E}"
}

unit_tests() {
  require_cmd pytest
  run_pytest_step "unit tests" "${JUNIT_XML_UNIT}" \
    tests -v --tb=short -m "not benchmark" \
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

  run_pytest_step "integration tests" "${JUNIT_XML_INTEGRATION}" \
    tests/test_connectors_integration.py tests/test_private_repo_access.py -q
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
    run_pytest_step "e2e tests" "${JUNIT_XML_E2E}" tests/e2e -v --tb=short
  else
    run_pytest_step "e2e tests" "${JUNIT_XML_E2E}" tests -v --tb=short -k "e2e"
  fi
}

live_e2e_tests() {
  run_step "live backend e2e" bash ./ci/run_live_backend_e2e.sh
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
  run_pytest_step "unit tests with coverage >= ${coverage_threshold}" "${JUNIT_XML_UNIT}" \
    tests -v --tb=short -m "not benchmark" \
    --ignore=tests/test_connectors_integration.py \
    --ignore=tests/test_private_repo_access.py \
    --cov=. --cov-report=xml --cov-report=term-missing --cov-fail-under="${coverage_threshold}"
  integration_tests
  e2e_tests
}

emit_junit_paths

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
  live-e2e)
    live_e2e_tests
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

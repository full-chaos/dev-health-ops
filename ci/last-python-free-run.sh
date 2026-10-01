#!/usr/bin/env bash
# last-python-free-run.sh -- PASSED, FAILED or NOT RUN: what did the newest go-python-free run say?
#
#   ci/last-python-free-run.sh [--repo OWNER/REPO] [--max-age-hours 8]
#
# The go-python-free workflow runs on a schedule, by hand and on a push that changes its own files, so a
# missing run is a real state and must not be read as green (CHAOS-7384). The newest COMPLETED run of
# the workflow on main decides:
#   PASSED   its conclusion is success and it is newer than --max-age-hours
#   FAILED   its conclusion is anything else (failure, cancelled, timed out)
#   NOT RUN  no completed run, or the newest is older than --max-age-hours
# Prints one line `go-python-free: STATE run=<id> sha=<sha> conclusion=<c> age-hours=<n>`.
# Exit: 0 PASSED, 1 FAILED, 3 NOT RUN, 2 usage or the API could not be read.
# LAST_PYTHON_FREE_RUN_GH overrides the `gh` command (the tooling test feeds fixtures);
# LAST_PYTHON_FREE_RUN_NOW overrides the clock (epoch seconds).
set -euo pipefail

repo="${GITHUB_REPOSITORY:-full-chaos/dev-health-ops}" max_age=8
while [ "$#" -gt 0 ]; do
  case "$1" in
    --repo) repo="${2:?--repo needs a value}"; shift 2 ;;
    --max-age-hours) max_age="${2:?--max-age-hours needs a value}"; shift 2 ;;
    *) printf 'usage: ci/last-python-free-run.sh [--repo OWNER/REPO] [--max-age-hours N]\n' >&2; exit 2 ;;
  esac
done
case "${max_age}" in ""|*[!0-9]*) printf 'last-python-free-run: --max-age-hours must be a number\n' >&2; exit 2 ;; esac
command -v jq >/dev/null 2>&1 || { printf 'last-python-free-run: jq is required\n' >&2; exit 2; }
GH="${LAST_PYTHON_FREE_RUN_GH:-gh}"
now="${LAST_PYTHON_FREE_RUN_NOW:-$(date -u +%s)}"

runs="$(${GH} api "repos/${repo}/actions/workflows/go-python-free.yml/runs?branch=main&status=completed&per_page=1")" \
  || { printf 'last-python-free-run: could not list the go-python-free runs\n' >&2; exit 2; }
read -r id sha conclusion created < <(printf '%s' "${runs}" | jq -r '(.workflow_runs[0] // {}) | "\(.id // "-") \(.head_sha // "-") \(.conclusion // "-") \(.created_at // "-")"')
if [ "${id}" = "-" ]; then
  printf 'go-python-free: NOT RUN run=- sha=- conclusion=- age-hours=-\n'
  exit 3
fi
created_epoch="$(date -u -d "${created}" +%s)"
age=$(( (now - created_epoch) / 3600 ))
if [ "${age}" -gt "${max_age}" ]; then
  printf 'go-python-free: NOT RUN run=%s sha=%s conclusion=%s age-hours=%s\n' "${id}" "${sha}" "${conclusion}" "${age}"
  exit 3
fi
if [ "${conclusion}" = "success" ]; then
  printf 'go-python-free: PASSED run=%s sha=%s conclusion=%s age-hours=%s\n' "${id}" "${sha}" "${conclusion}" "${age}"
  exit 0
fi
printf 'go-python-free: FAILED run=%s sha=%s conclusion=%s age-hours=%s\n' "${id}" "${sha}" "${conclusion}" "${age}"
exit 1

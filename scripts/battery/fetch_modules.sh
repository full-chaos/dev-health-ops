#!/usr/bin/env bash
# Warm the module cache for the tree under test, with retries.
#
# Usage: fetch_modules.sh <tree-root> [tries] [initial-backoff-seconds]
#
# WHY THIS IS ITS OWN STEP. A module fetch is the one part of an arm that can
# fail for reasons that have nothing to do with the mutation: the Go proxy
# resets a connection, a CDN 503s, DNS blips. Left inside `go build`, such a
# failure arrives as a non-zero build and is indistinguishable at a glance from
# "the mutant does not compile" -- and a battery arm was in fact reported as
# BUILD_FAILED, telling a reader to re-aim a mutant that compiled perfectly,
# when the proxy had reset mid-download of klauspost/compress.
#
# Pulling the fetch out front means a transient failure fails a RETRYABLE step
# before any mutation is applied, instead of corrupting a measurement. It is
# also strictly cheaper: on a warm runner (setup-go's cache keyed on the tree's
# own go.sum) it downloads nothing and returns in a second.
#
# This does NOT replace the classifier split in run_arm.sh -- a fetch can still
# fail later, mid-build, and run_arm.sh still has to tell the two causes apart.
# It reduces how often that path is reached; it does not make it unreachable.
set -uo pipefail

ROOT="${1:?usage: fetch_modules.sh <tree-root> [tries] [backoff-seconds]}"
TRIES="${2:-3}"
BACKOFF="${3:-5}"

cd "$ROOT" || { echo "fetch_modules.sh: cannot cd $ROOT" >&2; exit 2; }

attempt=1
while : ; do
  echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] go mod download: attempt $attempt/$TRIES in $ROOT"
  if go mod download all; then
    echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] ok modules: cache warm after $attempt attempt(s)"
    exit 0
  fi
  rc=$?
  if [ "$attempt" -ge "$TRIES" ]; then
    # FAIL LOUDLY AND EARLY. Better a red step named "could not fetch modules"
    # before any mutation than a battery whose arms each rediscover it and
    # report it as a per-arm build failure.
    echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] FAILED: go mod download rc=$rc after $TRIES attempts -- refusing to start a battery on a cold, unreachable module cache" >&2
    exit "$rc"
  fi
  echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] rc=$rc; sleeping ${BACKOFF}s before retry"
  sleep "$BACKOFF"
  BACKOFF=$((BACKOFF * 2))
  attempt=$((attempt + 1))
done

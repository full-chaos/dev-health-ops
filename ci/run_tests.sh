#!/usr/bin/env bash
set -u -o pipefail

EXIT_USAGE=2
EXIT_FAILURE=10

# The usage text, held as a shell string rather than a here-document.
#
# WHY NOT A HERE-DOCUMENT (CHAOS-3489, same class as CHAOS-3362 — do not
# "simplify" it back). Bash delivers a here-document by writing it into a pipe
# and only THEN forking the command that reads it, so the writing shell briefly
# holds both ends of that pipe itself. If the document does not fit in the pipe
# buffer, the write can never complete: nothing is reading, and because the
# writer owns the read end it cannot even get EPIPE. It blocks forever.
#
# Measured on this host with `cat >/dev/null <<EOF`, timeout 5:
#
#     400 bytes -> ok, 0.30s
#     512 bytes -> WEDGED
#    1024 bytes -> WEDGED
#    4000 bytes -> WEDGED
#
# while `lsof` reports those same pipes with the nominal 16384-byte capacity.
# Nominal is not actual: macOS hands out a small pipe buffer and defers
# expansion under kernel pipe-memory pressure, which a host running many
# concurrent agent sessions sits in persistently.
#
# Hosted CI runners have normal pipe buffers, so this never wedged in CI. It
# wedges on a developer machine — and because usage() fires only on `--help` or
# a bad argument, it wedged exactly when someone mistyped an invocation, which
# is a bad moment to hang silently with no output at all.
#
# `printf` is a BUILTIN: it writes this string straight to stdout in-process,
# with no pipe and no fork. A temp file would be pure ceremony here — the
# payload is going to stdout, not to an interpreter that needs a path. Note
# that `cat >file <<EOF` would NOT have fixed this: the pipe IS the
# here-document delivery mechanism, not something the reader introduces, which
# is why the repro above uses `cat`.
#
# CONSTRAINT: this is a single-quoted shell string, so the text must contain no
# single-quote character. the Go heredoc guard enforces
# that, and budgets every here-document under ci/ at the measured 400 bytes.
USAGE_TEXT='Usage: ci/run_tests.sh <tier>

Tiers:
  live-e2e     Run the live backend e2e harness against local services

The Go checks run through ci/check_go.sh.
'

usage() {
  printf '%s' "${USAGE_TEXT}"
}

if [ "$#" -ne 1 ]; then
  usage
  exit "$EXIT_USAGE"
fi

TIER="$1"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR" || exit "$EXIT_FAILURE"

run_step() {
  local label="$1"
  shift
  echo "==> ${label}"
  "$@"
  local rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "ERROR: ${label} failed (exit ${rc})"
    exit "$EXIT_FAILURE"
  fi
}

live_e2e_tests() {
  run_step "live backend e2e" bash ./ci/run_live_backend_e2e.sh
}

case "$TIER" in
  live-e2e)
    live_e2e_tests
    ;;
  *)
    echo "ERROR: Unknown tier '$TIER'"
    usage
    exit "$EXIT_USAGE"
    ;;
esac

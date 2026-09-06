#!/usr/bin/env bash
# Run ONE battery arm and emit its verdict as JSON.
#
# Usage: run_arm.sh <repo-root> <arm-id> <spec-json|-> <package-path-list> <floor> <out-json>
#
#   <arm-id>   a mutant id from the table, or one of the two harness arms:
#                _BASELINE  pristine tree, must be GREEN
#                _SENTINEL  an inert comment applied, must stay GREEN
#   <spec-json> the single-mutant JSON file; "-" for _BASELINE/_SENTINEL.
#
# THE THREE-STATE RULE, and it is the whole point of this file:
#
#   KILLED         the suite failed AND at least one line matches
#                  "--- FAIL: Test". A named failing test is what a kill IS.
#   SURVIVED       the suite passed over at least <floor> tests.
#   HARNESS_ERROR  everything else: the mutant did not apply, the file digest
#                  did not move, the tree does not build or vet, the run did not
#                  reach the floor, the suite timed out, or the suite failed
#                  with NO named failing test.
#
# HARNESS_ERROR IS SPLIT BY CAUSE, because the two causes call for OPPOSITE
# actions and a single label sent a reader the wrong way:
#
#   HARNESS_ERROR (INFRASTRUCTURE)  the toolchain could not fetch a module, the
#                  proxy reset the connection, a disk filled. The MUTANT IS
#                  FINE. Re-MEASURE it. Observed for real: a battery arm came
#                  back "BUILD_FAILED ... re-aim at a compiling form" when the
#                  module proxy had reset mid-download and the mutant compiled
#                  perfectly -- the label would have sent someone to rewrite a
#                  healthy mutant.
#   HARNESS_ERROR (BUILD_FAILED - mutant)  the mutated source does not compile.
#                  Re-AIM the mutant at a compiling form.
#
# The discriminator is the log, and it is deliberately conservative: a compile
# error has the shape `path/file.go:LINE:COL: message`, and anything carrying a
# module-fetch or network signature is treated as infrastructure. When both
# appear, INFRASTRUCTURE wins -- a fetch failure can CAUSE a spurious compile
# error (a half-downloaded module), so calling that one a mutant defect is the
# dangerous direction.
#
# rc != 0 ALONE IS NOT A KILL. A build error, a cache eviction, a disk-full or a
# timeout all exit non-zero, and counting those as kills turns infrastructure
# noise into a mutation score -- silently, and always in the flattering
# direction. That is why the named-FAIL grep exists and why it reads a FILE.
#
# EVERY CLASSIFICATION READS A FILE, never a shell variable. A variable can be
# truncated by a subshell limit or mangled by a nested quote, and the failure
# mode is "no FAIL line found", which reads as SURVIVED.
#
# BUILD AND VET BOTH GATE THE TEST. `go vet` type-checks _test.go and `go build`
# does not, so without vet a mutant that breaks a test file's imports reads as
# SURVIVED rather than as the harness error it is.
#
# This file is the ONE AUTHORITY for arm classification: the bigboy harness and
# the hosted matrix workflow both call it, so the two venues cannot drift into
# disagreeing about what a battery measured.
set -uo pipefail

# RESOLVED BEFORE ANY cd. This script cds into the tree under test, which is a
# DIFFERENT checkout from the one this file lives in -- the harness is at the
# workflow's ref, the tree under test is at the tip being measured, and a tip
# older than the harness has no scripts/battery/ at all. A relative $0 or a
# relative output path silently resolves inside the tree under test after the
# cd: the sibling script is not found, and the verdict JSON is written where
# nothing collects it.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
abspath() { case "$1" in /*) printf '%s' "$1" ;; *) printf '%s/%s' "$PWD" "$1" ;; esac; }

ROOT="${1:?usage: run_arm.sh <repo-root> <arm-id> <spec-json|-> <packages> <floor> <out-json>}"
ARM_ID="${2:?arm id}"
SPEC="${3:?spec json or -}"
PKGS="${4:?package list}"
FLOOR="${5:?floor}"
OUT="$(abspath "${6:?out json}")"
[ "$SPEC" = "-" ] || SPEC="$(abspath "$SPEC")"

# The go test timeout is EXPLICIT and applied ONLY to `go test`. It is never
# routed through $PKGS (which is also the floor and vet input, where a stray
# flag corrupts the count and fails vet on every arm) and never through GOFLAGS
# (which the caller may already be using for -p). Default 20m because a
# whole-package suite on this repo has been measured at 559-600 s against go's 600 s
# default, and a suite that trips the default reports `panic: test timed out`
# with no named FAIL -- which this script must classify as HARNESS_ERROR, but
# which is better avoided than classified.
GO_TEST_TIMEOUT="${MB_GO_TEST_TIMEOUT:-20m}"

case "$ARM_ID" in
  */*|*' '*) echo "run_arm.sh: refusing an arm id with a path separator or space: [$ARM_ID]" >&2; exit 2 ;;
esac

LOG="$(abspath "${MB_ARM_LOG:-$(dirname "$OUT")/arm-$ARM_ID.log}")"
mkdir -p "$(dirname "$OUT")" "$(dirname "$LOG")"
: > "$LOG"

say() { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*" | tee -a "$LOG"; }

# Does this log carry a module-fetch / network failure? Read from the FILE.
INFRA_RE='connection reset by peer|i/o timeout|TLS handshake timeout|dial tcp|no such host|unexpected EOF|EOF$|proxy\.golang\.org|storage\.googleapis\.com|module lookup disabled|Get "https?://|500 Internal Server Error|502 Bad Gateway|503 Service Unavailable|504 Gateway|no space left on device|input/output error'
infra_hit() { grep -nE "$INFRA_RE" "$1" | head -1; }
# A real compile error: path/file.go:LINE:COL: message
compile_hit() { grep -nE '^[^[:space:]]+\.go:[0-9]+:[0-9]+: ' "$1" | head -1; }

# Classify a non-zero build/vet and emit. $1 = what failed, $2 = its rc.
emit_build_failure() {
  local what="$1" rc="$2" ih ch
  ih="$(infra_hit "$LOG")"
  ch="$(compile_hit "$LOG")"
  if [ -n "$ih" ]; then
    # INFRASTRUCTURE WINS OVER A COMPILE LINE. A partial fetch can produce a
    # spurious compile error, and mislabelling that as a mutant defect sends a
    # reader to rewrite code that is fine.
    emit HARNESS_ERROR "INFRASTRUCTURE ($what rc=$rc) -- the toolchain could not fetch or reach something; the MUTANT IS FINE, RE-MEASURE this arm. First signature: ${ih%%$'\n'*}"
  elif [ -n "$ch" ]; then
    emit HARNESS_ERROR "BUILD_FAILED - mutant ($what rc=$rc) -- the mutated source does not compile; RE-AIM the mutant at a compiling form. First compiler line: ${ch%%$'\n'*}"
  else
    emit HARNESS_ERROR "BUILD_FAILED - unclassified ($what rc=$rc) -- non-zero with neither a compiler line nor a network signature; read $LOG before believing either cause"
  fi
}

# state, detail, ran, named -> the JSON verdict. Written on EVERY path, so a
# missing artifact means the job died, not that an arm was skipped quietly.
emit() {
  python3 - "$OUT" "$ARM_ID" "$1" "$2" "${3:-0}" "${4:-0}" "$PKGS" "$FLOOR" "$GO_TEST_TIMEOUT" <<'PYEOF'
import json, sys
out, arm, state, detail, ran, named, pkgs, floor, timeout = sys.argv[1:10]
json.dump({
    "id": arm, "state": state, "detail": detail,
    "ran": int(ran), "named_failures": int(named),
    "packages": pkgs, "floor": int(floor), "go_test_timeout": timeout,
}, open(out, "w"), sort_keys=True, indent=2)
PYEOF
  say "$ARM_ID: $1 -- $2"
}

cd "$ROOT" || { emit HARNESS_ERROR "cannot cd $ROOT"; exit 0; }

say "arm=$ARM_ID packages=[$PKGS] floor=$FLOOR go-test-timeout=$GO_TEST_TIMEOUT"

# ---------------------------------------------------------------- the mutation
case "$ARM_ID" in
  _BASELINE)
    say "BASELINE: pristine tree, no mutation. It must be GREEN or every other arm's KILLED is void."
    ;;
  _SENTINEL)
    # An INERT edit. If this arm goes red the harness false-kills and every
    # KILLED verdict in the run is suspect; if it cannot build it proves
    # nothing, so the build is checked apart from the suite below.
    SENT_FILE="${MB_SENTINEL_FILE:?_SENTINEL needs MB_SENTINEL_FILE}"
    [ -f "$SENT_FILE" ] || { emit HARNESS_ERROR "sentinel file $SENT_FILE does not exist"; exit 0; }
    before=$(sha256sum "$SENT_FILE" | cut -d' ' -f1)
    printf '\n// mutation-battery sentinel: an inert comment. If this arm goes red the\n// harness false-kills and every KILLED verdict in this run is void.\n' >> "$SENT_FILE"
    after=$(sha256sum "$SENT_FILE" | cut -d' ' -f1)
    [ "$before" != "$after" ] || { emit HARNESS_ERROR "sentinel digest did not move -- the apply path is broken"; exit 0; }
    say "sentinel digest moved ${before:0:12} -> ${after:0:12} on $SENT_FILE"
    ;;
  *)
    [ -f "$SPEC" ] || { emit HARNESS_ERROR "no spec file at $SPEC"; exit 0; }
    applied=$(python3 "$SCRIPT_DIR/apply_mutant.py" --root "$ROOT" --spec "$SPEC" 2>>"$LOG")
    if [ "$applied" != "APPLIED" ]; then
      emit HARNESS_ERROR "apply=$applied -- an unapplied mutant is unproven, never a pass"
      exit 0
    fi
    ;;
esac

# ------------------------------------------------------------- build then vet
# Build is whole-repo: a mutant that breaks a package outside the tested set is
# still a harness error, and no scoping is allowed to hide that.
brc=0; go build ./... >> "$LOG" 2>&1 || brc=$?
if [ "$brc" -ne 0 ]; then
  emit_build_failure "go build" "$brc"
  exit 0
fi
vrc=0; go vet $PKGS >> "$LOG" 2>&1 || vrc=$?
if [ "$vrc" -ne 0 ]; then
  emit_build_failure "go vet" "$vrc"
  exit 0
fi
if [ "$ARM_ID" = "_SENTINEL" ]; then
  say "ok sentinel build+vet: rc=0 (checked apart from the suite)"
fi

# --------------------------------------------------------------------- the run
# WHOLE PACKAGES. No -run selector is derived anywhere: `go test -run` matching
# nothing exits 0 and reads as a survivor, so the floor replaces it.
trc=0
go test -count=1 -timeout="$GO_TEST_TIMEOUT" -v $PKGS >> "$LOG" 2>&1 || trc=$?

ran=$(grep -c '^=== RUN' "$LOG")
named=$(grep -cE '^[[:space:]]*--- FAIL: Test' "$LOG")
timedout=$(grep -cE 'panic: test timed out|test timed out after' "$LOG")
case "$ran" in ''|*[!0-9]*) ran=0 ;; esac
case "$named" in ''|*[!0-9]*) named=0 ;; esac
case "$timedout" in ''|*[!0-9]*) timedout=0 ;; esac
say "rc=$trc  === RUN $ran (floor $FLOOR)  named-FAIL $named  timed-out-lines $timedout"

# A TIMEOUT IS A HARNESS ERROR EVEN IF SOMETHING ELSE FAILED BY NAME. A suite
# that was cut off did not finish, so the arms it did not reach are unmeasured
# and "the ones that ran happened to fail" is not the same claim as a kill.
if [ "$timedout" -ne 0 ]; then
  emit HARNESS_ERROR "TIMED OUT (go test -timeout=$GO_TEST_TIMEOUT tripped; $named named FAIL line(s) present but the suite did not finish)" "$ran" "$named"
  exit 0
fi

if [ "$ran" -lt "$FLOOR" ]; then
  emit HARNESS_ERROR "=== RUN $ran below the floor $FLOOR -- the run did not cover the package list" "$ran" "$named"
  exit 0
fi

case "$ARM_ID" in
  _BASELINE|_SENTINEL)
    if [ "$trc" -ne 0 ]; then
      emit HARNESS_ERROR "$ARM_ID IS RED (rc=$trc, $named named failure(s)) -- every mutant verdict in this run would be void" "$ran" "$named"
    else
      emit PASS "green over $ran tests (floor $FLOOR)" "$ran" "$named"
    fi
    exit 0
    ;;
esac

if [ "$trc" -ne 0 ]; then
  if [ "$named" -eq 0 ]; then
    # rc != 0 with no named failing test gets the SAME split as a build
    # failure: a proxy reset or a cache eviction mid-suite is infrastructure to
    # re-measure, not a mutant to re-aim.
    ih="$(infra_hit "$LOG")"
    if [ -n "$ih" ]; then
      emit HARNESS_ERROR "INFRASTRUCTURE (go test rc=$trc over $ran tests, NO '--- FAIL: Test' line) -- the MUTANT IS FINE, RE-MEASURE this arm. First signature: ${ih%%$'\n'*}" "$ran" "$named"
    else
      hint=$(grep -E 'build failed|cannot find|permission denied|signal: killed' "$LOG" | head -2 | tr '\n' ';')
      emit HARNESS_ERROR "BUILD_FAILED - unclassified (go test rc=$trc over $ran tests, NO '--- FAIL: Test' line) $hint" "$ran" "$named"
    fi
  else
    first=$(grep -E '^[[:space:]]*--- FAIL: Test' "$LOG" | head -3 | tr '\n' ';')
    emit KILLED "rc=$trc, $ran ran, $named named failure(s): $first" "$ran" "$named"
  fi
else
  emit SURVIVED "rc=0 over $ran tests -- a surviving mutant is a FINDING, not a pass" "$ran" "$named"
fi
exit 0

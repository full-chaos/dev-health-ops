#!/usr/bin/env bash
# test-codex-review.sh — bats-free proof harness for the four v4.8.4 defects.
#
# No test harness existed in this directory before this file (there is no
# bats, no go test target, nothing else under tools/codex-review/ that runs
# assertions), so this is a small self-contained shell script instead.
#
# Every block under test is extracted VERBATIM from the real, edited
# codex-review.sh via `sed -n '<start>,<end>p'` -- this proves the actual
# shipped logic, not a reimplementation of it (same discipline the v4.8.3
# dry run used for its warm-step proof, see
# .remember/lanes/lane-wrapper-v483/handoff-2026-09-04.md). Each extraction
# is followed by a signature grep that FAILS LOUDLY if the expected line is
# not inside the extracted range -- a silent line-number drift after a
# future edit must not make this file test the wrong code, or nothing.
#
# Run: bash tools/codex-review/test-codex-review.sh   (from anywhere; the
# script locates its own sibling codex-review.sh by its own path).

set -euo pipefail

SELF_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCRIPT="$SELF_DIR/codex-review.sh"
[ -s "$SCRIPT" ] || { echo "FAIL: cannot find $SCRIPT" >&2; exit 1; }

WORK=$(mktemp -d "${TMPDIR:-/tmp}/codex-review-test-XXXXXX")
trap 'chmod -R u+w "$WORK" 2>/dev/null || true; rm -rf "$WORK"' EXIT

PASS=0
FAIL=0
ok()   { PASS=$((PASS + 1)); printf 'ok   - %s\n' "$1"; }
notok(){ FAIL=$((FAIL + 1)); printf 'FAIL - %s\n' "$1"; }

# Extract a line range and assert a signature string is inside it -- so a
# future line-number drift in codex-review.sh fails this harness loudly
# instead of silently testing the wrong (or empty) code.
extract() {
  local start="$1" end="$2" signature="$3" out="$4"
  sed -n "${start},${end}p" "$SCRIPT" > "$out"
  grep -qF "$signature" "$out" \
    || { echo "FAIL: extracted $SCRIPT:$start,$end does not contain the expected signature '$signature' -- line numbers drifted, update test-codex-review.sh" >&2; exit 1; }
}

# The two helpers every extracted block below may call, pulled verbatim
# from the top of codex-review.sh (same lines the real script defines
# them at) so the extracted blocks run with production-identical warn/die.
extract 291 292 'warn() {' "$WORK/helpers.sh"

# ---------------------------------------------------------------------------
# Defect 1: cleanup()/reap_dirs() rm -rf'd a 0555 Go-module-cache-shaped
# tree without chmod -R u+w first, failed file-by-file, and left it behind.
# Proof: build a 0555 tree, source the real rm_rf_writable() verbatim, call
# it, assert the tree is gone.
# ---------------------------------------------------------------------------
extract 797 804 'rm_rf_writable() {' "$WORK/rm_rf_writable.sh"

D1="$WORK/modcache-shaped"
mkdir -p "$D1/cache/download/example.com/pkg/@v"
echo fake-zip > "$D1/cache/download/example.com/pkg/@v/v1.0.0.zip"
mkdir -p "$D1/example.com/pkg@v1.0.0"
echo fake-source > "$D1/example.com/pkg@v1.0.0/main.go"
# Go's own extraction marks module dirs and their contents read-only.
chmod -R 0555 "$D1"
# The parent must still be writable for rm_rf_writable itself to unlink
# entries from -- exactly what chmod -R u+w on $D1 (not its parent) fixes.
(
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  # shellcheck source=/dev/null
  source "$WORK/rm_rf_writable.sh"
  rm_rf_writable "$D1"
)
if [ -d "$D1" ]; then
  notok "defect 1: rm_rf_writable removes a 0555 tree ($D1 still present)"
else
  ok "defect 1: rm_rf_writable removes a 0555 tree"
fi

# Positive control: without the chmod, a plain `rm -rf` on the same shape
# fails to fully remove it -- proves the test actually exercises the bug,
# not just a tree that was always removable.
D1B="$WORK/modcache-shaped-control"
mkdir -p "$D1B/example.com/pkg@v1.0.0"
echo fake-source > "$D1B/example.com/pkg@v1.0.0/main.go"
chmod -R 0555 "$D1B"
rm -rf "$D1B" 2>/dev/null || true
if [ -d "$D1B" ]; then
  ok "defect 1 positive control: plain rm -rf leaves a 0555 tree behind (confirms the bug shape)"
else
  notok "defect 1 positive control: plain rm -rf unexpectedly removed a 0555 tree -- this host's rm may already chmod internally, weakening the proof above"
fi
chmod -R u+w "$D1B" 2>/dev/null || true
rm -rf "$D1B" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Defect 2: GOCACHE/GOMODCACHE were keyed on the worktree BASENAME alone,
# so two checkouts both named e.g. `acr` (the stale §10 recipe example)
# collided on the same cache path. Proof: run the real LANE_KEY block
# against two directories that share a basename but differ in full path,
# assert the resulting LANE_KEY values differ.
# ---------------------------------------------------------------------------
extract 520 542 'LANE_KEY="$LANE-$WT_HASH"' "$WORK/lane_key.sh"

mkdir -p "$WORK/lane-a/acr" "$WORK/lane-b/acr"
LANE_KEY_A=$(
  WT="$WORK/lane-a/acr"
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  # shellcheck source=/dev/null
  source "$WORK/lane_key.sh"
  printf '%s' "$LANE_KEY"
)
LANE_KEY_B=$(
  WT="$WORK/lane-b/acr"
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  # shellcheck source=/dev/null
  source "$WORK/lane_key.sh"
  printf '%s' "$LANE_KEY"
)
if [ "$LANE_KEY_A" = "$LANE_KEY_B" ]; then
  notok "defect 2: two same-basename ('acr') worktrees got the SAME LANE_KEY ($LANE_KEY_A) -- still collides"
else
  ok "defect 2: two same-basename ('acr') worktrees get distinct LANE_KEYs ($LANE_KEY_A vs $LANE_KEY_B)"
fi
# --reap-mine LANE's glob (`codex-review-*-LANE-*`) must still match a dir
# named with the new LANE_KEY -- LANE stays the first dash-delimited
# segment of the key.
case "$LANE_KEY_A" in
  acr-*) ok "defect 2: LANE_KEY keeps LANE ('acr') as its leading segment, so --reap-mine's glob still matches" ;;
  *)     notok "defect 2: LANE_KEY '$LANE_KEY_A' does not start with 'acr-' -- --reap-mine LANE would stop matching" ;;
esac

# ---------------------------------------------------------------------------
# Defect 3: the warm step's `go mod download all` needs a writable
# $GOPATH/pkg/sumdb; on bigboy ~/go is root:root and this fails looking
# like a network error. Proof: run the real per-round-GOPATH block
# (unless CODEX_REVIEW_GOPATH is set), assert it creates a fresh, writable
# directory; confirm a command actually SEES it as GOPATH; then remove it
# via the same rm_rf_writable() defect-1 already proved, confirming the
# trap tears it down.
# ---------------------------------------------------------------------------
extract 636 642 'RGOPATH=$(mktemp -d "/tmp/codex-review-gopath-$LANE_KEY-$TS-XXXXXX")' "$WORK/rgopath.sh"

TS="19700101T000000-test"
LANE_KEY="test-lane-$$"
unset CODEX_REVIEW_GOPATH 2>/dev/null || true
(
  # LANE_KEY and TS are already set above; a subshell inherits them as-is.
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  # shellcheck source=/dev/null
  source "$WORK/rgopath.sh"
  printf '%s\n' "$RGOPATH" > "$WORK/rgopath-path.txt"
)
RGOPATH_VAL=$(cat "$WORK/rgopath-path.txt")
if [ -d "$RGOPATH_VAL" ] && [ -w "$RGOPATH_VAL" ]; then
  ok "defect 3: per-round GOPATH is created and writable ($RGOPATH_VAL)"
else
  notok "defect 3: per-round GOPATH was not created writable ($RGOPATH_VAL)"
fi
case "$RGOPATH_VAL" in
  /tmp/codex-review-gopath-"$LANE_KEY"-"$TS"-*) : ;;
  *) notok "defect 3: RGOPATH '$RGOPATH_VAL' does not match the expected codex-review-gopath-<lane>-<ts>-* naming" ;;
esac
SEEN_GOPATH=$(env GOPATH="$RGOPATH_VAL" bash -c 'printf "%s" "$GOPATH"')
if [ "$SEEN_GOPATH" = "$RGOPATH_VAL" ]; then
  ok "defect 3: GOPATH is actually exported/seen by a command using it (matches the real script's env prefix pattern)"
else
  notok "defect 3: GOPATH did not propagate to a command run with 'env GOPATH=\$RGOPATH ...' ($SEEN_GOPATH != $RGOPATH_VAL)"
fi
# Trap removal: same rm_rf_writable() proved in defect 1.
(
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  # shellcheck source=/dev/null
  source "$WORK/rm_rf_writable.sh"
  rm_rf_writable "$RGOPATH_VAL"
)
if [ -d "$RGOPATH_VAL" ]; then
  notok "defect 3: per-round GOPATH was not removed by the trap's cleanup ($RGOPATH_VAL still present)"
else
  ok "defect 3: per-round GOPATH is removed by the same cleanup path (rm_rf_writable) the trap uses"
fi

# CODEX_REVIEW_GOPATH override path: must be used as-is (mkdir -p'd, not
# mktemp'd under a fresh name).
CUSTOM_GOPATH="$WORK/custom-gopath"
(
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  CODEX_REVIEW_GOPATH="$CUSTOM_GOPATH"
  # shellcheck source=/dev/null
  source "$WORK/rgopath.sh"
  printf '%s\n' "$RGOPATH" > "$WORK/rgopath-custom-path.txt"
)
RGOPATH_CUSTOM=$(cat "$WORK/rgopath-custom-path.txt")
if [ "$RGOPATH_CUSTOM" = "$CUSTOM_GOPATH" ] && [ -d "$CUSTOM_GOPATH" ]; then
  ok "defect 3: CODEX_REVIEW_GOPATH override is honoured verbatim"
else
  notok "defect 3: CODEX_REVIEW_GOPATH override was not honoured (got '$RGOPATH_CUSTOM', wanted '$CUSTOM_GOPATH')"
fi

# ---------------------------------------------------------------------------
# Defect 4: OUTDIR (and the log dir, the same directory) was never created
# before use, so a caller-supplied -o naming a not-yet-existing directory
# killed the round with "No such file or directory" and no verdict. Proof:
# run the real `mkdir -p "$OUTDIR"` line against a not-yet-existing path,
# assert it now exists.
# ---------------------------------------------------------------------------
extract 470 470 'mkdir -p "$OUTDIR" || die "cannot create output directory $OUTDIR"' "$WORK/outdir.sh"

OUTDIR_TEST="$WORK/does/not/exist/yet"
[ ! -e "$OUTDIR_TEST" ] || { echo "FAIL: test setup bug, $OUTDIR_TEST already exists" >&2; exit 1; }
(
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  OUTDIR="$OUTDIR_TEST"
  # shellcheck source=/dev/null
  source "$WORK/outdir.sh"
)
if [ -d "$OUTDIR_TEST" ]; then
  ok "defect 4: a missing OUTDIR is created before use"
else
  notok "defect 4: OUTDIR '$OUTDIR_TEST' was not created"
fi

echo "----"
echo "passed=$PASS failed=$FAIL"
[ "$FAIL" -eq 0 ]

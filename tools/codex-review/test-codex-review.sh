#!/usr/bin/env bash
# test-codex-review.sh — bats-free proof harness for the four v4.8.4 defects
# plus the v4.8.5 silent-death fix (warm step killed the whole script under
# set -euo pipefail in a repo with no go.mod; see that changelog block).
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
extract 329 330 'warn() {' "$WORK/helpers.sh"

# ---------------------------------------------------------------------------
# Defect 1: cleanup()/reap_dirs() rm -rf'd a 0555 Go-module-cache-shaped
# tree without chmod -R u+w first, failed file-by-file, and left it behind.
# Proof: build a 0555 tree, source the real rm_rf_writable() verbatim, call
# it, assert the tree is gone.
# ---------------------------------------------------------------------------
extract 844 851 'rm_rf_writable() {' "$WORK/rm_rf_writable.sh"

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
extract 567 589 'LANE_KEY="$LANE-$WT_HASH"' "$WORK/lane_key.sh"

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
extract 683 689 'RGOPATH=$(mktemp -d "/tmp/codex-review-gopath-$LANE_KEY-$TS-XXXXXX")' "$WORK/rgopath.sh"

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
extract 508 508 'mkdir -p "$OUTDIR" || die "cannot create output directory $OUTDIR"' "$WORK/outdir.sh"

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

# ---------------------------------------------------------------------------
# v4.8.5 item 3: the round .log ($L) is created immediately once its path is
# resolved, before the warm step or anything else that could die -- so a
# death anywhere downstream can never leave "no .log at all" behind. Proof:
# run the real TS/V/L/touch block verbatim against a fresh OUTDIR, assert
# $L exists (and is empty) right after, well before any warm-step logic.
# ---------------------------------------------------------------------------
extract 521 533 ': >"$L" || die "cannot create round log $L"' "$WORK/create-log.sh"

OUTDIR_LOG_TEST="$WORK/log-test-outdir"
mkdir -p "$OUTDIR_LOG_TEST"
NAME_LOG_TEST="round-name"
(
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  OUTDIR="$OUTDIR_LOG_TEST" NAME="$NAME_LOG_TEST" V="" L=""
  # shellcheck source=/dev/null
  source "$WORK/create-log.sh"
  printf '%s\n' "$L" > "$WORK/created-log-path.txt"
)
CREATED_LOG=$(cat "$WORK/created-log-path.txt")
if [ -e "$CREATED_LOG" ]; then
  ok "v4.8.5 item 3: the round .log exists immediately once its path is resolved ($CREATED_LOG)"
else
  notok "v4.8.5 item 3: the round .log '$CREATED_LOG' was not created"
fi

# ---------------------------------------------------------------------------
# v4.8.5 defect a: a count pipeline against a not-yet-existing cache/download
# dir killed the whole script under `set -euo pipefail` (a no-go.mod repo's
# `go mod download all` never gets far enough to create that path). Proof:
# run the real WARM_MODULES line verbatim against a nonexistent RGOMODCACHE,
# under set -euo pipefail, and assert the NEXT line still runs.
# ---------------------------------------------------------------------------
extract 963 963 'WARM_MODULES=$(find "$RGOMODCACHE/cache/download" -name' "$WORK/warm_modules.sh"

# NOTE: each probe below is run as `set +e; ( set -euo pipefail; ... ); RC=$?;
# set -e` rather than `( ... ) || true`. Bash disables -e propagation for
# EVERYTHING inside a compound command that is itself the left side of `||`
# -- even a `set -euo pipefail` restated as the subshell's own first line --
# so `(...) || true` would make BOTH the fixed and unfixed line look like
# they "survive", proving nothing either way. Measured directly on this host
# (bash 5.3.15) before writing this the long way.
REACHED_FILE="$WORK/warm-modules-reached.txt"
rm -f "$REACHED_FILE"
set +e
(
  set -euo pipefail
  RGOMODCACHE="$WORK/does-not-exist-zzz"
  # shellcheck source=/dev/null
  source "$WORK/warm_modules.sh"
  printf 'reached with WARM_MODULES=%s\n' "$WARM_MODULES" > "$REACHED_FILE"
)
RC_A=$?
set -e
if [ "$RC_A" -eq 0 ] && [ -s "$REACHED_FILE" ]; then
  ok "v4.8.5 defect a: WARM_MODULES count against a nonexistent cache dir does not kill the script under set -euo pipefail ($(cat "$REACHED_FILE"))"
else
  notok "v4.8.5 defect a: the line after WARM_MODULES was never reached (rc=$RC_A) — the pipeline still kills the script"
fi

# Negative control: the SAME line with the trailing `|| true` stripped DOES
# kill the script the same way, under the same options — proves the test
# above is exercising the actual bug shape, not a harness quirk.
sed 's/ || true$//' "$WORK/warm_modules.sh" > "$WORK/warm_modules_unfixed.sh"
REACHED_FILE_NEG="$WORK/warm-modules-reached-negative.txt"
rm -f "$REACHED_FILE_NEG"
set +e
(
  set -euo pipefail
  RGOMODCACHE="$WORK/does-not-exist-zzz"
  # shellcheck source=/dev/null
  source "$WORK/warm_modules_unfixed.sh"
  printf 'reached\n' > "$REACHED_FILE_NEG"
)
RC_A_NEG=$?
set -e
if [ "$RC_A_NEG" -eq 0 ] && [ -s "$REACHED_FILE_NEG" ]; then
  notok "v4.8.5 defect a negative control: the pre-fix line (no || true) unexpectedly survived — this host's find/pipefail may not reproduce the bug shape"
else
  ok "v4.8.5 defect a negative control: the pre-fix line (no || true) DOES kill the script (confirms the bug shape)"
fi

# ---------------------------------------------------------------------------
# v4.8.5 defect b/c: the warm step now runs only when $RW/go.mod exists at
# the reviewed tip; it is skipped (logging warm-step: SKIPPED
# reason=no-go.mod, and always leaving a non-empty $L) for a repo with none.
# Proof: run the real if/else block verbatim against a throwaway RW with no
# go.mod (b), then again with a go.mod present and a stubbed `go` on PATH
# that only proves the WARM branch actually runs (c) — not a full real Go
# build, which this harness has no repo fixture for.
# ---------------------------------------------------------------------------
extract 923 1007 'if [ -f "$RW/go.mod" ]; then' "$WORK/warm_step.sh"
grep -qF 'reason=no-go.mod' "$WORK/warm_step.sh" \
  || { echo "FAIL: extracted warm_step.sh block does not contain the SKIPPED branch" >&2; exit 1; }

# (b) no go.mod: SKIPPED, and $L is non-empty.
RW_NOGOMOD="$WORK/rw-no-gomod"
mkdir -p "$RW_NOGOMOD"
L_NOGOMOD="$WORK/round-no-gomod.log"
: > "$L_NOGOMOD"
(
  set -euo pipefail
  RW="$RW_NOGOMOD" L="$L_NOGOMOD" TIP="test-tip-no-gomod"
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  # shellcheck source=/dev/null
  source "$WORK/warm_step.sh"
)
if grep -qF 'warm-step: SKIPPED reason=no-go.mod' "$L_NOGOMOD"; then
  ok "v4.8.5 defect b: a repo with no go.mod logs warm-step: SKIPPED reason=no-go.mod"
else
  notok "v4.8.5 defect b: no-go.mod round did not log the SKIPPED line ($L_NOGOMOD: $(cat "$L_NOGOMOD" 2>/dev/null))"
fi
if [ -s "$L_NOGOMOD" ]; then
  ok "v4.8.5 defect b: the round .log is non-empty for a no-go.mod repo (the wrapper never silently dies with no .log)"
else
  notok "v4.8.5 defect b: the round .log is EMPTY for a no-go.mod repo — the silent-death shape is back"
fi

# (c) go.mod present: the WARM branch runs, not the skip branch. Stub `go`
# on PATH so this proves the BRANCH TAKEN, not a real Go toolchain result —
# this harness has no Go module fixture to build.
RW_GOMOD="$WORK/rw-gomod"
mkdir -p "$RW_GOMOD"
: > "$RW_GOMOD/go.mod"
STUBBIN="$WORK/stubbin"
mkdir -p "$STUBBIN"
cat > "$STUBBIN/go" <<'STUB_GO'
#!/usr/bin/env bash
# Minimal stub: every subcommand this warm step invokes succeeds instantly.
exit 0
STUB_GO
chmod +x "$STUBBIN/go"
RGOTMPDIR_C="$WORK/gotmp-c"
RGOMODCACHE_C="$WORK/modcache-c"
RGOCACHE_C="$WORK/gocache-c"
RGOPATH_C="$WORK/gopath-c"
mkdir -p "$RGOTMPDIR_C" "$RGOMODCACHE_C" "$RGOCACHE_C" "$RGOPATH_C"
L_GOMOD="$WORK/round-gomod.log"
: > "$L_GOMOD"
(
  set -euo pipefail
  PATH="$STUBBIN:$PATH"
  RW="$RW_GOMOD" L="$L_GOMOD" TIP="test-tip-gomod"
  RGOTMPDIR="$RGOTMPDIR_C" RGOMODCACHE="$RGOMODCACHE_C" RGOCACHE="$RGOCACHE_C" RGOPATH="$RGOPATH_C"
  RTMPDIR="$WORK" RGOFLAGS="-p=2" RGOMAXPROCS=4
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  # shellcheck source=/dev/null
  source "$WORK/warm_step.sh"
)
if grep -qF 'warm-step: OK' "$L_GOMOD" && ! grep -qF 'SKIPPED' "$L_GOMOD"; then
  ok "v4.8.5 defect c: a repo WITH go.mod still runs the full warm step (warm-step: OK, no SKIPPED line)"
else
  notok "v4.8.5 defect c: a repo with go.mod did not take the warm branch ($L_GOMOD: $(cat "$L_GOMOD" 2>/dev/null))"
fi

echo "----"
echo "passed=$PASS failed=$FAIL"
[ "$FAIL" -eq 0 ]

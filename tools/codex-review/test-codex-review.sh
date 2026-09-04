#!/usr/bin/env bash
# test-codex-review.sh — bats-free proof harness for the four v4.8.4 defects,
# the v4.8.5 silent-death fix (warm step killed the whole script under
# set -euo pipefail in a repo with no go.mod; see that changelog block), and
# the v4.8.6 Linux-shared-cache change (bigboy GOCACHE/GOMODCACHE/GOPATH move
# to the fleet-shared /var/lib/oci-cache volume, never a per-round dir, never
# reaped; macOS unchanged; plus the two codex-round-found defects layered on
# top: a malformed uname -s value used to silently pick a cache-removal
# branch, and command -p uname closing off a PATH-shadowed uname; see the
# file's own v4.8.6 changelog block).
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
# v4.8.6 SAFETY NOTE (defaults): the Linux GOCACHE/GOMODCACHE defaults are
# now literal, host-absolute paths (/var/lib/oci-cache/go-build,
# /var/lib/oci-cache/go-mod) -- NOT $HOME-relative -- so a test that actually
# SOURCES the default-taking branch and lets its `mkdir -p` run would try to
# create real directories under /var/lib on whatever machine runs this
# harness. The "default" assertions below therefore use a value-only
# extraction (the if/elif/else WITHOUT the two mkdir -p lines that follow
# it) and check the resolved STRING only -- no directory is ever created for
# the literal-default case by this harness.
#
# v4.8.6 SAFETY/DESIGN NOTE (HOST_OS): the shipped HOST_OS resolution line
# is `HOST_OS="$(command -p uname -s)"` (fixed by this version specifically
# so that PATH-based stubbing CANNOT override it -- see the "command -p
# closes off a PATH-shadowed uname" test near the end of this file, which
# exists to prove exactly that). Every OTHER test below that needs a
# specific HOST_OS value therefore sets `HOST_OS=Linux` / `HOST_OS=Darwin`
# / a malformed value DIRECTLY as a shell variable, and extracts a range
# that starts AFTER the `command -p uname -s` assignment line -- never by
# trying to fake `uname` on PATH, which the shipped line is now specifically
# designed to defeat.
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
extract 399 400 'warn() {' "$WORK/helpers.sh"

# A stub `uname` on PATH -- used ONLY by the dedicated "command -p closes off
# a PATH-shadowed uname" test near the end of this file. Nothing else uses
# PATH-based stubbing any more (see the SAFETY/DESIGN NOTE above): every
# other HOST_OS-dependent test sets $HOST_OS directly instead, since the
# shipped resolution line is specifically designed to ignore this stub.
STUBBIN_UNAME="$WORK/stubbin-uname"
mkdir -p "$STUBBIN_UNAME"
make_uname_stub() {
  local os="$1"
  cat > "$STUBBIN_UNAME/uname" <<STUB_UNAME
#!/usr/bin/env bash
printf '%s\n' "$os"
STUB_UNAME
  chmod +x "$STUBBIN_UNAME/uname"
}

# ---------------------------------------------------------------------------
# Defect 1: cleanup()/reap_dirs() rm -rf'd a 0555 Go-module-cache-shaped
# tree without chmod -R u+w first, failed file-by-file, and left it behind.
# Proof: build a 0555 tree, source the real rm_rf_writable() verbatim, call
# it, assert the tree is gone.
# ---------------------------------------------------------------------------
extract 1164 1171 'rm_rf_writable() {' "$WORK/rm_rf_writable.sh"

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
#
# This same lane_key.sh extraction is reused below by every v4.8.6 test
# that needs $LANE_KEY (the macOS per-round cache paths still use it) --
# it sits entirely BEFORE the HOST_OS resolution line, so it has no
# HOST_OS dependency of its own.
# ---------------------------------------------------------------------------
extract 691 713 'LANE_KEY="$LANE-$WT_HASH"' "$WORK/lane_key.sh"

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
# Defect 3 (v4.8.4, macOS branch unchanged by v4.8.6): the warm step's
# `go mod download all` needs a writable $GOPATH/pkg/sumdb; on bigboy ~/go
# used to be root:root and this failed looking like a network error. Proof:
# run the real per-round-GOPATH block (unless CODEX_REVIEW_GOPATH is set,
# with $HOST_OS set directly to Darwin -- see the SAFETY/DESIGN NOTE at the
# top of this file for why this is no longer done via a uname stub), assert
# it creates a fresh, writable directory; confirm a command actually SEES it
# as GOPATH; then remove it via the same rm_rf_writable() defect-1 already
# proved, confirming the trap tears it down.
# ---------------------------------------------------------------------------
extract 1000 1009 'RGOPATH=$(mktemp -d "/tmp/codex-review-gopath-$LANE_KEY-$TS-XXXXXX")' "$WORK/rgopath.sh"

TS="19700101T000000-test"
LANE_KEY="test-lane-$$"
unset CODEX_REVIEW_GOPATH 2>/dev/null || true
(
  # LANE_KEY and TS are already set above; a subshell inherits them as-is.
  HOST_OS=Darwin
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
# mktemp'd under a fresh name). Takes the FIRST branch of the if/elif/else,
# so $HOST_OS's value doesn't matter here -- still set for consistency.
CUSTOM_GOPATH="$WORK/custom-gopath"
(
  HOST_OS=Darwin
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
extract 632 632 'mkdir -p "$OUTDIR" || die "cannot create output directory $OUTDIR"' "$WORK/outdir.sh"

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
extract 645 657 ': >"$L" || die "cannot create round log $L"' "$WORK/create-log.sh"

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
extract 1297 1297 'WARM_MODULES=$(find "$RGOMODCACHE/cache/download" -name' "$WORK/warm_modules.sh"

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
extract 1257 1341 'if [ -f "$RW/go.mod" ]; then' "$WORK/warm_step.sh"
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

# ---------------------------------------------------------------------------
# v4.8.6: --version prints the current version and exits 0. Runs the real
# script directly (a top-level dispatch, not an extracted fragment) --
# `--version` never touches a worktree, so this is safe to invoke as-is.
# ---------------------------------------------------------------------------
VERSION_OUT=$(bash "$SCRIPT" --version)
VERSION_RC=$?
if [ "$VERSION_RC" -eq 0 ] && printf '%s' "$VERSION_OUT" | grep -qE '^codex-review\.sh v4\.8\.7$'; then
  ok "v4.8.7: --version prints 'codex-review.sh v4.8.7' and exits 0 (got '$VERSION_OUT')"
else
  notok "v4.8.7: --version did not print the expected string (rc=$VERSION_RC, got '$VERSION_OUT')"
fi

# ---------------------------------------------------------------------------
# v4.8.6: on Linux, RGOCACHE/RGOMODCACHE default to the SHARED
# /var/lib/oci-cache volume (go-build, go-mod -- see the SAFETY NOTE at the
# top of this file for why the "default" sub-test below never lets a real
# mkdir run against that literal path), honour the caller's plain
# GOCACHE/GOMODCACHE env vars when set, and CODEX_REVIEW_GOCACHE/
# CODEX_REVIEW_GOMODCACHE still win over both. No $LANE_KEY/$TS suffix
# appears in any of the three cases -- that suffix is what made the old
# path per-round.
#
# Per the SAFETY/DESIGN NOTE at the top of this file, $HOST_OS is set
# DIRECTLY as a shell variable for every case below, never via a uname
# stub -- the extraction ranges start AFTER the command -p uname -s line on
# purpose, so setting $HOST_OS beforehand actually takes effect.
# ---------------------------------------------------------------------------

# VALUE-ONLY extraction: the ruling comment + if/elif/else, WITHOUT the two
# mkdir -p lines that follow it in the real script. Used ONLY for the (a)
# default-value check below, so that case never touches the filesystem at
# all. Starts right after the HOST_OS validation case/esac block ends.
extract 789 807 'if [ "$HOST_OS" = Linux ]; then' "$WORK/cache_resolve_value_only.sh"
grep -qF '/var/lib/oci-cache/go-build' "$WORK/cache_resolve_value_only.sh" \
  || { echo "FAIL: extracted cache_resolve_value_only.sh does not contain the shared-GOCACHE default" >&2; exit 1; }
if grep -qE '^mkdir -p "\$RGOCACHE"' "$WORK/cache_resolve_value_only.sh"; then
  echo "FAIL: cache_resolve_value_only.sh unexpectedly contains a mkdir line -- the value-only/mkdir split in codex-review.sh has drifted, fix the extract range before trusting test (a) below" >&2
  exit 1
fi

# FULL extraction, mkdir lines included -- used for (b)/(c)/(d), which all
# point at $WORK-scoped fake paths (override or macOS per-round /tmp) and
# never fall through to the real /var/lib/oci-cache default, so their mkdir
# is always safe.
extract 789 814 'if [ "$HOST_OS" = Linux ]; then' "$WORK/cache_resolve_full.sh"

run_cache_resolve_value_only() {
  # $1=WT $2=TS $3=HOST_OS  env GOCACHE/GOMODCACHE/CODEX_REVIEW_GOCACHE/
  # CODEX_REVIEW_GOMODCACHE already exported by the caller before invoking.
  (
    WT="$1" TS="$2" HOST_OS="$3"
    # shellcheck source=/dev/null
    source "$WORK/helpers.sh"
    # shellcheck source=/dev/null
    source "$WORK/lane_key.sh"
    # shellcheck source=/dev/null
    source "$WORK/cache_resolve_value_only.sh"
    printf 'RGOCACHE=%s\nRGOMODCACHE=%s\n' "$RGOCACHE" "$RGOMODCACHE"
  )
}

# (a) Linux DEFAULTS: no caller env, no CODEX_REVIEW_* override -> the
# literal shared-volume paths. VALUE ONLY -- no mkdir runs, see the
# extraction above, so this is safe on any host including this one.
unset GOCACHE GOMODCACHE CODEX_REVIEW_GOCACHE CODEX_REVIEW_GOMODCACHE 2>/dev/null || true
RESOLVE_A=$(run_cache_resolve_value_only "$WORK/lane-a/acr" "19700101T000000-a" Linux)
RGOCACHE_A=$(printf '%s' "$RESOLVE_A" | sed -n 's/^RGOCACHE=//p')
RGOMODCACHE_A=$(printf '%s' "$RESOLVE_A" | sed -n 's/^RGOMODCACHE=//p')
if [ "$RGOCACHE_A" = "/var/lib/oci-cache/go-build" ]; then
  ok "v4.8.6a: Linux default RGOCACHE is /var/lib/oci-cache/go-build ($RGOCACHE_A)"
else
  notok "v4.8.6a: Linux default RGOCACHE wrong (got '$RGOCACHE_A', wanted '/var/lib/oci-cache/go-build')"
fi
if [ "$RGOMODCACHE_A" = "/var/lib/oci-cache/go-mod" ]; then
  ok "v4.8.6a: Linux default RGOMODCACHE is /var/lib/oci-cache/go-mod ($RGOMODCACHE_A)"
else
  notok "v4.8.6a: Linux default RGOMODCACHE wrong (got '$RGOMODCACHE_A', wanted '/var/lib/oci-cache/go-mod')"
fi
case "$RGOCACHE_A$RGOMODCACHE_A" in
  *19700101T000000-a*) notok "v4.8.6a: a per-round \$TS suffix leaked into the Linux shared-cache path -- it is supposed to be gone" ;;
  *) ok "v4.8.6a: no per-round \$TS suffix in the Linux shared-cache paths" ;;
esac

# (b) Linux honours the CALLER's plain GOCACHE/GOMODCACHE (new in v4.8.6)
# over the shared-volume default. Points at $WORK-scoped fake paths, so
# the FULL (mkdir-including) extraction is safe to use here.
CALLER_GOCACHE="$WORK/caller-gocache"
CALLER_GOMODCACHE="$WORK/caller-gomodcache"
unset CODEX_REVIEW_GOCACHE CODEX_REVIEW_GOMODCACHE 2>/dev/null || true
RESOLVE_B=$(
  WT="$WORK/lane-b/acr" TS="19700101T000000-b" HOST_OS=Linux
  GOCACHE="$CALLER_GOCACHE" GOMODCACHE="$CALLER_GOMODCACHE"
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  # shellcheck source=/dev/null
  source "$WORK/lane_key.sh"
  # shellcheck source=/dev/null
  source "$WORK/cache_resolve_full.sh"
  printf 'RGOCACHE=%s\nRGOMODCACHE=%s\n' "$RGOCACHE" "$RGOMODCACHE"
)
RGOCACHE_B=$(printf '%s' "$RESOLVE_B" | sed -n 's/^RGOCACHE=//p')
RGOMODCACHE_B=$(printf '%s' "$RESOLVE_B" | sed -n 's/^RGOMODCACHE=//p')
if [ "$RGOCACHE_B" = "$CALLER_GOCACHE" ] && [ "$RGOMODCACHE_B" = "$CALLER_GOMODCACHE" ] \
   && [ -d "$RGOCACHE_B" ] && [ -d "$RGOMODCACHE_B" ]; then
  ok "v4.8.6b: Linux honours the caller's own GOCACHE/GOMODCACHE env values over the shared default, and creates them"
else
  notok "v4.8.6b: caller GOCACHE/GOMODCACHE not honoured (got RGOCACHE='$RGOCACHE_B', RGOMODCACHE='$RGOMODCACHE_B')"
fi

# (c) CODEX_REVIEW_GOCACHE/CODEX_REVIEW_GOMODCACHE still win over the
# caller's plain env vars (existing v4.8.2 override, unchanged precedence).
OVERRIDE_GOCACHE="$WORK/override-gocache"
OVERRIDE_GOMODCACHE="$WORK/override-gomodcache"
RESOLVE_C=$(
  WT="$WORK/lane-c/acr" TS="19700101T000000-c" HOST_OS=Linux
  GOCACHE="$CALLER_GOCACHE" GOMODCACHE="$CALLER_GOMODCACHE"
  CODEX_REVIEW_GOCACHE="$OVERRIDE_GOCACHE" CODEX_REVIEW_GOMODCACHE="$OVERRIDE_GOMODCACHE"
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  # shellcheck source=/dev/null
  source "$WORK/lane_key.sh"
  # shellcheck source=/dev/null
  source "$WORK/cache_resolve_full.sh"
  printf 'RGOCACHE=%s\nRGOMODCACHE=%s\n' "$RGOCACHE" "$RGOMODCACHE"
)
RGOCACHE_C=$(printf '%s' "$RESOLVE_C" | sed -n 's/^RGOCACHE=//p')
RGOMODCACHE_C=$(printf '%s' "$RESOLVE_C" | sed -n 's/^RGOMODCACHE=//p')
if [ "$RGOCACHE_C" = "$OVERRIDE_GOCACHE" ] && [ "$RGOMODCACHE_C" = "$OVERRIDE_GOMODCACHE" ]; then
  ok "v4.8.6c: CODEX_REVIEW_GOCACHE/CODEX_REVIEW_GOMODCACHE still win over the caller's plain env on Linux"
else
  notok "v4.8.6c: CODEX_REVIEW_GOCACHE/CODEX_REVIEW_GOMODCACHE override not honoured (got RGOCACHE='$RGOCACHE_C', RGOMODCACHE='$RGOMODCACHE_C')"
fi
unset GOCACHE GOMODCACHE CODEX_REVIEW_GOCACHE CODEX_REVIEW_GOMODCACHE 2>/dev/null || true

# (d) macOS branch is unchanged: still the per-round /tmp path keyed on
# LANE_KEY-TS. Same (full) block, $HOST_OS=Darwin instead -- always
# resolves under /tmp regardless of caller env, so this is also safe.
RESOLVE_D=$(
  WT="$WORK/lane-d/acr" TS="19700101T000000-d" HOST_OS=Darwin
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  # shellcheck source=/dev/null
  source "$WORK/lane_key.sh"
  # shellcheck source=/dev/null
  source "$WORK/cache_resolve_full.sh"
  printf 'RGOCACHE=%s\nRGOMODCACHE=%s\nLANE_KEY=%s\n' "$RGOCACHE" "$RGOMODCACHE" "$LANE_KEY"
)
RGOCACHE_D=$(printf '%s' "$RESOLVE_D" | sed -n 's/^RGOCACHE=//p')
RGOMODCACHE_D=$(printf '%s' "$RESOLVE_D" | sed -n 's/^RGOMODCACHE=//p')
LANE_KEY_D=$(printf '%s' "$RESOLVE_D" | sed -n 's/^LANE_KEY=//p')
if [ "$RGOCACHE_D" = "/tmp/codex-review-gocache-$LANE_KEY_D-19700101T000000-d" ] \
   && [ "$RGOMODCACHE_D" = "/tmp/codex-review-modcache-$LANE_KEY_D-19700101T000000-d" ] \
   && [ -d "$RGOCACHE_D" ] && [ -d "$RGOMODCACHE_D" ]; then
  ok "v4.8.6d: macOS RGOCACHE/RGOMODCACHE are UNCHANGED -- still a fresh per-round /tmp path, created"
else
  notok "v4.8.6d: macOS cache paths changed unexpectedly (RGOCACHE='$RGOCACHE_D', RGOMODCACHE='$RGOMODCACHE_D', LANE_KEY='$LANE_KEY_D')"
fi
rm -rf "${RGOCACHE_D:-/nonexistent-guard}" "${RGOMODCACHE_D:-/nonexistent-guard}" 2>/dev/null || true

# ---------------------------------------------------------------------------
# v4.8.6: GOPATH on Linux follows the same override precedence as
# GOCACHE/GOMODCACHE above (CODEX_REVIEW_GOPATH > caller's GOPATH >
# $HOME/go), no per-round mktemp dir. Unlike GOCACHE/GOMODCACHE, GOPATH's
# Linux default IS $HOME-relative (see the codex-review.sh comment: neither
# ruling names a GOPATH target, so it keeps Go's own $HOME/go default), so
# sandboxing it with a fake $HOME is sufficient -- no /var/lib literal-path
# safety concern here. macOS keeps its per-round mktemp'd GOPATH (already
# proved as "defect 3" above, with $HOST_OS set directly to Darwin there).
# ---------------------------------------------------------------------------
extract 1000 1009 'RGOPATH=$(mktemp -d "/tmp/codex-review-gopath-$LANE_KEY-$TS-XXXXXX")' "$WORK/rgopath_v486.sh"
FAKE_HOME_GP="$WORK/fake-home-gopath"
mkdir -p "$FAKE_HOME_GP"
unset CODEX_REVIEW_GOPATH GOPATH 2>/dev/null || true
RGOPATH_LINUX_DEFAULT=$(
  HOME="$FAKE_HOME_GP" HOST_OS=Linux
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  # shellcheck source=/dev/null
  source "$WORK/rgopath_v486.sh"
  printf '%s' "$RGOPATH"
)
if [ "$RGOPATH_LINUX_DEFAULT" = "$FAKE_HOME_GP/go" ] && [ -d "$RGOPATH_LINUX_DEFAULT" ]; then
  ok "v4.8.6e: Linux default GOPATH is \$HOME/go, created ($RGOPATH_LINUX_DEFAULT)"
else
  notok "v4.8.6e: Linux default GOPATH wrong (got '$RGOPATH_LINUX_DEFAULT', wanted '$FAKE_HOME_GP/go')"
fi

CALLER_GOPATH="$WORK/caller-gopath"
RGOPATH_LINUX_CALLER=$(
  HOME="$FAKE_HOME_GP" GOPATH="$CALLER_GOPATH" HOST_OS=Linux
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  # shellcheck source=/dev/null
  source "$WORK/rgopath_v486.sh"
  printf '%s' "$RGOPATH"
)
if [ "$RGOPATH_LINUX_CALLER" = "$CALLER_GOPATH" ]; then
  ok "v4.8.6f: Linux honours the caller's own GOPATH env value"
else
  notok "v4.8.6f: caller GOPATH not honoured (got '$RGOPATH_LINUX_CALLER', wanted '$CALLER_GOPATH')"
fi

# ---------------------------------------------------------------------------
# v4.8.6, safety-critical: cleanup() must NEVER remove RGOCACHE/RGOMODCACHE/
# RGOPATH on Linux (they are the shared, persistent bigboy caches now), but
# must STILL remove them on macOS exactly as v4.8.2/v4.8.4 did. Proof: run
# the real cleanup()-internal if/else verbatim with a FAKE rm_rf_writable
# that only records its argument (never touches disk), once per host, and
# assert which paths it was called with.
# ---------------------------------------------------------------------------
extract 1189 1208 'if [ "$HOST_OS" = Linux ]; then' "$WORK/cleanup_cache_branch.sh"
grep -qF 'rm_rf_writable "${RGOCACHE:-}"' "$WORK/cleanup_cache_branch.sh" \
  || { echo "FAIL: extracted cleanup_cache_branch.sh does not contain the RGOCACHE removal call" >&2; exit 1; }

run_cleanup_cache_branch() {
  local host_os="$1" gopath="$2" gocache="$3" gomodcache="$4" calls_file="$5"
  : > "$calls_file"
  (
    HOST_OS="$host_os" RGOPATH="$gopath" RGOCACHE="$gocache" RGOMODCACHE="$gomodcache"
    # shellcheck source=/dev/null
    source "$WORK/helpers.sh"
    rm_rf_writable() { printf '%s\n' "$1" >> "$calls_file"; }
    # shellcheck source=/dev/null
    source "$WORK/cleanup_cache_branch.sh"
  )
}

LINUX_GOPATH="$WORK/shared-gopath-marker"
LINUX_GOCACHE="$WORK/shared-gocache-marker"
LINUX_GOMODCACHE="$WORK/shared-gomodcache-marker"
CALLS_LINUX="$WORK/cleanup-calls-linux.txt"
run_cleanup_cache_branch Linux "$LINUX_GOPATH" "$LINUX_GOCACHE" "$LINUX_GOMODCACHE" "$CALLS_LINUX"
if [ ! -s "$CALLS_LINUX" ]; then
  ok "v4.8.6g: cleanup() on Linux calls rm_rf_writable ZERO times on the shared GOPATH/GOCACHE/GOMODCACHE (never reaped)"
else
  notok "v4.8.6g: cleanup() on Linux still tried to remove something -- $(tr '\n' ' ' < "$CALLS_LINUX")"
fi

DARWIN_GOPATH="$WORK/per-round-gopath-marker"
DARWIN_GOCACHE="$WORK/per-round-gocache-marker"
DARWIN_GOMODCACHE="$WORK/per-round-gomodcache-marker"
CALLS_DARWIN="$WORK/cleanup-calls-darwin.txt"
run_cleanup_cache_branch Darwin "$DARWIN_GOPATH" "$DARWIN_GOCACHE" "$DARWIN_GOMODCACHE" "$CALLS_DARWIN"
if grep -qF "$DARWIN_GOPATH" "$CALLS_DARWIN" && grep -qF "$DARWIN_GOCACHE" "$CALLS_DARWIN" && grep -qF "$DARWIN_GOMODCACHE" "$CALLS_DARWIN"; then
  ok "v4.8.6h: cleanup() on macOS still calls rm_rf_writable on GOPATH/GOCACHE/GOMODCACHE (unchanged from v4.8.4)"
else
  notok "v4.8.6h: cleanup() on macOS did not remove its own per-round dirs as before -- $(tr '\n' ' ' < "$CALLS_DARWIN")"
fi

# ---------------------------------------------------------------------------
# v4.8.6 P1 (round 1, lane-wrapper-v486-20260904T080604, EXECUTED and
# independently reproduced by the lane before this fix): every
# `[ "$HOST_OS" = Linux ]` check does an exact string match, and a malformed
# `uname -s` output (e.g. a trailing `\r`) failed that match at EVERY site
# -- including cleanup()'s removal branch. Combined with the SUPPORTED
# CODEX_REVIEW_GOCACHE/CODEX_REVIEW_GOMODCACHE override pointed at the real
# shared /var/lib/oci-cache paths, this misrouted a genuinely-Linux host
# into the macOS/`else` removal branch and called rm_rf_writable on the
# shared bigboy cache -- the exact "delete the shared cache" incident class
# this whole version exists to prevent. Fix: HOST_OS is validated
# immediately after resolution; anything other than exact `Linux` or
# `Darwin` now `die`s before any cache path is even resolved.
#
# This extraction is JUST the case/esac validation block (not the
# `command -p uname -s` assignment line above it) -- $HOST_OS is set
# directly, per the file-level SAFETY/DESIGN NOTE.
# ---------------------------------------------------------------------------
extract 774 777 'case "$HOST_OS" in' "$WORK/host_os_validate.sh"

# (a) malformed HOST_OS ("Linux\r", set directly, not via a uname stub —
# see the note above) now DIES with the expected message instead of
# silently resolving a cache branch.
set +e
HOSTOS_MALFORMED_OUT=$( (
  HOST_OS=$'Linux\r'
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  # shellcheck source=/dev/null
  source "$WORK/host_os_validate.sh"
  printf 'UNEXPECTEDLY REACHED, HOST_OS=%q\n' "$HOST_OS"
) 2>&1 )
RC_HOSTOS_MALFORMED=$?
set -e
if [ "$RC_HOSTOS_MALFORMED" -ne 0 ] && printf '%s' "$HOSTOS_MALFORMED_OUT" | grep -qi 'unrecognised or malformed'; then
  ok "v4.8.6 P1 fix: a malformed HOST_OS (embedded CR) DIES with the expected message instead of silently resolving a cache branch"
else
  notok "v4.8.6 P1 fix: malformed HOST_OS did NOT die as expected (rc=$RC_HOSTOS_MALFORMED, out='$HOSTOS_MALFORMED_OUT')"
fi

# (b) MUTATION negative control: the exact same probe, run against the
# validation block with its case/esac guard stripped (simulating the
# pre-fix file) -- must NOT die, proving (a) above actually exercises the
# fix rather than dying for an unrelated reason.
sed '/^case "\$HOST_OS" in$/,/^esac$/d' "$WORK/host_os_validate.sh" > "$WORK/host_os_validate_unfixed.sh"
grep -qF 'case "$HOST_OS" in' "$WORK/host_os_validate_unfixed.sh" \
  && { echo "FAIL: mutation strip of host_os_validate_unfixed.sh did not remove the case block -- fix the sed pattern" >&2; exit 1; }
set +e
HOSTOS_UNFIXED_OUT=$( (
  HOST_OS=$'Linux\r'
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  # shellcheck source=/dev/null
  source "$WORK/host_os_validate_unfixed.sh"
  printf 'REACHED, HOST_OS=%q\n' "$HOST_OS"
) 2>&1 )
RC_HOSTOS_UNFIXED=$?
set -e
if [ "$RC_HOSTOS_UNFIXED" -eq 0 ] && printf '%s' "$HOSTOS_UNFIXED_OUT" | grep -q '^REACHED'; then
  ok "v4.8.6 P1 negative control: the same malformed input, with the guard stripped, silently resolves instead of dying (confirms the bug shape the fix closes)"
else
  notok "v4.8.6 P1 negative control: stripping the guard did not reproduce the pre-fix silent-resolve behaviour (rc=$RC_HOSTOS_UNFIXED, out='$HOSTOS_UNFIXED_OUT') -- the positive result above may not be testing what it claims to"
fi

# (c) legitimate exact values ("Linux", "Darwin") still pass through
# unharmed -- guard against an overzealous fix that rejects valid input.
for legit_os in Linux Darwin; do
  LEGIT_OUT=$(
    HOST_OS="$legit_os"
    # shellcheck source=/dev/null
    source "$WORK/helpers.sh"
    # shellcheck source=/dev/null
    source "$WORK/host_os_validate.sh"
    printf '%s' "$HOST_OS"
  )
  if [ "$LEGIT_OUT" = "$legit_os" ]; then
    ok "v4.8.6 P1 fix: legitimate HOST_OS='$legit_os' still passes the validation unharmed"
  else
    notok "v4.8.6 P1 fix: legitimate HOST_OS='$legit_os' was rejected or altered (got '$LEGIT_OUT')"
  fi
done

# ---------------------------------------------------------------------------
# v4.8.6 P1, round 2 (lane-wrapper-v486-20260904T082800, EXECUTED): the
# malformed-value guard above closes accidental garbage in `uname -s`'s
# output, but the resolution line itself, `HOST_OS="$(uname -s)"`, resolved
# `uname` through the CALLER's PATH -- so a caller-controlled shim earlier
# on PATH could make it resolve to an exact, VALID-looking-but-WRONG token
# (e.g. a Linux host with a PATH-shadowed `uname` that prints exact
# "Darwin"), which passes the case/esac guard above just as legitimately as
# a real value would, then picks the wrong (removal) branch anyway.
#
# v4.8.6 P1, round 3 (lane-wrapper-v486-20260904T084834, EXECUTED): round
# 2's own first fix, `HOST_OS="$(command -p uname -s)"`, was ITSELF still
# beatable -- `command` is a NAME, and a caller's environment can shadow it
# with a shell FUNCTION (e.g. via `BASH_ENV`, which non-interactive bash
# sources before this script even starts), which wins over the real
# `command` builtin the exact same way a PATH-shadowed `uname` won over the
# real `uname`. codex's own repro used exactly that: a `BASH_ENV`-defined
# `command() { ... }` function.
#
# Final fix (this version): `HOST_OS="$(builtin command -p uname -s)"` --
# `builtin` looks its argument up as a shell BUILTIN specifically, skipping
# function (and alias) resolution for that name, so neither a PATH shim on
# `uname` NOR a function shadow of `command` can redirect it. Proof, in
# order: (1) a BARE `uname -s` is fooled by a PATH-shadowed uname (confirms
# the PATH-stubbing technique is real); (2) a PLAIN `command -p uname -s`
# (round 2's fix, without `builtin`) is STILL fooled by a BASH_ENV-shadowed
# `command` function (confirms round 3's repro shape is real, and that
# round 2's fix alone was insufficient); (3) `builtin command -p uname -s`
# defeats BOTH attacks; (4) the ACTUAL shipped HOST_OS assignment line,
# extracted verbatim, also resolves to the real value under both.
# ---------------------------------------------------------------------------
extract 755 755 'HOST_OS="$(builtin command -p uname -s)"' "$WORK/host_os_assign.sh"

REAL_UNAME_S=$(command -p uname -s)
make_uname_stub 'TotallyFakeOS'

BARE_UNAME_RESULT=$(PATH="$STUBBIN_UNAME:$PATH" bash -c 'uname -s')
if [ "$BARE_UNAME_RESULT" = "TotallyFakeOS" ]; then
  ok "v4.8.6 P1 (round 2) setup check: a bare 'uname -s' IS fooled by a PATH-shadowed uname (confirms the stubbing technique used below is real)"
else
  notok "v4.8.6 P1 (round 2) setup check: bare 'uname -s' was NOT fooled by the stub (got '$BARE_UNAME_RESULT') -- the test below may not be exercising anything"
fi

# A shell FUNCTION named `command`, sourced the way BASH_ENV would source
# it into a non-interactive shell -- codex's own round-3 repro shape.
BASH_ENV_SHADOW_SCRIPT="$WORK/bash-env-shadow-command.sh"
cat > "$BASH_ENV_SHADOW_SCRIPT" <<'BASH_ENV_EOF'
command() {
  if [ "$1" = "-p" ] && [ "$2" = "uname" ] && [ "$3" = "-s" ]; then
    printf 'BashEnvFakeOS\n'
  else
    builtin command "$@"
  fi
}
BASH_ENV_EOF

PLAIN_COMMAND_P_UNDER_BASH_ENV=$(BASH_ENV="$BASH_ENV_SHADOW_SCRIPT" bash -c 'command -p uname -s')
if [ "$PLAIN_COMMAND_P_UNDER_BASH_ENV" = "BashEnvFakeOS" ]; then
  ok "v4.8.6 P1 (round 3) setup check: a PLAIN 'command -p uname -s' (round 2's fix alone) IS fooled by a BASH_ENV-shadowed 'command' function (confirms round 3's repro shape is real, and that round 2's fix by itself was insufficient)"
else
  notok "v4.8.6 P1 (round 3) setup check: plain 'command -p uname -s' was NOT fooled by the BASH_ENV shadow (got '$PLAIN_COMMAND_P_UNDER_BASH_ENV') -- the test below may not be exercising anything"
fi

BUILTIN_COMMAND_P_UNDER_PATH_SHADOW=$(PATH="$STUBBIN_UNAME:$PATH" bash -c 'builtin command -p uname -s')
if [ "$BUILTIN_COMMAND_P_UNDER_PATH_SHADOW" = "$REAL_UNAME_S" ]; then
  ok "v4.8.6 P1 fix mechanism: 'builtin command -p uname -s' ignores a PATH-shadowed uname and returns the real value ('$REAL_UNAME_S')"
else
  notok "v4.8.6 P1 fix mechanism: 'builtin command -p uname -s' did NOT return the real value under a poisoned PATH (got '$BUILTIN_COMMAND_P_UNDER_PATH_SHADOW', wanted '$REAL_UNAME_S')"
fi

BUILTIN_COMMAND_P_UNDER_BASH_ENV=$(BASH_ENV="$BASH_ENV_SHADOW_SCRIPT" bash -c 'builtin command -p uname -s')
if [ "$BUILTIN_COMMAND_P_UNDER_BASH_ENV" = "$REAL_UNAME_S" ]; then
  ok "v4.8.6 P1 fix mechanism: 'builtin command -p uname -s' ignores a BASH_ENV-shadowed 'command' function and returns the real value ('$REAL_UNAME_S')"
else
  notok "v4.8.6 P1 fix mechanism: 'builtin command -p uname -s' did NOT return the real value under a BASH_ENV shadow (got '$BUILTIN_COMMAND_P_UNDER_BASH_ENV', wanted '$REAL_UNAME_S')"
fi

SHIPPED_LINE_UNDER_PATH_SHADOW=$(
  PATH="$STUBBIN_UNAME:$PATH"
  # shellcheck source=/dev/null
  source "$WORK/host_os_assign.sh"
  printf '%s' "$HOST_OS"
)
if [ "$SHIPPED_LINE_UNDER_PATH_SHADOW" = "$REAL_UNAME_S" ]; then
  ok "v4.8.6 P1 fix: the ACTUAL shipped HOST_OS assignment line resolves to the real host OS ('$REAL_UNAME_S') under a PATH-shadowed uname"
else
  notok "v4.8.6 P1 fix: the shipped HOST_OS assignment line was fooled by the PATH-shadowed uname (got '$SHIPPED_LINE_UNDER_PATH_SHADOW', wanted '$REAL_UNAME_S')"
fi

# BASH_ENV is read only when bash STARTS a new non-interactive process --
# a `(...)` subshell forked from an already-running bash never re-sources
# it (measured directly: harmless in a subshell, active under `bash -c`) --
# so this one must spawn a genuinely new bash via `bash -c`, unlike every
# other subtest in this file.
SHIPPED_LINE_UNDER_BASH_ENV=$(BASH_ENV="$BASH_ENV_SHADOW_SCRIPT" bash -c '
  source "'"$WORK"'/host_os_assign.sh"
  printf "%s" "$HOST_OS"
')
if [ "$SHIPPED_LINE_UNDER_BASH_ENV" = "$REAL_UNAME_S" ]; then
  ok "v4.8.6 P1 fix: the ACTUAL shipped HOST_OS assignment line resolves to the real host OS ('$REAL_UNAME_S') under a BASH_ENV-shadowed 'command' function"
else
  notok "v4.8.6 P1 fix: the shipped HOST_OS assignment line was fooled by the BASH_ENV-shadowed 'command' function (got '$SHIPPED_LINE_UNDER_BASH_ENV', wanted '$REAL_UNAME_S') -- 'builtin' is not doing its job"
fi

# ---------------------------------------------------------------------------
# v4.8.6 P2 (round 2, lane-wrapper-v486-20260904T082800, EXECUTED, plus a
# team-lead-directed addendum the same day): the reviewer-facing
# module-cache fallback text used to unconditionally point a retry at
# $HOME/go/pkg/mod -- correct pre-v4.8.6 (the round's own cache was COLD, so
# the host's real long-lived default module cache was the only useful
# fallback), but that path is now the ruling's own LEGACY path (no lane
# writes there) on Linux, where $RGOMODCACHE already IS the persistent
# shared cache. Fixed: Linux gets no location-based fallback at all (report
# the gap instead). ADDENDUM: $HOME/go/pkg/mod is not a safe suggestion on
# macOS EITHER -- it names a different user's cache on any host but this
# one -- so macOS was changed too, to quote the round's OWN resolved
# $RGOMODCACHE (the same value already stated in the sentence before it)
# instead of switching location. Proof: extract the real if/else/heredoc
# block, run it once per host, assert the generated prompt-fragment text.
# ---------------------------------------------------------------------------
extract 1427 1437 'MODCACHE_FALLBACK_LINE=' "$WORK/modcache_fallback.sh"

FALLBACK_LINUX=$(
  RW="$WORK/fallback-rw-linux" HOST_OS=Linux RGOMODCACHE=/var/lib/oci-cache/go-mod HOME=/home/ubuntu
  mkdir -p "$RW"
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  # shellcheck source=/dev/null
  source "$WORK/modcache_fallback.sh"
  cat "$RW/prompt.md"
)
if printf '%s' "$FALLBACK_LINUX" | grep -q 'Do NOT retry against \$HOME/go/pkg/mod' \
   && ! printf '%s' "$FALLBACK_LINUX" | grep -q 'retry ONCE with GOMODCACHE='; then
  ok "v4.8.6 P2 fix: on Linux, the reviewer prompt does NOT suggest retrying against the legacy \$HOME/go/pkg/mod path"
else
  notok "v4.8.6 P2 fix: Linux fallback text still suggests the legacy path or lost its own warning ($FALLBACK_LINUX)"
fi

FALLBACK_DARWIN=$(
  RW="$WORK/fallback-rw-darwin" HOST_OS=Darwin RGOMODCACHE="$WORK/darwin-modcache" HOME="$WORK/darwin-home"
  mkdir -p "$RW"
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  # shellcheck source=/dev/null
  source "$WORK/modcache_fallback.sh"
  cat "$RW/prompt.md"
)
if printf '%s' "$FALLBACK_DARWIN" | grep -q "retry ONCE with GOMODCACHE=$WORK/darwin-modcache" \
   && ! printf '%s' "$FALLBACK_DARWIN" | grep -q 'go/pkg/mod'; then
  ok "v4.8.6 P2 fix (addendum): on macOS, the reviewer prompt suggests retrying the round's OWN resolved GOMODCACHE, never \$HOME/go/pkg/mod"
else
  notok "v4.8.6 P2 fix (addendum): macOS fallback text does not quote the round's own GOMODCACHE, or still mentions go/pkg/mod ($FALLBACK_DARWIN)"
fi

# ---------------------------------------------------------------------------
# v4.8.6 addendum (team-lead, two-lane measurement): a `go test`/`run`/
# `build` failing with "creating work dir: ... operation not permitted" on
# the FIRST invocation (macOS read-only sandbox) can succeed on an
# immediate retry with no other change. The injected STANDING_RULES prompt
# text must tell the reviewer to retry exactly once before declaring go
# unavailable. Proof: extract the real STANDING_RULES heredoc BODY (between
# its literal open/close marker lines in the shipped script) and check it.
# ---------------------------------------------------------------------------
extract 1355 1393 'go test unavailable' "$WORK/standing_rules_body.txt"
if grep -q "creating work dir" "$WORK/standing_rules_body.txt" \
   && grep -qi "RETRY IT EXACTLY ONCE" "$WORK/standing_rules_body.txt"; then
  ok "v4.8.6 addendum: the injected prompt tells the reviewer to retry exactly once on a 'creating work dir' failure"
else
  notok "v4.8.6 addendum: the injected prompt is missing the retry-once-on-creating-work-dir instruction"
fi

# ---------------------------------------------------------------------------
# v4.8.6 addendum (chris via team-lead, bigboy boot-drive-full incident): on
# Linux, every wrapper-owned per-round scratch dir (review worktree, Go's
# work dir, shell TMPDIR) moves under
# /var/lib/oci-cache/lane-scratch/<lane>/ instead of /tmp, fails closed if
# that root cannot be created. macOS is unchanged.
#
# v4.8.6 (generalized 09-04): this block no longer sanitizes $NAME itself --
# that moved to ONE central gate (NAME_ALLOWLIST_RE, tested separately
# below), and this block trusts its caller the same way every other site
# that uses $NAME now does. Confirm that trust assumption explicitly: the
# extracted block must NOT contain a local SAFE_LANE_NAME/basename/case
# re-sanitization step any more (a leftover duplicate would be dead code at
# best and a second place to get it wrong at worst).
#
# v4.8.7, confirmation-pass round #4 (P1, mechanism EXECUTED): the block now
# also does symlink/containment checks (`[ -L ... ]`, `cd ... && pwd -P`) --
# real filesystem OPERATORS, not commands, so they cannot be intercepted by
# overriding a `mkdir` shell function the way the pre-round-4 version could
# be. Testing the Linux branch's real behaviour therefore now needs a real
# (but fully throwaway, $WORK-scoped) directory tree rather than a stub --
# achieved by RELOCATING the hardcoded /var/lib/oci-cache/lane-scratch
# prefix to a $WORK path via sed on the extracted text, never by touching
# the real /var/lib on whatever host runs this harness. The mandated real
# prefix itself is still asserted separately, against the UNMODIFIED
# extracted text, immediately below.
# ---------------------------------------------------------------------------
extract 884 954 'LANE_SCRATCH_ROOT=' "$WORK/lane_scratch_root.sh"
grep -qF 'LANE_SCRATCH_ROOT="/var/lib/oci-cache/lane-scratch/$NAME"' "$WORK/lane_scratch_root.sh" \
  || { echo "FAIL: extracted lane_scratch_root.sh does not contain the expected lane-scratch path template" >&2; exit 1; }
if grep -qE '^\s*SAFE_LANE_NAME=' "$WORK/lane_scratch_root.sh"; then
  echo "FAIL: extracted lane_scratch_root.sh still re-sanitizes NAME locally (SAFE_LANE_NAME=...) -- the single-gate refactor left a duplicate" >&2
  exit 1
fi

FAKE_LANE_SCRATCH_PARENT="$WORK/fake-lane-scratch-parent"
mkdir -p "$FAKE_LANE_SCRATCH_PARENT"
sed "s#/var/lib/oci-cache/lane-scratch#$FAKE_LANE_SCRATCH_PARENT#g" \
  "$WORK/lane_scratch_root.sh" > "$WORK/lane_scratch_root_relocated.sh"
grep -qF "$FAKE_LANE_SCRATCH_PARENT" "$WORK/lane_scratch_root_relocated.sh" \
  || { echo "FAIL: relocation sed did not rewrite the lane-scratch prefix -- fix the sed pattern" >&2; exit 1; }

run_lane_scratch_root_darwin() {
  local name="$1"
  (
    HOST_OS=Darwin NAME="$name" TMPDIR="$WORK/fake-tmpdir-486"
    # shellcheck source=/dev/null
    source "$WORK/helpers.sh"
    # shellcheck source=/dev/null
    source "$WORK/lane_scratch_root.sh"
    printf 'RW_BASE=%s\nRGOTMPDIR_BASE=%s\nRTMPDIR_BASE=%s\n' "$RW_BASE" "$RGOTMPDIR_BASE" "$RTMPDIR_BASE"
  )
}

# The Darwin (else) branch never touches the filesystem at all (plain var
# assignments, no mkdir/cd/pwd -P) -- safe to source the UNMODIFIED
# extracted text directly, no relocation needed.
RESOLVE_SCRATCH_DARWIN=$(run_lane_scratch_root_darwin test-lane-486)
if printf '%s' "$RESOLVE_SCRATCH_DARWIN" | grep -qF "RW_BASE=$WORK/fake-tmpdir-486" \
   && printf '%s' "$RESOLVE_SCRATCH_DARWIN" | grep -q '^RGOTMPDIR_BASE=/tmp$' \
   && printf '%s' "$RESOLVE_SCRATCH_DARWIN" | grep -q '^RTMPDIR_BASE=/tmp$'; then
  ok "v4.8.6 scratch: macOS bases are UNCHANGED (\$TMPDIR / literal /tmp), no filesystem touched at all on macOS (unchanged from before)"
else
  notok "v4.8.6 scratch: macOS scratch bases changed unexpectedly (got: $RESOLVE_SCRATCH_DARWIN)"
fi

run_lane_scratch_root_linux() {
  local name="$1" script="$2"
  (
    HOST_OS=Linux NAME="$name" TMPDIR="$WORK/unused-tmpdir"
    # shellcheck source=/dev/null
    source "$WORK/helpers.sh"
    # shellcheck source=/dev/null
    source "$script"
    printf 'RW_BASE=%s\nRGOTMPDIR_BASE=%s\nRTMPDIR_BASE=%s\n' "$RW_BASE" "$RGOTMPDIR_BASE" "$RTMPDIR_BASE"
  )
}

# WORK_REAL, not a lexical-normalize helper, predicts what the real
# script's own `cd ... && pwd -P` will produce -- on macOS, $WORK itself
# resolves through a real ancestor symlink (/var -> /private/var), which a
# purely lexical normalize (e.g. python's os.path.normpath) would NOT
# reproduce, causing a spurious mismatch that has nothing to do with the
# fix under test. Resolving $WORK itself once, the same way, keeps this
# comparison apples-to-apples regardless of the host's own symlink layout.
WORK_REAL=$(cd "$WORK" && pwd -P)
RESOLVE_SCRATCH_LINUX=$(run_lane_scratch_root_linux test-lane-486 "$WORK/lane_scratch_root_relocated.sh")
EXPECTED_LINUX_ROOT="$FAKE_LANE_SCRATCH_PARENT/test-lane-486"
EXPECTED_LINUX_ROOT_REAL="$WORK_REAL/fake-lane-scratch-parent/test-lane-486"
if printf '%s' "$RESOLVE_SCRATCH_LINUX" | grep -qF "RW_BASE=$EXPECTED_LINUX_ROOT_REAL" \
   && printf '%s' "$RESOLVE_SCRATCH_LINUX" | grep -qF "RGOTMPDIR_BASE=$EXPECTED_LINUX_ROOT_REAL" \
   && printf '%s' "$RESOLVE_SCRATCH_LINUX" | grep -qF "RTMPDIR_BASE=$EXPECTED_LINUX_ROOT_REAL" \
   && [ -d "$EXPECTED_LINUX_ROOT" ] && [ ! -L "$EXPECTED_LINUX_ROOT" ]; then
  ok "v4.8.6 scratch: on Linux, RW/RGOTMPDIR/RTMPDIR all base under the mandated per-lane root (relocated for the test), a real directory is created (find-proof), never a symlink"
else
  notok "v4.8.6 scratch: Linux scratch bases wrong (got: $RESOLVE_SCRATCH_LINUX; expected real=$EXPECTED_LINUX_ROOT_REAL; dir exists=$([ -d "$EXPECTED_LINUX_ROOT" ] && echo yes || echo no))"
fi
rm -rf "$EXPECTED_LINUX_ROOT"

# Fail-closed: make the relocated parent read-only so mkdir -p for a NEW
# (not-yet-existing) lane name genuinely fails -- a real permission
# failure, not a stubbed one, safe because it is entirely inside $WORK.
chmod 555 "$FAKE_LANE_SCRATCH_PARENT"
set +e
RESOLVE_SCRATCH_FAIL=$(run_lane_scratch_root_linux test-lane-486-fail "$WORK/lane_scratch_root_relocated.sh" 2>&1)
RC_SCRATCH_FAIL=$?
set -e
chmod 755 "$FAKE_LANE_SCRATCH_PARENT"
if [ "$RC_SCRATCH_FAIL" -ne 0 ] && printf '%s' "$RESOLVE_SCRATCH_FAIL" | grep -qi 'refusing to silently fall back'; then
  ok "v4.8.6 scratch: fails CLOSED (dies) when the Linux scratch root cannot be created, instead of silently falling back to /tmp"
else
  notok "v4.8.6 scratch: did not fail closed on an unwritable Linux scratch root (rc=$RC_SCRATCH_FAIL, out='$RESOLVE_SCRATCH_FAIL')"
fi

# ---------------------------------------------------------------------------
# v4.8.7, confirmation-pass round #4 (P1, mechanism EXECUTED, independently
# reproduced by the lane in an isolated temp dir before fixing): a
# pre-existing SYMLINK at lane-scratch/$NAME was followed silently by
# `mkdir -p`, and every mktemp call after it then wrote through the
# symlink, outside the mandated root. Proof: pre-plant a real symlink
# under the RELOCATED (safe, $WORK-scoped) parent pointing at a separate
# real directory, run the REAL extracted block against it, and confirm (a)
# it dies with the expected message, (b) NOTHING was ever written to the
# symlink's target (find-proof the redirect target stays empty).
# ---------------------------------------------------------------------------
SYMLINK_TARGET="$WORK/symlink-attack-target"
mkdir -p "$SYMLINK_TARGET"
ln -s "$SYMLINK_TARGET" "$FAKE_LANE_SCRATCH_PARENT/attacked-lane"
set +e
RESOLVE_SYMLINK_ATTACK=$(run_lane_scratch_root_linux attacked-lane "$WORK/lane_scratch_root_relocated.sh" 2>&1)
RC_SYMLINK_ATTACK=$?
set -e
if [ "$RC_SYMLINK_ATTACK" -ne 0 ] \
   && printf '%s' "$RESOLVE_SYMLINK_ATTACK" | grep -qi 'already exists as a SYMLINK' \
   && [ -z "$(find "$SYMLINK_TARGET" -mindepth 1 2>/dev/null)" ]; then
  ok "v4.8.6 scratch symlink fix: a pre-planted symlink at the lane's own scratch path is REJECTED (dies) before any mktemp call, and the symlink's target received NOTHING (find-proof)"
else
  notok "v4.8.6 scratch symlink fix: a pre-planted symlink was NOT rejected as expected (rc=$RC_SYMLINK_ATTACK, out='$RESOLVE_SYMLINK_ATTACK', target contents: $(find "$SYMLINK_TARGET" -mindepth 1 2>/dev/null))"
fi

# Negative control: this fix has TWO independent, overlapping layers (the
# `-L` checks AND the physical-containment check below them) -- stripping
# only one via sed still leaves the other catching the exact same attack,
# which would make a single-layer mutation strip a FALSE negative control
# (it would still die, for the OTHER reason, and look like it "still
# works" when what's actually being tested is incomplete). So this
# reconstructs the PRE-FIX shape by hand instead -- exactly what
# LANE_SCRATCH_ROOT's assignment plus a bare `mkdir -p` looked like before
# ANY of round #4's checks existed, the same "reconstruct the historical
# mechanism" technique the multi-line NAME red-check above uses for the
# same reason (mutating the real file is not reliable when a fix has
# defense in depth).
cat > "$WORK/lane_scratch_root_prefix4.sh" <<'PREFIX4_SCRIPT'
if [ "$HOST_OS" = Linux ]; then
  LANE_SCRATCH_ROOT="__FAKE_PARENT__/$NAME"
  mkdir -p "$LANE_SCRATCH_ROOT" \
    || die "cannot create/find the mandated Linux scratch root $LANE_SCRATCH_ROOT -- refusing to silently fall back to /tmp or $HOME"
  RW_BASE="$LANE_SCRATCH_ROOT"
  RGOTMPDIR_BASE="$LANE_SCRATCH_ROOT"
  RTMPDIR_BASE="$LANE_SCRATCH_ROOT"
else
  RW_BASE="${TMPDIR:-/tmp}"
  RGOTMPDIR_BASE="/tmp"
  RTMPDIR_BASE="/tmp"
fi
PREFIX4_SCRIPT
sed "s#__FAKE_PARENT__#$FAKE_LANE_SCRATCH_PARENT#g" \
  "$WORK/lane_scratch_root_prefix4.sh" > "$WORK/lane_scratch_root_relocated_unfixed.sh"
# The pre-fix shape trusts RW_BASE lexically -- it never resolves it, that
# IS the bug -- so RW_BASE printed by the sourced script is just the
# symlink's own path text. Resolve its REAL destination from the test
# harness side (the same way a subsequent real `mktemp -d "$RW_BASE/..."`
# would land) to prove it actually points through the symlink into
# $SYMLINK_TARGET, not merely that the string looks unchanged.
set +e
RESOLVE_SYMLINK_ATTACK_NEG=$(
  HOST_OS=Linux NAME=attacked-lane TMPDIR="$WORK/unused-tmpdir"
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  # shellcheck source=/dev/null
  source "$WORK/lane_scratch_root_relocated_unfixed.sh"
  printf 'RW_BASE=%s\n' "$RW_BASE"
)
RC_SYMLINK_ATTACK_NEG=$?
set -e
NEG_RW_BASE=$(printf '%s' "$RESOLVE_SYMLINK_ATTACK_NEG" | sed -n 's/^RW_BASE=//p')
NEG_RW_BASE_REAL=""
[ -n "$NEG_RW_BASE" ] && [ -d "$NEG_RW_BASE" ] && NEG_RW_BASE_REAL=$(cd "$NEG_RW_BASE" && pwd -P)
SYMLINK_TARGET_REAL=$(cd "$SYMLINK_TARGET" && pwd -P)
if [ "$RC_SYMLINK_ATTACK_NEG" -eq 0 ] \
   && [ -n "$NEG_RW_BASE_REAL" ] && [ "$NEG_RW_BASE_REAL" = "$SYMLINK_TARGET_REAL" ]; then
  ok "v4.8.6 scratch symlink negative control: the reconstructed pre-round-4 shape (bare mkdir -p, no -L/containment checks) DOES get its RW_BASE redirected through the same pre-planted symlink (real path resolves into the symlink's target), confirming the positive test exercises the real fix"
else
  notok "v4.8.6 scratch symlink negative control: the reconstructed pre-round-4 shape did NOT get redirected as expected (rc=$RC_SYMLINK_ATTACK_NEG, RW_BASE real='$NEG_RW_BASE_REAL', target real='$SYMLINK_TARGET_REAL') -- the positive test above may not be exercising the real bug"
fi
rm -rf "$SYMLINK_TARGET" "$FAKE_LANE_SCRATCH_PARENT/attacked-lane"


# ---------------------------------------------------------------------------
# v4.8.6, GENERALIZED per chris/team-lead ruling 09-04, after the bigboy
# confirmation pass's round #2 finding (`basename -- '..'` unchanged --
# LANE_SCRATCH_ROOT resolved to /var/lib/oci-cache itself) AND team-lead's
# own follow-up finding ("item 3": the SAME unsanitized $NAME reached
# V/L -- this round's own verdict+log filenames -- and RESIDUE_DIR, neither
# of which ever called the Linux-only basename/case helper at all). Fixed
# with ONE positive-allowlist gate (NAME_ALLOWLIST_RE), validated ONCE
# right after NAME is resolved, covering every downstream site at once.
# Proof: extract the real gate and run it directly against the exact test
# values team-lead specified -- '..', '.', empty, 'a/b',
# '../../../../tmp', and one legitimate name -- against the REAL block,
# never a re-typed copy of the regex.
#
# TWO extracts, deliberately: `name_validate.sh` (574-605, the full
# resolution -- `NAME=${NAME:-$(basename "$WT")}` PLUS the gate) proves the
# real end-to-end behaviour including the DEFAULT-substitution idiom;
# `name_check_only.sh` (602-605, the gate alone) isolates the allowlist
# mechanism itself. This split matters for exactly one value in team-lead's
# list: NAME='' is not just "rejected by the gate" here -- bash's
# `${NAME:-fallback}` treats unset AND empty identically, so an explicit
# `-n ''` never reaches the gate as empty at all, it silently becomes
# `basename "$WT"` first (the same default as omitting -n). Testing '' as
# a "does the gate reject it" case against the FULL block would therefore
# either pass for the wrong reason (if $WT happens to be unset too, an
# unrelated "unbound variable" error also makes RC nonzero) or fail
# outright (if $WT is set, as it always is in the real script) -- neither
# result says anything about the gate. The gate-only extract sidesteps the
# default-substitution idiom entirely and tests the regex mechanism on its
# own merits; a separate dedicated test below covers the full block's
# actual (safe) behaviour for NAME=''.
# ---------------------------------------------------------------------------
extract 574 624 'NAME_ALLOWLIST_RE=' "$WORK/name_validate.sh"
grep -qF "NAME_ALLOWLIST_RE='^[A-Za-z0-9][A-Za-z0-9._-]*\$'" "$WORK/name_validate.sh" \
  || { echo "FAIL: extracted name_validate.sh does not contain the expected allowlist regex -- line numbers drifted or the regex changed" >&2; exit 1; }
extract 602 624 'NAME_ALLOWLIST_RE=' "$WORK/name_check_only.sh"

run_name_check_only() {
  local name="$1"
  (
    NAME="$name"
    # shellcheck source=/dev/null
    source "$WORK/helpers.sh"
    # shellcheck source=/dev/null
    source "$WORK/name_check_only.sh"
    printf 'NAME=%s\n' "$NAME"
  )
}

for BAD_NAME in '..' '.' '' 'a/b' '../../../../tmp' "$(printf 'lane\n../../../escaped/owned')"; do
  set +e
  RESOLVE_NAME_BAD=$(run_name_check_only "$BAD_NAME" 2>&1)
  RC_NAME_BAD=$?
  set -e
  if [ "$RC_NAME_BAD" -ne 0 ] && printf '%s' "$RESOLVE_NAME_BAD" | grep -qi 'not a safe path/filename component'; then
    ok "v4.8.6 NAME allowlist gate: NAME=$(printf '%q' "$BAD_NAME") is REJECTED (dies) by the real gate"
  else
    notok "v4.8.6 NAME allowlist gate: NAME=$(printf '%q' "$BAD_NAME") was NOT rejected (rc=$RC_NAME_BAD, out='$RESOLVE_NAME_BAD')"
  fi
done

RESOLVE_NAME_LEGIT=$(run_name_check_only 'my-legit-lane_1.2')
if [ "$RESOLVE_NAME_LEGIT" = 'NAME=my-legit-lane_1.2' ]; then
  ok "v4.8.6 NAME allowlist gate: a legitimate NAME ('my-legit-lane_1.2') still passes the real gate unharmed"
else
  notok "v4.8.6 NAME allowlist gate: a legitimate NAME was wrongly rejected or altered (got: $RESOLVE_NAME_LEGIT)"
fi

# ---------------------------------------------------------------------------
# v4.8.6, found by confirmation-pass round #3 on bigboy, EXECUTED,
# independently reproduced by the lane: the FIRST version of this gate
# piped NAME through `grep -Eq`, which is LINE-oriented -- grep -q succeeds
# if ANY line of a multi-line input matches, and `^`/`$` anchor to LINE
# boundaries, not the whole string's. NAME=$'lane\n../../../escaped/owned'
# has a safe first line ("lane") that alone satisfies the regex, so the
# old grep-based gate accepted the whole (dangerous) multi-line value.
# Fixed: bash's own `[[ =~ ]]`, which matches the entire string in one
# pass. Proof: reconstruct the OLD grep-based check as an explicit mutation
# (never re-run historical vulnerable code from git history -- this is a
# same-shape rebuild of the exact line this file used to have) and show it
# WOULD have accepted the multi-line value (red); the REAL shipped gate
# rejects the same value (green, already proven by the loop above, cited
# again here for the pairing).
# ---------------------------------------------------------------------------
MULTILINE_NAME=$(printf 'lane\n../../../escaped/owned')
OLD_GREP_RE='^[A-Za-z0-9][A-Za-z0-9._-]*$'
if printf '%s' "$MULTILINE_NAME" | LC_ALL=C grep -Eq "$OLD_GREP_RE"; then
  ok "v4.8.6 multi-line NAME fix, red check: the OLD grep-based mechanism (reconstructed, same regex) DOES accept the multi-line value, confirming the historical vulnerability shape was real"
else
  notok "v4.8.6 multi-line NAME fix, red check: could not reproduce the old grep-based mechanism accepting the multi-line value -- the red/green pair below may not prove anything"
fi
set +e
RESOLVE_MULTILINE_GREEN=$(run_name_check_only "$MULTILINE_NAME" 2>&1)
RC_MULTILINE_GREEN=$?
set -e
if [ "$RC_MULTILINE_GREEN" -ne 0 ] && printf '%s' "$RESOLVE_MULTILINE_GREEN" | grep -qi 'not a safe path/filename component'; then
  ok "v4.8.6 multi-line NAME fix, green check: the REAL shipped gate (bash [[ =~ ]]) rejects the same multi-line value the old grep-based mechanism accepted"
else
  notok "v4.8.6 multi-line NAME fix, green check: the real gate did NOT reject the multi-line value (rc=$RC_MULTILINE_GREEN, out='$RESOLVE_MULTILINE_GREEN')"
fi

# ---------------------------------------------------------------------------
# team-lead directive, after the round-#3 finding: assert explicitly that
# the allowlist rejects OTHER control characters too, not just the one
# byte (newline) the round happened to use -- CR, TAB, and a low
# non-printable byte adjacent to where a NUL would be (a REAL NUL cannot
# be represented in a bash string or passed through argv at all -- C
# strings, which is what argv entries are, terminate at the first NUL, so
# "NAME contains a NUL" is not a reachable shape via -n in the first place;
# this is noted rather than tested, since there is nothing to execute).
# `[A-Za-z0-9._-]` under LC_ALL=C is a closed allowlist -- every one of
# these bytes falls outside it structurally, the same way `/` and `..` do.
# ---------------------------------------------------------------------------
for CTRL_DESC_VAL in "CR:$(printf 'lane\rx')" "TAB:$(printf 'lane\tx')" "low-nonprintable(0x01):$(printf 'lane\001x')"; do
  CTRL_DESC="${CTRL_DESC_VAL%%:*}"
  CTRL_NAME="${CTRL_DESC_VAL#*:}"
  set +e
  RESOLVE_CTRL=$(run_name_check_only "$CTRL_NAME" 2>&1)
  RC_CTRL=$?
  set -e
  if [ "$RC_CTRL" -ne 0 ] && printf '%s' "$RESOLVE_CTRL" | grep -qi 'not a safe path/filename component'; then
    ok "v4.8.6 NAME allowlist gate, control-char sweep: NAME containing $CTRL_DESC is REJECTED by the real gate"
  else
    notok "v4.8.6 NAME allowlist gate, control-char sweep: NAME containing $CTRL_DESC was NOT rejected (rc=$RC_CTRL, out='$RESOLVE_CTRL')"
  fi
done

# Negative control: strip the gate's if/die block and confirm the same bad
# values do NOT die -- proves the tests above exercise the real fix, not a
# harness quirk.
sed '/^NAME_ALLOWLIST_RE=/,/^fi$/d' "$WORK/name_check_only.sh" > "$WORK/name_check_only_unfixed.sh"
grep -qF 'NAME_ALLOWLIST_RE' "$WORK/name_check_only_unfixed.sh" \
  && { echo "FAIL: mutation strip of name_check_only_unfixed.sh did not remove the allowlist gate -- fix the sed pattern" >&2; exit 1; }
for BAD_NAME in '..' '.' '' 'a/b' '../../../../tmp'; do
  set +e
  RESOLVE_NAME_BAD_NEG=$(
    NAME="$BAD_NAME"
    # shellcheck source=/dev/null
    source "$WORK/helpers.sh"
    # shellcheck source=/dev/null
    source "$WORK/name_check_only_unfixed.sh"
    printf 'NAME=%s\n' "$NAME"
  )
  RC_NAME_BAD_NEG=$?
  set -e
  if [ "$RC_NAME_BAD_NEG" -eq 0 ]; then
    ok "v4.8.6 NAME allowlist gate negative control: with the gate stripped, NAME='$BAD_NAME' DOES pass through unrejected, confirming the positive tests exercise the real fix"
  else
    notok "v4.8.6 NAME allowlist gate negative control: with the gate stripped, NAME='$BAD_NAME' still didn't pass through (rc=$RC_NAME_BAD_NEG) -- the positive tests above may not be exercising the real bug"
  fi
done

# Dedicated test for the full block's REAL behaviour on NAME='' (the case
# the isolated gate test above deliberately does not exercise, see the
# comment at the top of this section): with WT resolved to a real,
# legitimate path (as the actual script always has it by this point), an
# empty/omitted -n must fall through safely to basename(WT), landing on a
# name that itself still passes the allowlist -- never an empty string,
# never a die, never a value derived from anything attacker-controlled
# (WT itself is a real filesystem path the caller pointed -w at and that
# the script already validated as a git worktree before this line runs).
RESOLVE_NAME_EMPTY_FULL=$(
  NAME='' WT="$WORK/some-real-lane-worktree"
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  # shellcheck source=/dev/null
  source "$WORK/name_validate.sh"
  printf 'NAME=%s\n' "$NAME"
)
if [ "$RESOLVE_NAME_EMPTY_FULL" = 'NAME=some-real-lane-worktree' ]; then
  ok "v4.8.6 NAME allowlist gate: the FULL block's real behaviour for NAME='' is to fall through safely to basename(\$WT) ('some-real-lane-worktree'), never an empty/rejected value -- this is the one case the isolated gate test above cannot exercise meaningfully"
else
  notok "v4.8.6 NAME allowlist gate: NAME='' through the full block did not resolve to the expected basename(\$WT) default (got: $RESOLVE_NAME_EMPTY_FULL)"
fi


# ---------------------------------------------------------------------------
# "Item 3" itself: the verdict/log filename construction (V/L) that reaches
# unsanitized $NAME before this fix. Two parts: (a) RED -- prove the
# pre-fix vulnerability shape actually existed, by building V/L directly
# from a malicious NAME with NO gate in front of it (this is exactly what
# the shipped code did before this commit); (b) GREEN -- prove the REAL
# shipped code never lets that malicious NAME reach V/L at all, by running
# the real gate immediately followed by the real V/L lines in one sequence
# and showing execution never gets past the gate.
# ---------------------------------------------------------------------------
extract 646 647 'V="$OUTDIR/$NAME-$TS.md"' "$WORK/name_sites_vl.sh"

# Reuse the same resolved-vs-lexical path helper the earlier traversal fix
# established: a raw string prefix match on "$OUTDIR/../../.." would still
# start with $OUTDIR as TEXT while resolving somewhere else entirely.
resolve_path() { python3 -c 'import os,sys; print(os.path.normpath(sys.argv[1]))' "$1"; }


# (a) RED: V/L alone, no gate, malicious NAME -- must escape OUTDIR once resolved.
VL_RED=$(
  OUTDIR="$WORK/fake-outdir-486" NAME='../../../../tmp/evil' TS='20260904T000000'
  # shellcheck source=/dev/null
  source "$WORK/name_sites_vl.sh"
  printf 'V=%s\nL=%s\n' "$V" "$L"
)
VL_RED_V=$(printf '%s' "$VL_RED" | sed -n 's/^V=//p')
VL_RED_V_RESOLVED=$(resolve_path "$VL_RED_V")
case "$VL_RED_V_RESOLVED" in
  "$WORK/fake-outdir-486"/*)
    notok "v4.8.6 item-3 red check: V escaping OUTDIR could not be reproduced with the gate absent -- the red/green pair below may not prove anything (resolved='$VL_RED_V_RESOLVED')"
    ;;
  *)
    ok "v4.8.6 item-3 red check: with NO gate in front of it (the pre-fix shape), a malicious NAME DOES make V resolve outside OUTDIR (resolved='$VL_RED_V_RESOLVED'), confirming item 3 was a real vulnerability"
    ;;
esac

# (b) GREEN: the real gate immediately followed by the real V/L lines, same
# malicious NAME. The gate must die BEFORE V/L ever runs -- V/L therefore
# never executes an unsafe path is fine and expected, and the pattern's
# print (which would only run if both sourced files completed) proves it.
set +e
VL_GREEN=$(
  NAME='../../../../tmp/evil' OUTDIR="$WORK/fake-outdir-486" TS='20260904T000000'
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  # shellcheck source=/dev/null
  source "$WORK/name_validate.sh"
  # shellcheck source=/dev/null
  source "$WORK/name_sites_vl.sh"
  printf 'REACHED_VL V=%s L=%s\n' "$V" "$L"
)
RC_VL_GREEN=$?
set -e
if [ "$RC_VL_GREEN" -ne 0 ] && ! printf '%s' "$VL_GREEN" | grep -q '^REACHED_VL'; then
  ok "v4.8.6 item-3 fix: the REAL gate, run immediately before the REAL V/L lines, dies on the same malicious NAME before V/L ever executes -- item 3 is closed at the source, not by fixing V/L itself"
else
  notok "v4.8.6 item-3 fix: the malicious NAME reached V/L despite the gate running first (rc=$RC_VL_GREEN, out='$VL_GREEN')"
fi

# RESIDUE_DIR (preserve_residue() and its two call sites) follows the
# identical "$OUTDIR/$NAME-$TS-..." pattern as V/L and is covered by the
# same upstream gate -- confirm the shipped line still matches that pattern
# byte-for-byte (line-number drift or a rewritten construction would want
# its own dedicated red/green pair, same as V/L above).
grep -qF 'RESIDUE_DIR="$OUTDIR/$NAME-$TS-worktree-residue"' "$SCRIPT" \
  && ok "v4.8.6 item-3 fix: RESIDUE_DIR still follows the same \$OUTDIR/\$NAME-\$TS pattern as V/L, covered by the same NAME_ALLOWLIST_RE gate" \
  || notok "v4.8.6 item-3 fix: RESIDUE_DIR's construction changed shape -- re-derive whether it still needs (or now lacks) the same gate coverage as V/L"


# ---------------------------------------------------------------------------
# v4.8.6 (found in the field, same day): a lane keying only on "does stdout
# contain a VERDICT= line" (not the exit code) misread a FAILED codex round
# as a real verdict, because the wrapper printed `VERDICT=$V` even when
# codex exited non-zero and $V was untrustworthy. Fixed: that path now
# prints `NO VERDICT (codex rc=N)` instead, deliberately NOT shaped like
# the real `VERDICT=<path>` line, so a naive grep cannot confuse the two.
# Proof: extract the real RC-check line, run it once with RC=0 (line must
# NOT fire) and once with RC=7 (line must fire, printing the NO-VERDICT
# form, never a VERDICT= line, and exiting 7).
# ---------------------------------------------------------------------------
extract 1667 1667 'NO VERDICT (codex rc=' "$WORK/rc_check.sh"

RC_CHECK_OUT_OK=$(
  set +e
  ( RC=0 V="$WORK/some-verdict.md" L="$WORK/some.log"
    # shellcheck source=/dev/null
    source "$WORK/helpers.sh"
    # shellcheck source=/dev/null
    source "$WORK/rc_check.sh"
    printf 'REACHED_PAST_RC_CHECK\n'
  )
)
if printf '%s' "$RC_CHECK_OUT_OK" | grep -q '^REACHED_PAST_RC_CHECK$'; then
  ok "v4.8.6 NO-VERDICT fix: RC=0 does not trigger the failure branch at all"
else
  notok "v4.8.6 NO-VERDICT fix: RC=0 unexpectedly triggered the failure branch ($RC_CHECK_OUT_OK)"
fi

set +e
RC_CHECK_OUT_FAIL=$( (
  RC=7 V="$WORK/some-verdict.md" L="$WORK/some.log"
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  # shellcheck source=/dev/null
  source "$WORK/rc_check.sh"
  printf 'REACHED_PAST_RC_CHECK\n'
) 2>&1 )
RC_CHECK_EXIT=$?
set -e
if [ "$RC_CHECK_EXIT" -eq 7 ] \
   && printf '%s' "$RC_CHECK_OUT_FAIL" | grep -q 'NO VERDICT (codex rc=7)' \
   && ! printf '%s' "$RC_CHECK_OUT_FAIL" | grep -q '^VERDICT=' \
   && ! printf '%s' "$RC_CHECK_OUT_FAIL" | grep -q 'REACHED_PAST_RC_CHECK'; then
  ok "v4.8.6 NO-VERDICT fix: a non-zero codex rc prints 'NO VERDICT (codex rc=N)', never a VERDICT= line, and exits with that rc"
else
  notok "v4.8.6 NO-VERDICT fix: wrong behaviour on RC!=0 (exit=$RC_CHECK_EXIT, out='$RC_CHECK_OUT_FAIL')"
fi

echo "----"
echo "passed=$PASS failed=$FAIL"
[ "$FAIL" -eq 0 ]

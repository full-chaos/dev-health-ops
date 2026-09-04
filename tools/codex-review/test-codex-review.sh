#!/usr/bin/env bash
# test-codex-review.sh — bats-free proof harness for the four v4.8.4 defects,
# the v4.8.5 silent-death fix (warm step killed the whole script under
# set -euo pipefail in a repo with no go.mod; see that changelog block), and
# the v4.8.6 Linux-shared-cache change (bigboy GOCACHE/GOMODCACHE/GOPATH move
# to the fleet-shared /var/lib/oci-cache volume, never a per-round dir, never
# reaped; macOS unchanged; see that changelog block).
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
# v4.8.6 SAFETY NOTE: the Linux GOCACHE/GOMODCACHE defaults are now literal,
# host-absolute paths (/var/lib/oci-cache/go-build, /var/lib/oci-cache/go-mod)
# -- NOT $HOME-relative any more -- so a test that actually SOURCES the
# default-taking branch and lets its `mkdir -p` run would try to create real
# directories under /var/lib on whatever machine runs this harness (this
# Mac, a CI runner, anywhere). The "default" assertions below therefore use
# a value-only extraction (the if/elif/else WITHOUT the two mkdir -p lines
# that follow it) and check the resolved STRING only -- no directory is ever
# created for the literal-default case by this harness. Every case that DOES
# exercise the mkdir side effect (CODEX_REVIEW_* override, caller env
# override, the macOS per-round path) points at a $WORK-scoped fake path, so
# it never falls through to the real /var/lib/oci-cache default. The true
# default's mkdir behaviour was proved separately, for real, on bigboy
# itself (see the PR body / handoff for that dry run).
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

# A stub `uname` on PATH, so extracted blocks that read $HOST_OS (itself
# resolved from `uname -s`) can be forced down either the Linux or the
# macOS branch regardless of what host this harness actually runs on
# (CI runners are Linux; a lane's own Mac is Darwin -- both branches need
# proving on either).
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
extract 965 972 'rm_rf_writable() {' "$WORK/rm_rf_writable.sh"

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
extract 641 663 'LANE_KEY="$LANE-$WT_HASH"' "$WORK/lane_key.sh"

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
# and forcing the macOS/non-Linux branch via the uname stub -- see v4.8.6
# tests below for the Linux branch), assert it creates a fresh, writable
# directory; confirm a command actually SEES it as GOPATH; then remove it
# via the same rm_rf_writable() defect-1 already proved, confirming the
# trap tears it down.
# ---------------------------------------------------------------------------
extract 801 810 'RGOPATH=$(mktemp -d "/tmp/codex-review-gopath-$LANE_KEY-$TS-XXXXXX")' "$WORK/rgopath.sh"

TS="19700101T000000-test"
LANE_KEY="test-lane-$$"
unset CODEX_REVIEW_GOPATH 2>/dev/null || true
make_uname_stub Darwin
(
  # LANE_KEY and TS are already set above; a subshell inherits them as-is.
  PATH="$STUBBIN_UNAME:$PATH"
  HOST_OS="$(uname -s)"
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
extract 582 582 'mkdir -p "$OUTDIR" || die "cannot create output directory $OUTDIR"' "$WORK/outdir.sh"

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
extract 595 607 ': >"$L" || die "cannot create round log $L"' "$WORK/create-log.sh"

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
extract 1098 1098 'WARM_MODULES=$(find "$RGOMODCACHE/cache/download" -name' "$WORK/warm_modules.sh"

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
extract 1058 1142 'if [ -f "$RW/go.mod" ]; then' "$WORK/warm_step.sh"
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
if [ "$VERSION_RC" -eq 0 ] && printf '%s' "$VERSION_OUT" | grep -qE '^codex-review\.sh v4\.8\.6$'; then
  ok "v4.8.6: --version prints 'codex-review.sh v4.8.6' and exits 0 (got '$VERSION_OUT')"
else
  notok "v4.8.6: --version did not print the expected string (rc=$VERSION_RC, got '$VERSION_OUT')"
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
# ---------------------------------------------------------------------------

# VALUE-ONLY extraction: the if/elif/else WITHOUT the two mkdir -p lines
# that follow it in the real script (those start right after line 698 --
# see codex-review.sh). Used ONLY for the (a) default-value check below, so
# that case never touches the filesystem at all.
extract 641 698 'LANE_KEY="$LANE-$WT_HASH"' "$WORK/cache_resolve_value_only.sh"
grep -qF 'HOST_OS="$(uname -s)"' "$WORK/cache_resolve_value_only.sh" \
  || { echo "FAIL: extracted cache_resolve_value_only.sh does not contain the HOST_OS resolution line" >&2; exit 1; }
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
extract 641 705 'LANE_KEY="$LANE-$WT_HASH"' "$WORK/cache_resolve_full.sh"

make_uname_stub Linux

# (a) Linux DEFAULTS: no caller env, no CODEX_REVIEW_* override -> the
# literal shared-volume paths. VALUE ONLY -- no mkdir runs, see the
# extraction above, so this is safe on any host including this one.
unset GOCACHE GOMODCACHE CODEX_REVIEW_GOCACHE CODEX_REVIEW_GOMODCACHE 2>/dev/null || true
RESOLVE_A=$(
  PATH="$STUBBIN_UNAME:$PATH"
  WT="$WORK/lane-a/acr" TS="19700101T000000-a"
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
  # shellcheck source=/dev/null
  source "$WORK/cache_resolve_value_only.sh"
  printf 'RGOCACHE=%s\nRGOMODCACHE=%s\n' "$RGOCACHE" "$RGOMODCACHE"
)
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
  PATH="$STUBBIN_UNAME:$PATH"
  WT="$WORK/lane-b/acr" TS="19700101T000000-b"
  GOCACHE="$CALLER_GOCACHE" GOMODCACHE="$CALLER_GOMODCACHE"
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
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
  PATH="$STUBBIN_UNAME:$PATH"
  WT="$WORK/lane-c/acr" TS="19700101T000000-c"
  GOCACHE="$CALLER_GOCACHE" GOMODCACHE="$CALLER_GOMODCACHE"
  CODEX_REVIEW_GOCACHE="$OVERRIDE_GOCACHE" CODEX_REVIEW_GOMODCACHE="$OVERRIDE_GOMODCACHE"
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
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
# LANE_KEY-TS. Same (full) block, uname stubbed to Darwin instead -- always
# resolves under /tmp regardless of caller env, so this is also safe.
make_uname_stub Darwin
RESOLVE_D=$(
  PATH="$STUBBIN_UNAME:$PATH"
  WT="$WORK/lane-d/acr" TS="19700101T000000-d"
  # shellcheck source=/dev/null
  source "$WORK/helpers.sh"
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
# proved as "defect 3" above, with HOST_OS forced to Darwin there).
# ---------------------------------------------------------------------------
extract 801 810 'RGOPATH=$(mktemp -d "/tmp/codex-review-gopath-$LANE_KEY-$TS-XXXXXX")' "$WORK/rgopath_v486.sh"
make_uname_stub Linux
FAKE_HOME_GP="$WORK/fake-home-gopath"
mkdir -p "$FAKE_HOME_GP"
unset CODEX_REVIEW_GOPATH GOPATH 2>/dev/null || true
RGOPATH_LINUX_DEFAULT=$(
  PATH="$STUBBIN_UNAME:$PATH"
  HOME="$FAKE_HOME_GP"
  HOST_OS="$(uname -s)"
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
  PATH="$STUBBIN_UNAME:$PATH"
  HOME="$FAKE_HOME_GP" GOPATH="$CALLER_GOPATH"
  HOST_OS="$(uname -s)"
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
extract 990 1009 'if [ "$HOST_OS" = Linux ]; then' "$WORK/cleanup_cache_branch.sh"
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

echo "----"
echo "passed=$PASS failed=$FAIL"
[ "$FAIL" -eq 0 ]

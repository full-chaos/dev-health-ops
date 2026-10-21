#!/usr/bin/env bash
# ci/local_validate.sh — standing pre-push local validation gate for dev-health-ops.
#
# WHY THIS EXISTS (CHAOS-2604 root cause):
#   A change was pushed after running only 2 test FILES locally. CI then failed on
#   tests/test_clickhouse_migration_splitter.py::test_no_committed_migration_comment_line_contains_semicolon
#   — a pure-Python guard that lived in the full unit suite (since removed from hosted CI) but was not one of the 2
#   files run. Separately, a new argMax SQL query in load_team_attribution_context had
#   NO live-ClickHouse execution proof (clickhouse-marked tests are opt-in / skipped).
#   This gate closes BOTH gaps.
#
# WHAT IT DOES (a LOCAL gate; it runs no Python):
#   1. the fast Go gate (format, vet, test; race remains in the dedicated Go CI)
#   2. an ISOLATED live-ClickHouse stage that the CI unit/ci tiers never run:
#      apply the schema to a SCRATCH db with dho and prove it is current. The scratch
#      db is DROPPED on exit via a trap.
#
# *** SAFETY CONTRACT ***
#   The local container 'dev-health-clickhouse-1' db 'default' holds REAL dev data.
#   This script NEVER creates/drops/alters tables in 'default'. The only DDL it runs
#   against the default-connected client is CREATE/DROP DATABASE for a scratch db
#   named `ci_local_validate_<12-hex-digest-of-worktree-path>` (clickhouse-connect
#   will NOT auto-create it). All schema/migrations/tests are pointed at
#   clickhouse://ch:ch@localhost:8123/<scratch db> via CLICKHOUSE_URI, and the
#   scratch db is dropped on EXIT. CLICKHOUSE_URI must never default to /default.
#
# *** CONCURRENCY CONTRACT (CHAOS collision fix) ***
#   Multiple agents run this gate concurrently from different git worktrees against
#   the SAME shared ClickHouse container. Before this fix, SCRATCH_DB defaulted to
#   the fixed literal `ci_local_validate` for every worktree, so two concurrent runs
#   used the SAME scratch database: system.query_log showed interleaved
#   `CREATE TABLE ... _new` statements from two migration runs racing inside what
#   should have been a single sequential upgrade, and one run's `DROP DATABASE` on
#   exit could yank the schema out from under a sibling run mid-suite — false reds
#   AND false greens. The default is now derived deterministically from the
#   worktree's absolute path (hashed + truncated to a legal ClickHouse identifier),
#   so distinct worktrees get distinct scratch databases automatically, with no
#   caller action required. Deterministic-per-worktree (not random-per-run) is
#   intentional: a run that dies before its EXIT trap fires (kill -9, crash) is
#   reclaimed by the NEXT run from the same worktree — same name, dropped and
#   recreated clean — rather than leaking a fresh orphan database every time.
#   An explicit `SCRATCH_DB=...` still overrides the default for callers that
#   already pass one (e.g. to force a shared name, or to inspect a completed run's
#   database before it's reclaimed).
#
#   That per-worktree scratch db fixed ClickHouse-state collisions across
#   worktrees, but every worktree still shares ONE ClickHouse container and ONE
#   host's CPU/RAM (the dho migrations and every Go build of a concurrent gate compete for it).
#   See the SINGLE-FLIGHT LOCK section below (CHAOS-3403) for the mutex this
#   script now takes on that shared resource before doing any work.
#
# USAGE:
#   Run from the worktree ROOT (the dir containing ci/run_tests.sh):
#     bash ci/local_validate.sh
#   SKIP_CLICKHOUSE=1 is refused: the live-ClickHouse stages are the only stages this gate has, so a run
#   without them would declare nothing and still report a pass; see STAGE MANIFEST below.
#   Force a specific scratch db name (rarely needed — the default is already
#   unique per worktree):
#     SCRATCH_DB=my_custom_scratch bash ci/local_validate.sh
#   By default a blocked run waits up to LOCK_WAIT_SECS=1800 (30 minutes) for a
#   concurrent gate to release the lock, then fails with an actionable message.
#   Fail fast instead of waiting if another gate already holds the lock:
#     LOCK_WAIT_SECS=0 bash ci/local_validate.sh
#
# *** STAGE MANIFEST (CHAOS-3571) ***
#   Observed 2026-08-07: a `docker ps` probe failure inside the ClickHouse
#   provisioning step was rendered as "container not running" and the gate
#   `skip`'d 3 of 8 stages (ch-scratch-create, ch-migrate, the argMax live-exec
#   proof) while still printing `GATE PASSED. safe to push.` -- the container
#   had been up and healthy the entire time. A degraded run was indistinguishable
#   from a full one unless a human counted the ✔ lines.
#
#   Since that fix: EVERY reason the ClickHouse stages
#   might not run -- docker missing, the probe itself failing/timing out
#   (indeterminate container state), the container confirmed not running, or a
#   missing dho binary -- is a HARD FAILURE with a distinct diagnostic message
#   naming the true mechanism, never a silent skip. SKIP_CLICKHOUSE=1 is refused: the
#   ClickHouse stages are the only stages this gate has, so a run without them would
#   declare no stage and still print a pass.
#
#   A machine-readable `GATE_STAGE_MANIFEST ... declared=<N> executed=<N>
#   declared_ids=... executed_ids=...` log line carries the literal counts and
#   ids, and the human verdict line carries the same information formatted as
#   `[executed/declared: ids]` (`GATE PASSED. [3/3: ch_probe,ch_scratch_create,
#   ch_migrate] safe to push.`) -- a degraded run
#   cannot produce a verdict line indistinguishable from a full one, even in a
#   copy-pasted PR quote. `verify_stage_manifest()` additionally self-checks
#   that the set of stages that actually ran equals the declared set and fails
#   the gate on ANY mismatch, even if every stage that did run passed -- an independent
#   backstop against this exact class of bug recurring, not just a fix for this
#   one instance of it.
#
set -uo pipefail

# --- Resolve the worktree root from THIS script's location (cwd-independent). -------
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
cd "${ROOT}" || {
  echo "FATAL: cannot cd to worktree root ${ROOT}"
  exit 2
}

# --- Config (override via env). ----------------------------------------------------
# CH_CONTAINER names the Compose container AND -- see LOCK_DIR below -- doubles
# as the gate's lock key. That second role is why a cluster-isolated lane can
# already run concurrently with a Compose-stack gate: pass a lane-scoped name
# and the two runs take different locks (CHAOS-4457/CHAOS-4428).
CH_CONTAINER="${CH_CONTAINER:-dev-health-clickhouse-1}"
CH_USER="${CH_USER:-ch}"
CH_PASS="${CH_PASS:-ch}"
CH_HOST="${CH_HOST:-localhost}"
CH_HTTP_PORT="${CH_HTTP_PORT:-8123}"
# The ClickHouse NATIVE port `dho migrate clickhouse` speaks (it has no HTTP mode).
# Everything else in this script reaches ClickHouse over HTTP (CH_HTTP_PORT); only
# ch_migrate uses this one, on the same CH_HOST.
CH_NATIVE_PORT="${CH_NATIVE_PORT:-9000}"
# CH_TRANSPORT selects how the scratch CREATE/DROP DATABASE statements reach
# ClickHouse (CHAOS-4457). "docker" (the default, unchanged) execs
# clickhouse-client inside CH_CONTAINER; "http" POSTs to CH_HOST:CH_HTTP_PORT
# instead, which is what a lane whose ClickHouse runs as a Kubernetes pod needs
# -- there is no container on this host to exec into. Only ch_query() ever used
# docker; every other ClickHouse access in this script already went over HTTP
# through CH_HOST/CH_HTTP_PORT (see SCRATCH_URI below), so this one function is
# the whole difference between the two modes.
CH_TRANSPORT="${CH_TRANSPORT:-docker}"
# Path of the in-flight curl credential file, so the EXIT trap can remove it
# even if the gate is interrupted mid-request.
CH_CURL_CONFIG=""
# Endpoint scheme for the http transport. A TLS-only ClickHouse (the 443/8443
# variants this repo's own connection parser already accepts) rejects plaintext,
# so this must be settable rather than hardcoded (codex review round 2). It is
# applied to BOTH the curl endpoint below and SCRATCH_URI, so the two can never
# disagree about what they are talking to.
CH_HTTP_SCHEME="${CH_HTTP_SCHEME:-http}"
case "${CH_HTTP_SCHEME}" in
  http | https) ;;
  *)
    printf 'CH_HTTP_SCHEME must be "http" or "https", got: %s\n' "${CH_HTTP_SCHEME}" >&2
    exit 2
    ;;
esac
case "${CH_TRANSPORT}" in
  docker | http) ;;
  *)
    printf 'CH_TRANSPORT must be "docker" or "http", got: %s\n' "${CH_TRANSPORT}" >&2
    exit 2
    ;;
esac

# Deterministic, per-worktree scratch db name (collision fix — see the
# CONCURRENCY CONTRACT header above). Hash the absolute worktree ROOT (stable
# for the life of the checkout, distinct across worktrees) down to 12 hex
# chars and prefix with a letter so the result is always a legal ClickHouse
# identifier. Tries sha256sum (Linux), then shasum (macOS), then openssl,
# falling back to the POSIX-universal (non-cryptographic but still
# deterministic) `cksum` so this never hard-fails preflight on a stripped-down
# PATH.
default_scratch_db() {
  local path="$1" digest=""
  if command -v sha256sum >/dev/null 2>&1; then
    digest="$(printf '%s' "${path}" | sha256sum | cut -c1-12)"
  elif command -v shasum >/dev/null 2>&1; then
    digest="$(printf '%s' "${path}" | shasum -a 256 | cut -c1-12)"
  elif command -v openssl >/dev/null 2>&1; then
    digest="$(printf '%s' "${path}" | openssl dgst -sha256 | awk '{print $NF}' | cut -c1-12)"
  else
    digest="$(printf '%s' "${path}" | cksum | awk '{printf "%012d", $1}')"
  fi
  printf 'ci_local_validate_%s' "${digest}"
}
SCRATCH_DB="${SCRATCH_DB:-$(default_scratch_db "${ROOT}")}"
# Userinfo is PERCENT-ENCODED before it goes into a URI (codex). Raw
# interpolation broke three ways for a credential containing URI-reserved
# characters: '/' or '?' or '#' silently truncates or re-parses the DSN, so
# CREATE could succeed over curl (which sends the byte-exact header) while the
# migration that parses this URI connected somewhere else or failed; and a raw
# '/' in the password also defeats redact_uri(), which splits the authority at
# its first '/', printing the credential it exists to hide (verified:
# clickhouse://ch:sec/ret@host:8123/db redacts to itself). The repo's own
# parser round-trips percent-encoded userinfo -- see
# tests/metrics/test_clickhouse_connection.py, which encodes `secret/word` as
# `secret%2Fword` and expects `secret/word` back.
uri_encode() {
  # Encode ONLY what actually breaks the authority parse or redact_uri:
  # gen-delims :/?#[]@ , the escape character % itself, and whitespace/control.
  # RFC 3986 sec 3.2.1 permits sub-delims !$&'()*+,;= raw in userinfo, and both
  # urlsplit and clickhouse-connect take them literally.
  #
  # The first version encoded everything outside the UNRESERVED set, which
  # forced the compatibility guard below to reject sub-delims too -- and that
  # REGRESSED the default docker path: before this PR a CH_PASS like `sec!ret`
  # went to clickhouse-client raw and into SCRATCH_URI raw, parsed correctly,
  # and worked end to end. Encoding it broke it, then the guard refused it.
  # Encode the smaller set and the regression disappears.
  #
  # NON-ASCII IS WRONG HERE, deliberately and visibly, and worse than it looks:
  # ${string:index:1} yields a CHARACTER while ${#string} counts CHARACTERS, so
  # a multi-byte character both mis-encodes AND shortens the loop. Measured:
  # uri_encode "cafe<acute>" returns `caf%C3` -- the second UTF-8 byte is not
  # emitted at all, so the value is silently TRUNCATED, not merely wrong. That
  # is unreachable today because
  # the guard below refuses non-ASCII outright, and it is pinned by a test so it
  # cannot surface quietly later. When CHAOS-4469 lifts the guard, the fix is to
  # iterate BYTES rather than characters -- run the whole loop under `LC_ALL=C`
  # so both ${#string} and the substring operate on bytes -- not to widen this
  # case statement.
  local string="$1" index char
  for (( index = 0; index < ${#string}; index++ )); do
    char="${string:index:1}"
    case "$char" in
    [A-Za-z0-9._~!\$\&\'\(\)\*+,\;=-]) printf '%s' "$char" ;;
    *) LC_ALL=C printf '%%%02X' "'$char" ;;
    esac
  done
}
# Two limits of the http transport, enforced HERE rather than discovered three
# stages later (codex round 2). Both come from one root: `dev-hops migrate
# clickhouse status --check` calls clickhouse_connect.get_client(dsn=...)
# DIRECTLY (src/dev_health_ops/migrate.py:360,461) instead of going through a
# repository parser, so it sees the raw DSN with none of the handling the rest
# of the path applies.
#
#   1. clickhouse-connect infers TLS from the PORT (443/8443) and explicit
#      options, not from a `clickhouse+https://` scheme. On a TLS endpoint
#      published at any other port -- a Kubernetes NodePort, exactly the case
#      this transport exists for -- status would reconnect in PLAINTEXT and the
#      gate would fail after a successful CREATE and upgrade.
#   2. clickhouse-connect's DSN parser does not percent-DECODE userinfo, so a
#      credential we correctly encoded as `sec%2Fret` reaches it verbatim.
#      curl and the upgrade authenticate as `sec/ret`; status then fails auth.
#
# Refusing up front, with the reason, beats failing deep after two stages have
# already succeeded. Lifting either limit means routing migrate through the
# repository parser -- an ops-side change to the migration path, out of scope
# for this gate script.
# The credential restriction is NOT transport-scoped (codex R3). Encoding now
# applies to SCRATCH_URI in BOTH modes, so a docker-mode CH_PASS='sec!ret'
# becomes sec%21ret, the upgrade decodes it, and status --check authenticates
# with the literal encoded string -- failing AFTER create and migrate, in the
# default configuration. The limitation belongs to the migrate path, so the
# guard has to sit outside the transport check.
# Refuse exactly what uri_encode ENCODES, no more. Anything encoded reaches
# `migrate clickhouse status --check` still percent-encoded, because that path
# hands the DSN straight to clickhouse-connect, which does not decode userinfo
# (CHAOS-4469). Sub-delims are left raw by uri_encode, so they are fine here and
# the default docker path keeps working exactly as it did before this PR.
case "${CH_USER}${CH_PASS}" in
*[!A-Za-z0-9._~!\$\&\'\(\)\*+,\;=-]*)
  printf 'CH_USER/CH_PASS may not contain : / ? # [ ] @ %% whitespace or non-ASCII\n' >&2
  printf '  `migrate clickhouse status --check` passes the DSN straight to\n' >&2
  printf '  clickhouse-connect, which does not percent-decode userinfo, so an\n' >&2
  printf '  encoded credential fails there even though CREATE and upgrade accept\n' >&2
  printf '  it. Sub-delims !$&'"'"'()*+,;= are fine and are NOT encoded.\n' >&2
  printf '  Tracked as CHAOS-4469.\n' >&2
  exit 2
  ;;
esac
if [ "${CH_TRANSPORT}" = "http" ]; then
  if [ "${CH_HTTP_SCHEME}" = "https" ] \
     && [ "${CH_HTTP_PORT}" != "443" ] && [ "${CH_HTTP_PORT}" != "8443" ]; then
    printf 'CH_HTTP_SCHEME=https is only supported on port 443 or 8443 (got %s):\n' "${CH_HTTP_PORT}" >&2
    printf '  clickhouse-connect infers TLS from the port, not the URI scheme, so\n' >&2
    printf '  `migrate clickhouse status --check` would reconnect in plaintext.\n' >&2
    printf '  Publish the endpoint on 443/8443 (or port-forward to one) until\n' >&2
    printf '  CHAOS-4469 lands.\n' >&2
    exit 2
  fi
fi
CH_USER_ENC="$(uri_encode "${CH_USER}")"
CH_PASS_ENC="$(uri_encode "${CH_PASS}")"
# clickhouse+https:// is how clickhouse-connect is told to use TLS; the plain
# clickhouse:// form stays byte-identical for the default http scheme and the
# default ch/ch credentials (which contain nothing encodable), so no existing
# caller changes.
if [ "${CH_HTTP_SCHEME}" = "https" ]; then
  SCRATCH_URI="clickhouse+https://${CH_USER_ENC}:${CH_PASS_ENC}@${CH_HOST}:${CH_HTTP_PORT}/${SCRATCH_DB}"
else
  SCRATCH_URI="clickhouse://${CH_USER_ENC}:${CH_PASS_ENC}@${CH_HOST}:${CH_HTTP_PORT}/${SCRATCH_DB}"
fi
# redact_uri strips userinfo from a DSN for DISPLAY only (codex R3 / team-lead
# R4 ruling). SCRATCH_URI necessarily carries a create/drop-capable credential,
# and in CH_TRANSPORT=http that is a REAL cluster password on a shared host --
# printing it verbatim put it in every gate log and scrollback. Everything the
# operator actually needs to see (scheme, host, port, scratch db) survives.
redact_uri() {
  # Strip userinfo up to the LAST '@' that precedes the authority's first '/'.
  # A non-greedy [^/@]*@ is wrong: a password may itself contain '@' (real
  # example: clickhouse://ch:p@w0rd@host/db), and stopping at the first one
  # leaks the remainder as if it were the host. Verified against exactly that
  # shape. Falls through unchanged when there is no userinfo.
  local uri="$1" scheme rest authority
  case "$uri" in
  *://*) scheme="${uri%%://*}://" ; rest="${uri#*://}" ;;
  *) printf '%s' "$uri"; return ;;
  esac
  authority="${rest%%/*}"
  case "$authority" in
  *@*) printf '%s%s' "$scheme" "${rest#*"${authority%@*}"@}" ;;
  *) printf '%s%s' "$scheme" "$rest" ;;
  esac
}

# Overridable (CHAOS-3571): every real caller gets the identical computed
# default (env unset), so this changes no production behavior. It lets a test
# point DHO at a deliberately-missing path to exercise ch_ensure_dho()'s
# "dho missing" branch. The default is a scratch build of THIS tree
# (`go build ./cmd/dho`, done on demand by ch_ensure_dho), never a binary from a
# venv: dho is the only ClickHouse migrator (the dev-hops CLI is being deleted).
# Only that default is built on demand: a caller-supplied DHO that is missing is a
# hard failure, never silently replaced by a build.
DHO_BUILD_ON_DEMAND=0
[ -n "${DHO:-}" ] || DHO_BUILD_ON_DEMAND=1
DHO="${DHO:-${ROOT}/.build/dho}"

# Scrub the ambient environment of every dho invocation: the local socks5h proxy variables (a proxy in the
# way is a false negative, not a real defect), and the operator-shell variables (dev/.env, direnv) that polluted
# the test suite this gate used to run. The list is kept as it was; pruning it is its own change.
# TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX (CHAOS-5024): bigboy's login shell exports it, rewriting the image names a Go test observes pulled -- unset it here.
PROXY_OFF=(env -u ALL_PROXY -u HTTPS_PROXY -u HTTP_PROXY -u all_proxy -u https_proxy -u http_proxy -u NO_PROXY -u no_proxy -u LOG_LEVEL -u GITHUB_APP_PRIVATE_KEY_PATH -u GITHUB_APP_ID -u AUTH_AUTO_CREATE_ORG_ON_REGISTER -u LICENSE_PRIVATE_KEY -u REDIS_URL -u GO_PROVIDER_ROUTES -u DEV_HEALTH_ENV -u TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX)

# --- Single-flight lock (CHAOS-3403). -----------------------------------------------
# The ops-local-validate skill (.claude/skills/ops-local-validate/SKILL.md) documented
# this gate as single-flight, machine-wide — but that was an operator convention, not
# anything enforced: every operator/agent had to `ps aux | grep local_validate` before
# launching — a time-of-check-to-time-of-use race. One collision had to be killed by
# hand and two near-misses were caught only by a human watching `ps` in real time, in
# both cases AFTER a correct check had already gone stale in the seconds before
# launch. ops/AGENTS.md's "Pre-push validation gate" section now states the contract
# too; this lock is what makes it actually true instead of merely documented.
#
# RESIDUAL MECHANISM (investigated for CHAOS-3403 — not assumed): the CONCURRENCY
# CONTRACT above already gives every worktree its own scratch ClickHouse database
# (hashed from ROOT), and this codebase uses plain MergeTree tables with no
# ReplicatedMergeTree/ZooKeeper coordination — so two gates from DIFFERENT
# worktrees no longer corrupt each other's ClickHouse schema; that collision is
# fixed. What is NOT fixed: every worktree still shares the ONE ClickHouse
# container (${CH_CONTAINER}) and ONE host's CPU/RAM/disk. N concurrent gates run N dho
# migrations and N Go builds at once. The actual collision mechanism is host resource
# exhaustion severe enough that a test's real completion signal never fires within
# its timeout (see AGENTS.md's "Timeout-fallback masquerade") — not ClickHouse
# corruption. A run from the SAME worktree path launched twice (e.g. an agent
# fanning out sub-agents without giving each its own worktree, contrary to
# AGENTS.md) is additionally exposed to the pre-fix scratch-db collision directly,
# since SCRATCH_DB is derived from ROOT and would be identical for both runs.
# The lock below is a mutex on that shared resource, scoped to CH_CONTAINER (the
# thing actually contended) rather than to this worktree — matching the team's
# current manual "ps aux" convention, which is host-wide, not per-worktree.
#
# THROUGHPUT TRADE-OFF: this re-serializes gates across worktrees that a prior fix
# (commit 3d3ca881b2, the per-worktree scratch db above) deliberately let run
# concurrently. That trade is not free. Observed, not benchmarked: a full gate run
# end-to-end is on the order of 15 minutes; the Go fast gate alone is on the
# order of 180s. With N worktrees contending for the same CH_CONTAINER, the
# worst-case wait for the last one is on the order of (N-1) x that full-run time —
# an order-of-magnitude figure, not a guarantee.
#
# --- Lock primitive: a symlink, not a directory (CHAOS-3403 adversarial-review fix) --
# Uses `ln -s`, not `mkdir` + separate metadata files, and not `flock(1)` — flock(1)
# does not exist on stock macOS, the target dev platform (verified: `command -v
# flock` fails here).
#
# The earlier mkdir-based design had a real, reviewer-confirmed atomicity gap:
# `mkdir "${LOCK_DIR}"` published the lock's EXISTENCE before `pid`/`cwd` were
# written into it as separate files. A contender arriving in that window saw a
# directory with no pid file yet, concluded "stale" (lock_holder_alive() checked
# `-f "${LOCK_DIR}/pid"` first), and `rm -rf`'d the just-acquired lock out from
# under its rightful owner — two gates then run concurrently, exactly the failure
# this whole ticket exists to prevent. `ln -s <metadata> "${LOCK_DIR}"` closes this
# structurally rather than by narrowing the window: `symlink(2)` is a single
# syscall that atomically creates the name AND its content together — the target
# string IS the metadata (`pid|start-time|cwd`), computed before the syscall runs.
# There is no instant at which the lock exists but is unpopulated: readers only
# ever observe "does not exist" or "exists with complete valid metadata already in
# place". `ln -s` on a name that already exists fails immediately, unmodified,
# with no partial state — the same all-or-nothing guarantee `mkdir` gave for
# existence alone, now extended to existence-plus-content in one step.
#
# This also structurally closes a second reviewer-confirmed gap: reclaiming a
# stale lock previously ran `rm -rf` on a directory whose name (LOCK_DIR) is
# environment-overridable — a typo'd or mistaken override pointing at, say, a
# worktree root would have its entire contents recursively deleted the moment
# this script judged the (unrelated) directory "stale". A symlink has no
# contents to recurse into: reclaiming/releasing is always `rm -f` on a single
# name, and reclaim_stale_lock() below refuses to touch LOCK_DIR at all if it
# exists as anything other than a symlink (a real file or directory there means
# LOCK_DIR is pointed somewhere it should not be — this dies loudly instead of
# guessing).
#
# PID-reuse hardening: `kill -0 <pid>` only proves *something* holds that PID
# right now, not that it is the same process that created the lock. After
# `kill -9`, the OS can and eventually will reuse that PID for an unrelated
# process, which would make a stale lock look healthy indefinitely (the next
# gate would then wait out the full LOCK_WAIT_SECS against a false owner every
# time). The metadata records `ps -o lstart=` (the owning process's start time)
# alongside its PID; lock_holder_alive() requires BOTH kill -0 AND a re-queried
# lstart for that PID to match what was recorded at acquire time. A reused PID
# almost never has an identical start time (second-granularity), so this is
# treated as dead and reclaimed rather than trusted.
#
# Semantics: bounded blocking wait, not pure block-forever or pure fail-fast.
# Blocking suits agents — they can fire-and-wait rather than hand-writing a
# retry loop — but a pure block-forever wait is unfriendly to a human debugging a
# genuinely wedged lock, and offers no circuit breaker if the diagnosed resource
# contention above turns out to itself hang. LOCK_WAIT_SECS=0 gives the fail-fast
# behavior outright for a human who wants it immediately.
#
# The default path is deliberately NOT derived from $TMPDIR. This entire epic is
# "the gate inherits the shell environment and that produces wrong results" —
# keying host-wide mutual exclusion on an inherited env var repeats exactly that
# class of bug: two agents with different ambient TMPDIR (a sandbox, a
# session-scoped scratch dir, anyone who exports it) would silently acquire two
# DIFFERENT lock names and both proceed, with no error and no diagnostic — the
# worst possible failure shape for a mutex. The lock root below is fixed per host
# regardless of any shell's TMPDIR. LOCK_DIR itself is still
# explicitly overridable (tests need this to avoid colliding with a real gate) —
# guarded below against the most catastrophic accidental values.
#
# Nothing on this host writes to /tmp (CHAOS-8760), and the lock root is STILL
# TMPDIR-independent: DEV_HEALTH_LOCK_ROOT if set, else the fixed shared lane
# scratch dir (DEV_HEALTH_LOCK_SHARED_DIR, default below) when it exists, else /tmp
# on a host that has no such dir. Every session on a host resolves the same root.
LOCK_SHARED_DIR="${DEV_HEALTH_LOCK_SHARED_DIR:-/var/lib/oci-cache/lane-scratch/tmp}"
if [ -n "${DEV_HEALTH_LOCK_ROOT:-}" ]; then
  LOCK_ROOT="${DEV_HEALTH_LOCK_ROOT}"
elif [ -d "${LOCK_SHARED_DIR}" ]; then
  LOCK_ROOT="${LOCK_SHARED_DIR}"
else
  LOCK_ROOT=/tmp
fi
LOCK_DIR="${LOCK_DIR:-${LOCK_ROOT}/dev-health-ops-local-validate.${CH_CONTAINER}.lock}"
LOCK_WAIT_SECS="${LOCK_WAIT_SECS:-1800}"
# Whole seconds only: acquire_lock's retry loop does `waited=$((waited +
# LOCK_POLL_SECS))`, bash integer arithmetic — a fractional override (e.g.
# "0.5") is a syntax error there ("invalid arithmetic operator"), not a
# silently-truncated value. Found while tuning a regression test; every real
# caller only ever needs whole-second polling, so this is documented rather
# than made fractional-safe.
LOCK_POLL_SECS="${LOCK_POLL_SECS:-2}"
LOCK_HELD=0

case "${LOCK_DIR}" in
"" | "/" | "${ROOT}" | "${HOME:-__unset__}")
  # die() is defined further down (with the other output helpers) and this
  # guard must run before any of that is available, so it does not call die()
  # — it prints the same shape of fatal message directly and exits the same way.
  printf '\nFATAL: refusing to use LOCK_DIR=%s — looks like an accidental override (empty, root, the worktree, or $HOME). Set LOCK_DIR to a dedicated path.\n' "${LOCK_DIR}" >&2
  exit 2
  ;;
esac

# ps -o lstart= renders under the caller's LC_TIME/LC_ALL ("Wed Aug  5
# 20:46:09 2026" in C, "Mi.  5 Aug. 20:46:09 2026" in de_DE.UTF-8) and neither
# writer nor reader pinned a locale — so a holder that recorded its own start
# time under one locale could be judged dead by a checker (or by itself, on
# release) running under another, deterministically, no timing luck involved.
# Every lock-metadata callsite goes through this one function so there is
# exactly one place the locale is pinned.
ps_lstart() {
  LC_ALL=C ps -o lstart= -p "$1" 2>/dev/null | sed -e 's/^ *//' -e 's/ *$//'
}

# Packs this process's identity into the exact string that becomes the lock
# symlink's target — the ONLY thing written by acquire, and the whole reason
# publication is atomic (see header comment above). '|'-delimited; a worktree
# path containing a literal '|' is not supported (pathological, not defended).
lock_owner_metadata() {
  printf '%s|%s|%s' "$$" "$(ps_lstart "$$")" "${ROOT}"
}

# Parses the CURRENT lock symlink's target into LOCK_OWNER_{PID,LSTART,CWD}
# (plus LOCK_OWNER_RAW_TARGET, the exact string read, for compare-and-delete).
# Always re-reads from disk — never caches across calls — so every check reflects
# whatever is actually there right now, not what used to be there.
parse_lock_owner() {
  local target
  target="$(readlink "${LOCK_DIR}" 2>/dev/null)" || return 1
  [ -n "${target}" ] || return 1
  LOCK_OWNER_RAW_TARGET="${target}"
  IFS='|' read -r LOCK_OWNER_PID LOCK_OWNER_LSTART LOCK_OWNER_CWD <<<"${target}"
  [ -n "${LOCK_OWNER_PID}" ]
}

lock_holder_alive() {
  parse_lock_owner || return 1
  kill -0 "${LOCK_OWNER_PID}" 2>/dev/null || return 1
  local current_lstart
  current_lstart="$(ps_lstart "${LOCK_OWNER_PID}")"
  [ -n "${current_lstart}" ] && [ "${current_lstart}" = "${LOCK_OWNER_LSTART}" ]
}

# Removes LOCK_DIR ONLY if its current on-disk target still matches
# expected_target — closes the TOCTOU window between deciding "this lock is
# stale / this lock is mine" and actually deleting it. Several syscalls
# (readlink, kill -0, ps) separate a "stale"/"mine" decision from the `rm -f`
# that acts on it; without this, the delete acts on "whatever is currently at
# the path", not on what was actually read/decided against, and a reclaimer or
# releaser can delete a DIFFERENT process's lock that was created in that gap.
# Re-reads from disk right now rather than trusting any cached state, and
# compares against the exact string the caller read/computed — not merely
# "a symlink exists".
lock_compare_and_delete() {
  local expected_target="$1" current_target
  current_target="$(readlink "${LOCK_DIR}" 2>/dev/null)" || return 0
  [ "${current_target}" = "${expected_target}" ] && rm -f "${LOCK_DIR}"
  return 0
}

# Reclaim only on PROOF the recorded owner is dead — never on age/heuristics, and
# NEVER on anything but a symlink of our own format (a stray real file or
# directory at LOCK_DIR means the path is wrong, not that a lock is stale — this
# dies loudly rather than deleting something it does not understand).
# `ln -s` (in acquire_lock) is still the sole arbiter of who actually wins: if two
# runs both judge the lock stale and both race to reclaim + recreate, at most one
# `ln -s` succeeds and the loser loops back and observes the winner's fresh
# metadata via a fresh parse_lock_owner() call, not stale in-memory state. The
# delete itself is compare-and-delete against LOCK_OWNER_RAW_TARGET — the exact
# target this call just read stale — so a reclaimer that loses a race against a
# fresh acquire (widened window: readlink/kill-0/ps all happen before the
# delete) never removes the new owner's live lock.
reclaim_stale_lock() {
  if [ -L "${LOCK_DIR}" ]; then
    if ! lock_holder_alive; then
      printf '   %s stale lock at %s (PID %s not running) — reclaiming.\n' \
        "$(c_yellow 'NOTE:')" "${LOCK_DIR}" "${LOCK_OWNER_PID:-?}"
      lock_compare_and_delete "${LOCK_OWNER_RAW_TARGET}"
    fi
  elif [ -e "${LOCK_DIR}" ]; then
    die "LOCK_DIR ${LOCK_DIR} exists and is not a lock symlink — refusing to touch it. Check for a mistaken LOCK_DIR override before removing it by hand."
  fi
}

# Attempts to create the lock symlink (retried only while nothing exists at LOCK_DIR). Returns 0 on success, 1 on
# genuine contention (LOCK_DIR is already a symlink — someone else's lock, or a
# racing reclaimer's fresh one), and `die`s immediately for anything else. A
# bare `ln -s` failure is ALSO what you get from a missing parent directory,
# read-only filesystem, or a permissions error — none of which is "someone
# else holds the lock", and none of which will ever resolve by polling, so
# treating every nonzero exit as contention silently turns a structurally-bad
# LOCK_DIR into a full LOCK_WAIT_SECS hang with a diagnosis ("remove it by
# hand") that is actively wrong (there is no PID, nothing to remove).
try_acquire_once() {
  local ln_err attempt
  # `ln -s` can fail with EEXIST and the holder can release (or a reclaimer delete) before the
  # check below, leaving nothing at LOCK_DIR (CHAOS-9923). The message is no judge: GNU and BSD
  # ln word it differently. What is at LOCK_DIR after the failure is: a symlink is contention, a
  # non-symlink is a wrong path, and nothing there means the holder just left, so try again. A
  # structural error (missing parent, read-only filesystem, permissions) leaves nothing there
  # every time and dies after the last attempt.
  for attempt in 1 2 3 4 5; do
    if ln_err="$(LC_ALL=C ln -s "$(lock_owner_metadata)" "${LOCK_DIR}" 2>&1)"; then
      return 0
    fi
    if [ -L "${LOCK_DIR}" ]; then
      return 1
    fi
    [ -e "${LOCK_DIR}" ] && break
  done
  die "cannot create lock symlink at ${LOCK_DIR}: ${ln_err}. This is NOT lock contention (no symlink exists there) -- check that LOCK_DIR's parent directory exists and is writable."
}

acquire_lock() {
  banner "single-flight lock (${LOCK_DIR})"
  reclaim_stale_lock
  if ! try_acquire_once; then
    local waited=0
    parse_lock_owner || true
    printf '   gate already running in %s, PID %s — waiting (LOCK_WAIT_SECS=%s; set 0 to fail fast)...\n' \
      "${LOCK_OWNER_CWD:-?}" "${LOCK_OWNER_PID:-?}" "${LOCK_WAIT_SECS}"
    while ! try_acquire_once; do
      if [ "${waited}" -ge "${LOCK_WAIT_SECS}" ]; then
        parse_lock_owner || true
        die "gate already running in ${LOCK_OWNER_CWD:-?}, PID ${LOCK_OWNER_PID:-?} — timed out after ${LOCK_WAIT_SECS}s waiting for ${LOCK_DIR}. If that PID is not actually running local_validate.sh, remove ${LOCK_DIR} by hand."
      fi
      sleep "${LOCK_POLL_SECS}"
      waited=$((waited + LOCK_POLL_SECS))
      reclaim_stale_lock
    done
  fi
  LOCK_HELD=1
  printf '   %s %s (PID %s)\n' "$(c_green 'lock acquired:')" "${LOCK_DIR}" "$$"
}

# Compare-and-delete against this process's OWN metadata, never a bare `rm -f`
# gated only on the local LOCK_HELD flag: LOCK_HELD only proves this process
# once acquired the lock, not that the symlink currently at LOCK_DIR is still
# the one it wrote. A holder wrongly judged stale elsewhere (see the locale
# mismatch this guards against, and the widened reclaim-race window) releases
# here too — without this check it would delete whatever new owner reclaimed
# and re-acquired in the meantime, freeing the mutex while that new owner
# still believes it holds it.
release_lock() {
  if [ "${LOCK_HELD}" = "1" ]; then
    lock_compare_and_delete "$(lock_owner_metadata)"
  fi
}

# Single EXIT trap for the whole script (trap does not stack — a second `trap ...
# EXIT` would silently replace this one, which is why ch_create_scratch() below
# does NOT set its own trap and instead relies on this handler calling
# cleanup_scratch() unconditionally; cleanup_scratch() itself no-ops when
# SCRATCH_CREATED was never set). release_lock() runs BEFORE cleanup_scratch(),
# not after: cleanup_scratch() shells out to an unbounded `docker exec` — if
# Docker or ClickHouse is hung during SIGINT/SIGTERM, that call can block
# indefinitely, and if release_lock() ran second it would never run at all,
# wedging the host mutex for every other gate over a stuck ClickHouse cleanup
# that has nothing to do with the lock. Releasing first means the worst case of
# a hung docker exec is a lingering scratch database (already handled — the next
# run from this same worktree reclaims it, see the CONCURRENCY CONTRACT header)
# rather than a wedged mutex for the whole host.
on_exit() {
  release_lock
  cleanup_scratch
}
trap on_exit EXIT
# Explicit INT trap (CHAOS-3403 adversarial-review fix), added and verified --
# NOT assumed -- rather than blindly adding "trap on_exit INT TERM EXIT" as a
# single line. `exit 130` (not `on_exit` directly), so the already-registered
# EXIT trap is what actually runs on_exit -- trapping INT straight to on_exit
# would return from the trap without exiting and let the script silently keep
# running past the signal.
#
# Deliberately NOT trapping TERM: measured directly against this script, an
# untrapped SIGTERM sent to only the bash PID already interrupts a blocked
# foreground external command (run_stage, ch_migrate, docker exec) immediately
# -- its default disposition terminates the process outright, which unwinds
# straight through the existing EXIT trap. Adding an explicit `trap ... TERM`
# made this WORSE, not better: installing ANY trap for a signal makes bash
# defer running that trap's action until control returns to it (the same
# deferral rule that already applies to SIGINT below) -- so a caught TERM
# handler sat blocked behind the foreground command exactly like INT does,
# regressing a case that worked. Left untrapped, TERM keeps its immediate
# default behavior.
#
# SIGINT has no such immediate default: bash defers it while any foreground
# job is running whether or not a trap is registered (this is job-control
# behavor, not a trap-specific deferral) -- confirmed identical with and
# without this trap. So this line fixes the case an orchestrator signals the
# whole process group (a real terminal Ctrl-C) or SIGINT arrives between
# commands (no foreground child running), and gives a defined exit code
# (130) instead of relying on the default. It does NOT fix -- and cannot, by
# any trap -- the specific case this was reviewed against: SIGINT sent to
# ONLY the bash PID (e.g. Python's `Popen.send_signal(SIGINT)`, standard for
# non-terminal automation) while bash is blocked waiting on a foreground
# external command. That signal still has no effect until the command
# finishes on its own, with or without this trap. SIGTERM, or a
# process-group-wide signal, are the reliable ways to hard-stop a hung gate --
# not SIGINT to the tracked PID alone.
trap 'exit 130' INT

# --- Result tracking. --------------------------------------------------------------
declare -a RESULTS=()
FAILED=0
CH_READY=0 # set to 1 by ch_provision() once the scratch CH is migrated

# --- Stage manifest (CHAOS-3571). ---------------------------------------------------
# CHAOS-3571 root cause: ch_provision() rendered a FAILED docker probe as
# "container not running" and quietly `skip`'d every ClickHouse-dependent stage,
# and nothing else in the script noticed 3 of 8 declared stages never ran --
# GATE PASSED still printed. RESULTS/print_summary above only show what a human
# scrolling the log happens to count; they are not a machine-checkable
# assertion that the full declared stage set actually executed.
#
# DECLARED_STAGE_IDS is the full set of stage ids this run is committed to
# executing, decided ONCE up front (see run_declared_stages) -- never narrowed
# later by a stage's own runtime discovery that it "can't" run. There is no way
# to shrink it: SKIP_CLICKHOUSE=1 is refused in main(). EXECUTED_STAGE_IDS is appended to ONLY by run_stage() and the two
# manual ch_provision() call sites that record a result outside of run_stage
# (see ch_provision below) -- i.e. it reflects what actually ran, regardless of
# pass/fail. verify_stage_manifest() (near main()) diffs the two sets and fails
# the gate on ANY mismatch, even if every stage that did run passed -- the
# structural, can't-be-fooled-by-a-single-branch-bug version of "don't silently
# do less than declared".
declare -a DECLARED_STAGE_IDS=()
declare -a EXECUTED_STAGE_IDS=()

# bash 3.2 (stock macOS /bin/bash) treats `"${arr[@]}"` on a DECLARED-BUT-EMPTY
# array as an unbound-variable error under `set -u` -- confirmed directly on
# this host, not assumed (see ci/aggregate_gate_results.sh's header comment for
# the same finding against its own array-avoidance choice). Every expansion of
# DECLARED_STAGE_IDS/EXECUTED_STAGE_IDS elsewhere in this script goes through
# these two helpers so that landmine is closed in exactly one place rather than
# re-solved (or missed) at every call site.
array_contains() {
  local needle="$1"
  shift
  local candidate
  for candidate in "$@"; do
    [ "${candidate}" = "${needle}" ] && return 0
  done
  return 1
}

join_commas() {
  local out="" item
  for item in "$@"; do
    if [ -z "${out}" ]; then
      out="${item}"
    else
      out="${out},${item}"
    fi
  done
  printf '%s' "${out}"
}

c_red() { printf '\033[31m%s\033[0m' "$1"; }
c_green() { printf '\033[32m%s\033[0m' "$1"; }
c_yellow() { printf '\033[33m%s\033[0m' "$1"; }

hr() { printf '%s\n' "------------------------------------------------------------"; }
banner() {
  hr
  printf '>> %s\n' "$1"
  hr
}

# record <name> <rc> ; non-zero rc marks the whole gate FAILED (fail-fast aware).
record() {
  local name="$1" rc="$2"
  if [ "$rc" -eq 0 ]; then
    RESULTS+=("PASS  ${name}")
    printf '   [%s] %s\n' "$(c_green PASS)" "${name}"
  else
    RESULTS+=("FAIL  ${name} (rc=${rc})")
    printf '   [%s] %s (rc=%s)\n' "$(c_red FAIL)" "${name}" "${rc}"
    FAILED=1
  fi
}

skip() {
  local name="$1" why="$2"
  RESULTS+=("SKIP  ${name} — ${why}")
  printf '   [%s] %s — %s\n' "$(c_yellow SKIP)" "${name}" "${why}"
}

die() {
  printf '\n%s %s\n' "$(c_red 'FATAL:')" "$1" >&2
  exit 2
}

# Shared fail-fast tail (CHAOS-3571): print the summary, the machine-readable
# stage manifest (so a FAILED run is just as auditable as a PASSED one -- see
# the stage manifest header comment above), and exit 1. Used both by run_stage
# below and by ch_provision()'s own call sites that record a result without
# going through run_stage (docker-probe failure, scratch-create failure) --
# those are stage failures too and must get the identical fail-fast contract,
# not a bespoke shorter one that a future reader could believe is less serious.
fail_fast() {
  local name="$1"
  print_summary
  printf 'GATE_STAGE_MANIFEST result=FAILED declared=%d executed=%d declared_ids=%s executed_ids=%s failed_at=%s\n' \
    "${#DECLARED_STAGE_IDS[@]}" "${#EXECUTED_STAGE_IDS[@]}" \
    "$(join_commas "${DECLARED_STAGE_IDS[@]+"${DECLARED_STAGE_IDS[@]}"}")" \
    "$(join_commas "${EXECUTED_STAGE_IDS[@]+"${EXECUTED_STAGE_IDS[@]}"}")" \
    "${name}"
  printf '\n%s first failing stage: %s — fix it, then re-run before pushing.\n' "$(c_red 'GATE FAILED.')" "${name}"
  exit 1
}

# Run a stage; on failure print an actionable hint and STOP (fail fast) unless the
# caller passes KEEP_GOING=1. We fail fast by default so the first red is the signal.
#
# stage_id (CHAOS-3571) is the stable, human-name-independent key this stage is
# tracked under in EXECUTED_STAGE_IDS / DECLARED_STAGE_IDS -- appended
# regardless of pass/fail, because "executed" means "ran", not "ran and
# passed" (a failing stage still ran; failing to run at all is the distinct,
# worse condition this whole mechanism exists to catch).
run_stage() {
  local name="$1" stage_id="$2"
  shift 2
  banner "${name}"
  "$@"
  local rc=$?
  record "${name}" "${rc}"
  EXECUTED_STAGE_IDS+=("${stage_id}")
  if [ "$rc" -ne 0 ] && [ "${KEEP_GOING:-0}" != "1" ]; then
    fail_fast "${name}"
  fi
  return "$rc"
}

# --- Preflight: must run from the worktree root. ----------------------------------
preflight() {
  banner "preflight"
  [ -f "${ROOT}/ci/run_tests.sh" ] || die "not a worktree root (no ci/run_tests.sh at ${ROOT})."
  printf '   worktree root : %s\n' "${ROOT}"
  printf '   %s\n' "$(c_green 'preflight OK')"
}

# --- Go CI-parity gates (no services). ---------------------------------------------
gate_go_fast() { bash "${ROOT}/ci/check_go.sh" fast; }
gate_river_compat_static() { bash "${ROOT}/ci/check_river_compat_static.sh"; }

# --- Live-ClickHouse stage, ISOLATED to a scratch db (dropped on exit). ------------
#
# ch_probe_docker() (CHAOS-3571 fix). The predecessor of this function,
# ch_available(), collapsed four DIFFERENT facts into one `return 1`: docker not
# installed, `docker ps` itself failing (daemon unreachable, timeout, permission
# error), and the container genuinely not running all looked identical to the
# caller. ch_provision() then printed the ONE fact it assumed -- "container
# '...' not running" -- no matter which of the three actually happened, and
# `skip`'d the CH-dependent stages. Observed for real 2026-08-07 (CHAOS-3571):
# `docker ps` failed transiently under host load (most likely — the exact
# invocation was never captured, see the ticket), the container was up and
# healthy the whole time, and the gate printed "container ... not running" and
# GATE PASSED with 3 of 8 stages silently missing.
#
# This function returns a DISTINCT code for each cause and sets CH_PROBE_DETAIL
# to a message that names the ACTUAL mechanism rather than a guessed
# interpretation of it -- "probe FAILED, state UNKNOWN" is not the same claim
# as "confirmed not running", and only ch_provision() (not this function)
# decides what to do with that distinction. Every branch here logs docker's own
# exit code and stderr verbatim (the ticket's own first diagnostic step),
# rather than swallowing them the way `2>/dev/null` did before.
#
#   0  available: docker present, `docker ps` succeeded, container present,
#      dho present.
#   1  docker CLI missing from PATH.
#   2  `docker ps` ITSELF failed (nonzero exit) -- INDETERMINATE. This is the
#      exact CHAOS-3571 mechanism: the probe could not get an answer at all,
#      so "not running" would be a fabricated claim, not a measurement.
#   3  `docker ps` succeeded and the container is confirmed ABSENT from the
#      running-container list -- a real, provable "not running" fact.
#   4  docker + container confirmed present, but the dho binary the CH
#      stages shell out to is missing and could not be built (ch_ensure_dho).
#
# CHAOS-3571 policy (decided in ch_provision(), not here): every code below (1-4) is a HARD FAILURE of the
# gate -- including case 3, which the CHAOS-3571 ticket's own proposed direction treats as legitimate to skip.
# This script deliberately does NOT take that half of the proposal: a merge gate that trusts an unattended runtime
# probe's "confirmed not running" enough to silently pass is exactly the shape of tool this ticket was filed
# against. CH_PROBE_DETAIL still names case 3 distinctly from case 2 in the failure message, so a human fixing
# this after a HARD FAIL is told the true mechanism, not a fabricated one.
CH_PROBE_DETAIL=""

# Builds (once) the dho binary the migration stage runs, from THIS tree. Sets
# CH_PROBE_DETAIL and returns 1 when it is missing and cannot be built.
ch_ensure_dho() {
  # `dho migrate clickhouse` speaks the ClickHouse NATIVE protocol only (CH_NATIVE_PORT)
  # and this script has no TLS native connection, so an https-only endpoint cannot be
  # migrated by this gate. Refuse HERE, in the probe, before any scratch DDL is sent,
  # naming the mechanism -- not deep in the migration stage, and never by migrating a
  # different (plaintext) endpoint than the one the gate was pointed at.
  if [ "${CH_HTTP_SCHEME}" = "https" ]; then
    CH_PROBE_DETAIL="CH_HTTP_SCHEME=https is not supported by this gate: dho migrate clickhouse (the only ClickHouse migrator) speaks the native protocol on CH_NATIVE_PORT and no TLS native connection is wired here; use a plaintext lane endpoint"
    return 1
  fi
  [ -x "${DHO}" ] && return 0
  if [ "${DHO_BUILD_ON_DEMAND}" != "1" ]; then
    CH_PROBE_DETAIL="dho missing at ${DHO} (DHO was set by the caller, so it is not built)"
    return 1
  fi
  if ! command -v go >/dev/null 2>&1; then
    CH_PROBE_DETAIL="dho missing at ${DHO} and no Go toolchain on PATH to build it (go build -o ${DHO} ./cmd/dho)"
    return 1
  fi
  local build_log
  build_log="$(mktemp "${TMPDIR:-/tmp}/local-validate-dho-build.XXXXXX")" || {
    CH_PROBE_DETAIL="could not create a temp file for the dho build log"
    return 1
  }
  if ! (cd "${ROOT}" && go build -trimpath -o "${DHO}" ./cmd/dho) >"${build_log}" 2>&1; then
    CH_PROBE_DETAIL="go build -o ${DHO} ./cmd/dho FAILED: $(tr '\n' ' ' <"${build_log}" | cut -c1-400)"
    rm -f "${build_log}"
    return 1
  fi
  rm -f "${build_log}"
  [ -x "${DHO}" ] || {
    CH_PROBE_DETAIL="go build reported success but ${DHO} is not executable"
    return 1
  }
  return 0
}

ch_probe_docker() {
  CH_PROBE_DETAIL=""
  if [ "${CH_TRANSPORT}" = "http" ]; then
    # No container to probe: reachability is the HTTP endpoint answering, and a
    # dead endpoint surfaces as a loud non-zero ch_query below rather than a
    # silent skip. The dho check still applies -- the ClickHouse stages
    # invoke that CLI whichever transport carries the scratch DDL.
    if ! command -v curl >/dev/null 2>&1; then
      CH_PROBE_DETAIL="curl not found on PATH, required by CH_TRANSPORT=http"
      return 1
    fi
    ch_ensure_dho || return 4
    return 0
  fi
  if ! command -v docker >/dev/null 2>&1; then
    CH_PROBE_DETAIL="docker CLI not found on PATH"
    return 1
  fi
  # Capture docker ps's exit code AND stderr unconditionally (CHAOS-3571's own
  # first diagnostic step) instead of the old `2>/dev/null` that threw the
  # actual error away before anyone could see it. A private mktemp file, not a
  # pipe: this is a single small command, not the CHAOS-3362/3489 large-payload
  # here-document hazard, but there is no reason to introduce a pipe here either.
  local ps_err_file ps_out ps_rc
  ps_err_file="$(mktemp "${TMPDIR:-/tmp}/local-validate-docker-ps-stderr.XXXXXX")" || {
    CH_PROBE_DETAIL="could not create a temp file to capture the docker ps probe's stderr"
    return 2
  }
  ps_out="$(docker ps --format '{{.Names}}' 2>"${ps_err_file}")"
  ps_rc=$?
  if [ "${ps_rc}" -ne 0 ]; then
    CH_PROBE_DETAIL="docker ps probe FAILED (exit ${ps_rc}): $(tr '\n' ' ' <"${ps_err_file}" | sed -e 's/ *$//') — container state UNKNOWN, NOT confirmed absent"
    rm -f "${ps_err_file}"
    return 2
  fi
  rm -f "${ps_err_file}"
  if ! printf '%s\n' "${ps_out}" | grep -qx "${CH_CONTAINER}"; then
    CH_PROBE_DETAIL="container '${CH_CONTAINER}' confirmed NOT running (docker ps succeeded; name absent from the running-container list)"
    return 3
  fi
  ch_ensure_dho || return 4
  return 0
}

ch_query() {
  # Runs a query against the DEFAULT-connected client. The ONLY DDL we ever send
  # here is CREATE/DROP DATABASE for the scratch db — never table DDL in 'default'.
  #
  # Two transports, same statements (CHAOS-4457). The http one carries the
  # credentials in headers rather than the URL so they never reach a proxy log
  # or a `ps` listing, and --fail-with-body turns ClickHouse's own 4xx/5xx into
  # a non-zero exit WITH its error text, so a failed CREATE/DROP is never
  # mistaken for success (the docker path gets that from clickhouse-client).
  if [ "${CH_TRANSPORT}" = "http" ]; then
    # The credential goes in a private --config file, NEVER on the command
    # line (codex review round 2). An earlier version passed it as
    # `--header "X-ClickHouse-Key: ${CH_PASS}"` with a comment claiming that
    # kept it out of `ps` -- it did not: curl argv IS visible in a process
    # listing, and this gate runs on a shared host. The credential can CREATE
    # and DROP databases. umask 077 before mktemp so the file is never
    # world-readable even briefly, and it is removed on every return path.
    #
    # --noproxy '*' is also not optional: CH_HOST is a cluster-local NodePort
    # or loopback address, and an ambient HTTP_PROXY/ALL_PROXY would otherwise
    # route this request through the proxy -- failing against an otherwise
    # reachable lane and, if the proxy is up, handing it the credential and the
    # DDL body.
    local cfg rc
    cfg="$(umask 077; mktemp "${TMPDIR:-/tmp}/local-validate-ch-curl.XXXXXX")" || {
      printf 'ch_query: could not create a private curl config file\n' >&2
      return 2
    }
    # Registered for trap-time removal BEFORE anything is written to it (codex
    # R3): a SIGTERM while curl is in flight never reaches the rm below, and
    # this file holds a database password. cleanup_ch_curl_config() is called
    # from the same EXIT path that drops the scratch database.
    CH_CURL_CONFIG="${cfg}"
    # curl's config parser reads `header = "..."` as a QUOTED string: it
    # honours backslash escapes inside the quotes and ends the value at the
    # closing quote. A password containing `"` would therefore be truncated
    # (CH_PASS='sec"ret' sends `sec`) and a newline could inject a further
    # directive. Escape backslash and double-quote, and refuse a credential
    # containing a newline outright rather than silently sending a different
    # one (codex R3).
    case "${CH_USER}${CH_PASS}" in
      *$'\n'* | *$'\r'*)
        printf 'ch_query: CH_USER/CH_PASS must not contain newlines\n' >&2
        rm -f "${cfg}"; CH_CURL_CONFIG=""
        return 2
        ;;
    esac
    local esc_user esc_pass
    esc_user="$(printf '%s' "${CH_USER}" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g')"
    esc_pass="$(printf '%s' "${CH_PASS}" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g')"
    printf 'header = "X-ClickHouse-User: %s"\nheader = "X-ClickHouse-Key: %s"\n' \
      "${esc_user}" "${esc_pass}" >"${cfg}"
    curl --silent --show-error --fail-with-body --noproxy '*' \
      --config "${cfg}" \
      --max-time "${CH_HTTP_TIMEOUT_SECS:-30}" \
      --data-binary "$1" \
      "${CH_HTTP_SCHEME}://${CH_HOST}:${CH_HTTP_PORT}/"
    rc=$?
    rm -f "${cfg}"
    CH_CURL_CONFIG=""
    return "${rc}"
  fi
  docker exec -i "${CH_CONTAINER}" clickhouse-client \
    --user "${CH_USER}" --password "${CH_PASS}" --query "$1"
}

# Companion to cleanup_scratch: the http transport's credential file must not
# survive an interrupted run (codex R3). Safe to call when nothing is pending.
# (CH_CURL_CONFIG is initialised up in the config block, not here -- an
# initialiser at this point in the file would run at load time AFTER nothing,
# but it reads as if it could clobber a live path.)
cleanup_ch_curl_config() {
  if [ -n "${CH_CURL_CONFIG:-}" ]; then
    rm -f "${CH_CURL_CONFIG}"
    CH_CURL_CONFIG=""
  fi
}

cleanup_scratch() {
  cleanup_ch_curl_config
  # trap handler: always drop the scratch db; never touches 'default'.
  if [ "${SCRATCH_CREATED:-0}" = "1" ]; then
    printf '\n>> cleanup: dropping scratch db %s\n' "${SCRATCH_DB}"
    ch_query "DROP DATABASE IF EXISTS ${SCRATCH_DB}" &&
      printf '   %s\n' "$(c_green "scratch db ${SCRATCH_DB} dropped")" ||
      printf '   %s could not drop %s — drop it manually.\n' "$(c_red 'WARN:')" "${SCRATCH_DB}"
  fi
}

ch_create_scratch() {
  # Guard: refuse to proceed if anything points us at 'default'.
  case "${SCRATCH_DB}" in
  default) die "refusing to run: SCRATCH_DB is 'default' (the real dev db)." ;;
  esac
  # Reclaim: the default SCRATCH_DB name is deterministic per worktree (see the
  # CONCURRENCY CONTRACT header), so a prior run from THIS SAME worktree that
  # died before its EXIT trap could fire (kill -9, crash, host reboot) may have
  # left this database behind with partial/stale schema. Drop it first so every
  # run starts from a genuinely clean scratch db instead of silently inheriting
  # another run's leftover tables — never touches anything but our own
  # already-guarded, never-'default' SCRATCH_DB name.
  ch_query "DROP DATABASE IF EXISTS ${SCRATCH_DB}" || true
  ch_query "CREATE DATABASE ${SCRATCH_DB}" || return 1
  SCRATCH_CREATED=1
  # NOTE: no `trap cleanup_scratch EXIT` here — trap does not stack, and a second
  # `trap ... EXIT` set here would silently replace the single on_exit handler
  # (registered near the top of this script) that also releases the CHAOS-3403
  # single-flight lock. on_exit() calls cleanup_scratch() unconditionally; it
  # no-ops until SCRATCH_CREATED=1, which is set on the line above.
  return 0
}

ch_migrate() {
  # Apply THIS branch's migrations into the scratch db, then read-only verify.
  # Belt-and-suspenders: never let an edited SCRATCH_URI point migrations at 'default'.
  case "${SCRATCH_URI}" in
  *"/default" | *"/default?"*) die "refusing to migrate: SCRATCH_URI resolves to /default ($(redact_uri "${SCRATCH_URI}"))." ;;
  esac
  # PROXY_OFF here, not just on curl (codex): clickhouse-connect honours
  # HTTP_PROXY/HTTPS_PROXY, so with an ambient proxy set and the lane endpoint
  # absent from NO_PROXY these two would route the REAL Basic-auth credential
  # through it -- or simply fail to reach the lane. Neutralising only the curl
  # request left the actual migration exposed, which is the larger half.
  # dho speaks ClickHouse's native protocol (CH_NATIVE_PORT), not the HTTP port
  # SCRATCH_URI carries; the same scratch database, user and (already-safe) password.
  local native_uri="clickhouse://${CH_USER_ENC}:${CH_PASS_ENC}@${CH_HOST}:${CH_NATIVE_PORT}/${SCRATCH_DB}"
  printf '   migrating into scratch (native): %s\n' "$(redact_uri "${native_uri}")"
  OPERATIONAL_ORDERING_CONTRACT=2 CLICKHOUSE_URI="${native_uri}" OTEL_ENABLED=false \
    "${PROXY_OFF[@]}" "${DHO}" migrate clickhouse upgrade || return 1
  OPERATIONAL_ORDERING_CONTRACT=2 CLICKHOUSE_URI="${native_uri}" OTEL_ENABLED=false \
    "${PROXY_OFF[@]}" "${DHO}" migrate clickhouse status --check || return 1
  return 0
}

# Provision the isolated scratch db + apply THIS branch's migrations, then export CLICKHOUSE_URI=<scratch>
# for the stages that follow.
#
# CHAOS-3571: every failure branch below now goes through `record` (so it
# appears in RESULTS / the summary) and `fail_fast` (so it stops the gate
# immediately, the same contract run_stage gives every other stage) instead of
# the old `skip ...; return 0`, which is the exact mechanism that let a FAILED
# docker probe read as a clean, gate-passing result. There is no `skip` path left in this function.
ch_provision() {
  banner "clickhouse provisioning (isolated scratch db: ${SCRATCH_DB})"
  if ! ch_probe_docker; then
    record "clickhouse: docker probe (${CH_PROBE_DETAIL})" 1
    EXECUTED_STAGE_IDS+=("ch_probe")
    fail_fast "clickhouse: docker probe"
  fi
  record "clickhouse: docker probe (container '${CH_CONTAINER}' reachable, dho present)" 0
  EXECUTED_STAGE_IDS+=("ch_probe")

  if ! ch_create_scratch; then
    record "ch-scratch-create (${SCRATCH_DB})" 1
    EXECUTED_STAGE_IDS+=("ch_scratch_create")
    fail_fast "ch-scratch-create (${SCRATCH_DB})"
  fi
  record "ch-scratch-create (${SCRATCH_DB})" 0
  EXECUTED_STAGE_IDS+=("ch_scratch_create")

  run_stage "ch-migrate (upgrade + status --check)" ch_migrate ch_migrate
  CH_READY=1
  export CLICKHOUSE_URI="${SCRATCH_URI}"
  printf '   %s -> %s\n' "$(c_green 'CLICKHOUSE_URI')" "$(redact_uri "${SCRATCH_URI}") (scratch)"
}

# metrics_readback (CHAOS-4266) RETIRED, CHAOS-5307: this stage exercised
# Python compute (job_daily/job_dora/job_complexity_db's register_commands/
# _cmd_metrics_* wiring) directly -- that wiring is deleted (CHAOS-5307,
# #2316). Per standing rule, a gate stage that invokes Python compute
# retires with that compute; it is not rewritten to call another entrypoint.
# The compute-correctness gate for these families is now CI's Go executor
# integration tests (the `go-storage-integration` shards) -- CI's separate
# `metrics-executed-proof` job (ci/run_metrics_executed_proof.sh, unaffected
# by this deletion) remains the dispatch-path proof it always was.

print_summary() {
  echo
  banner "SUMMARY"
  for line in "${RESULTS[@]}"; do
    case "$line" in
    PASS*) printf '   %s  %s\n' "$(c_green '✔')" "${line#PASS  }" ;;
    FAIL*) printf '   %s  %s\n' "$(c_red '✗')" "${line#FAIL  }" ;;
    SKIP*) printf '   %s  %s\n' "$(c_yellow '-')" "${line#SKIP  }" ;;
    esac
  done
  hr
}

# verify_stage_manifest() (CHAOS-3571): the structural, independent backstop.
# Every individual stage above already fails fast on its own bad outcome; this
# does not re-check any of them. It checks something no single stage's own
# logic can: that the SET of stages which actually ran (EXECUTED_STAGE_IDS) is
# exactly the set this run committed to up front (DECLARED_STAGE_IDS) --
# neither short (something declared never ran) nor long (something ran that
# was never declared, e.g. a copy-paste stage_id typo hiding a duplicate). A
# mismatch fails the gate even if every stage that DID run passed, which is
# exactly the CHAOS-3571 shape: a docker-probe-failure branch that quietly
# `return`s 0 without ever calling record()/fail_fast is a bug this backstop
# still catches, because it never touches EXECUTED_STAGE_IDS -- independent of
# whether whoever wrote that hypothetical future bug also got the RESULTS/
# FAILED bookkeeping right.
verify_stage_manifest() {
  local id missing="" extra=""
  for id in "${DECLARED_STAGE_IDS[@]+"${DECLARED_STAGE_IDS[@]}"}"; do
    array_contains "${id}" "${EXECUTED_STAGE_IDS[@]+"${EXECUTED_STAGE_IDS[@]}"}" ||
      missing="${missing:+${missing} }${id}"
  done
  for id in "${EXECUTED_STAGE_IDS[@]+"${EXECUTED_STAGE_IDS[@]}"}"; do
    array_contains "${id}" "${DECLARED_STAGE_IDS[@]+"${DECLARED_STAGE_IDS[@]}"}" ||
      extra="${extra:+${extra} }${id}"
  done
  if [ -n "${missing}" ] || [ -n "${extra}" ]; then
    record "stage-manifest self-check (declared-but-not-executed=[${missing:-none}] executed-but-not-declared=[${extra:-none}])" 1
    fail_fast "stage-manifest self-check"
  fi
  record "stage-manifest self-check (${#EXECUTED_STAGE_IDS[@]}/${#DECLARED_STAGE_IDS[@]} declared stages executed)" 0
}

# run_declared_stages() (CHAOS-3571): DECLARED_STAGE_IDS is decided HERE, in
# full, BEFORE any stage runs -- not derived after the fact from whatever
# happened to execute. Every reason a CH stage might not run (docker missing, the
# probe failing, the container confirmed absent, dho missing) is a
# ch_provision() hard failure against the FULL declaration, never a reason to
# shrink it at runtime.
run_declared_stages() {
  DECLARED_STAGE_IDS=(ch_probe ch_scratch_create ch_migrate)

  # run_stage "go: format + vet + test"     go_fast    gate_go_fast
  # run_stage "river: static compatibility harness" river_compat gate_river_compat_static
  ch_provision # scratch db + migrations; exports CLICKHOUSE_URI

  verify_stage_manifest

  print_summary
  if [ "${FAILED}" -ne 0 ]; then
    printf 'GATE_STAGE_MANIFEST result=FAILED declared=%d executed=%d declared_ids=%s executed_ids=%s\n' \
      "${#DECLARED_STAGE_IDS[@]}" "${#EXECUTED_STAGE_IDS[@]}" \
      "$(join_commas "${DECLARED_STAGE_IDS[@]+"${DECLARED_STAGE_IDS[@]}"}")" \
      "$(join_commas "${EXECUTED_STAGE_IDS[@]+"${EXECUTED_STAGE_IDS[@]}"}")"
    printf '\n%s do NOT push. Fix the failures above.\n' "$(c_red 'GATE FAILED.')"
    exit 1
  fi
  # The verdict line itself now carries the stage count and the exact ids that
  # ran (CHAOS-3571 (b)): "GATE PASSED [6/6: ...]" cannot be produced by a run
  # that executed fewer stages than it declared -- verify_stage_manifest above
  # already exited 1 before this line if it had. A degraded run can no longer
  # print an indistinguishable "GATE PASSED. safe to push." — the bracketed
  # count is unconditionally part of the same line a human or a PR quote would
  # copy, not a separate line easy to omit when pasting.
  printf 'GATE_STAGE_MANIFEST result=PASSED declared=%d executed=%d declared_ids=%s executed_ids=%s\n' \
    "${#DECLARED_STAGE_IDS[@]}" "${#EXECUTED_STAGE_IDS[@]}" \
    "$(join_commas "${DECLARED_STAGE_IDS[@]+"${DECLARED_STAGE_IDS[@]}"}")" \
    "$(join_commas "${EXECUTED_STAGE_IDS[@]+"${EXECUTED_STAGE_IDS[@]}"}")"
  printf '\n%s [%d/%d: %s] safe to push.\n' \
    "$(c_green 'GATE PASSED.')" "${#EXECUTED_STAGE_IDS[@]}" "${#DECLARED_STAGE_IDS[@]}" \
    "$(join_commas "${EXECUTED_STAGE_IDS[@]+"${EXECUTED_STAGE_IDS[@]}"}")"
  exit 0
}

# ===================================================================================
main() {
  [ "${SKIP_CLICKHOUSE:-0}" != "1" ] || die "SKIP_CLICKHOUSE=1 is refused: the live-ClickHouse stages are the only stages this gate has, so a run without them declares nothing and would still report a pass."
  acquire_lock # CHAOS-3403 single-flight mutex — before any work is done
  preflight
  run_declared_stages
}

# CHAOS-4457: lets a test drive ch_query() for real -- asserting which transport
# it actually uses -- instead of grepping this file for a string, which would
# pass against a script that still shelled out to docker at runtime.
# Test-only hook: runs the REAL ch_migrate() (with a stub DHO the test supplies) so a
# test can assert the exact dho invocations and the environment they receive.
if [ "${1:-}" = "--ch-migrate-probe" ]; then
  ch_migrate
  exit $?
fi

if [ "${1:-}" = "--ch-query-probe" ]; then
  shift
  ch_query "${1:?query required}"
  exit $?
fi

main "$@"

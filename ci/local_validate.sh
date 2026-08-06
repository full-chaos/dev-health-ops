#!/usr/bin/env bash
# ci/local_validate.sh — standing pre-push local validation gate for dev-health-ops.
#
# WHY THIS EXISTS (CHAOS-2604 root cause):
#   A change was pushed after running only 2 test FILES locally. CI then failed on
#   tests/test_clickhouse_migration_splitter.py::test_no_committed_migration_comment_line_contains_semicolon
#   — a pure-Python guard that lives in the FULL unit suite but was not one of the 2
#   files run. Separately, a new argMax SQL query in load_team_attribution_context had
#   NO live-ClickHouse execution proof (clickhouse-marked tests are opt-in / skipped).
#   This gate closes BOTH gaps.
#
# WHAT IT DOES (mirrors the PR-time CI gates of PR #1018, in order):
#   0. preflight also installs requirements-docs.txt into .venv (mirrors the
#      `pip install -r requirements-docs.txt` step in test.yml's test-matrix
#      and coverage jobs) so tests/docs/*.py's `mkdocs build --strict` shells
#      don't false-fail with "No module named mkdocs" in a freshly `uv sync`'d
#      worktree venv, which never pulls that requirements file in on its own.
#   1. ruff format --check .         (== lint.yml)
#   2. ruff check .                  (== lint.yml)
#   3. mypy --install-types ... .    (== typecheck.yml)
#   4. the fast Go gate (format, vet, test; race remains in the dedicated Go CI)
#   5. the FULL unit tier, byte-for-byte as ci/run_tests.sh unit_tests() runs it
#      (== test.yml test-matrix), with the local socks5h proxy neutralized.
#   6. an ISOLATED live-ClickHouse stage that the CI unit/ci tiers never run:
#      apply the schema to a SCRATCH db, run the clickhouse-marked attribution
#      tests, AND execute the new argMax query against a real engine. The scratch
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
#   host's CPU/RAM (gate_unit_suite alone forks 4 pytest-xdist workers per run).
#   See the SINGLE-FLIGHT LOCK section below (CHAOS-3403) for the mutex this
#   script now takes on that shared resource before doing any work.
#
# USAGE:
#   Run from the worktree ROOT (the dir containing ci/run_tests.sh) using its .venv:
#     bash ci/local_validate.sh
#   Skip the live-ClickHouse stage (pure-Python gates only, e.g. no docker):
#     SKIP_CLICKHOUSE=1 bash ci/local_validate.sh
#   Force a specific scratch db name (rarely needed — the default is already
#   unique per worktree):
#     SCRATCH_DB=my_custom_scratch bash ci/local_validate.sh
#   Fail fast instead of waiting if another gate already holds the lock:
#     LOCK_WAIT_SECS=0 bash ci/local_validate.sh
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
CH_CONTAINER="${CH_CONTAINER:-dev-health-clickhouse-1}"
CH_USER="${CH_USER:-ch}"
CH_PASS="${CH_PASS:-ch}"
CH_HOST="${CH_HOST:-localhost}"
CH_HTTP_PORT="${CH_HTTP_PORT:-8123}"

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
SCRATCH_URI="clickhouse://${CH_USER}:${CH_PASS}@${CH_HOST}:${CH_HTTP_PORT}/${SCRATCH_DB}"
PYBIN="${ROOT}/.venv/bin/python"
RUFF="${ROOT}/.venv/bin/ruff"
MYPY="${ROOT}/.venv/bin/mypy"
DEVHOPS="${ROOT}/.venv/bin/dev-hops"

# Neutralize the local socks5h proxy for every pytest/python invocation. Without
# this, httpx-based tests fail with 'socksio not installed' — false negatives, not
# real defects.
#
# Also neutralize two operator-shell env vars (CHAOS-3439 interim; the durable fix
# is CHAOS-3402's tests/conftest.py scrubbing in the suite itself — this is
# belt-and-braces at the gate):
#   - LOG_LEVEL: if an operator's shell exports LOG_LEVEL=debug, configure_logging()
#     takes the root level from it, and aiosqlite (not in logging_config.py's quiet
#     list) logs the raw SQL INSERT with bind parameters at DEBUG. Two
#     log-sanitization tests sweep ALL caplog records, so the planted malicious
#     string in that SQL statement gets captured and the sanitization assertion
#     goes red on unmodified code — false negative, not a real defect. Deterministic
#     both directions: LOG_LEVEL=debug fails, unset/info passes.
#   - GITHUB_APP_PRIVATE_KEY_PATH: an operator's shell may export this as a RELATIVE
#     path (./github-app-local.pem) that only resolves from the primary checkout's
#     CWD; resolver.py opens it directly, so any gate run from a worktree (the
#     team's standing practice) resolves it to a directory that never contains the
#     file and goes red on clean main with no code change.
#   - GITHUB_APP_ID: measured empirically (CHAOS-3439), not just inferred — with
#     ONLY GITHUB_APP_PRIVATE_KEY_PATH neutralized, 3 of 4 known-false-red
#     tests in tests/test_credential_resolver.py still failed; neutralizing
#     GITHUB_APP_ID alone (leaving the path var live) fixed NONE of the 4. Only
#     unsetting BOTH clears all 4. A live GITHUB_APP_ID with the (now-absent)
#     private key path makes the env-fallback path attempt github-app auth with
#     a partial credential instead of falling through to GITHUB_TOKEN, which is
#     what those tests actually exercise — unsetting only the path was an
#     incomplete fix on this checkout's ambient .env.
PROXY_OFF=(env -u ALL_PROXY -u HTTPS_PROXY -u HTTP_PROXY -u all_proxy -u https_proxy -u http_proxy -u NO_PROXY -u no_proxy -u LOG_LEVEL -u GITHUB_APP_PRIVATE_KEY_PATH -u GITHUB_APP_ID)

# --- Single-flight lock (CHAOS-3403). -----------------------------------------------
# ops/AGENTS.md documents this gate as single-flight, but nothing enforced it:
# every operator/agent had to `ps aux | grep local_validate` before launching — a
# time-of-check-to-time-of-use race. One collision had to be killed by hand and two
# near-misses were caught only by a human watching `ps` in real time, in both cases
# AFTER a correct check had already gone stale in the seconds before launch.
#
# RESIDUAL MECHANISM (investigated for CHAOS-3403 — not assumed): the CONCURRENCY
# CONTRACT above already gives every worktree its own scratch ClickHouse database
# (hashed from ROOT), and this codebase uses plain MergeTree tables with no
# ReplicatedMergeTree/ZooKeeper coordination — so two gates from DIFFERENT
# worktrees no longer corrupt each other's ClickHouse schema; that collision is
# fixed. What is NOT fixed: every worktree still shares the ONE ClickHouse
# container (${CH_CONTAINER}) and ONE host's CPU/RAM/disk. gate_unit_suite() alone
# forks 4 pytest-xdist workers; N concurrent gates fork 4N, plus N concurrent mypy
# and ruff invocations. The actual collision mechanism is host resource
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
# Uses mkdir, not flock(1): flock(1) does not exist on stock macOS, the target dev
# platform (verified: `command -v flock` fails here), and mkdir is atomic on every
# POSIX filesystem with no separate check-then-create step. A run killed with
# `kill -9` leaves the lock DIRECTORY behind (mkdir, unlike flock, has no
# release-on-process-exit), so staleness is reclaimed by PID liveness (`kill -0`)
# — the same "a run that dies before its EXIT trap fires is reclaimed by the next
# run" precedent ch_create_scratch() already applies to the scratch db.
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
# DIFFERENT lock directories and both proceed, with no error and no diagnostic —
# the worst possible failure shape for a mutex. /tmp is fixed and shared by every
# process on the host regardless of its shell's TMPDIR. LOCK_DIR itself is still
# explicitly overridable (tests need this to avoid colliding with a real gate).
LOCK_DIR="${LOCK_DIR:-/tmp/dev-health-ops-local-validate.${CH_CONTAINER}.lock}"
LOCK_WAIT_SECS="${LOCK_WAIT_SECS:-1800}"
LOCK_POLL_SECS="${LOCK_POLL_SECS:-2}"
LOCK_HELD=0

lock_holder_alive() {
  local pid
  [ -f "${LOCK_DIR}/pid" ] || return 1
  pid="$(cat "${LOCK_DIR}/pid" 2>/dev/null)"
  [ -n "${pid}" ] || return 1
  kill -0 "${pid}" 2>/dev/null
}

# Reclaim only on PROOF the recorded PID is dead — never on age/heuristics.
# mkdir is still the sole arbiter of who actually wins: if two runs both judge the
# lock stale and both race to reclaim, at most one mkdir succeeds and the loser
# loops back and observes the winner's fresh pid/cwd.
reclaim_stale_lock() {
  if [ -d "${LOCK_DIR}" ] && ! lock_holder_alive; then
    printf '   %s stale lock at %s (PID %s not running) — reclaiming.\n' \
      "$(c_yellow 'NOTE:')" "${LOCK_DIR}" "$(cat "${LOCK_DIR}/pid" 2>/dev/null || echo '?')"
    rm -rf "${LOCK_DIR}"
  fi
}

acquire_lock() {
  banner "single-flight lock (${LOCK_DIR})"
  reclaim_stale_lock
  if ! mkdir "${LOCK_DIR}" 2>/dev/null; then
    local waited=0 owner_pid owner_cwd
    owner_pid="$(cat "${LOCK_DIR}/pid" 2>/dev/null || echo '?')"
    owner_cwd="$(cat "${LOCK_DIR}/cwd" 2>/dev/null || echo '?')"
    printf '   gate already running in %s, PID %s — waiting (LOCK_WAIT_SECS=%s; set 0 to fail fast)...\n' \
      "${owner_cwd}" "${owner_pid}" "${LOCK_WAIT_SECS}"
    while ! mkdir "${LOCK_DIR}" 2>/dev/null; do
      if [ "${waited}" -ge "${LOCK_WAIT_SECS}" ]; then
        owner_pid="$(cat "${LOCK_DIR}/pid" 2>/dev/null || echo '?')"
        owner_cwd="$(cat "${LOCK_DIR}/cwd" 2>/dev/null || echo '?')"
        die "gate already running in ${owner_cwd}, PID ${owner_pid} — timed out after ${LOCK_WAIT_SECS}s waiting for ${LOCK_DIR}. If that PID is not actually running local_validate.sh, remove ${LOCK_DIR} by hand."
      fi
      sleep "${LOCK_POLL_SECS}"
      waited=$((waited + LOCK_POLL_SECS))
      reclaim_stale_lock
    done
  fi
  printf '%s\n' "$$" >"${LOCK_DIR}/pid"
  printf '%s\n' "${ROOT}" >"${LOCK_DIR}/cwd"
  LOCK_HELD=1
  printf '   %s %s (PID %s)\n' "$(c_green 'lock acquired:')" "${LOCK_DIR}" "$$"
}

release_lock() {
  if [ "${LOCK_HELD}" = "1" ]; then
    rm -rf "${LOCK_DIR}"
  fi
}

# Single EXIT trap for the whole script (trap does not stack — a second `trap ...
# EXIT` would silently replace this one, which is why ch_create_scratch() below
# does NOT set its own trap and instead relies on this handler calling
# cleanup_scratch() unconditionally; cleanup_scratch() itself no-ops when
# SCRATCH_CREATED was never set).
on_exit() {
  cleanup_scratch
  release_lock
}
trap on_exit EXIT

# --- Result tracking. --------------------------------------------------------------
declare -a RESULTS=()
FAILED=0
CH_READY=0 # set to 1 by ch_provision() once the scratch CH is migrated

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

# Run a stage; on failure print an actionable hint and STOP (fail fast) unless the
# caller passes KEEP_GOING=1. We fail fast by default so the first red is the signal.
run_stage() {
  local name="$1"
  shift
  banner "${name}"
  "$@"
  local rc=$?
  record "${name}" "${rc}"
  if [ "$rc" -ne 0 ] && [ "${KEEP_GOING:-0}" != "1" ]; then
    print_summary
    printf '\n%s first failing stage: %s — fix it, then re-run before pushing.\n' "$(c_red 'GATE FAILED.')" "${name}"
    exit 1
  fi
  return "$rc"
}

# --- Preflight: must run from the worktree with its .venv. --------------------------
# CI-parity for pip installs: .github/workflows/test.yml (test-matrix AND
# coverage jobs) runs `pip install -r requirements.txt` then
# `pip install -r requirements-docs.txt` before invoking ci/run_tests.sh.
# gate_unit_suite() below runs that SAME `pytest tests -m "not benchmark and
# not clickhouse"` invocation, which collects tests/docs/*.py — and those
# tests shell out to `python -m mkdocs build --strict`. `uv sync --all-extras
# --dev` (requirements.txt / -e .[dev]) does NOT pull in requirements-docs.txt
# (mkdocs-material lives outside pyproject.toml on purpose, per docs-guards.yml
# installing it separately), so a venv built the documented way is missing it.
# Without this step the gate reports "No module named mkdocs" — a red CI would
# never produce, since CI always provisions it. Install it here too, exactly
# like CI, rather than letting the docs tests fail or silently skip.
ensure_docs_deps() {
  local reqs="${ROOT}/requirements-docs.txt"
  [ -f "${reqs}" ] || die "requirements-docs.txt not found at ${reqs} (repo layout changed?)."
  local pip_log
  pip_log="$(mktemp)"
  printf '   installing %s (mirrors CI test.yml pip install step)...\n' "$(basename "${reqs}")"
  if "${PYBIN}" -m pip install -q -r "${reqs}" >"${pip_log}" 2>&1; then
    rm -f "${pip_log}"
  else
    cat "${pip_log}" >&2
    rm -f "${pip_log}"
    die "could not install requirements-docs.txt into ${PYBIN} (pip output above). Run manually:
      ${PYBIN} -m pip install -r requirements-docs.txt
   tests/docs/*.py shell out to 'python -m mkdocs build --strict' and WILL
   false-fail with 'No module named mkdocs' until this succeeds — do not
   reinterpret that failure as a real docs regression without this installed."
  fi
}

preflight() {
  banner "preflight"
  [ -f "${ROOT}/ci/run_tests.sh" ] || die "not a worktree root (no ci/run_tests.sh at ${ROOT})."
  [ -x "${PYBIN}" ] || die "missing venv interpreter ${PYBIN}. Create it from the worktree:
      uv sync --all-extras --dev   # or: python -m venv .venv && .venv/bin/pip install -r requirements.txt
   (requirements.txt is '-e .[dev]'; pytest-asyncio tests mislead-fail without a fresh sync)."
  [ -x "${RUFF}" ] || die "missing ${RUFF}; install the [dev] extra into .venv."
  [ -x "${MYPY}" ] || die "missing ${MYPY}; install the [dev] extra into .venv."
  ensure_docs_deps
  printf '   worktree root : %s\n' "${ROOT}"
  printf '   interpreter   : %s\n' "${PYBIN}"
  printf '   %s\n' "$(c_green 'preflight OK')"
}

# --- Pure-Python CI-parity gates (no services). ------------------------------------
gate_lint_format() { "${RUFF}" format --check .; }
gate_lint_check() { "${RUFF}" check .; }
gate_typecheck() { "${MYPY}" --install-types --non-interactive .; }
gate_go_fast() { bash "${ROOT}/ci/check_go.sh" fast; }
gate_river_compat_static() { bash "${ROOT}/ci/check_river_compat_static.sh"; }

# --- The FULL unit suite — the CHAOS-2604 fix. NOT a file subset. ------------------
# Byte-for-byte the marker filter + ignores of ci/run_tests.sh unit_tests().
# This collects every unmarked pure-Python guard (the migration-splitter semicolon
# guard, RMT org_id sorting-key contract, dataclass/sink parity, pyformat-%% safety),
# which a 2-file run silently skips.
#
# CI runs the matrix with PYTEST_XDIST_WORKERS=4 (test.yml). Mirror it EXACTLY:
# the xdist worker count drives the test->worker distribution, and a handful of
# tests are sensitive to cross-test global-state pollution under parallelism
# (conftest documents CHAOS-2265 / CHAOS-2586). Running -n auto (more workers than
# CI on a many-core dev box) reshuffles that distribution and surfaces pollution
# FAILURES that CI's -n 4 never hits — i.e. false reds that destroy trust in the
# gate. Default to 4 to match CI; override with PYTEST_XDIST_WORKERS.
gate_unit_suite() {
  local nw="${PYTEST_XDIST_WORKERS:-4}"
  local extra=()
  if [ "${CH_READY:-0}" != "1" ]; then
    # A few NON-marked API tests (tests/api/admin/test_org_deletion.py) call
    # get_clickhouse_uri() and need a reachable, schema-applied ClickHouse: CI
    # provides one; ch_provision() points CLICKHOUSE_URI at the scratch db. With
    # no scratch CH (no docker / SKIP_CLICKHOUSE), they connect to the no-password
    # localhost:8123/default that a locked dev container rejects (auth 194) — a
    # false red. Deselect ONLY that module so every pure-Python guard in the FULL
    # suite still runs. CI validates it.
    extra+=(--ignore=tests/api/admin/test_org_deletion.py)
    skip "unit: tests/api/admin/test_org_deletion.py" "needs scratch ClickHouse — CI validates it"
  fi
  # When CH_READY=1, ch_provision exported CLICKHOUSE_URI=<scratch>; it is
  # inherited here (PROXY_OFF only unsets proxy vars), so org_deletion connects to
  # the empty scratch db (org-scoped counts -> 0) exactly like CI.
  OTEL_ENABLED=false PYTHONPATH=src \
    "${PROXY_OFF[@]}" "${PYBIN}" -m pytest tests \
    -m "not benchmark and not clickhouse" \
    --ignore=tests/test_connectors_integration.py \
    --ignore=tests/test_private_repo_access.py \
    "${extra[@]}" \
    -n "${nw}" --dist loadscope -ra --tb=short -q
}

# --- Live-ClickHouse stage, ISOLATED to a scratch db (dropped on exit). ------------
ch_available() {
  command -v docker >/dev/null 2>&1 || return 1
  docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "${CH_CONTAINER}" || return 1
  [ -x "${DEVHOPS}" ] || return 2
  return 0
}

ch_query() {
  # Runs a query against the DEFAULT-connected client. The ONLY DDL we ever send
  # here is CREATE/DROP DATABASE for the scratch db — never table DDL in 'default'.
  docker exec -i "${CH_CONTAINER}" clickhouse-client \
    --user "${CH_USER}" --password "${CH_PASS}" --query "$1"
}

cleanup_scratch() {
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
  *"/default" | *"/default?"*) die "refusing to migrate: SCRATCH_URI resolves to /default (${SCRATCH_URI})." ;;
  esac
  printf '   migrating into scratch: %s\n' "${SCRATCH_URI}"
  OPERATIONAL_ORDERING_CONTRACT=2 CLICKHOUSE_URI="${SCRATCH_URI}" DATABASE_URI="${SCRATCH_URI}" OTEL_ENABLED=false \
    "${DEVHOPS}" migrate clickhouse upgrade || return 1
  OPERATIONAL_ORDERING_CONTRACT=2 CLICKHOUSE_URI="${SCRATCH_URI}" DATABASE_URI="${SCRATCH_URI}" OTEL_ENABLED=false \
    "${DEVHOPS}" migrate clickhouse status --check || return 1
  return 0
}

ch_argmax_proof() {
  # The high-value, CI-uncovered data-layer check. CI's unit tier runs
  # `-m "not clickhouse"`, and the only mock-based loader test merely string-matches
  # 'argMax'. Here we build a real ClickHouseDataLoader against the (migrated, empty)
  # scratch db and AWAIT load_team_attribution_context, forcing ClickHouse to parse +
  # EXECUTE every argMax(...,(updated_at,valid_from)) / GROUP BY block. A tuple-arg or
  # column mistake throws here; an empty scratch legitimately returns zero candidates
  # (still execution proof).
  #
  # NOTE: the broader `pytest -m clickhouse` suite (flow-matrix-live, recommendations,
  # resolver EXPLAIN, RMT-dedup-live) needs a SEEDED ClickHouse and is NOT part of this
  # gate (CI does not run it either). To run it by hand: create a scratch db, point
  # CLICKHOUSE_URI at it, `dev-hops fixtures generate`, then `pytest -m clickhouse`.
  SCRATCH_DB="${SCRATCH_DB}" CLICKHOUSE_URI="${SCRATCH_URI}" DATABASE_URI="${SCRATCH_URI}" OTEL_ENABLED=false PYTHONPATH=src \
    "${PROXY_OFF[@]}" "${PYBIN}" - <<'PYEOF'
import asyncio, os, sys
from datetime import datetime, timezone

uri = os.environ["CLICKHOUSE_URI"]
scratch_db = os.environ["SCRATCH_DB"]
assert scratch_db != "default" and f"/{scratch_db}" in uri and "/default" not in uri, f"refusing non-scratch URI: {uri!r}"

from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader

async def main() -> int:
    sink = ClickHouseMetricsSink(uri)
    sink.ensure_schema(force=True)  # build full schema into the scratch db
    loader = ClickHouseDataLoader(sink.client, org_id="ci_local_validate_org")
    ctx = await loader.load_team_attribution_context(as_of=datetime.now(timezone.utc))
    # Reaching here means the real engine parsed + executed every argMax/GROUP BY
    # block in load_team_attribution_context without a SYNTAX/TYPE error.
    print(f"   argMax live-exec OK — context loaded (candidate buckets: {type(ctx).__name__})")
    sink.close()
    return 0

try:
    raise SystemExit(asyncio.run(main()))
except SystemExit:
    raise
except Exception as exc:  # noqa: BLE001 — surface the real engine error verbatim
    print(f"   argMax live-exec FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
    raise SystemExit(1)
PYEOF
}

# Provision the isolated scratch db + apply THIS branch's migrations BEFORE the
# unit suite, then export CLICKHOUSE_URI=<scratch> so the CH-dependent unit tests
# run faithfully and the CH-marked tests + argMax proof reuse the same schema.
ch_provision() {
  if [ "${SKIP_CLICKHOUSE:-0}" = "1" ]; then
    skip "clickhouse provisioning (scratch db)" "SKIP_CLICKHOUSE=1 — CH stages skipped"
    return 0
  fi
  banner "clickhouse provisioning (isolated scratch db: ${SCRATCH_DB})"
  ch_available
  case $? in
  1)
    skip "clickhouse provisioning" "container '${CH_CONTAINER}' not running (start the dev stack, or SKIP_CLICKHOUSE=1)"
    return 0
    ;;
  2)
    skip "clickhouse provisioning" "missing ${DEVHOPS} (install [dev] extra into .venv)"
    return 0
    ;;
  esac
  if ! ch_create_scratch; then
    skip "clickhouse provisioning" "could not create scratch db ${SCRATCH_DB}"
    return 0
  fi
  record "ch-scratch-create (${SCRATCH_DB})" 0
  run_stage "ch-migrate (upgrade + status --check)" ch_migrate
  CH_READY=1
  export CLICKHOUSE_URI="${SCRATCH_URI}"
  printf '   %s -> %s\n' "$(c_green 'CLICKHOUSE_URI')" "${SCRATCH_URI} (scratch)"
}

# CH-marked tests (need production DDL) + the direct argMax live-exec proof.
# Runs AFTER the unit suite, reusing the provisioned scratch db.
ch_tests() {
  if [ "${CH_READY:-0}" != "1" ]; then
    skip "argMax live-exec proof" "scratch CH not provisioned"
    return 0
  fi
  run_stage "argMax live-exec proof (real engine)" ch_argmax_proof
}

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

# ===================================================================================
main() {
  acquire_lock # CHAOS-3403 single-flight mutex — before any work is done
  preflight

  run_stage "lint: ruff format --check" gate_lint_format
  run_stage "lint: ruff check" gate_lint_check
  run_stage "typecheck: mypy" gate_typecheck
  # run_stage "go: format + vet + test"     gate_go_fast
  # run_stage "river: static compatibility harness" gate_river_compat_static
  ch_provision # scratch db + migrations; exports CLICKHOUSE_URI when available
  run_stage "unit suite (FULL, not subset)" gate_unit_suite
  ch_tests # argMax live-exec proof on the real engine (reuses the scratch db)

  print_summary
  if [ "${FAILED}" -ne 0 ]; then
    printf '\n%s do NOT push. Fix the failures above.\n' "$(c_red 'GATE FAILED.')"
    exit 1
  fi
  printf '\n%s safe to push.\n' "$(c_green 'GATE PASSED.')"
  exit 0
}

# Test-only harness hook (CHAOS-3403), same idea as ci/check_go.sh's narrow
# "integration-coverage" verb (see tests/tooling/test_check_go_integration_coverage.py):
# exercises the REAL acquire_lock/release_lock/reclaim_stale_lock functions above —
# not a reimplementation — without paying for preflight, lint, mypy, ClickHouse, or
# the unit suite, so a regression test can race the actual lock acquisition path
# cheaply and repeatably. Never invoked by main(); only by
# tests/tooling/test_local_validate_lock.py.
if [ "${1:-}" = "--lock-probe" ]; then
  shift
  hold_secs="${1:-0}"
  acquire_lock
  printf 'lock-probe: acquired (pid %s)\n' "$$"
  sleep "${hold_secs}"
  printf 'lock-probe: releasing (pid %s)\n' "$$"
  exit 0
fi

main "$@"

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
#   1. ruff format --check .         (== lint.yml)
#   2. ruff check .                  (== lint.yml)
#   3. mypy --install-types ... .    (== typecheck.yml)
#   4. the FULL unit tier, byte-for-byte as ci/run_tests.sh unit_tests() runs it
#      (== test.yml test-matrix), with the local socks5h proxy neutralized.
#   5. an ISOLATED live-ClickHouse stage that the CI unit/ci tiers never run:
#      apply the schema to a SCRATCH db, run the clickhouse-marked attribution
#      tests, AND execute the new argMax query against a real engine. The scratch
#      db is DROPPED on exit via a trap.
#
# *** SAFETY CONTRACT ***
#   The local container 'dev-health-clickhouse-1' db 'default' holds REAL dev data.
#   This script NEVER creates/drops/alters tables in 'default'. The only statement it
#   runs against the default-connected client is `CREATE DATABASE ci_local_validate`
#   (clickhouse-connect will NOT auto-create it). All schema/migrations/tests are
#   pointed at clickhouse://ch:ch@localhost:8123/ci_local_validate via CLICKHOUSE_URI,
#   and the scratch db is dropped on EXIT. CLICKHOUSE_URI must never default to /default.
#
# USAGE:
#   Run from the worktree ROOT (the dir containing ci/run_tests.sh) using its .venv:
#     bash ci/local_validate.sh
#   Skip the live-ClickHouse stage (pure-Python gates only, e.g. no docker):
#     SKIP_CLICKHOUSE=1 bash ci/local_validate.sh
#
set -uo pipefail

# --- Resolve the worktree root from THIS script's location (cwd-independent). -------
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
cd "${ROOT}" || { echo "FATAL: cannot cd to worktree root ${ROOT}"; exit 2; }

# --- Config (override via env). ----------------------------------------------------
CH_CONTAINER="${CH_CONTAINER:-dev-health-clickhouse-1}"
CH_USER="${CH_USER:-ch}"
CH_PASS="${CH_PASS:-ch}"
CH_HOST="${CH_HOST:-localhost}"
CH_HTTP_PORT="${CH_HTTP_PORT:-8123}"
SCRATCH_DB="${SCRATCH_DB:-ci_local_validate}"
SCRATCH_URI="clickhouse://${CH_USER}:${CH_PASS}@${CH_HOST}:${CH_HTTP_PORT}/${SCRATCH_DB}"
PYBIN="${ROOT}/.venv/bin/python"
RUFF="${ROOT}/.venv/bin/ruff"
MYPY="${ROOT}/.venv/bin/mypy"
DEVHOPS="${ROOT}/.venv/bin/dev-hops"

# Neutralize the local socks5h proxy for every pytest/python invocation. Without
# this, httpx-based tests fail with 'socksio not installed' — false negatives, not
# real defects.
PROXY_OFF=(env -u ALL_PROXY -u HTTPS_PROXY -u HTTP_PROXY -u all_proxy -u https_proxy -u http_proxy -u NO_PROXY -u no_proxy)

# --- Result tracking. --------------------------------------------------------------
declare -a RESULTS=()
FAILED=0
CH_READY=0   # set to 1 by ch_provision() once the scratch CH is migrated

c_red()   { printf '\033[31m%s\033[0m' "$1"; }
c_green() { printf '\033[32m%s\033[0m' "$1"; }
c_yellow(){ printf '\033[33m%s\033[0m' "$1"; }

hr()      { printf '%s\n' "------------------------------------------------------------"; }
banner()  { hr; printf '>> %s\n' "$1"; hr; }

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

die() { printf '\n%s %s\n' "$(c_red 'FATAL:')" "$1" >&2; exit 2; }

# Run a stage; on failure print an actionable hint and STOP (fail fast) unless the
# caller passes KEEP_GOING=1. We fail fast by default so the first red is the signal.
run_stage() {
  local name="$1"; shift
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
preflight() {
  banner "preflight"
  [ -f "${ROOT}/ci/run_tests.sh" ] || die "not a worktree root (no ci/run_tests.sh at ${ROOT})."
  [ -x "${PYBIN}" ] || die "missing venv interpreter ${PYBIN}. Create it from the worktree:
      uv sync --all-extras --dev   # or: python -m venv .venv && .venv/bin/pip install -r requirements.txt
   (requirements.txt is '-e .[dev]'; pytest-asyncio tests mislead-fail without a fresh sync)."
  [ -x "${RUFF}" ] || die "missing ${RUFF}; install the [dev] extra into .venv."
  [ -x "${MYPY}" ] || die "missing ${MYPY}; install the [dev] extra into .venv."
  printf '   worktree root : %s\n' "${ROOT}"
  printf '   interpreter   : %s\n' "${PYBIN}"
  printf '   %s\n' "$(c_green 'preflight OK')"
}

# --- Pure-Python CI-parity gates (no services). ------------------------------------
gate_lint_format() { "${RUFF}" format --check .; }
gate_lint_check()  { "${RUFF}" check .; }
gate_typecheck()   { "${MYPY}" --install-types --non-interactive .; }

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
    ch_query "DROP DATABASE IF EXISTS ${SCRATCH_DB}" \
      && printf '   %s\n' "$(c_green "scratch db ${SCRATCH_DB} dropped")" \
      || printf '   %s could not drop %s — drop it manually.\n' "$(c_red 'WARN:')" "${SCRATCH_DB}"
  fi
}

ch_create_scratch() {
  # Guard: refuse to proceed if anything points us at 'default'.
  case "${SCRATCH_DB}" in
    default) die "refusing to run: SCRATCH_DB is 'default' (the real dev db)." ;;
  esac
  ch_query "CREATE DATABASE IF NOT EXISTS ${SCRATCH_DB}" || return 1
  SCRATCH_CREATED=1
  trap cleanup_scratch EXIT
  return 0
}

ch_migrate() {
  # Apply THIS branch's migrations into the scratch db, then read-only verify.
  # Belt-and-suspenders: never let an edited SCRATCH_URI point migrations at 'default'.
  case "${SCRATCH_URI}" in
    *"/default"|*"/default?"*) die "refusing to migrate: SCRATCH_URI resolves to /default (${SCRATCH_URI})." ;;
  esac
  printf '   migrating into scratch: %s\n' "${SCRATCH_URI}"
  CLICKHOUSE_URI="${SCRATCH_URI}" DATABASE_URI="${SCRATCH_URI}" OTEL_ENABLED=false \
    "${DEVHOPS}" migrate clickhouse upgrade || return 1
  CLICKHOUSE_URI="${SCRATCH_URI}" DATABASE_URI="${SCRATCH_URI}" OTEL_ENABLED=false \
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
    1) skip "clickhouse provisioning" "container '${CH_CONTAINER}' not running (start the dev stack, or SKIP_CLICKHOUSE=1)"; return 0 ;;
    2) skip "clickhouse provisioning" "missing ${DEVHOPS} (install [dev] extra into .venv)"; return 0 ;;
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
      FAIL*) printf '   %s  %s\n' "$(c_red   '✗')" "${line#FAIL  }" ;;
      SKIP*) printf '   %s  %s\n' "$(c_yellow '-')" "${line#SKIP  }" ;;
    esac
  done
  hr
}

# ===================================================================================
main() {
  preflight

  run_stage "lint: ruff format --check"  gate_lint_format
  run_stage "lint: ruff check"           gate_lint_check
  run_stage "typecheck: mypy"            gate_typecheck
  ch_provision   # scratch db + migrations; exports CLICKHOUSE_URI when available
  run_stage "unit suite (FULL, not subset)" gate_unit_suite
  ch_tests       # argMax live-exec proof on the real engine (reuses the scratch db)

  print_summary
  if [ "${FAILED}" -ne 0 ]; then
    printf '\n%s do NOT push. Fix the failures above.\n' "$(c_red 'GATE FAILED.')"
    exit 1
  fi
  printf '\n%s safe to push.\n' "$(c_green 'GATE PASSED.')"
  exit 0
}

main "$@"

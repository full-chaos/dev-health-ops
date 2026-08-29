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
#   Skip the live-ClickHouse stage (pure-Python gates only, e.g. no docker) --
#   this is now the ONLY way to run without them; see STAGE MANIFEST below:
#     SKIP_CLICKHOUSE=1 bash ci/local_validate.sh
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
#   Since that fix: without SKIP_CLICKHOUSE=1, EVERY reason the ClickHouse stages
#   might not run -- docker missing, the probe itself failing/timing out
#   (indeterminate container state), the container confirmed not running, or a
#   missing dev-hops CLI -- is a HARD FAILURE with a distinct diagnostic message
#   naming the true mechanism, never a silent skip. SKIP_CLICKHOUSE=1 is the
#   ONLY sanctioned way to run without the CH-dependent stages: it is an
#   explicit, logged, caller-initiated decision that shrinks the gate's
#   DECLARED stage set up front, not a runtime probe result the gate decided on
#   its own to trust.
#
#   A machine-readable `GATE_STAGE_MANIFEST ... declared=<N> executed=<N>
#   declared_ids=... executed_ids=...` log line carries the literal counts and
#   ids, and the human verdict line carries the same information formatted as
#   `[executed/declared: ids]` (`GATE PASSED. [8/8: lint_format,lint_check,
#   typecheck,ch_probe,ch_scratch_create,ch_migrate,unit_suite,ch_argmax_proof]
#   safe to push.`, or `[4/4: ...]` under SKIP_CLICKHOUSE=1) -- a degraded run
#   cannot produce a verdict line indistinguishable from a full one, even in a
#   copy-pasted PR quote. `verify_stage_manifest()` additionally self-checks
#   that the set of stages that actually ran equals the declared set and fails
#   the gate on ANY mismatch, even if every stage that did run passed -- an independent
#   backstop against this exact class of bug recurring, not just a fix for this
#   one instance of it. See tests/tooling/test_local_validate_stage_manifest.py.
#
set -uo pipefail

# --- Resolve the worktree root from THIS script's location (cwd-independent). -------
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd -P)"
cd "${ROOT}" || {
  echo "FATAL: cannot cd to worktree root ${ROOT}"
  exit 2
}

# --- Per-worktree uv cache (CHAOS-4411) ---------------------------------------------
# uv's default cache dir is ~/.cache/uv, shared machine-wide. Every worktree's own
# `.venv` is already isolated, but a shared cache means concurrent `uv sync` runs in
# different worktrees serialize on ~/.cache/uv/.lock (and the editable sdist build
# lock under ~/.cache/uv/sdists-v9/editable/<hash>/.lock) -- costing lanes 30+ minutes
# waiting on each other. Redirect to a per-worktree, gitignored cache dir, same
# isolation pattern as SCRATCH_DB above. This only affects a `uv` invocation that
# inherits this script's environment; it does NOT change what `uv sync` you run
# yourself before this gate (see the preflight die() message below -- export the
# same var there).
export UV_CACHE_DIR="${UV_CACHE_DIR:-${ROOT}/.uv-cache}"

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
# CH_TRANSPORT selects how the scratch CREATE/DROP DATABASE statements reach
# ClickHouse (CHAOS-4457). "docker" (the default, unchanged) execs
# clickhouse-client inside CH_CONTAINER; "http" POSTs to CH_HOST:CH_HTTP_PORT
# instead, which is what a lane whose ClickHouse runs as a Kubernetes pod needs
# -- there is no container on this host to exec into. Only ch_query() ever used
# docker; every other ClickHouse access in this script already went over HTTP
# through CH_HOST/CH_HTTP_PORT (see SCRATCH_URI below), so this one function is
# the whole difference between the two modes.
CH_TRANSPORT="${CH_TRANSPORT:-docker}"
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
SCRATCH_URI="clickhouse://${CH_USER}:${CH_PASS}@${CH_HOST}:${CH_HTTP_PORT}/${SCRATCH_DB}"
PYBIN="${ROOT}/.venv/bin/python"
RUFF="${ROOT}/.venv/bin/ruff"
MYPY="${ROOT}/.venv/bin/mypy"
# Overridable (CHAOS-3571): every real caller gets the identical computed
# default (env unset), so this changes no production behavior. It lets a test
# point DEVHOPS at a deliberately-missing path to exercise ch_probe_docker()'s
# "dev-hops CLI missing" branch without touching the real, shared venv.
DEVHOPS="${DEVHOPS:-${ROOT}/.venv/bin/dev-hops}"

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
#   - GITHUB_APP_ID: measured empirically (CHAOS-3439), not just inferred, and
#     the base matters — this was bisected by SUBTRACTING from an already
#     fully-populated ops/.env shell (GITHUB_APP_ID/CLIENT_ID/CLIENT_SECRET/
#     SLUG/CALLBACK_URL all live throughout, matching the operational case a
#     gate actually runs in), not by adding one var at a time to a clean
#     `env -i` base. From that populated base: unsetting ONLY
#     GITHUB_APP_PRIVATE_KEY_PATH left 3 of 4 known-false-red tests in
#     tests/test_credential_resolver.py still failing; unsetting ONLY
#     GITHUB_APP_ID (path still live) fixed NONE of the 4; only unsetting BOTH
#     cleared all 4. A live GITHUB_APP_ID with the path absent makes the
#     env-fallback path attempt github-app auth with a partial credential
#     instead of falling through to GITHUB_TOKEN, which is what those tests
#     actually exercise. (A clean-base bisection that adds only
#     GITHUB_APP_PRIVATE_KEY_PATH to `env -i` may reproduce all 4 failures from
#     that var alone — the two experiments start from different bases and are
#     not in tension; CLIENT_ID/CLIENT_SECRET/SLUG being simultaneously live
#     here plausibly changes which partial-credential branch is reached. Both
#     vars are neutralized here regardless of which base is authoritative.)
#
# The three below were found by an actual end-to-end gate run (CHAOS-3403), not
# inferred — 10 unit-suite failures on an unmodified checkout, all traced to
# ops/.env's ambient values and confirmed with a red/green pair each:
#   - AUTH_AUTO_CREATE_ORG_ON_REGISTER: dev/.env sets this "false" (a real
#     product feature flag — auth/config.py's auth_auto_create_org_on_register()
#     — for local guided-onboarding testing). The default is True, and
#     tests/api/auth/test_register.py + tests/api/test_new_user_journey.py
#     assert the True (auto-create) behavior. With "false" live, registration
#     silently skips org/membership creation ("registered without organization
#     for first-run onboarding") and every org_id-shaped assertion in those
#     tests fails — 8 of the 10 observed failures.
#   - LICENSE_PRIVATE_KEY: dev/.env sets this to a real dev signing key.
#     tests/test_cli_preflight.py::test_admin_license_create_is_not_a_postgres_preflight_false_positive
#     expects the CLI to fail with "LICENSE_PRIVATE_KEY" in stdout (i.e. the var
#     absent); the test's own _run_cli() already scrubs CLICKHOUSE_URI/
#     POSTGRES_URI/DATABASE_URI/DATABASE_URL/ORG_ID for exactly this reason but
#     LICENSE_PRIVATE_KEY was missing from that list, so the live key reaches
#     the subprocess and the CLI fails on "seed must be exactly 32 bytes" instead.
#   - REDIS_URL: dev/.env points this at the real shared valkey container. Unlike
#     the other two, this is a cross-TEST pollution mechanism, not a single
#     wrong-branch call: tests/test_linear_provider.py::test_429_backoff_grows_exponentially
#     passes in total isolation but fails when run with its sibling tests in the
#     same file/worker, because a live Redis carries rate-limit state between
#     tests that an isolated/fake backend would not. Confirmed red running the
#     whole file with REDIS_URL live, green with it unset — matches the same
#     class already fixed for web/ci/run_tests.sh's rate-limit.test.ts.
#     CHECKED, not assumed, before unsetting this unconditionally:
#     tests/test_external_ingest_customer_push_live.py reads REDIS_URL at
#     import and skipif's its WHOLE MODULE when absent (it needs live
#     CLICKHOUSE_URI + POSTGRES_URI + REDIS_URL together — the only module in
#     the repo that does), so an unconditional unset here could silently
#     convert "runs" into "skips" if this gate ever selected it. It does not:
#     that module is pytest.mark.clickhouse-marked, gate_unit_suite() below
#     runs only `-m "not benchmark and not clickhouse"`, and ch_tests() below
#     never runs a broader `-m clickhouse` pytest pass (only the direct
#     ch_argmax_proof script) — confirmed with --collect-only against this
#     gate's exact invocation (zero matches) and absent from full gate run
#     logs entirely (no pass/fail/skip line, meaning never collected, not
#     silently skipped). The module's own skip message says as much: "run via
#     ci/run_live_backend_e2e.sh, not ci/local_validate.sh". If this gate ever
#     starts selecting clickhouse-marked tests, this unset needs the
#     conditional-keep shape used elsewhere for the live-e2e lane (scrub by
#     default, retain when LIVE_E2E_BASE_URL is also set) instead of staying
#     unconditional.
#
#     A SEPARATE Codex-review concern on this same var was investigated and
#     REFUTED (recorded here, not silently dropped, so it isn't re-raised):
#     that unsetting REDIS_URL might remove distributed/Redis-backed rate-limit
#     coverage, leaving the suite green while a real Redis-backed code path is
#     broken. It does not. .github/workflows/test.yml's unit-tier `env:` block
#     (CLICKHOUSE_URI/POSTGRES_URI/DATABASE_URI/SECONDARY_DATABASE_URI/
#     OTEL_ENABLED/PYTEST_XDIST_WORKERS/PYTEST_ADDOPTS) sets no REDIS_URL —
#     CI's unit tier has ALWAYS run the rate limiter on memory://, never on a
#     live Redis backend. The tests that actually assert the Redis-backend
#     contract (tests/api/test_rate_limit_config.py's `_reload_rate_limit()`
#     helper, tests/test_distributed_rate_limit.py) build their OWN
#     REDIS_URL via monkeypatch/patch.dict and importlib.reload the module
#     under it, independent of whatever the ambient shell provides — so they
#     assert real Redis-backend behavior with or without this unset, and pass
#     55/55 under `env -i` with no REDIS_URL anywhere in the process
#     (confirmed independently, including a mutation kill: forcing
#     rate_limit.py's backend selection to unconditional memory:// makes two
#     of those assertions fail). Unsetting REDIS_URL here does not remove
#     coverage; it removes a DIVERGENCE — a developer's local run silently
#     exercising a real shared Redis that CI's unit tier never did, which is
#     exactly what made test_429_backoff_grows_exponentially fail locally and
#     pass in CI in the first place.
#   - GO_PROVIDER_ROUTES / DEV_HEALTH_ENV (CHAOS-3988): ops/.env's direnv setup
#     exports GO_PROVIDER_ROUTES=all and DEV_HEALTH_ENV=local for local `dev-hops`
#     CLI convenience. _provider_route_environment()
#     (workers/provider_unit_route.py:107-135) treats that pair as the "local
#     all-routes" preset and setdefaults every WORKER_<provider>_<dataset>_ENABLED
#     switch to true. tests/test_sync_units.py has 20 tests asserting the
#     default-OFF behavior of that same switch; with the preset live every one
#     goes red — an ambient-env artifact, not a real defect. `ci/check_go.sh`'s
#     live-Python-oracle stage inherits the same pair (it shells out to python3
#     with the ambient environment attached) and produces a matching false-red in
#     TestBuildScheduledPlanMatchesLivePythonPlanner; see check_go.sh's own
#     GO_ENV_OFF for that side. Confirmed by two lanes in one morning
#     (2026-08-21): one false-GREEN that masked a real switch-off case, one
#     35-test false-RED across the full gate — both traced to this exact pair,
#     both cleared the moment it was unset. CHAOS-3986 and CHAOS-3987 were filed
#     and cancelled from this same contamination before the cause was found.
PROXY_OFF=(env -u ALL_PROXY -u HTTPS_PROXY -u HTTP_PROXY -u all_proxy -u https_proxy -u http_proxy -u NO_PROXY -u no_proxy -u LOG_LEVEL -u GITHUB_APP_PRIVATE_KEY_PATH -u GITHUB_APP_ID -u AUTH_AUTO_CREATE_ORG_ON_REGISTER -u LICENSE_PRIVATE_KEY -u REDIS_URL -u GO_PROVIDER_ROUTES -u DEV_HEALTH_ENV)

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
# THROUGHPUT TRADE-OFF: this re-serializes gates across worktrees that a prior fix
# (commit 3d3ca881b2, the per-worktree scratch db above) deliberately let run
# concurrently. That trade is not free. Observed, not benchmarked: a full gate run
# end-to-end is on the order of 15 minutes; the unit-suite tier alone is on the
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
# worst possible failure shape for a mutex. /tmp is fixed and shared by every
# process on the host regardless of its shell's TMPDIR. LOCK_DIR itself is still
# explicitly overridable (tests need this to avoid colliding with a real gate) —
# guarded below against the most catastrophic accidental values.
LOCK_DIR="${LOCK_DIR:-/tmp/dev-health-ops-local-validate.${CH_CONTAINER}.lock}"
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

# Attempts to create the lock symlink exactly once. Returns 0 on success, 1 on
# genuine contention (LOCK_DIR is already a symlink — someone else's lock, or a
# racing reclaimer's fresh one), and `die`s immediately for anything else. A
# bare `ln -s` failure is ALSO what you get from a missing parent directory,
# read-only filesystem, or a permissions error — none of which is "someone
# else holds the lock", and none of which will ever resolve by polling, so
# treating every nonzero exit as contention silently turns a structurally-bad
# LOCK_DIR into a full LOCK_WAIT_SECS hang with a diagnosis ("remove it by
# hand") that is actively wrong (there is no PID, nothing to remove).
try_acquire_once() {
  local ln_err
  if ln_err="$(ln -s "$(lock_owner_metadata)" "${LOCK_DIR}" 2>&1)"; then
    return 0
  fi
  if [ -L "${LOCK_DIR}" ]; then
    return 1
  fi
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

# Temp dir holding the argMax proof program (CHAOS-3362). Empty until
# ch_argmax_proof() creates it; declared here, ABOVE the trap, so the EXIT
# handler can reference it under `set -u` even on the earliest `die`.
ARGMAX_PROOF_TMPDIR=""
cleanup_argmax_tmpdir() {
  if [ -n "${ARGMAX_PROOF_TMPDIR:-}" ] && [ -d "${ARGMAX_PROOF_TMPDIR}" ]; then
    rm -rf "${ARGMAX_PROOF_TMPDIR}"
  fi
  ARGMAX_PROOF_TMPDIR=""
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
  # Second: a local `rm -rf` of our own mktemp -d, which cannot block on anything
  # external, so it is safe to run before the unbounded docker exec below. This
  # is what stops a SIGINT/SIGTERM mid-argMax-stage from leaking the temp dir the
  # stage's own inline cleanup would otherwise have removed.
  cleanup_argmax_tmpdir
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
# later by a stage's own runtime discovery that it "can't" run. The only
# sanctioned way to shrink it is the explicit, caller-supplied SKIP_CLICKHOUSE=1
# opt-out, which removes the CH-dependent ids from the declaration itself
# BEFORE anything runs, rather than letting a stage quietly not show up after
# the fact. EXECUTED_STAGE_IDS is appended to ONLY by run_stage() and the two
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
  [ -x "${PYBIN}" ] || die "missing venv interpreter ${PYBIN}. Create it from the worktree.
   SAFE recipe (recommended first — cannot hang; pytest.ini's pythonpath=src covers imports,
   ruff/mypy don't need the project installed):
      UV_CACHE_DIR=\"\$(git rev-parse --show-toplevel)/.uv-cache\" SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0 \\
        uv sync --all-extras --dev --no-install-project
      SKIP_CLICKHOUSE=1 bash ci/local_validate.sh   # this venv has no dev-hops CLI, so ch_probe
                                                     # below cannot pass without this flag
   FULL recipe (only if you need dev-hops / the ClickHouse-dependent stages locally instead of
   via CI — CAN hang forever at 0% CPU on a wedged 'git check-attr' child, the known
   setuptools_scm worktree deadlock, CHAOS-4181/4407, unrelated to this gate):
      UV_CACHE_DIR=\"\$(git rev-parse --show-toplevel)/.uv-cache\" uv sync --all-extras --dev
   (requirements.txt is '-e .[dev]'; pytest-asyncio tests mislead-fail without a fresh sync.
   UV_CACHE_DIR keeps either recipe off the shared ~/.cache/uv/.lock other worktrees hold —
   CHAOS-4411.)"
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
#      dev-hops present.
#   1  docker CLI missing from PATH.
#   2  `docker ps` ITSELF failed (nonzero exit) -- INDETERMINATE. This is the
#      exact CHAOS-3571 mechanism: the probe could not get an answer at all,
#      so "not running" would be a fabricated claim, not a measurement.
#   3  `docker ps` succeeded and the container is confirmed ABSENT from the
#      running-container list -- a real, provable "not running" fact.
#   4  docker + container confirmed present, but the dev-hops CLI the CH
#      stages shell out to is missing from this venv.
#
# CHAOS-3571 policy (decided in ch_provision(), not here): ONLY an explicit,
# caller-supplied SKIP_CLICKHOUSE=1 may turn "CH stages did not run" into a
# clean, gate-passing SKIP. Every code below (1-4) is a HARD FAILURE of the
# gate when SKIP_CLICKHOUSE is not set -- including case 3, which the
# CHAOS-3571 ticket's own proposed direction treats as legitimate to skip.
# This script deliberately does NOT take that half of the proposal: a merge
# gate that trusts an unattended runtime probe's "confirmed not running" enough
# to silently pass is exactly the shape of tool this ticket was filed against,
# and the whole point of (a)/(c) below is that NOTHING short of the caller's
# own explicit, logged opt-in may shrink what "PASSED" claims to have run.
# CH_PROBE_DETAIL still names case 3 distinctly from case 2 in the failure
# message, so a human fixing this after a HARD FAIL is told the true
# mechanism, not a fabricated one -- that half of the ticket's ask is honored.
CH_PROBE_DETAIL=""

ch_probe_docker() {
  CH_PROBE_DETAIL=""
  if [ "${CH_TRANSPORT}" = "http" ]; then
    # No container to probe: reachability is the HTTP endpoint answering, and a
    # dead endpoint surfaces as a loud non-zero ch_query below rather than a
    # silent skip. The dev-hops check still applies -- the ClickHouse stages
    # invoke that CLI whichever transport carries the scratch DDL.
    if ! command -v curl >/dev/null 2>&1; then
      CH_PROBE_DETAIL="curl not found on PATH, required by CH_TRANSPORT=http"
      return 1
    fi
    if [ ! -x "${DEVHOPS}" ]; then
      CH_PROBE_DETAIL="dev-hops CLI missing at ${DEVHOPS} — see the CH_TRANSPORT=docker branch below for the install recipe, or run with SKIP_CLICKHOUSE=1"
      return 4
    fi
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
  if [ ! -x "${DEVHOPS}" ]; then
    CH_PROBE_DETAIL="dev-hops CLI missing at ${DEVHOPS} — either the [dev] extra was never installed, or this venv came from 'uv sync --no-install-project' (the CHAOS-4181/4407 setuptools_scm-hang workaround, which never installs the dev-hops console script); install with UV_CACHE_DIR=\"\$(git rev-parse --show-toplevel)/.uv-cache\" uv sync --all-extras --dev, or run with SKIP_CLICKHOUSE=1 to skip this stage"
    return 4
  fi
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
    # --noproxy '*' is not optional (codex review): CH_HOST is a cluster-local
    # NodePort or loopback address, and an ambient HTTP_PROXY/ALL_PROXY would
    # otherwise route this request through the proxy -- which fails against an
    # otherwise reachable lane AND, if the proxy is up, hands it the
    # X-ClickHouse-Key header and the DDL body. Never send a credential to a
    # proxy the operator did not intend to involve.
    curl --silent --show-error --fail-with-body --noproxy '*' \
      --max-time "${CH_HTTP_TIMEOUT_SECS:-30}" \
      --header "X-ClickHouse-User: ${CH_USER}" \
      --header "X-ClickHouse-Key: ${CH_PASS}" \
      --data-binary "$1" \
      "http://${CH_HOST}:${CH_HTTP_PORT}/"
    return $?
  fi
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

# The argMax proof PROGRAM, held as a shell string rather than a here-document.
#
# WHY NOT A HERE-DOCUMENT (CHAOS-3362 — this is the whole fix, do not "simplify"
# it back). Bash delivers a here-document by writing it into a pipe and only
# THEN forking the command that reads it, so the writing shell briefly holds
# both ends of that pipe itself. If the document does not fit in the pipe
# buffer, the write can never complete: nothing is reading, and because the
# writer owns the read end it cannot even get EPIPE. It blocks forever.
#
# That is not theoretical here. Measured on this host, `cat >/dev/null <<EOF`:
#
#     400 bytes -> completes in 0.3s
#     512 bytes -> BLOCKS (killed at 5s)
#    1024 bytes -> BLOCKS
#    4000 bytes -> BLOCKS
#
# while `lsof` reports the same pipes with the nominal 16384-byte capacity.
# Nominal is not actual: macOS hands out a small pipe buffer and defers
# expansion under kernel pipe-memory pressure, and a host running many
# concurrent agent sessions sits in that state persistently. So "the program is
# small, the pipe is fine" is not a defense at any size worth writing.
#
# This program is 1269 bytes, i.e. permanently over the line. That is the real
# mechanism behind CHAOS-3362's "gate hangs entering the argMax stage": the
# stage never reached ClickHouse and never even exec'd Python — the forensics on
# that ticket found the writer subshell in write() on 1802 of 1808 samples with
# fd 3 and fd 4 both on the same pipe, and no Python child at all. The argMax
# attribution was pointing at the wrong subsystem the whole time. Same class as
# CHAOS-3468 (lock-test probes blocking in write() at ~370 bytes of output).
#
# The fix is to remove the pipe, not to shrink the program: the text below is
# written to a private temp file with the `printf` BUILTIN (in-process, no pipe,
# no fork) and Python is handed that path.
#
# CONSTRAINT: this is a single-quoted shell string, so the program text must
# contain NO single-quote character. tests/tooling/test_local_validate_heredocs.py
# enforces that, that it is valid Python, and that no here-document anywhere in
# this script grows back over the measured threshold.
ARGMAX_PROOF_PY='import asyncio, os, sys
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
    #
    # ...unless nothing was awaited. Dropping the await above leaves a coroutine
    # that is never run: no query reaches ClickHouse, Python exits 0, and this
    # stage prints OK. Measured, not imagined -- the only visible trace was
    # "candidate buckets: coroutine" and a RuntimeWarning nobody reads. This
    # stage exists to prove the engine executed, so refuse to claim it did.
    if asyncio.iscoroutine(ctx):
        raise AssertionError("load_team_attribution_context was never awaited - no query reached ClickHouse")
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
'

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
  local rc
  # A private DIRECTORY, not a bare temp file: `python <script>` puts the
  # script's directory on sys.path[0] (where `python -` put the CWD). An empty
  # dir of our own is the closest safe equivalent — a shared /tmp on sys.path
  # would let any stray /tmp/*.py shadow a real module. PYTHONPATH=src below is
  # what makes dev_health_ops importable either way.
  ARGMAX_PROOF_TMPDIR="$(mktemp -d "${TMPDIR:-/tmp}/local-validate-argmax.XXXXXX")" || {
    printf '   %s could not create a temp dir for the argMax proof program.\n' "$(c_red 'FAILED:')" >&2
    return 1
  }
  # `printf` is a BUILTIN writing straight to the redirected file: no pipe, no
  # fork, nothing that can block. Using `cat >file <<EOF` here would reintroduce
  # exactly the here-document pipe this whole change exists to remove.
  if ! printf '%s' "${ARGMAX_PROOF_PY}" >"${ARGMAX_PROOF_TMPDIR}/argmax_proof.py"; then
    printf '   %s could not write the argMax proof program to %s.\n' "$(c_red 'FAILED:')" "${ARGMAX_PROOF_TMPDIR}" >&2
    cleanup_argmax_tmpdir
    return 1
  fi
  SCRATCH_DB="${SCRATCH_DB}" CLICKHOUSE_URI="${SCRATCH_URI}" DATABASE_URI="${SCRATCH_URI}" OTEL_ENABLED=false PYTHONPATH=src \
    "${PROXY_OFF[@]}" "${PYBIN}" "${ARGMAX_PROOF_TMPDIR}/argmax_proof.py"
  rc=$?
  # Capture rc FIRST: the cleanup below must never become the return value, or a
  # real argMax mismatch would be reported as a pass.
  cleanup_argmax_tmpdir
  return "${rc}"
}

# Provision the isolated scratch db + apply THIS branch's migrations BEFORE the
# unit suite, then export CLICKHOUSE_URI=<scratch> so the CH-dependent unit tests
# run faithfully and the CH-marked tests + argMax proof reuse the same schema.
#
# CHAOS-3571: every failure branch below now goes through `record` (so it
# appears in RESULTS / the summary) and `fail_fast` (so it stops the gate
# immediately, the same contract run_stage gives every other stage) instead of
# the old `skip ...; return 0`, which is the exact mechanism that let a FAILED
# docker probe read as a clean, gate-passing result. The ONLY remaining `skip`
# path left in this function is the top one: an explicit, caller-supplied
# SKIP_CLICKHOUSE=1. That is a loud, logged, opt-in decision the caller made on
# purpose -- categorically different from the gate silently deciding on its own
# that it "can't" run these stages, which is what CHAOS-3571 was filed against.
ch_provision() {
  if [ "${SKIP_CLICKHOUSE:-0}" = "1" ]; then
    skip "clickhouse provisioning (scratch db)" "SKIP_CLICKHOUSE=1 — CH stages explicitly excluded by caller (see DECLARED_STAGE_IDS)"
    return 0
  fi
  banner "clickhouse provisioning (isolated scratch db: ${SCRATCH_DB})"
  if ! ch_probe_docker; then
    record "clickhouse: docker probe (${CH_PROBE_DETAIL})" 1
    EXECUTED_STAGE_IDS+=("ch_probe")
    fail_fast "clickhouse: docker probe"
  fi
  record "clickhouse: docker probe (container '${CH_CONTAINER}' reachable, dev-hops present)" 0
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
  printf '   %s -> %s\n' "$(c_green 'CLICKHOUSE_URI')" "${SCRATCH_URI} (scratch)"
}

# CH-marked tests (need production DDL) + the direct argMax live-exec proof.
# Runs AFTER the unit suite, reusing the provisioned scratch db.
ch_tests() {
  if [ "${CH_READY:-0}" != "1" ]; then
    # CHAOS-3571: this branch is reachable ONLY via the explicit
    # SKIP_CLICKHOUSE=1 opt-out now -- ch_provision() fail_fast's the whole
    # gate on every other reason CH_READY could be unset, so CH_READY=0
    # reaching here with no opt-out set would itself be a bug in ch_provision,
    # not a legitimate runtime state. DECLARED_STAGE_IDS already excludes
    # ch_argmax_proof under SKIP_CLICKHOUSE=1 (see run_declared_stages), so
    # this skip does not need its own EXECUTED_STAGE_IDS entry.
    skip "argMax live-exec proof" "scratch CH not provisioned (SKIP_CLICKHOUSE=1)"
    return 0
  fi
  run_stage "argMax live-exec proof (real engine)" ch_argmax_proof ch_argmax_proof
}

# --- metrics_readback (CHAOS-4266): executed-proof for the metrics.daily
# family. CHAOS-4263/CHAOS-4264 ran undetected on prod and local for a week
# because every existing check asserted a job RAN (trigger fired, zero rows
# logged) rather than that it produced rows for the org it computed for
# (feedback_metrics_merge_requires_ch_readback.md). This stage seeds real
# source rows (ci runs, deployments, incidents, test results — via `fixtures
# generate` WITHOUT --with-metrics, so no derived metric table is
# pre-seeded), computes daily/dora/complexity SYNCHRONOUSLY through the real
# `dev-hops metrics` CLI, then asserts rows actually landed for the seeded
# org's live ClickHouse repo ids (ci/assert_metrics_executed_proof.py, shared
# with CI's live-e2e gate).
#
# Unlike CI's live-e2e `metrics-executed-proof` job (CHAOS-4266 item 1), this
# stage does NOT drive the Go dispatch/post-sync path — it calls the same
# compute functions the Go bridge calls, synchronously, which is the
# faithful-enough proof for a fast local gate but does not on its own catch a
# dispatch-layer defect (CHAOS-4263's wrong repo-id space reaching the
# partition, or CHAOS-4264's bridge OOM). CI is what catches those.
#
# Seeding source rows directly via `fixtures generate` (rather than through a
# real or synthetic provider sync_run, as CI's job does) is ACCEPTABLE HERE,
# specifically because this stage's job is proving compute+readback
# synchronously, not proving the sync -> dispatch -> compute chain end to
# end. Do not cite a green run of THIS stage as evidence the dispatch path
# works — only CI's `metrics-executed-proof` job is that proof.
METRICS_READBACK_ORG_ID="${METRICS_READBACK_ORG_ID:-c0ffee00-dead-4bee-8bad-f00dfeedface}"
METRICS_READBACK_REPO_NAME="${METRICS_READBACK_REPO_NAME:-ci-local-validate/metrics-readback}"

# Decide ONCE, up front (CHAOS-3571 discipline — see run_declared_stages):
# does the diff touch a path this stage exists to guard? Fails OPEN (runs the
# stage) on any git error or unresolvable base ref — being unable to tell is a
# reason to be MORE careful, never less. Deliberately not gated behind any
# "under contention" carve-out (feedback_local_verify_under_contention.md):
# chris's 2026-08-25 ruling is that a metrics-family PR merges only with a
# real ClickHouse readback in evidence, and host load does not change what
# the PR touches.
metrics_readback_diff_relevant() {
  local base changed
  base="$(git -C "${ROOT}" merge-base origin/main HEAD 2>/dev/null || true)"
  if [ -z "${base}" ]; then
    return 0 # can't resolve a base ref — run it rather than guess.
  fi
  changed="$(git -C "${ROOT}" diff --name-only "${base}"...HEAD 2>/dev/null || true)"
  if [ -z "${changed}" ]; then
    return 0 # diff failed, or genuinely empty (e.g. no commits yet) — run it.
  fi
  # Includes the oracle script and its synthetic-seeding path too (codex
  # review, CHAOS-4266) -- a PR that only touches
  # ci/assert_metrics_executed_proof.py or sync_synthetic_target itself, as
  # this one does, must not have this stage silently skip.
  #
  # CHAOS-4319: a here-string, not `printf ... | grep -qE ...`. Under this
  # script's `set -uo pipefail`, `grep -q` exits the instant it finds its
  # first match without draining the rest of stdin -- if `changed` has more
  # lines still queued, the upstream `printf` gets SIGPIPE on its next write
  # and exits 141, and pipefail reports THAT as the pipeline's status
  # instead of grep's real (matching, 0) result. The bug is silent and
  # match-position-dependent: it only fires when the match is early enough
  # in `changed` that printf is still writing when grep exits, which is
  # exactly the common case for a real PR's small file count -- this exact
  # function returned 141 (falsely "not relevant") against this ticket's
  # own diff, which very much touches internal/jobs/metrics/. A here-string
  # has no live producer process to SIGPIPE.
  grep -qE \
    '^(src/dev_health_ops/metrics/|internal/jobs/metrics/|internal/syncdispatchruntime/|src/dev_health_ops/api/internal/worker_metrics|ci/assert_metrics_executed_proof\.py|ci/run_metrics_executed_proof\.sh|src/dev_health_ops/processors/sync\.py|src/dev_health_ops/fixtures/)' \
    <<<"${changed}"
}

metrics_readback() {
  case "${SCRATCH_URI}" in
  *"/default" | *"/default?"*) die "refusing metrics_readback: SCRATCH_URI resolves to /default (${SCRATCH_URI})." ;;
  esac

  local run_start
  run_start="$("${PYBIN}" -c 'import datetime; print(datetime.datetime.now(datetime.timezone.utc).isoformat())')"

  printf '   seeding real source rows (ci runs, deployments, incidents, test results) into scratch CH — no --with-metrics\n'
  # POSTGRES_URI/DATABASE_URI/DATABASE_URL must be UNSET here, not pointed at
  # the (ClickHouse) SCRATCH_URI: fixtures/runner.py's auth/org seeding opens a
  # separate SQLAlchemy engine from whichever of those env vars it finds first,
  # and a clickhouse:// URI has no such SQLAlchemy dialect registered
  # (NoSuchModuleError). run_live_backend_e2e.sh hits the same hazard and
  # unsets all three for the identical reason -- mirrored here, not guessed.
  OPERATIONAL_ORDERING_CONTRACT=2 ORG_ID="${METRICS_READBACK_ORG_ID}" CLICKHOUSE_URI="${SCRATCH_URI}" OTEL_ENABLED=false PYTHONPATH=src \
    "${PROXY_OFF[@]}" env -u POSTGRES_URI -u DATABASE_URI -u DATABASE_URL \
    "${DEVHOPS}" fixtures generate \
    --sink "${SCRATCH_URI}" --db-type clickhouse \
    --repo-name "${METRICS_READBACK_REPO_NAME}" \
    --provider synthetic \
    --days 7 --commits-per-day 3 --pr-count 5 \
    --seed 20260825 || return 1

  printf '   computing daily/dora/complexity SYNCHRONOUSLY (dev-hops metrics ...)\n'
  # --sink here takes a BACKEND NAME ("clickhouse"), not a URI -- unlike
  # fixtures generate's --sink above. The connection itself comes from
  # CLICKHOUSE_URI (add_sink_arg / validate_sink in utils/cli.py); passing the
  # scratch URI as --sink fails closed with "Unknown sink" rather than
  # silently reading the wrong database, which is how this was caught.
  # --backfill 7 covers the whole 7-day window `fixtures generate` seeded
  # above (day-to-day variance in the fixture generator can leave any single
  # day's bucket empty for a given family, which is not the defect this stage
  # checks for) — verified locally 2026-08-25.
  OPERATIONAL_ORDERING_CONTRACT=2 ORG_ID="${METRICS_READBACK_ORG_ID}" CLICKHOUSE_URI="${SCRATCH_URI}" OTEL_ENABLED=false PYTHONPATH=src \
    "${PROXY_OFF[@]}" "${DEVHOPS}" metrics daily --backfill 7 || return 1
  OPERATIONAL_ORDERING_CONTRACT=2 ORG_ID="${METRICS_READBACK_ORG_ID}" CLICKHOUSE_URI="${SCRATCH_URI}" OTEL_ENABLED=false PYTHONPATH=src \
    "${PROXY_OFF[@]}" "${DEVHOPS}" metrics dora --backfill 7 || return 1
  OPERATIONAL_ORDERING_CONTRACT=2 ORG_ID="${METRICS_READBACK_ORG_ID}" CLICKHOUSE_URI="${SCRATCH_URI}" OTEL_ENABLED=false PYTHONPATH=src \
    "${PROXY_OFF[@]}" "${DEVHOPS}" metrics complexity || return 1

  printf '   asserting ClickHouse readback (rows with computed_at >= %s, repo_ids ⊆ repos.id)\n' "${run_start}"
  # "incident" is deliberately excluded — see CHAOS-4269. `fixtures generate`
  # DOES seed a valid operational_service_repository_mappings row (via the
  # real map_issue_incidents path); the family is still permanently zero-row
  # because active_incidents_query's `valid_from <= {as_of}` filter
  # (src/dev_health_ops/metrics/active_incidents.py:60) has no NULL-OK guard,
  # unlike the symmetric `valid_to IS NULL OR ...` clause beside it — and
  # map_issue_incidents never sets valid_from, so the mapping is silently
  # excluded (confirmed by hand: 0/7 days with 10 seeded incidents AND a
  # correct, present mapping row). That is a real bug (CHAOS-4269, filed
  # 2026-08-25), not the CHAOS-4263/4264 defect this stage exists to catch,
  # so it is cited rather than papered over or misdiagnosed as a fixture gap.
  # "file_hotspots" (compute_file_hotspots -> file_metrics_daily) is included
  # here but NOT in ci/run_metrics_executed_proof.sh's list: `fixtures
  # generate` above seeds real commit stats (--commits-per-day 3), which
  # `metrics daily` needs to compute it, but the CI job's synthetic
  # cicd/deployments/incidents/tests seeding never touches git data at all.
  PYTHONPATH=src "${PROXY_OFF[@]}" "${PYBIN}" "${ROOT}/ci/assert_metrics_executed_proof.py" \
    --clickhouse-uri "${SCRATCH_URI}" \
    --org-id "${METRICS_READBACK_ORG_ID}" \
    --run-start "${run_start}" \
    --families cicd deploy testops_pipeline testops_test repo_user_commit dora complexity file_hotspots
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
# happened to execute. The only branch is the explicit SKIP_CLICKHOUSE=1
# opt-out; every other reason a CH stage might not run (docker missing, the
# probe failing, the container confirmed absent, dev-hops missing) is a
# ch_provision() hard failure against the FULL 8-id declaration, never a
# reason to shrink it at runtime. Factored out of main() so the CHAOS-3571
# `--stage-manifest-probe` test-only hook near the bottom of this file can
# exercise this exact sequencing (with the expensive leaf stages stubbed) --
# same precedent as `--lock-probe` exercising the real acquire_lock/
# release_lock without paying for preflight/lint/mypy/the unit suite.
run_declared_stages() {
  local metrics_readback_needed=0
  if [ "${SKIP_CLICKHOUSE:-0}" != "1" ] && metrics_readback_diff_relevant; then
    metrics_readback_needed=1
  fi

  if [ "${SKIP_CLICKHOUSE:-0}" = "1" ]; then
    DECLARED_STAGE_IDS=(lint_format lint_check typecheck unit_suite)
  elif [ "${metrics_readback_needed}" = "1" ]; then
    DECLARED_STAGE_IDS=(lint_format lint_check typecheck ch_probe ch_scratch_create ch_migrate metrics_readback unit_suite ch_argmax_proof)
  else
    DECLARED_STAGE_IDS=(lint_format lint_check typecheck ch_probe ch_scratch_create ch_migrate unit_suite ch_argmax_proof)
  fi

  run_stage "lint: ruff format --check" lint_format gate_lint_format
  run_stage "lint: ruff check" lint_check gate_lint_check
  run_stage "typecheck: mypy" typecheck gate_typecheck
  # run_stage "go: format + vet + test"     go_fast    gate_go_fast
  # run_stage "river: static compatibility harness" river_compat gate_river_compat_static
  ch_provision # scratch db + migrations; exports CLICKHOUSE_URI when available
  if [ "${metrics_readback_needed}" = "1" ]; then
    run_stage "metrics executed-proof readback (CHAOS-4266)" metrics_readback metrics_readback
  else
    skip "metrics executed-proof readback (CHAOS-4266)" "diff does not touch src/dev_health_ops/metrics, internal/jobs/metrics, internal/syncdispatchruntime, or the metrics bridge (or SKIP_CLICKHOUSE=1)"
  fi
  run_stage "unit suite (FULL, not subset)" unit_suite gate_unit_suite
  ch_tests # argMax live-exec proof on the real engine (reuses the scratch db)

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
  # ran (CHAOS-3571 (b)): "GATE PASSED [8/8: ...]" cannot be produced by a run
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
  acquire_lock # CHAOS-3403 single-flight mutex — before any work is done
  preflight
  run_declared_stages
}

# High-resolution timestamp for --lock-probe, in order of preference: bash's
# own EPOCHREALTIME builtin (no fork); `date +%s.%N` where it is actually
# interpreted (GNU date, and modern BSD date); python3 -- available under this
# harness's PATH and virtually every CI image, and unlike EPOCHREALTIME/date it
# cannot silently degrade to whole seconds; perl's Time::HiRes as a last resort.
#
# This exists because whole-second resolution doesn't just make the probe
# coarser -- it makes the tests that consume it UNFALSIFIABLE. Those tests
# assert non-overlapping [acquired, released) intervals specifically so a
# reintroduced microsecond-scale double-acquisition bug can be caught; at
# whole-second resolution two racing acquisitions round to IDENTICAL integer
# timestamps, and `a_end <= b_start` treats equal integers as "no overlap" --
# i.e. PASS. That failure mode is silent (a test that cannot fail reads as
# coverage), so a source that can't actually produce sub-second resolution
# must FAIL LOUDLY here rather than quietly handing back whole seconds.

# Accepts only a string that is purely digits-and-a-single-dot (e.g.
# "1785988080.123456") -- rejects empty output, a literal "N" (old BSD date's
# unsubstituted %N), and anything else a fallback might emit on failure.
# `command -v` / a zero exit status from a fallback proves the TOOL exists,
# not that its OUTPUT this time was actually a valid high-res timestamp --
# every fallback below is validated the same way rather than trusted blind.
_looks_like_subsecond_time() {
  case "$1" in
  '' | *[!0-9.]*) return 1 ;;
  *.*) return 0 ;;
  *) return 1 ;;
  esac
}

lock_probe_time() {
  if [ -n "${EPOCHREALTIME:-}" ]; then
    printf '%s' "${EPOCHREALTIME}"
    return 0
  fi
  local t
  t="$(date +%s.%N 2>/dev/null)"
  if _looks_like_subsecond_time "${t}"; then
    printf '%s' "${t}"
    return 0
  fi
  if command -v python3 >/dev/null 2>&1; then
    t="$(python3 -c 'import time; print(f"{time.time():.6f}")' 2>/dev/null)"
    if _looks_like_subsecond_time "${t}"; then
      printf '%s' "${t}"
      return 0
    fi
  fi
  if command -v perl >/dev/null 2>&1; then
    t="$(perl -MTime::HiRes=time -e 'printf "%.6f", time()' 2>/dev/null)"
    if _looks_like_subsecond_time "${t}"; then
      printf '%s' "${t}"
      return 0
    fi
  fi
  return 1
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
  acquired_at="$(lock_probe_time)" || {
    printf 'lock-probe: FATAL -- no sub-second time source available (EPOCHREALTIME unset, date +%%N not interpreted, no python3/perl on PATH); refusing to silently degrade to whole-second resolution.\n' >&2
    exit 1
  }
  printf 'lock-probe: acquired (pid %s) at %s\n' "$$" "${acquired_at}"
  sleep "${hold_secs}"
  released_at="$(lock_probe_time)" || {
    printf 'lock-probe: FATAL -- lost sub-second time source between acquire and release.\n' >&2
    exit 1
  }
  printf 'lock-probe: releasing (pid %s) at %s\n' "$$" "${released_at}"
  exit 0
fi

# Test-only harness hook (CHAOS-3403 adversarial-review fix): proves
# release_lock() runs BEFORE cleanup_scratch() inside the consolidated on_exit()
# trap, by swapping cleanup_scratch() for a stand-in that sleeps and then writes
# a marker file. A test can then observe the real lock symlink disappear (via
# the real release_lock()) strictly before the marker appears — i.e. the mutex
# is free before the slow step even finishes, not after. Never invoked by
# main(); only by tests/tooling/test_local_validate_lock.py.
if [ "${1:-}" = "--lock-probe-exit-order" ]; then
  shift
  hold_secs="${1:?hold_secs required}"
  cleanup_delay="${2:?cleanup_delay required}"
  marker="${3:?marker path required}"
  cleanup_scratch() {
    sleep "${cleanup_delay}"
    : >"${marker}"
  }
  acquire_lock
  printf 'lock-probe-exit-order: acquired (pid %s)\n' "$$"
  # Without an explicit hold, acquire-then-immediately-exit leaves the lock
  # symlink existing for a near-zero window (a handful of shell statements,
  # no I/O wait) before on_exit() removes it — too short for an external
  # poll loop to reliably observe "it existed" at all, which produced
  # exactly that flaky non-observation during this hook's own development.
  # hold_secs makes the window a real, controllable duration to poll for.
  sleep "${hold_secs}"
  exit 0
fi

# Test-only harness hook (CHAOS-3571): exercises the REAL ch_probe_docker()
# function in isolation -- no lock, no preflight, no lint/mypy/unit suite, no
# scratch db. Prints its return code and CH_PROBE_DETAIL so a test can plant a
# specific docker failure mode (missing binary, `docker ps` erroring, a clean
# "container absent" result) via a PATH-shadowing stub `docker` binary, and
# assert the exact distinguishing message CHAOS-3571 was filed over -- a probe
# FAILURE reported as a confirmed "not running" fact. Never invoked by main();
# only by tests/tooling/test_local_validate_stage_manifest.py.
if [ "${1:-}" = "--ch-probe-only" ]; then
  shift
  ch_probe_docker
  ch_probe_rc=$?
  printf 'ch-probe-result: rc=%s detail=%s\n' "${ch_probe_rc}" "${CH_PROBE_DETAIL}"
  exit "${ch_probe_rc}"
fi

# Test-only harness hook (CHAOS-3571): exercises the REAL stage-declaration /
# stage-execution / verify_stage_manifest / verdict-line bookkeeping in
# run_declared_stages() end-to-end, with every expensive or environment-
# dependent LEAF stage swapped for a trivial stand-in -- same technique as
# --lock-probe-exit-order's cleanup_scratch() override above, applied to eight
# functions instead of one. This is deliberately NOT a test of any individual
# gate's correctness (lint/mypy/pytest are not being exercised here at all);
# it is a test that the manifest machinery itself -- the part CHAOS-3571 was
# actually about -- correctly declares, tracks, diffs, and reports, without
# paying for the ~15-minute real gate or touching a real docker/ClickHouse.
# Never invoked by main(); only by
# tests/tooling/test_local_validate_stage_manifest.py.
if [ "${1:-}" = "--stage-manifest-probe" ]; then
  shift
  gate_lint_format() { return 0; }
  gate_lint_check() { return 0; }
  gate_typecheck() { return 0; }
  gate_unit_suite() { return 0; }
  ch_probe_docker() {
    CH_PROBE_DETAIL="stubbed: available"
    return 0
  }
  # NOTE: does NOT set SCRATCH_CREATED=1 -- the real cleanup_scratch() (run
  # unconditionally by the on_exit EXIT trap registered near the top of this
  # script, which this hook does not disable) gates its `docker exec ...
  # DROP DATABASE` on that flag. Leaving it unset means on_exit's real
  # cleanup_scratch() call is a guaranteed no-op here, so this hook makes
  # zero docker calls end to end -- consistent with "no docker commands
  # against real containers" for a probe whose whole point is to test the
  # manifest bookkeeping, not ClickHouse.
  ch_create_scratch() { return 0; }
  ch_migrate() { return 0; }
  ch_argmax_proof() { return 0; }
  # CHAOS-4266: pin the diff-relevance check true so the declared set is
  # deterministic (9 stages) regardless of this checkout's actual git state --
  # same reasoning as every other stub above, applied to the one stage whose
  # declaration is itself conditional.
  metrics_readback_diff_relevant() { return 0; }
  metrics_readback() { return 0; }
  run_declared_stages
fi

# Test-only harness hook (CHAOS-3571): exercises verify_stage_manifest() in
# total isolation from run_declared_stages -- feeds it a DECLARED_STAGE_IDS /
# EXECUTED_STAGE_IDS pair with a deliberate gap (something declared that never
# "ran") and confirms it fails loudly by name, rather than trusting that a
# normal gate run can ever actually produce a mismatch to test against (under
# the CHAOS-3571 fix it structurally can't -- run_stage/ch_provision append to
# EXECUTED_STAGE_IDS unconditionally on every path, so the only way to observe
# this function's own refusal logic is to hand it a gap directly). Reads the
# fake declared/executed id lists from argv so a test can vary them freely.
# Never invoked by main(); only by
# tests/tooling/test_local_validate_stage_manifest.py.
if [ "${1:-}" = "--stage-manifest-mismatch-probe" ]; then
  shift
  declared_csv="${1:?declared csv required}"
  executed_csv="${2:?executed csv required}"
  IFS=',' read -r -a DECLARED_STAGE_IDS <<<"${declared_csv}"
  IFS=',' read -r -a EXECUTED_STAGE_IDS <<<"${executed_csv}"
  verify_stage_manifest
  printf 'stage-manifest-mismatch-probe: no mismatch detected (unexpected)\n'
  exit 0
fi

# CHAOS-4457: lets a test drive ch_query() for real -- asserting which transport
# it actually uses -- instead of grepping this file for a string, which would
# pass against a script that still shelled out to docker at runtime. Same
# argument-hook convention as --stage-manifest-mismatch-probe above.
if [ "${1:-}" = "--ch-query-probe" ]; then
  shift
  ch_query "${1:?query required}"
  exit $?
fi

main "$@"

"""Shared test fixtures for the test suite."""

import logging
import mimetypes
import os
import shutil
import uuid
import zlib
from pathlib import Path

import pytest
from git import Repo as GitRepo

#: CHAOS-8310: ``web.env.BACKEND_URL`` is required when ``web.enabled`` (the chart default), so a
#: ``helm template`` that sets nothing about web fails. Most helm tests render other things (hooks,
#: workers, listeners): this is the ONE shared place that gives every ``helm template`` a placeholder
#: value -- a ``helm`` wrapper first on PATH that puts ``--set web.env.BACKEND_URL=<this>`` right after
#: ``template`` (helm gives ``--set`` precedence over every ``-f`` values file, in either order: a test that renders a profile's own value must set ``HELM_SHIM_OFF=1``; only a later ``--set`` of the same key wins). ``HELM_SHIM_OFF=1`` in a command's
#: environment bypasses it: ``tests/test_helm_web_extra_volumes.py::test_web_backend_url_is_required`` is the
#: test that must see the refusal.
HELM_TEST_WEB_BACKEND_URL = "http://backend.test:8000"


@pytest.fixture(scope="session", autouse=True)
def _helm_web_backend_url_shim(tmp_path_factory):
    real = shutil.which("helm")
    if real is None:
        yield
        return
    shim_dir = tmp_path_factory.mktemp("helm-shim")
    shim = shim_dir / "helm"
    shim.write_text(
        "#!/usr/bin/env bash\n"
        'if [ "${HELM_SHIM_OFF:-}" = 1 ] || [ "${1:-}" != template ]; then '
        f'exec "{real}" "$@"; fi\n'
        f'shift\nexec "{real}" template --set "web.env.BACKEND_URL={HELM_TEST_WEB_BACKEND_URL}" "$@"\n',
        encoding="utf-8",
    )
    shim.chmod(0o755)
    saved = os.environ.get("PATH", "")
    os.environ["PATH"] = f"{shim_dir}{os.pathsep}{saved}"
    try:
        yield
    finally:
        os.environ["PATH"] = saved


@pytest.fixture(autouse=True)
def setup_test_env(monkeypatch):
    """Ensure a default DATABASE_URI and JWT_SECRET_KEY are set for tests."""
    monkeypatch.setenv("DATABASE_URI", "sqlite:///:memory:")
    # JWT_SECRET_KEY is now a hard requirement with no derivation fallback
    # (CHAOS-1266). Provide a safe default for tests; tests that need to
    # assert the "unset" behaviour use monkeypatch.delenv to override.
    monkeypatch.setenv(
        "JWT_SECRET_KEY",
        "test-jwt-secret-key-at-least-32-characters-long",
    )
    # Rate limiter (CHAOS-1554) hard-fails at startup when REDIS_URL is unset
    # in non-dev environments. Mark tests as a dev-equivalent environment.
    monkeypatch.setenv("ENVIRONMENT", "test")
    # CHAOS-4629 (codex review, final gate round -- EXECUTED repros against
    # test_sync_configs.py and test_new_user_journey.py): the flag's
    # production default flipped to true, but MANY pre-existing sqlite-based
    # test fixtures across the suite use a reduced table set that predates
    # scheduled_sync_occurrences/sync_manual_triggers (CHAOS-4602) and were
    # never written expecting a planner-managed config's manual/backfill
    # trigger to route through the Go hand-off at all. Pinning the OLD
    # default here, suite-wide, is the single fix for every such fixture
    # instead of a per-file patch (the whack-a-mole this comment replaces:
    # test_backfill_jobrun.py got its own file-local fixture first, then
    # codex found two more files with the identical latent gap). Tests that
    # specifically assert the NEW default use monkeypatch.delenv to
    # override, same pattern as JWT_SECRET_KEY above.
    monkeypatch.setenv("SYNC_GO_MANUAL_BACKFILL_PLANNER_ENABLED", "false")


@pytest.fixture
def repo_path():
    """Return the path to the current repository for testing."""
    return str(Path(__file__).parent.parent)


@pytest.fixture
def repo_uuid():
    """Return a test UUID for the repository."""
    return uuid.uuid4()


@pytest.fixture
def git_repo(repo_path):
    """Return a GitRepo instance for testing."""
    return GitRepo(repo_path)


@pytest.fixture
def test_file(repo_path):
    """Return a path to an existing file in the repository."""
    return os.path.join(repo_path, "README.md")


@pytest.fixture
def quiet_aiosqlite_logger():
    """Stop the test harness's own SQL driver from echoing bind parameters.

    ``aiosqlite`` logs every operation — including INSERT statements with their
    bind parameters — at DEBUG. Tests that assert "this secret never reaches the
    logs" by sweeping every captured record therefore rediscover their own
    planted secret whenever the root level is DEBUG, even though production never
    runs on aiosqlite (it is a test-fixture-only driver, see ops/AGENTS.md) and
    the code under test never logged it.

    ``LOG_LEVEL`` is scrubbed suite-wide (CHAOS-3402), so this is the belt to
    that braces: it states the precondition at the tests that depend on it, and
    keeps holding if a future run configures logging some other way. It narrows
    only the driver, so a genuine DEBUG-level leak from application code is still
    captured and still fails the assertion.
    """
    driver_logger = logging.getLogger("aiosqlite")
    previous = driver_logger.level
    driver_logger.setLevel(logging.INFO)
    try:
        yield
    finally:
        driver_logger.setLevel(previous)


def pytest_configure(config):
    # Ensure TypeScript files are treated as text, not video/mp2t.
    mimetypes.add_type("text/x-typescript", ".ts")


#: Env var selecting one slice of the suite, `K/N` (both 1-based). The hosted
#: `test-matrix` job runs N legs, leg K with PYTEST_SHARD=K/N (CHAOS-6692).
SHARD_ENV = "PYTEST_SHARD"


def parse_shard(raw: str) -> tuple[int, int]:
    """Parse `K/N` (1 <= K <= N); raises ``pytest.UsageError`` on anything else."""
    parts = raw.split("/")
    if (
        len(parts) != 2
        or not all(p.isascii() and p.isdigit() for p in parts)
        or not 1 <= int(parts[0]) <= int(parts[1])
    ):
        raise pytest.UsageError(
            f"{SHARD_ENV}={raw!r} is not K/N with 1 <= K <= N (e.g. 1/2)"
        )
    return int(parts[0]), int(parts[1])


def shard_of(nodeid: str, count: int) -> int:
    """0-based shard of a test: a stable hash of its FILE, so one file's tests
    stay together (module-scoped fixtures) and every xdist worker, which collects
    on its own, computes the same partition."""
    return zlib.crc32(nodeid.split("::", 1)[0].encode()) % count


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config, items):
    raw = os.environ.get(SHARD_ENV, "").strip()
    if not raw:
        return
    shard, count = parse_shard(raw)
    keep = [item for item in items if shard_of(item.nodeid, count) == shard - 1]
    dropped = [item for item in items if shard_of(item.nodeid, count) != shard - 1]
    if not keep:
        raise pytest.UsageError(
            f"{SHARD_ENV}={raw} selects none of the {len(items)} collected tests: "
            "the matrix is wider than the suite, so this leg would read green "
            "while running nothing"
        )
    config.hook.pytest_deselected(items=dropped)
    items[:] = keep
    workerid = getattr(config, "workerinput", {}).get("workerid", "gw0")
    reporter = config.pluginmanager.get_plugin("terminalreporter")
    if reporter is not None and workerid == "gw0":
        files = {item.nodeid.split("::", 1)[0] for item in keep}
        reporter.write_line(
            f"pytest shard {shard}/{count}: selected {len(keep)} of "
            f"{len(keep) + len(dropped)} test(s) from {len(files)} file(s)"
        )


def pytest_report_header(config):
    """Say out loud which slice of the suite this leg runs."""
    lines = []
    shard = os.environ.get(SHARD_ENV, "").strip()
    if shard:
        lines.append(
            f"{SHARD_ENV}={shard} (CHAOS-6692): this leg runs its slice of the test files only"
        )
    return lines


def answer_catalogue_probes(client):
    """Make a mock ClickHouse client answer catalogue probes truthfully.

    Migrations run whenever a ClickHouse store is constructed, and migration 084
    asks the catalogue two questions before it plans anything: does
    `work_graph_issue_pr` exist, and what shape is it. A real server answers
    `EXISTS TABLE` with exactly 0 or 1, so 084 RAISES on anything else -- an
    unanswerable probe means the client is not answering the question asked, and
    guessing would either rebuild or decline to rebuild on a fiction.

    Most mock clients in this suite return `result_rows=[]` for every query
    because they were never meant to answer anything; they exist to assert that
    `insert` or `command` was called. Under 084's contract that empty answer is
    a refusal, so the migration raises and an unrelated test fails.

    The fix belongs here rather than in the migration: production code is not
    bent to satisfy a mock, and a per-test patch means every future
    store-constructing test has to learn about a probe it has no interest in.

    `[(0,)]` is the TRUTHFUL answer, not merely a convenient one -- these tests
    construct a store against a client with no tables at all, so the table
    genuinely does not exist. 084 then skips at its own existence check and
    never reaches DESCRIBE.

    Migration 087 asks the same kind of question a different way: `SELECT
    count() FROM system.tables WHERE ...` (its `_table_exists`), expecting an
    int it can compare with `> 0`. Against a generic mock with a canned
    `result_rows` tuple (e.g. a migration-version string left over from an
    unrelated assertion), that comparison raises `TypeError: '>' not
    supported between instances of 'str' and 'int'` -- a fail-CLOSED probe
    surfacing a mock mismatch as a crash, not a fail-open swallow, so this
    belongs here rather than loosening the migration back to fail-open.
    `[(0,)]` is truthful for the same reason as 084's: these tests construct
    a store against a client with none of migration 087's tables, so they
    genuinely do not exist, and `upgrade()` skips each one at its own
    existence check.

    Call tracking survives: this sets `side_effect` on the existing mock rather
    than replacing it, so `client.query.called` and friends still work. The
    fall-through reads `return_value` LAZILY, so a test that configures its rows
    AFTER calling this -- which is the common order -- still gets them.

    CHAOS-4296/#2262 extension: several migrations (027/042/048/049/055/061/067/
    075/088, the shadow-table-rebuild family) ask the SAME existence question a
    different way -- `SELECT count() FROM system.tables WHERE ... AND name =
    {name:String}` -- rather than `EXISTS TABLE`. Migration 088's own probe used
    to swallow any exception (including a mock returning a non-numeric
    `result_rows[0][0]`) and treat it as "table absent" -- CHAOS-4296's own
    codex r1 finding correctly removed that swallow, since a genuine query
    failure reading as "absent" is worse than surfacing it. That fix then
    exposed this gap: a mock with no specific stub for this query pattern falls
    through to whatever `client.query.return_value` was configured to be for
    UNRELATED reasons in a given test (a migration-version string in one test,
    a bare MagicMock in another), and comparing that to `0` raises a TypeError
    instead of participating in the same truthful "no tables exist" answer this
    function already gives EXISTS TABLE. Answered here for the same reason as
    EXISTS TABLE: these tests build a store against a client with NO tables at
    all, so `[(0,)]` is truthful, not a guess -- the migration's own existing
    "table does not exist, skipping" path then takes over cleanly.
    """
    from types import SimpleNamespace

    def _answer(sql="", *args, **kwargs):
        text = str(sql).strip().upper()
        if text.startswith("EXISTS TABLE"):
            return SimpleNamespace(result_rows=[(0,)])
        if text.startswith("DESCRIBE TABLE"):
            return SimpleNamespace(result_rows=[])
        if text.startswith("SELECT COUNT() FROM SYSTEM.TABLES"):
            return SimpleNamespace(result_rows=[(0,)])
        return client.query.return_value

    client.query.side_effect = _answer
    return client

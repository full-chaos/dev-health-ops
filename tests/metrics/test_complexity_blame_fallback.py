"""Tests for CHAOS-2861 (CS-C): honest git_blame fallback gating.

``job_complexity_db``'s blame-reconstruction fallback (``_load_blame_contents``)
is structurally dead for provider-synced repos today: GitHub's Blame API does
not return line text at all, and GitLab currently discards it too (pending
CHAOS-2860), so ``git_blame.line`` is NULL. Reconstructing text from all-NULL
lines silently produces newline-only strings, and the job used to log a
misleading "falling back to git_blame" / "No scannable contents" message that
promised a recovery path which could not exist.

These tests exercise ``_has_blame_line_text`` and its two call sites in
``run_complexity_db_job``:
  (a) repos with SOME git_files contents, filling missing paths from blame.
  (b) repos with ZERO git_files contents, going all-blame.

The fake ClickHouse client below follows the same pattern as
``tests/test_github_content_backfill.py::_EmptyClickHouseClient`` /
``tests/test_metrics_complexity_db.py::FakeClickHouseClient`` -- a query-text
dispatcher that returns canned ``result_rows`` for each query shape the job
issues.
"""

from __future__ import annotations

import uuid
from datetime import date

import dev_health_ops.metrics.job_complexity_db as job


class _FakeQueryResult:
    def __init__(self, rows):
        self.result_rows = rows


class _FakeSink:
    """Minimal ``ClickHouseMetricsSink`` stand-in, same shape as the other
    complexity-job tests' fake sinks."""

    def __init__(self, client):
        self.client = client
        self.snapshots = []
        self.dailies = []

    def ensure_tables(self):
        return None

    def write_file_complexity_snapshots(self, rows):
        self.snapshots.extend(rows)

    def write_repo_complexity_daily(self, rows):
        self.dailies.extend(rows)

    def close(self):
        return None


class _BlameFallbackClient:
    """Query-text dispatcher covering every query shape
    ``run_complexity_db_job`` issues for a single repo:

    - ``count()`` over ``git_files`` -> (total, non_empty)
    - ``maxOrNull(last_synced)`` over ``git_files`` / ``git_blame``
    - ``SELECT path, contents FROM git_files`` (non-empty contents)
    - ``SELECT path FROM git_files`` (missing/NULL contents)
    - the CHAOS-2861 probe: ``SELECT 1 FROM git_blame ... line IS NOT NULL``
    - the reconstruction query: ``SELECT path, arrayStringConcat(...)``

    ``blame_has_text`` controls whether the probe (and, if reached, the
    reconstruction query) reports usable line text.
    """

    def __init__(
        self,
        *,
        total_files: int,
        non_empty_files: int,
        missing_paths: list[str] | None = None,
        blame_has_text: bool,
        blame_reconstructed: list[tuple[str, str]] | None = None,
        non_empty_rows: list[tuple[str, str]] | None = None,
    ):
        self.total_files = total_files
        self.non_empty_files = non_empty_files
        self.missing_paths = missing_paths or []
        self.blame_has_text = blame_has_text
        self.blame_reconstructed = blame_reconstructed or []
        self.non_empty_rows = non_empty_rows or []
        self.queries: list[tuple[str, dict]] = []

    def query(self, query: str, parameters=None):
        params = parameters or {}
        self.queries.append((query, params))

        if "count()" in query and "FROM git_files" in query:
            return _FakeQueryResult([[self.total_files, self.non_empty_files]])

        if "maxOrNull(last_synced)" in query:
            return _FakeQueryResult([[None]])

        if "FROM git_files" in query and "contents" in query and "IS NULL" not in query:
            return _FakeQueryResult(self.non_empty_rows)

        if "FROM git_files" in query and "contents" in query and "IS NULL" in query:
            return _FakeQueryResult([[p] for p in self.missing_paths])

        # CHAOS-2861 probe: cheap existence check for usable blame line text.
        if "SELECT 1" in query and "FROM git_blame" in query:
            return _FakeQueryResult([[1]] if self.blame_has_text else [])

        # Reconstruction query (arrayStringConcat over groupArray).
        if "FROM git_blame" in query:
            return _FakeQueryResult(self.blame_reconstructed)

        if "FROM repos" in query:
            return _FakeQueryResult([])

        raise AssertionError(f"Unexpected query: {query}")


def _run(client, monkeypatch, *, repo_id=None):
    sink = _FakeSink(client)
    monkeypatch.setattr(job, "ClickHouseMetricsSink", lambda _dsn: sink)
    rc = job.run_complexity_db_job(
        repo_id=repo_id or uuid.uuid4(),
        db_url="clickhouse://localhost:8123/default",
        date=date(2026, 6, 12),
        backfill_days=1,
        language_globs=None,
        max_files=None,
        org_id="test-org",
    )
    return rc, sink


def test_zero_contents_textless_blame_skips_fallback_and_logs_actionable_warning(
    monkeypatch, caplog
):
    """(a) Repo has zero git_files contents; git_blame rows exist but every
    ``line`` is NULL. The fallback must NOT attempt reconstruction, and must
    log a warning naming the real remedies instead of the dead-end
    "falling back to git_blame" message."""
    client = _BlameFallbackClient(
        total_files=2,
        non_empty_files=0,
        blame_has_text=False,
    )

    with caplog.at_level("WARNING"):
        rc, sink = _run(client, monkeypatch)

    assert rc == 1
    assert not sink.snapshots
    assert not sink.dailies

    warnings = [r.message for r in caplog.records if r.levelname == "WARNING"]
    assert not any("falling back to git_blame" in msg for msg in warnings), (
        "must not claim a blame fallback is happening when blame has no usable text"
    )
    actionable = [msg for msg in warnings if "no usable line text" in msg]
    assert actionable, (
        f"expected an actionable no-usable-line-text warning, got: {warnings}"
    )
    combined = " ".join(actionable)
    assert "CHAOS-2859" in combined
    assert "CHAOS-2860" in combined
    assert "CHAOS-2862" in combined
    assert "GitHub" in combined


def test_zero_contents_real_blame_text_reconstruction_unchanged(monkeypatch):
    """(b) Repo has zero git_files contents; git_blame carries real line
    text. Reconstruction must still happen and produce snapshots -- byte
    identical to pre-CHAOS-2861 behavior."""
    blame_rows = [
        ("src/alpha.py", "def alpha():\n    return 1\n"),
        ("src/beta.py", "def beta(x):\n    if x:\n        return x\n    return 0\n"),
    ]
    client = _BlameFallbackClient(
        total_files=0,
        non_empty_files=0,
        blame_has_text=True,
        blame_reconstructed=blame_rows,
    )

    rc, sink = _run(client, monkeypatch)

    assert rc == 0
    assert sink.snapshots
    assert sink.dailies
    assert {snap.file_path for snap in sink.snapshots} == {
        "src/alpha.py",
        "src/beta.py",
    }


def test_missing_paths_textless_blame_skips_fill_and_logs_actionable_warning(
    monkeypatch, caplog
):
    """(a) Repo has SOME git_files contents but is missing others; git_blame
    exists for the missing paths but carries no usable text. The fill must be
    skipped (not fabricated from NULL lines) and an actionable warning
    logged, while the files that DO have contents still get scanned."""
    client = _BlameFallbackClient(
        total_files=2,
        non_empty_files=1,
        missing_paths=["src/beta.py"],
        blame_has_text=False,
        non_empty_rows=[("src/alpha.py", "def alpha():\n    return 1\n")],
    )

    with caplog.at_level("WARNING"):
        rc, sink = _run(client, monkeypatch)

    assert rc == 0
    assert {snap.file_path for snap in sink.snapshots} == {"src/alpha.py"}, (
        "the file with real contents must still be scanned even though the "
        "blame fill for the missing path was skipped"
    )

    warnings = [r.message for r in caplog.records if r.levelname == "WARNING"]
    assert not any("filling with blame" in msg for msg in warnings)
    actionable = [msg for msg in warnings if "no usable line text" in msg]
    assert actionable, (
        f"expected an actionable no-usable-line-text warning, got: {warnings}"
    )


def test_missing_paths_real_blame_text_fill_unchanged(monkeypatch):
    """(a) Repo has SOME git_files contents and missing paths with real blame
    text. The existing fill-from-blame behavior must be unchanged."""
    client = _BlameFallbackClient(
        total_files=2,
        non_empty_files=1,
        missing_paths=["src/beta.py"],
        blame_has_text=True,
        blame_reconstructed=[
            ("src/beta.py", "def beta(x):\n    if x:\n        return x\n    return 0\n")
        ],
        non_empty_rows=[("src/alpha.py", "def alpha():\n    return 1\n")],
    )

    rc, sink = _run(client, monkeypatch)

    assert rc == 0
    assert {snap.file_path for snap in sink.snapshots} == {
        "src/alpha.py",
        "src/beta.py",
    }


def test_zero_rows_repo_distinguishable_message(monkeypatch, caplog):
    """(c) A repo with NO git_files rows and NO git_blame rows at all must
    log a distinguishable "no rows exist" message, not the generic fallback
    warning or the textless-blame remedy message (there is nothing to
    remedy -- the repo was simply never synced)."""
    client = _BlameFallbackClient(
        total_files=0,
        non_empty_files=0,
        blame_has_text=False,
    )

    with caplog.at_level("WARNING"):
        rc, sink = _run(client, monkeypatch)

    assert rc == 1
    assert not sink.snapshots

    warnings = [r.message for r in caplog.records if r.levelname == "WARNING"]
    scannable = [msg for msg in warnings if "No scannable contents found" in msg]
    assert scannable, f"expected a final no-scannable-contents warning, got: {warnings}"
    assert any("no git_files or git_blame rows exist" in msg for msg in scannable), (
        f"expected the zero-rows-specific message, got: {scannable}"
    )
    assert not any("no usable line text" in msg for msg in scannable), (
        "a repo with zero rows entirely should not get the "
        "blame-exists-but-textless message"
    )


def test_probe_query_is_org_scoped(monkeypatch):
    """The CHAOS-2861 probe must be scoped by both repo_id and org_id, like
    every other query in this job (multi-tenant safety)."""
    client = _BlameFallbackClient(
        total_files=0,
        non_empty_files=0,
        blame_has_text=False,
    )
    repo_id = uuid.uuid4()

    _run(client, monkeypatch, repo_id=repo_id)

    probe_queries = [
        (q, p) for q, p in client.queries if "SELECT 1" in q and "FROM git_blame" in q
    ]
    assert probe_queries, "expected the CHAOS-2861 probe query to run"
    for query, params in probe_queries:
        assert "org_id = {org_id:String}" in query
        assert params["org_id"] == "test-org"
        assert params["repo_id"] == str(repo_id)

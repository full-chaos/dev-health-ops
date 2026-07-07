"""Shared ClickHouse-query fake for CHAOS-2888 Workstream D complexity-
readiness regression tests.

Both ``tests/test_github_content_backfill.py`` and
``tests/test_gitlab_content_backfill.py`` prove that persisted ``git_files``
content -- fetched through the provider-specific code-host backfill path --
satisfies ``job_complexity_db.run_complexity_db_job``'s readiness contract.
This fake answers each of the job's queries from its actual persisted shape
(git_files counts, non-empty contents, missing-content paths, the
``git_blame`` usable-line-text probe, and ``maxOrNull`` last-synced lookups)
instead of a blanket substring match, and raises loudly on any query shape
neither test exercises so a future job change that adds or renames a query
is caught here instead of silently returning an empty result set.
"""

from __future__ import annotations


class ComplexityReadinessClient:
    """Minimal ClickHouse client stand-in that answers
    ``job_complexity_db``'s queries purely from the ``(path, contents)`` rows
    a code-host content backfill just wrote -- proving persisted
    ``git_files`` content satisfies the complexity job's readiness contract
    end-to-end (CHAOS-2888 Workstream D)."""

    def __init__(self, files: list[tuple[str, str | None]]):
        self._total = len(files)
        self._non_empty = [[path, contents] for path, contents in files if contents]
        self._missing_paths = [path for path, contents in files if not contents]

    def query(self, query, parameters=None):
        del parameters

        class _Result:
            result_rows: list = []

        result = _Result()
        is_git_files = "FROM git_files" in query
        is_git_blame = "FROM git_blame" in query

        if is_git_files and "count()" in query:
            # job_complexity_db._git_file_counts
            result.result_rows = [[self._total, len(self._non_empty)]]
        elif is_git_files and "contents IS NOT NULL" in query:
            # job_complexity_db._load_git_files
            result.result_rows = [list(row) for row in self._non_empty]
        elif is_git_files and "contents IS NULL" in query:
            # job_complexity_db._load_missing_paths
            result.result_rows = [[path] for path in self._missing_paths]
        elif is_git_blame and "line IS NOT NULL" in query:
            # job_complexity_db._has_blame_line_text -- this fixture never
            # seeds usable blame line text, so the probe always reports
            # "unusable", matching every real GitHub/GitLab sync today
            # (CHAOS-2861/CHAOS-2860).
            result.result_rows = []
        elif is_git_blame:
            # job_complexity_db._load_blame_contents -- no blame fallback
            # rows in this fixture.
            result.result_rows = []
        elif "maxOrNull" in query:
            # job_complexity_db._max_last_synced (git_files/git_blame probes).
            result.result_rows = [[None]]
        else:
            raise AssertionError(
                "ComplexityReadinessClient received an unrecognized query "
                "shape -- update this fixture to handle it explicitly "
                f"instead of silently returning empty rows: {query!r}"
            )
        return result


class ComplexityReadinessSink:
    def __init__(self, client: ComplexityReadinessClient):
        self.client = client
        self.snapshots: list = []
        self.dailies: list = []

    def ensure_tables(self) -> None:
        return None

    def write_file_complexity_snapshots(self, rows) -> None:
        self.snapshots.extend(rows)

    def write_repo_complexity_daily(self, rows) -> None:
        self.dailies.extend(rows)

    def close(self) -> None:
        return None

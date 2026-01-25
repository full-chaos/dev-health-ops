import uuid
from datetime import date, datetime

import dev_health_ops.metrics.job_complexity_db as job


class FakeQueryResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClickHouseClient:
    def __init__(self, files, last_synced=None):
        self.files = files
        self.last_synced = last_synced
        self.queries = []

    def query(self, query, parameters=None):
        self.queries.append((query, parameters or {}))
        if "FROM git_files" in query and "count()" in query:
            total = len(self.files)
            return FakeQueryResult([[total, total]])
        if "FROM git_files" in query and "contents" in query and "count()" not in query:
            return FakeQueryResult(self.files)
        if "FROM git_blame" in query:
            return FakeQueryResult([])
        if "max(last_synced)" in query and "git_files" in query:
            return FakeQueryResult([[self.last_synced]])
        if "max(last_synced)" in query and "git_blame" in query:
            return FakeQueryResult([[None]])
        if "FROM repos" in query:
            return FakeQueryResult([])
        raise AssertionError(f"Unexpected query: {query}")


def test_load_blame_contents_query_uses_arraysort():
    client = FakeClickHouseClient([])
    job._load_blame_contents(client, uuid.uuid4(), None, None)
    assert client.queries
    query, _params = client.queries[0]
    assert "arraySort" in query
    assert "groupArray" in query
    assert "ORDER BY line_no" not in query


class FakeClickHouseSink:
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


def test_complexity_db_job_scans_contents_without_gitpython(monkeypatch):
    repo_id = uuid.uuid4()
    files = [
        ("src/alpha.py", "def alpha():\n    return 1\n"),
        ("src/beta.py", "def beta(x):\n    if x:\n        return x\n    return 0\n"),
    ]
    last_synced = datetime(2025, 1, 2, 3, 4, 5)
    client = FakeClickHouseClient(files, last_synced=last_synced)
    sink = FakeClickHouseSink(client)

    monkeypatch.setattr(job, "ClickHouseMetricsSink", lambda _dsn: sink)

    import git

    def _raise_repo(*_args, **_kwargs):
        raise AssertionError("git.Repo should not be called in DB mode")

    monkeypatch.setattr(git, "Repo", _raise_repo)

    rc = job.run_complexity_db_job(
        repo_id=repo_id,
        db_url="clickhouse://localhost:8123/default",
        date=date(2025, 1, 5),
        backfill_days=1,
        language_globs=None,
        max_files=None,
    )

    assert rc == 0
    assert sink.snapshots
    assert sink.dailies
    assert {snap.file_path for snap in sink.snapshots} == {
        "src/alpha.py",
        "src/beta.py",
    }


class FakeClickHouseClientBlameOnly:
    def __init__(self, blame_files, last_synced=None):
        self.blame_files = blame_files
        self.last_synced = last_synced
        self.queries = []

    def query(self, query, parameters=None):
        self.queries.append((query, parameters or {}))
        if "FROM git_files" in query and "count()" in query:
            return FakeQueryResult([[0, 0]])
        if "FROM git_files" in query and "contents" in query:
            return FakeQueryResult([])
        if "max(last_synced)" in query and "git_files" in query:
            return FakeQueryResult([[None]])
        if "max(last_synced)" in query and "git_blame" in query:
            return FakeQueryResult([[self.last_synced]])
        if "FROM git_blame" in query:
            return FakeQueryResult(self.blame_files)
        raise AssertionError(f"Unexpected query: {query}")


def test_complexity_db_job_falls_back_to_blame(monkeypatch):
    repo_id = uuid.uuid4()
    blame_files = [
        ("src/alpha.py", "def alpha():\n    return 1\n"),
        ("src/beta.py", "def beta(x):\n    if x:\n        return x\n    return 0\n"),
    ]
    last_synced = datetime(2025, 1, 2, 3, 4, 5)
    client = FakeClickHouseClientBlameOnly(blame_files, last_synced=last_synced)
    sink = FakeClickHouseSink(client)

    monkeypatch.setattr(job, "ClickHouseMetricsSink", lambda _dsn: sink)

    rc = job.run_complexity_db_job(
        repo_id=repo_id,
        db_url="clickhouse://localhost:8123/default",
        date=date(2025, 1, 5),
        backfill_days=1,
        language_globs=None,
        max_files=None,
    )

    assert rc == 0
    assert sink.snapshots
    assert sink.dailies
    assert {snap.file_path for snap in sink.snapshots} == {
        "src/alpha.py",
        "src/beta.py",
    }

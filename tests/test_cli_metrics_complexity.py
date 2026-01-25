from datetime import date
from pathlib import Path
from types import SimpleNamespace


def _make_ns(**overrides):
    data = {
        "repo_path": ".",
        "repo_id": None,
        "search": None,
        "date": date(2025, 1, 1),
        "backfill": 1,
        "ref": "HEAD",
        "db": "clickhouse://localhost:8123/default",
        "max_files": None,
        "include_glob": None,
        "exclude_glob": None,
        "lang": None,
        "exclude": None,
    }
    data.update(overrides)
    return SimpleNamespace(**data)


def test_complexity_cli_does_not_touch_git_paths(monkeypatch, tmp_path):
    repo_path = tmp_path / ".git"

    calls = []
    import dev_health_ops.metrics.job_complexity_db as job

    def _fake_run_complexity_db_job(**kwargs):
        calls.append(kwargs)
        return 0

    monkeypatch.setattr(job, "run_complexity_db_job", _fake_run_complexity_db_job)

    orig_exists = Path.exists
    orig_is_dir = Path.is_dir

    def _guard_exists(self):
        if ".git" in self.parts:
            raise AssertionError("CLI should not inspect .git paths")
        return orig_exists(self)

    def _guard_is_dir(self):
        if ".git" in self.parts:
            raise AssertionError("CLI should not inspect .git paths")
        return orig_is_dir(self)

    monkeypatch.setattr(Path, "exists", _guard_exists)
    monkeypatch.setattr(Path, "is_dir", _guard_is_dir)

    ns = _make_ns(repo_path=str(repo_path))
    rc = job._cmd_metrics_complexity(ns)

    assert rc == 0
    assert len(calls) == 1
    assert calls[0]["repo_id"] is None

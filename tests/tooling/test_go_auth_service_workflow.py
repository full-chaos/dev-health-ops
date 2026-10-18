"""Pins for the Auth Control Plane gate workflow (CHAOS-4887).

The gate logic is ``ci/check_go.sh auth``; the workflow only schedules it. What
could rot silently, and is pinned here: a job reusing a required context name (a
second producer of a required check, the CHAOS-4834 defect class), the fuzz
matrix drifting from the registry in ``check_go.sh`` (a parser whose fuzz job no
longer exists), and a fan-in that stops failing on a skipped relevant job.
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "go-auth-service.yml"
CHECK_GO = ROOT / "ci" / "check_go.sh"

REQUIRED_CONTEXTS = {"test", "lint", "go-quality", "metrics-executed-proof"}


def _workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def _registry_targets() -> list[str]:
    text = CHECK_GO.read_text(encoding="utf-8")
    block = re.search(r"AUTH_FUZZ_TARGETS=\((?P<rows>.*?)\n\)", text, re.DOTALL)
    assert block, "AUTH_FUZZ_TARGETS is no longer one literal array in ci/check_go.sh"
    return re.findall(r'"\S+ (Fuzz\w+)"', block.group("rows"))


def test_job_names_do_not_reuse_a_required_context() -> None:
    jobs = _workflow()["jobs"]
    for key, job in jobs.items():
        name = job.get("name", key)
        assert key not in REQUIRED_CONTEXTS and name not in REQUIRED_CONTEXTS, key
        assert key.startswith("auth-"), f"{key} must carry the auth- prefix"


def test_the_trigger_has_no_path_filter() -> None:
    # Relevance is decided inside (auth-changes); a `paths:` filter in the trigger
    # would leave a required-style check unreported on an unrelated change.
    triggers = _workflow()[True]
    for event, config in triggers.items():
        assert not (
            isinstance(config, dict) and ("paths" in config or "paths-ignore" in config)
        ), event


def test_fuzz_matrix_equals_the_registry() -> None:
    matrix = _workflow()["jobs"]["auth-fuzz"]["strategy"]["matrix"]["target"]
    assert sorted(matrix) == sorted(_registry_targets()), (
        "every registered parser needs its own CI-visible fuzz job, and no job may name a missing target"
    )
    assert len(set(matrix)) == len(matrix)


def test_gate_jobs_run_only_on_a_relevant_change_and_the_gate_fans_in() -> None:
    jobs = _workflow()["jobs"]
    for key in ("auth-go-quality", "auth-test", "auth-fuzz"):
        assert jobs[key]["needs"] == ["auth-changes"], key
        assert "relevant == 'true'" in jobs[key]["if"], key
    gate = jobs["auth-gate"]
    assert gate["if"] == "always()"
    assert set(gate["needs"]) == {
        "auth-changes",
        "auth-go-quality",
        "auth-test",
        "auth-fuzz",
    }
    script = gate["steps"][0]["run"]
    # A relevant change whose gate job did not succeed (failed, cancelled OR skipped) fails.
    assert '[ "${pair#*=}" = "success" ]' in script


def test_source_commit_history_check_runs_only_where_history_is_fetched() -> None:
    jobs = _workflow()["jobs"]
    test_job = jobs["auth-test"]
    assert test_job["steps"][0]["with"]["fetch-depth"] == 0
    assert any(
        step.get("run") == "bash ci/check_go.sh auth history"
        for step in test_job["steps"]
    )
    for key in ("auth-go-quality", "auth-fuzz"):
        assert all(
            "auth history" not in step.get("run", "") for step in jobs[key]["steps"]
        ), key


def test_every_stage_the_workflow_calls_exists_in_the_verb() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")
    verb = CHECK_GO.read_text(encoding="utf-8")
    stages = set(re.findall(r"run: bash ci/check_go\.sh auth ([a-z-]+)", text))
    assert stages >= {
        "fmt",
        "vet",
        "test",
        "race",
        "fuzz",
        "history",
        "vuln",
        "middleware",
        "fuzz-registry",
        "paths-selftest",
        "scope-selftest",
    }
    for stage in stages:
        assert re.search(rf"^    {re.escape(stage)}\) ", verb, re.MULTILINE), stage


def test_closure_check_is_scheduled_and_the_allow_list_is_well_formed() -> None:
    steps = _workflow()["jobs"]["auth-test"]["steps"]
    assert any(s.get("run") == "bash ci/check_go.sh auth closure" for s in steps)
    rows = [
        line.split("\t")
        for line in (ROOT / "ci" / "auth_closure_allow.tsv")
        .read_text(encoding="utf-8")
        .splitlines()
        if line and not line.startswith("#")
    ]
    assert rows and all(len(r) == 2 and r[1].strip() for r in rows), (
        "every row needs a reason"
    )
    assert not any("clickhouse" in r[0].lower() for r in rows)


def _run_closure(allow: Path) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "AUTH_CLOSURE_ALLOW": str(allow)}
    return subprocess.run(
        ["bash", str(CHECK_GO), "auth", "closure"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_closure_check_executes_and_fails_on_a_forbidden_or_stale_module(
    tmp_path: Path,
) -> None:
    real = ROOT / "ci" / "auth_closure_allow.tsv"
    lines = real.read_text(encoding="utf-8").splitlines()

    ok = _run_closure(real)
    assert ok.returncode == 0, ok.stderr

    missing = tmp_path / "missing.tsv"
    missing.write_text(
        "\n".join(x for x in lines if not x.startswith("github.com/jackc/pgx/v5\t"))
        + "\n"
    )
    res = _run_closure(missing)
    assert res.returncode != 0 and "github.com/jackc/pgx/v5" in res.stderr

    stale = tmp_path / "stale.tsv"
    stale.write_text(
        "\n".join(lines) + "\ngithub.com/ClickHouse/clickhouse-go/v2\tplanted\n"
    )
    res = _run_closure(stale)
    assert res.returncode != 0 and "no longer link" in res.stderr


def test_closure_check_covers_the_production_build_configuration() -> None:
    dockerfile = (ROOT / "docker" / "go-worker.Dockerfile").read_text(encoding="utf-8")
    assert "CGO_ENABLED=0" in dockerfile
    text = CHECK_GO.read_text(encoding="utf-8")
    body = re.search(r"check_auth_closure\(\) \{(?P<b>.*?)\n\}", text, re.DOTALL)
    assert body, "check_auth_closure is no longer one function"
    assert "for cgo in 0 1" in body.group(
        "b"
    ) and "for arch in amd64 arm64" in body.group("b")

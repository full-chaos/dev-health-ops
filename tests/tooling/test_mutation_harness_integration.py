"""Public integration contracts for CHAOS-3807 sharded mutation runs."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.mutation_harness import main, verify
from scripts.mutation_harness_coordinator import (
    ChildSpec,
    _record_child,
    _write_manifest,
    begin_coordinator_run,
    select_and_assign,
)
from scripts.mutation_harness_execution_tree import (
    build_source_manifest,
    create_private_temp_root,
    source_manifest_to_dict,
    stage_execution_tree,
)

REPOSITORY_ROOT = Path(__file__).parents[2]
INTEGRATED_SCRIPTS = (
    "mutation_harness.py",
    "mutation_harness_coordinator.py",
    "mutation_harness_execution_tree.py",
    "mutation_harness_optin.py",
    "mutation_harness_recovery.py",
)


def _git(root: Path, *arguments: str) -> None:
    completed = subprocess.run(
        ["git", *arguments],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr


def _integration_repository(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "repository"
    scripts = root / "scripts"
    scripts.mkdir(parents=True)
    for name in INTEGRATED_SCRIPTS:
        shutil.copy2(REPOSITORY_ROOT / "scripts" / name, scripts / name)
    (root / ".gitignore").write_text(".mutation-harness/\n", encoding="utf-8")
    (root / "widget.txt").write_text(
        "guard-one=enabled\nguard-two=enabled\n", encoding="utf-8"
    )
    plan = root / "plan.json"
    plan.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "name": "lane-e-integration",
                "sharding": {
                    "max_shards": 2,
                    "workspace_inputs": [],
                    "external_resources": "none",
                    "shared_mutable_resource_exclusions": [],
                },
                "mutations": [
                    {
                        "id": "M1",
                        "file": "widget.txt",
                        "find": "guard-one=enabled",
                        "replace": "guard-one=disabled",
                        "proof": [
                            ["bash", "-c", "grep -q guard-one=enabled widget.txt"]
                        ],
                        "rationale": "the first guard must remain observable",
                    },
                    {
                        "id": "M2",
                        "file": "widget.txt",
                        "find": "guard-two=enabled",
                        "replace": "guard-two=disabled",
                        "proof": [
                            ["bash", "-c", "grep -q guard-two=enabled widget.txt"]
                        ],
                        "rationale": "the second guard must remain observable",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "mutation@example.invalid")
    _git(root, "config", "user.name", "Mutation Test")
    _git(root, "config", "commit.gpgsign", "false")
    _git(root, "add", ".")
    _git(root, "commit", "-qm", "fixture")
    return root, plan


def _add_editable_python_workspace(root: Path, plan: Path) -> None:
    with (root / ".gitignore").open("a", encoding="utf-8") as handle:
        handle.write(".venv/\n")
    package = root / "src/probe_pkg"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("SOURCE = 'fixture'\n", encoding="utf-8")
    environment = root / ".venv"
    subprocess.run(
        [sys.executable, "-m", "venv", str(environment)],
        check=True,
        capture_output=True,
        text=True,
    )
    site_packages = next(environment.glob("lib/python*/site-packages"))
    (site_packages / "__editable__.probe.pth").write_text(
        str((root / "src").resolve()) + "\n", encoding="utf-8"
    )
    distribution = site_packages / "probe-1.0.dist-info"
    distribution.mkdir()
    (distribution / "direct_url.json").write_text(
        json.dumps({"dir_info": {"editable": True}, "url": root.resolve().as_uri()}),
        encoding="utf-8",
    )
    (root / "proof.py").write_text(
        "import json, pathlib, sys\n"
        "direct_url = next(pathlib.Path('.venv').rglob('direct_url.json'))\n"
        "assert json.loads(direct_url.read_text())['url'] == "
        "pathlib.Path.cwd().resolve().as_uri()\n"
        "raise SystemExit(0 if sys.argv[1] in "
        "pathlib.Path('widget.txt').read_text() else 1)\n",
        encoding="utf-8",
    )
    raw = json.loads(plan.read_text(encoding="utf-8"))
    raw["sharding"]["workspace_inputs"] = [".venv"]
    for mutation in raw["mutations"]:
        guard = mutation["find"]
        mutation["proof"] = [
            [
                "bash",
                "-c",
                f'PYTHONPATH="$PWD/src" "$PWD/.venv/bin/python" proof.py {guard}',
            ]
        ]
    plan.write_text(json.dumps(raw), encoding="utf-8")


def test_public_cli_runs_two_isolated_shards_and_restores_source(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root, plan = _integration_repository(tmp_path)
    original = (root / "widget.txt").read_bytes()

    exit_code = main(
        [
            "--root",
            str(root),
            "run",
            "--plan",
            str(plan),
            "--shards",
            "2",
            "--assert-all-killed",
            "--progress",
            "none",
        ]
    )

    assert exit_code == 0
    assert (root / "widget.txt").read_bytes() == original
    report = json.loads(
        (root / ".mutation-harness" / "report.json").read_text(encoding="utf-8")
    )
    assert report["mode"] == "sharded"
    assert report["aggregate_status"] == "COMPLETE"
    assert [(item["id"], item["verdict"]) for item in report["results"]] == [
        ("M1", "KILLED"),
        ("M2", "KILLED"),
    ]
    assert all(
        set(item) == {"id", "verdict", "detail", "failing_proof", "warnings"}
        for item in report["results"]
    )
    assert not Path(report["shards"][0]["temporary_root"]).exists()
    events = [
        json.loads(line)
        for line in Path(report["event_log_path"])
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert events[-1]["event"] == "run_finished"
    assert all("proof" not in event for event in events)
    assert verify(root) == []
    assert "mutation harness:" not in capsys.readouterr().err


def test_public_two_shard_run_relocates_editable_direct_url_metadata(
    tmp_path: Path,
) -> None:
    root, plan = _integration_repository(tmp_path)
    _add_editable_python_workspace(root, plan)

    exit_code = main(
        [
            "--root",
            str(root),
            "run",
            "--plan",
            str(plan),
            "--shards",
            "2",
            "--assert-all-killed",
            "--progress",
            "none",
        ]
    )

    assert exit_code == 0
    report = json.loads(
        (root / ".mutation-harness" / "report.json").read_text(encoding="utf-8")
    )
    assert report["aggregate_status"] == "COMPLETE"
    assert [result["verdict"] for result in report["results"]] == [
        "KILLED",
        "KILLED",
    ]
    source_direct_url = next((root / ".venv").rglob("direct_url.json"))
    assert json.loads(source_direct_url.read_text(encoding="utf-8"))["url"] == (
        root.resolve().as_uri()
    )
    assert not Path(report["shards"][0]["temporary_root"]).exists()
    assert verify(root) == []


def test_public_cli_refuses_shards_without_closed_plan_opt_in(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root, plan = _integration_repository(tmp_path)
    raw = json.loads(plan.read_text(encoding="utf-8"))
    raw.pop("sharding")
    plan.write_text(json.dumps(raw), encoding="utf-8")

    exit_code = main(["--root", str(root), "run", "--plan", str(plan), "--shards", "2"])

    assert exit_code == 2
    assert "does not opt in to sharding" in capsys.readouterr().err
    assert not (root / ".mutation-harness" / "state.json").exists()


def test_public_cli_enforces_max_shards_before_staging(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root, plan = _integration_repository(tmp_path)

    exit_code = main(["--root", str(root), "run", "--plan", str(plan), "--shards", "3"])

    assert exit_code == 2
    assert "exceeds declared max_shards 2" in capsys.readouterr().err
    assert not (root / ".mutation-harness" / "state.json").exists()


def test_public_cli_applies_only_before_effective_shard_assignment(
    tmp_path: Path,
) -> None:
    root, plan = _integration_repository(tmp_path)

    exit_code = main(
        [
            "--root",
            str(root),
            "run",
            "--plan",
            str(plan),
            "--only",
            "M2",
            "--shards",
            "2",
            "--assert-all-killed",
            "--progress",
            "none",
        ]
    )

    assert exit_code == 0
    report = json.loads(
        (root / ".mutation-harness" / "report.json").read_text(encoding="utf-8")
    )
    assert report["requested_shards"] == 2
    assert report["effective_shards"] == 1
    assert [item["id"] for item in report["results"]] == ["M2"]


def test_public_recovery_cleans_an_owned_incomplete_run_and_unblocks_verify(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root, plan = _integration_repository(tmp_path)
    source_manifest = build_source_manifest(root)
    plan_digest = hashlib.sha256(plan.read_bytes()).hexdigest()
    run_id = "run-lane-e-recovery"
    assignment = select_and_assign(["M1"], None, 1)[0]
    temporary_root = create_private_temp_root(prefix="mutation-harness-recovery-")
    lease = begin_coordinator_run(
        root,
        run_id=run_id,
        source_head=source_manifest.head,
        source_manifest=source_manifest_to_dict(source_manifest),
        source_manifest_digest=source_manifest.digest,
        plan_path=plan,
        plan_digest=plan_digest,
        requested_shards=1,
        effective_shards=1,
        temporary_root_factory=lambda _run_id: temporary_root,
    )
    _write_manifest(lease)
    staged = stage_execution_tree(
        root,
        temporary_root / "shard-0",
        run_id=run_id,
        shard_index=0,
        source_manifest=source_manifest,
        plan_digest=plan_digest,
        workspace_inputs=(),
    )
    liveness_lock = staged.root / ".mutation-harness" / "child.liveness"
    liveness_lock.touch()
    _record_child(
        lease,
        ChildSpec(
            assignment=assignment,
            root=staged.root,
            source_root=root,
            temporary_root=temporary_root,
            argv=("unused",),
            result_stream=staged.root / ".mutation-harness" / "results.jsonl",
            ownership_marker=staged.ownership_marker,
            liveness_lock=liveness_lock,
        ),
    )
    lease.transition("running")
    lease.retain_and_release()

    assert main(["--root", str(root), "verify"]) == 1
    assert main(["--root", str(root), "recover-run", "--run-id", run_id]) == 0

    assert not staged.root.exists()
    assert verify(root) == []
    captured = capsys.readouterr()
    assert (
        "No result from this tree is trustworthy until it is recovered" in captured.err
    )
    assert "recovered run run-lane-e-recovery as aborted" in captured.out


def test_public_force_recovery_cleans_zero_recorded_staging_shards(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root, plan = _integration_repository(tmp_path)
    source_manifest = build_source_manifest(root)
    plan_digest = hashlib.sha256(plan.read_bytes()).hexdigest()
    run_id = "run-lane-e-zero-shard"
    temporary_root = create_private_temp_root(prefix="mutation-harness-zero-shard-")
    lease = begin_coordinator_run(
        root,
        run_id=run_id,
        source_head=source_manifest.head,
        source_manifest=source_manifest_to_dict(source_manifest),
        source_manifest_digest=source_manifest.digest,
        plan_path=plan,
        plan_digest=plan_digest,
        requested_shards=2,
        effective_shards=2,
        temporary_root_factory=lambda _run_id: temporary_root,
    )
    _write_manifest(lease)
    staged = stage_execution_tree(
        root,
        temporary_root / "shard-0",
        run_id=run_id,
        shard_index=0,
        source_manifest=source_manifest,
        plan_digest=plan_digest,
        workspace_inputs=(),
    )
    lease.transition("aborted")
    lease.retain_and_release()

    assert (
        json.loads(
            (root / ".mutation-harness" / "state.json").read_text(encoding="utf-8")
        )["coordinator_run"]["shards"]
        == []
    )
    assert (
        main(
            [
                "--root",
                str(root),
                "recover-run",
                "--run-id",
                run_id,
                "--force",
            ]
        )
        == 0
    )

    assert not staged.root.exists()
    assert not temporary_root.exists()
    assert verify(root) == []
    captured = capsys.readouterr()
    assert "FORCE RECOVERY PREFLIGHT run-lane-e-zero-shard" in captured.out
    assert "removed 1 owned staging shard(s)" in captured.out

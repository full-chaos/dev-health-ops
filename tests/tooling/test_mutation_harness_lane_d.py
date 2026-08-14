"""Lane D contracts for mutation-plan opt-in and recovery."""

from __future__ import annotations

import fcntl
import json
import os
import shutil
from pathlib import Path

import pytest

from scripts.mutation_harness_optin import (
    PlanContractError,
    ShardingPlan,
    external_resource_environment,
    load_plan_contract,
    validate_requested_shards,
)
from scripts.mutation_harness_recovery import (
    RecoveryError,
    hold_liveness_lock,
    recover_run,
)


def _mutation(proof: list[list[str]] | None = None) -> dict[str, object]:
    return {
        "id": "M1",
        "file": "widget.go",
        "find": "if guarded {",
        "replace": "if false {",
        "proof": proof or [["go", "test", "-count=1", "-run", "^TestGuard$", "./..."]],
        "rationale": "the guard must remain observable",
    }


def _plan(
    tmp_path: Path,
    *,
    mutation: dict[str, object] | None = None,
    sharding: dict[str, object] | None = None,
    **extra: object,
) -> Path:
    raw: dict[str, object] = {
        "schema_version": 1,
        "name": "lane-d-synthetic",
        "$limitation": "synthetic scope only",
        "mutations": [mutation or _mutation()],
        **extra,
    }
    if sharding is not None:
        raw["sharding"] = sharding
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    return path


def _opt_in(**overrides: object) -> dict[str, object]:
    contract: dict[str, object] = {
        "max_shards": 2,
        "workspace_inputs": [".venv"],
        "external_resources": "none",
        "shared_mutable_resource_exclusions": [
            "go-build-cache",
            "go-module-cache",
        ],
    }
    contract.update(overrides)
    return contract


@pytest.mark.parametrize("unknown", ["sharidng", "unknown_metadata"])
def test_closed_top_level_vocabulary_names_unknown_key(
    tmp_path: Path, unknown: str
) -> None:
    plan = _plan(tmp_path)
    raw = json.loads(plan.read_text(encoding="utf-8"))
    raw[unknown] = True
    plan.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(PlanContractError, match=unknown):
        load_plan_contract(plan)


def test_closed_mutation_vocabulary_names_unknown_key(tmp_path: Path) -> None:
    mutation = _mutation()
    mutation["proofs"] = mutation.pop("proof")
    with pytest.raises(PlanContractError, match="proofs"):
        load_plan_contract(_plan(tmp_path, mutation=mutation))


@pytest.mark.parametrize("max_shards", [0, -1, True, 1.5, "2"])
def test_max_shards_must_be_an_integer_at_least_one(
    tmp_path: Path, max_shards: object
) -> None:
    with pytest.raises(PlanContractError, match="max_shards"):
        load_plan_contract(_plan(tmp_path, sharding=_opt_in(max_shards=max_shards)))


@pytest.mark.parametrize(
    "workspace_input",
    [
        "../outside",
        "/absolute",
        ".git",
        ".git/hooks",
        ".mutation-harness",
        ".mutation-harness/runs",
    ],
)
def test_workspace_inputs_are_safe_repo_relative_paths(
    tmp_path: Path, workspace_input: str
) -> None:
    with pytest.raises(PlanContractError, match="workspace_inputs"):
        load_plan_contract(
            _plan(
                tmp_path,
                sharding=_opt_in(workspace_inputs=[workspace_input]),
            )
        )


@pytest.mark.parametrize("policy", ["", "readwrite", "NONE", 1, None])
def test_external_resources_use_the_closed_policy_set(
    tmp_path: Path, policy: object
) -> None:
    with pytest.raises(PlanContractError, match="external_resources"):
        load_plan_contract(_plan(tmp_path, sharding=_opt_in(external_resources=policy)))


@pytest.mark.parametrize("policy", ["none", "isolated", "shared-readonly"])
def test_each_external_resource_policy_is_accepted(tmp_path: Path, policy: str) -> None:
    _, contract = load_plan_contract(
        _plan(tmp_path, sharding=_opt_in(external_resources=policy))
    )
    assert contract is not None
    assert contract.external_resources == policy


def test_isolated_resources_receive_distinct_shard_namespaces(tmp_path: Path) -> None:
    _, contract = load_plan_contract(
        _plan(tmp_path, sharding=_opt_in(external_resources="isolated"))
    )
    assert contract is not None
    assert external_resource_environment(contract, "run-3807", 1) == {
        "MUTATION_HARNESS_RUN_ID": "run-3807",
        "MUTATION_HARNESS_SHARD_INDEX": "1",
    }


def test_go_cache_exclusions_must_be_explicitly_named(tmp_path: Path) -> None:
    with pytest.raises(PlanContractError, match="go-build-cache.*go-module-cache"):
        load_plan_contract(
            _plan(
                tmp_path,
                sharding=_opt_in(shared_mutable_resource_exclusions=[]),
            )
        )


def test_request_above_declared_max_is_refused(tmp_path: Path) -> None:
    _, contract = load_plan_contract(_plan(tmp_path, sharding=_opt_in()))
    assert contract is not None
    with pytest.raises(PlanContractError, match="requested 3.*max_shards 2"):
        validate_requested_shards(contract, 3)


def test_parallel_request_without_opt_in_is_refused() -> None:
    with pytest.raises(PlanContractError, match="does not opt in"):
        validate_requested_shards(None, 2)


@pytest.mark.parametrize(
    "proof",
    [
        [["python3", "-m", "pytest", "tests/tooling"]],
        [["pytest", "tests/tooling"]],
        [["uv", "run", "pytest", "tests/tooling"]],
        [["env", "PYTHONPATH=$PWD/src", "python3", "-m", "pytest"]],
        [["bash", "-c", "python -m pytest tests/tooling"]],
    ],
)
def test_path_resolved_python_entry_points_are_refused(
    tmp_path: Path, proof: list[list[str]]
) -> None:
    with pytest.raises(PlanContractError, match="PATH-resolved Python"):
        load_plan_contract(
            _plan(tmp_path, mutation=_mutation(proof), sharding=_opt_in())
        )


def test_python_without_shard_rooted_imports_is_refused(tmp_path: Path) -> None:
    proof = [["$PWD/.venv/bin/python", "-m", "pytest", "tests/tooling"]]
    with pytest.raises(PlanContractError, match="shard-rooted import"):
        load_plan_contract(
            _plan(tmp_path, mutation=_mutation(proof), sharding=_opt_in())
        )


def test_unsafe_pwd_anchored_console_script_is_refused(tmp_path: Path) -> None:
    proof = [["$PWD/.venv/bin/pytest", "tests/tooling"]]
    with pytest.raises(PlanContractError, match="console script"):
        load_plan_contract(
            _plan(tmp_path, mutation=_mutation(proof), sharding=_opt_in())
        )


def test_bash_wrapper_with_pwd_interpreter_and_import_path_is_accepted(
    tmp_path: Path,
) -> None:
    proof = [
        [
            "bash",
            "-c",
            'PYTHONPATH="$PWD/src" PYTHON="$PWD/.venv/bin/python" go test '
            "-count=1 -run '^TestGuard$' ./...",
        ]
    ]
    _, contract = load_plan_contract(
        _plan(tmp_path, mutation=_mutation(proof), sharding=_opt_in())
    )
    assert contract == ShardingPlan(
        max_shards=2,
        workspace_inputs=(".venv",),
        external_resources="none",
        shared_mutable_resource_exclusions=(
            "go-build-cache",
            "go-module-cache",
        ),
    )


def test_named_go_test_without_count_one_is_refused(tmp_path: Path) -> None:
    proof = [["go", "test", "-run", "^TestGuard$", "./..."]]
    with pytest.raises(PlanContractError, match="-count=1"):
        load_plan_contract(
            _plan(tmp_path, mutation=_mutation(proof), sharding=_opt_in())
        )


def test_named_go_test_with_count_one_is_accepted(tmp_path: Path) -> None:
    proof = [["go", "test", "-count=1", "-run", "^TestGuard$", "./..."]]
    _, contract = load_plan_contract(
        _plan(tmp_path, mutation=_mutation(proof), sharding=_opt_in())
    )
    assert contract is not None


def test_build_only_go_test_is_exempt_from_count_one(tmp_path: Path) -> None:
    proof = [["go", "test", "-run", "^$", "./..."]]
    _, contract = load_plan_contract(
        _plan(tmp_path, mutation=_mutation(proof), sharding=_opt_in())
    )
    assert contract is not None


def test_representative_gitlab_plan_is_accepted() -> None:
    root = Path(__file__).resolve().parents[2]
    plan = (
        root
        / "internal/providersync/testdata/mutation-plans/gitlab_feature_flags_route.json"
    )
    _, contract = load_plan_contract(plan)
    assert contract is not None
    validate_requested_shards(contract, 2)


def test_named_python_plan_is_refused_when_artificially_opted_in(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[2]
    source = root / "tests/tooling/mutation-plans/scheduled-reports.json"
    raw = json.loads(source.read_text(encoding="utf-8"))
    raw["sharding"] = _opt_in()
    plan = tmp_path / source.name
    plan.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(PlanContractError, match="PATH-resolved Python"):
        load_plan_contract(plan)


def _recovery_fixture(
    tmp_path: Path,
    *,
    run_id: str = "run-3807",
    marker: object | None = None,
    state_overrides: dict[str, object] | None = None,
) -> tuple[Path, Path, Path, Path]:
    root = tmp_path / "repo"
    run_dir = root / ".mutation-harness" / "runs" / run_id
    shard = tmp_path / "private-run" / "shard-0"
    shard.mkdir(parents=True)
    ownership_marker = shard / ".mutation-harness-owner.json"
    marker_payload = marker
    if marker_payload is None:
        marker_payload = {
            "schema_version": 1,
            "run_id": run_id,
            "shard_index": 0,
            "source_manifest_digest": "source-digest",
            "plan_digest": "plan-digest",
        }
    ownership_marker.write_text(json.dumps(marker_payload), encoding="utf-8")
    liveness_lock = shard / ".mutation-harness-liveness.lock"
    liveness_lock.write_text("", encoding="utf-8")

    manifest = {
        "schema_version": 1,
        "run_id": run_id,
        "source_root": str(root.resolve()),
        "source_manifest": {"head": "head", "entries": [], "digest": "source-digest"},
        "source_manifest_digest": "source-digest",
        "plan_digest": "plan-digest",
        "shards": [
            {
                "shard_index": 0,
                "root": str(shard.resolve()),
                "source_root": str(root.resolve()),
                "temporary_root": str((tmp_path / "private-run").resolve()),
                "ownership_marker": str(ownership_marker.resolve()),
                "liveness_lock": str(liveness_lock.resolve()),
                "assigned_ordinals": [0],
            }
        ],
    }
    run_dir.mkdir(parents=True)
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    coordinator: dict[str, object] = {
        "run_id": run_id,
        "pid": os.getpid(),
        "process_start_time": "unrelated-live-process",
        "lifecycle": "running",
        "source_manifest_digest": "source-digest",
        "plan_digest": "plan-digest",
        "manifest_path": str(manifest_path.resolve()),
        "shards": manifest["shards"],
    }
    if state_overrides:
        coordinator.update(state_overrides)
    state_path = root / ".mutation-harness" / "state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps({"schema_version": 1, "coordinator_run": coordinator}),
        encoding="utf-8",
    )
    return root, shard, ownership_marker, liveness_lock


def _remove_owned_tree(root: Path, marker: Path, shard_index: int) -> None:
    assert marker.is_relative_to(root)
    assert shard_index == 0
    shutil.rmtree(root)


def test_reused_live_unrelated_pid_does_not_block_without_held_lock(
    tmp_path: Path,
) -> None:
    root, shard, _, _ = _recovery_fixture(tmp_path)
    message = recover_run(root, "run-3807", cleanup_owned_tree=_remove_owned_tree)
    assert message == "recovered run run-3807 as aborted; removed 1 owned shard(s)"
    assert not shard.exists()
    state = json.loads(
        (root / ".mutation-harness/state.json").read_text(encoding="utf-8")
    )
    assert "coordinator_run" not in state


def test_held_liveness_lock_refuses_recovery(tmp_path: Path) -> None:
    root, shard, _, liveness_lock = _recovery_fixture(tmp_path)
    with hold_liveness_lock(liveness_lock):
        with pytest.raises(RecoveryError, match="liveness lock is held"):
            recover_run(root, "run-3807", cleanup_owned_tree=_remove_owned_tree)
    assert shard.exists()
    assert "coordinator_run" in json.loads(
        (root / ".mutation-harness/state.json").read_text(encoding="utf-8")
    )


def test_manifest_path_escape_is_refused(tmp_path: Path) -> None:
    root, shard, _, _ = _recovery_fixture(tmp_path)
    state_path = root / ".mutation-harness/state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    escaped = tmp_path / "outside-manifest.json"
    escaped.write_text("{}", encoding="utf-8")
    state["coordinator_run"]["manifest_path"] = str(escaped)
    state_path.write_text(json.dumps(state), encoding="utf-8")
    with pytest.raises(RecoveryError, match="manifest path"):
        recover_run(root, "run-3807", cleanup_owned_tree=_remove_owned_tree)
    assert shard.exists()


def test_manifest_owned_path_cannot_escape_its_shard(tmp_path: Path) -> None:
    root, shard, _, _ = _recovery_fixture(tmp_path)
    sentinel = tmp_path / "outside-owner.json"
    sentinel.write_text("keep", encoding="utf-8")
    state = json.loads(
        (root / ".mutation-harness/state.json").read_text(encoding="utf-8")
    )
    manifest_path = Path(state["coordinator_run"]["manifest_path"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["shards"][0]["ownership_marker"] = str(sentinel.resolve())
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(RecoveryError, match="escapes shard root"):
        recover_run(root, "run-3807", cleanup_owned_tree=_remove_owned_tree)
    assert shard.exists()
    assert sentinel.read_text(encoding="utf-8") == "keep"


@pytest.mark.parametrize("unreadable", [False, True])
def test_wrong_or_unreadable_ownership_marker_is_retained(
    tmp_path: Path, unreadable: bool
) -> None:
    root, shard, marker, _ = _recovery_fixture(
        tmp_path,
        marker={"schema_version": 1, "run_id": "foreign", "shard_index": 0},
    )
    if unreadable:
        marker.unlink()
        marker.mkdir()
    with pytest.raises(RecoveryError, match="ownership marker"):
        recover_run(root, "run-3807", cleanup_owned_tree=_remove_owned_tree)
    assert shard.exists()


def test_force_preflight_is_exact_and_never_removes_unlisted_path(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root, shard, _, _ = _recovery_fixture(tmp_path)
    sentinel = tmp_path / "outside-sentinel"
    sentinel.write_text("keep", encoding="utf-8")
    state_path = root / ".mutation-harness/state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    manifest_path = Path(state["coordinator_run"]["manifest_path"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    foreign = tmp_path / "private-run" / "shard-foreign"
    foreign.mkdir()
    foreign_marker = foreign / ".mutation-harness-owner.json"
    foreign_marker.write_text(
        json.dumps({"schema_version": 1, "run_id": "foreign", "shard_index": 1}),
        encoding="utf-8",
    )
    foreign_lock = foreign / ".mutation-harness-liveness.lock"
    foreign_lock.write_text("", encoding="utf-8")
    manifest["shards"].append(
        {
            "shard_index": 1,
            "root": str(foreign.resolve()),
            "source_root": str(root.resolve()),
            "temporary_root": str((tmp_path / "private-run").resolve()),
            "ownership_marker": str(foreign_marker.resolve()),
            "liveness_lock": str(foreign_lock.resolve()),
            "assigned_ordinals": [1],
        }
    )
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(RecoveryError, match="retained 1 shard"):
        recover_run(
            root,
            "run-3807",
            force=True,
            cleanup_owned_tree=_remove_owned_tree,
        )
    output = capsys.readouterr().out
    assert output == (
        "FORCE RECOVERY PREFLIGHT run-3807\n"
        "REMOVE:\n"
        f"  {shard.resolve()}\n"
        "LEAVE:\n"
        f"  {foreign.resolve()}\n"
        "UNKNOWN:\n"
        f"  {foreign_marker.resolve()}: ownership marker run_id is 'foreign', "
        "expected 'run-3807'\n"
    )
    assert not shard.exists()
    assert foreign.exists()
    assert sentinel.read_text(encoding="utf-8") == "keep"
    assert "coordinator_run" in json.loads(state_path.read_text(encoding="utf-8"))


def test_root_state_is_cleared_only_after_verified_cleanup(tmp_path: Path) -> None:
    root, _, _, _ = _recovery_fixture(tmp_path)
    state_path = root / ".mutation-harness/state.json"

    def cleanup(shard: Path, marker: Path, shard_index: int) -> None:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        assert state["coordinator_run"]["lifecycle"] == "recovering"
        _remove_owned_tree(shard, marker, shard_index)

    recover_run(root, "run-3807", cleanup_owned_tree=cleanup)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert "coordinator_run" not in state


def test_recovery_holds_the_acquired_liveness_lock_through_cleanup(
    tmp_path: Path,
) -> None:
    root, _, _, liveness_lock = _recovery_fixture(tmp_path)

    def cleanup(shard: Path, marker: Path, shard_index: int) -> None:
        descriptor = os.open(liveness_lock, os.O_RDWR | os.O_CLOEXEC)
        try:
            with pytest.raises(BlockingIOError):
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(descriptor)
        _remove_owned_tree(shard, marker, shard_index)

    recover_run(root, "run-3807", cleanup_owned_tree=cleanup)


def test_force_never_clears_unverified_applied_state(tmp_path: Path) -> None:
    root, shard, _, _ = _recovery_fixture(tmp_path)
    shard_state = shard / ".mutation-harness"
    shard_state.mkdir()
    (shard_state / "state.json").write_text(
        json.dumps({"schema_version": 1, "applied": {"mutation_id": "M1"}}),
        encoding="utf-8",
    )

    def refuse_restore(_: Path) -> str:
        raise RuntimeError("restore mismatch")

    with pytest.raises(RecoveryError, match="restore mismatch"):
        recover_run(
            root,
            "run-3807",
            force=True,
            restore_mutation=refuse_restore,
            cleanup_owned_tree=_remove_owned_tree,
        )
    assert shard.exists()
    assert json.loads(
        (shard / ".mutation-harness/state.json").read_text(encoding="utf-8")
    )["applied"]
    assert "coordinator_run" in json.loads(
        (root / ".mutation-harness/state.json").read_text(encoding="utf-8")
    )

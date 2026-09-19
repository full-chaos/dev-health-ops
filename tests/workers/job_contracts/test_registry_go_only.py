"""A registry kind marked go_only is Go's alone.

The Python loader accepts it without a Python payload type, still validates
its entry, and never hands it to a Python producer or decoder. Every other
shape of drift between registry.json and the Python contract types still
fails the load.
"""

from __future__ import annotations

import copy
import json
import shutil
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy.orm import SessionTransactionOrigin

from dev_health_ops.jobs.contracts import (
    ContractDecodeError,
    capabilities_for_queues,
    check_rollout_capabilities,
    decode_envelope,
    default_contract_root,
    load_migration_jobs,
    load_registry,
)
from dev_health_ops.jobs.outbox import OutboxEnqueueError, enqueue_worker_job

GO_ONLY_KIND = "system.go_only_probe"
_MISSING = object()


def _contract_tree(
    tmp_path: Path,
    *,
    kind: str = GO_ONLY_KIND,
    go_only: Any = True,
    mutate: Any = None,
) -> Path:
    """Copy the checked-in tree and add one kind cloned from system.heartbeat."""

    candidate = tmp_path / "v1"
    shutil.copytree(default_contract_root(), candidate)
    registry_path = candidate / "registry.json"
    registry = json.loads(registry_path.read_text())
    template = next(
        job for job in registry["jobs"] if job["kind"] == "system.heartbeat"
    )
    job = copy.deepcopy(template)
    job["kind"] = kind
    if go_only is not _MISSING:
        job["go_only"] = go_only
    registry["jobs"].append(job)
    registry["jobs"].sort(key=lambda entry: entry["kind"])
    if mutate is not None:
        mutate(registry)
    registry_path.write_text(json.dumps(registry))

    state_path = candidate / "migration-state.json"
    state = json.loads(state_path.read_text())
    policy = copy.deepcopy(
        next(entry for entry in state["jobs"] if entry["kind"] == "system.heartbeat")
    )
    policy["kind"] = kind
    state["jobs"].append(policy)
    state["jobs"].sort(key=lambda entry: entry["kind"])
    state_path.write_text(json.dumps(state))
    return candidate


def test_go_only_kind_loads_but_is_not_a_python_contract(tmp_path: Path) -> None:
    registry = load_registry(_contract_tree(tmp_path))

    assert set(registry.go_only_kinds) - set(load_registry().go_only_kinds) == {
        GO_ONLY_KIND
    }
    assert GO_ONLY_KIND not in {contract.kind for contract in registry.contracts}
    with pytest.raises(ContractDecodeError, match="unknown registry kind"):
        registry.by_kind(GO_ONLY_KIND)
    assert registry.by_kind("system.heartbeat").kind == "system.heartbeat"


def test_checked_in_registry_declares_no_go_only_kind_python_owns() -> None:
    registry = load_registry()

    assert not set(registry.go_only_kinds) & {
        contract.kind for contract in registry.contracts
    }


def test_go_only_policy_row_is_go_runtime_only(tmp_path: Path) -> None:
    candidate = _contract_tree(tmp_path)
    jobs = load_migration_jobs(candidate)

    assert GO_ONLY_KIND not in {job.kind for job in jobs}
    assert {job.kind for job in jobs} == {
        contract.kind for contract in load_registry(candidate).contracts
    }


def test_only_a_literal_true_marker_hides_a_policy_row(tmp_path: Path) -> None:
    jobs = load_migration_jobs(_contract_tree(tmp_path, go_only="true"))

    assert GO_ONLY_KIND in {job.kind for job in jobs}


def test_go_only_policy_row_is_still_validated(tmp_path: Path) -> None:
    candidate = _contract_tree(tmp_path)
    state_path = candidate / "migration-state.json"
    state = json.loads(state_path.read_text())
    for job in state["jobs"]:
        if job["kind"] == GO_ONLY_KIND:
            job["route"] = "shadow"
    state_path.write_text(json.dumps(state))

    with pytest.raises(ContractDecodeError, match="state route is inconsistent"):
        load_migration_jobs(candidate)


def test_rollout_check_ignores_go_only_kinds(tmp_path: Path) -> None:
    candidate = _contract_tree(tmp_path)
    registry = load_registry(candidate)
    report = capabilities_for_queues(registry, ["heartbeat"])

    assert GO_ONLY_KIND not in {contract.kind for contract in report.contracts}
    check_rollout_capabilities(
        tuple(
            job
            for job in load_migration_jobs(candidate)
            if job.required_queues == ("heartbeat",)
        ),
        (report,),
        (report,),
    )


def test_python_refuses_to_enqueue_a_go_only_kind(tmp_path: Path) -> None:
    registry = load_registry(_contract_tree(tmp_path))
    session = SimpleNamespace(
        get_transaction=lambda: SimpleNamespace(origin=SessionTransactionOrigin.BEGIN)
    )
    payload = SimpleNamespace(KIND=GO_ONLY_KIND)

    with pytest.raises(OutboxEnqueueError, match="worker job contract is invalid"):
        enqueue_worker_job(
            session,  # type: ignore[arg-type]
            payload,  # type: ignore[arg-type]
            correlation_id="ABC-123",
            idempotency_key="ABC-123",
            domain_id="ABC-123",
            registry=registry,
        )


def test_python_refuses_to_decode_a_go_only_kind() -> None:
    with pytest.raises(ContractDecodeError):
        decode_envelope(GO_ONLY_KIND, b"{}")


@pytest.mark.parametrize(
    ("value", "message"),
    [
        (False, "go_only must be true"),
        (None, "go_only must be true"),
        (0, "go_only must be true"),
        (1, "go_only must be true"),
        ("true", "go_only must be true"),
        ([True], "go_only must be true"),
        ({}, "go_only must be true"),
    ],
)
def test_go_only_accepts_only_literal_true(
    tmp_path: Path, value: object, message: str
) -> None:
    with pytest.raises(ContractDecodeError, match=message):
        load_registry(_contract_tree(tmp_path, go_only=value))


def test_unmarked_extra_kind_is_still_drift(tmp_path: Path) -> None:
    with pytest.raises(ContractDecodeError, match="drifts from Python contract types"):
        load_registry(_contract_tree(tmp_path, go_only=_MISSING))


def test_marking_a_python_kind_go_only_is_drift(tmp_path: Path) -> None:
    def mark_heartbeat(registry: dict[str, Any]) -> None:
        for job in registry["jobs"]:
            if job["kind"] == "system.heartbeat":
                job["go_only"] = True

    with pytest.raises(
        ContractDecodeError, match="marks a Python contract kind go_only"
    ):
        load_registry(_contract_tree(tmp_path, mutate=mark_heartbeat))


def test_go_only_kind_still_needs_its_schema(tmp_path: Path) -> None:
    def drop_schema(registry: dict[str, Any]) -> None:
        for job in registry["jobs"]:
            if job["kind"] == GO_ONLY_KIND:
                job["schema_versions"] = {"1": "schemas/absent.v1.schema.json"}

    with pytest.raises(ContractDecodeError):
        load_registry(_contract_tree(tmp_path, mutate=drop_schema))


def test_go_only_kind_takes_part_in_order_and_uniqueness(tmp_path: Path) -> None:
    def unsort(registry: dict[str, Any]) -> None:
        registry["jobs"].reverse()

    with pytest.raises(ContractDecodeError, match="not sorted"):
        load_registry(_contract_tree(tmp_path / "a", mutate=unsort))

    def go_only_first(registry: dict[str, Any]) -> None:
        jobs = registry["jobs"]
        index = next(i for i, job in enumerate(jobs) if job["kind"] == GO_ONLY_KIND)
        jobs.insert(0, jobs.pop(index))

    with pytest.raises(ContractDecodeError, match="not sorted"):
        load_registry(_contract_tree(tmp_path / "c", mutate=go_only_first))

    def duplicate(registry: dict[str, Any]) -> None:
        index = next(
            i for i, job in enumerate(registry["jobs"]) if job["kind"] == GO_ONLY_KIND
        )
        registry["jobs"].insert(index, copy.deepcopy(registry["jobs"][index]))

    with pytest.raises(ContractDecodeError, match="duplicate kinds"):
        load_registry(_contract_tree(tmp_path / "b", mutate=duplicate))


def test_go_only_kind_still_needs_every_registry_field(tmp_path: Path) -> None:
    def drop_queue(registry: dict[str, Any]) -> None:
        for job in registry["jobs"]:
            if job["kind"] == GO_ONLY_KIND:
                del job["queue"]

    with pytest.raises(ContractDecodeError, match="registry job must be an object"):
        load_registry(_contract_tree(tmp_path, mutate=drop_queue))


def test_migration_view_refuses_a_registry_without_jobs(tmp_path: Path) -> None:
    candidate = _contract_tree(tmp_path)
    (candidate / "registry.json").write_text(json.dumps({"jobs": {}}))

    with pytest.raises(ContractDecodeError, match="registry jobs are missing"):
        load_migration_jobs(candidate)

"""CUT-01: transitional workload inventory contract + CI enforcement.

Proves two things:

1. The real, checked-in inventory (contracts/jobs/v1/transitional-inventory.json)
   is currently consistent with independent code discovery on this tree --
   i.e. the CI gate (ci/check_transitional_inventory.py) passes today.
2. The gate actually *works*: a synthetic unowned Celery task, a synthetic
   duplicate-exclusive-ownership pair, and a synthetic stale anchor are each
   caught, using fixture trees so no real unowned surface is ever committed.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CHECKER_PATH = _REPO_ROOT / "ci" / "check_transitional_inventory.py"
_INVENTORY_PATH = (
    _REPO_ROOT / "contracts" / "jobs" / "v1" / "transitional-inventory.json"
)


def _load_checker():
    spec = importlib.util.spec_from_file_location(
        "check_transitional_inventory", _CHECKER_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


checker = _load_checker()


def test_real_tree_passes_the_gate():
    """The committed inventory must be consistent with real discovery today."""
    errors = checker.check(_REPO_ROOT, _INVENTORY_PATH)
    assert errors == [], "\n".join(errors)


def test_inventory_is_non_empty_and_matches_audit_row_count():
    inventory = checker.load_inventory(_INVENTORY_PATH)
    assert inventory["row_count"] == len(inventory["rows"])
    assert inventory["row_count"] == 147


def test_every_row_has_a_target_owner_and_acceptance_test():
    inventory = checker.load_inventory(_INVENTORY_PATH)
    for row in inventory["rows"]:
        assert row["target_owner"]["value"], row["id"]
        assert row["acceptance_test_id"], row["id"]


def test_no_two_primary_rows_share_a_target_kind_id():
    inventory = checker.load_inventory(_INVENTORY_PATH)
    seen: dict[str, str] = {}
    for row in inventory["rows"]:
        if row["owner_role"] != "primary":
            continue
        tkid = row["target_kind_id"]
        assert tkid, row["id"]
        assert tkid not in seen, (
            f"{tkid} claimed by both {seen.get(tkid)} and {row['id']}"
        )
        seen[tkid] = row["id"]


# ---------------------------------------------------------------------------
# Fixture-based proofs that the gate actually catches violations. None of these
# fixtures are real production surfaces; they are synthetic trees written to a
# tmp_path per test.
# ---------------------------------------------------------------------------


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def _minimal_valid_root(tmp_path: Path) -> Path:
    """A fixture root with one owned Celery task and a matching inventory row."""
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/workers/example_task.py",
        '"""Example worker module."""\n'
        "\n"
        "from dev_health_ops.workers.celery_app import celery_app\n"
        "\n"
        "\n"
        '@celery_app.task(bind=True, name="dev_health_ops.workers.tasks.owned_task")\n'
        "def owned_task(self):\n"
        "    return {}\n",
    )
    inventory = {
        "schema_version": 1,
        "row_count": 1,
        "rows": [
            {
                "id": "celery_task:src/dev_health_ops/workers/example_task.py:6",
                "surface": "owned_task",
                "class": "celery_task",
                "source": {
                    "file": "src/dev_health_ops/workers/example_task.py",
                    "line": 6,
                },
                "dispatch_mechanism": "celery_task_decorator",
                "owner_role": "primary",
                "target_owner": {"type": "native_process", "value": "Go ops profile"},
                "target_kind_id": "task:owned_task",
                "current_implementation_state": "celery_only",
                "verification_status": "verified",
                "compatibility_dependency": None,
                "deletion_evidence_requirement": "n/a (fixture)",
                "acceptance_test_id": "tests/workers/test_transitional_inventory_contract.py",
                "notes": "",
            }
        ],
    }
    _write(
        root / "contracts/jobs/v1/transitional-inventory.json",
        json.dumps(inventory, indent=2),
    )
    return root


def test_gate_passes_on_a_minimal_fully_owned_fixture_tree(tmp_path):
    root = _minimal_valid_root(tmp_path)
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    errors = checker.check(root, inventory_path)
    assert errors == []


def test_gate_catches_a_synthetic_unowned_celery_task(tmp_path):
    """Add a second Celery task decorator with no inventory row -- must fail."""
    root = _minimal_valid_root(tmp_path)
    _write(
        root / "src/dev_health_ops/workers/rogue_task.py",
        '"""Synthetic rogue task with no inventory row (test fixture only)."""\n'
        "\n"
        "from dev_health_ops.workers.celery_app import celery_app\n"
        "\n"
        "\n"
        '@celery_app.task(name="dev_health_ops.workers.tasks.unowned_rogue_task")\n'
        "def unowned_rogue_task():\n"
        "    return None\n",
    )
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    errors = checker.check(root, inventory_path)
    assert any("UNOWNED SURFACE" in e and "rogue_task.py:6" in e for e in errors), (
        errors
    )


def test_gate_catches_a_synthetic_unowned_beat_entry(tmp_path):
    """Add a Beat entry to config.py with no matching row -- must fail."""
    root = _minimal_valid_root(tmp_path)
    _write(
        root / "src/dev_health_ops/workers/config.py",
        "beat_schedule = {\n"
        '    "rogue-unowned-beat-entry": {\n'
        '        "task": "dev_health_ops.workers.tasks.unowned_rogue_task",\n'
        '        "schedule": 300.0,\n'
        "    },\n"
        "}\n",
    )
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    errors = checker.check(root, inventory_path)
    assert any("UNOWNED SURFACE" in e and "config.py:2" in e for e in errors), errors


def test_gate_catches_duplicate_exclusive_ownership(tmp_path):
    """Two rows both claiming owner_role=primary for the same target_kind_id
    must fail -- ownership of a given native target must be exclusive."""
    root = _minimal_valid_root(tmp_path)
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    inventory = checker.load_inventory(inventory_path)
    duplicate = dict(inventory["rows"][0])
    duplicate["id"] = "celery_task:src/dev_health_ops/workers/example_task.py:6b"
    duplicate["surface"] = "owned_task_duplicate_claim"
    # Same target_kind_id as the original row -- this is the violation.
    inventory["rows"].append(duplicate)
    inventory["row_count"] = len(inventory["rows"])
    _write(inventory_path, json.dumps(inventory, indent=2))

    errors = checker.check(root, inventory_path)
    assert any("DUPLICATE EXCLUSIVE OWNERSHIP" in e for e in errors), errors


def test_gate_catches_a_row_with_no_target_owner(tmp_path):
    root = _minimal_valid_root(tmp_path)
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    inventory = checker.load_inventory(inventory_path)
    inventory["rows"][0]["target_owner"] = {"type": "native_process", "value": ""}
    _write(inventory_path, json.dumps(inventory, indent=2))

    errors = checker.check(root, inventory_path)
    assert any("NO TARGET OWNER" in e for e in errors), errors


def test_gate_catches_a_stale_anchor_pointing_at_a_deleted_file(tmp_path):
    root = _minimal_valid_root(tmp_path)
    (root / "src/dev_health_ops/workers/example_task.py").unlink()

    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    errors = checker.check(root, inventory_path)
    assert any("STALE ANCHOR" in e for e in errors), errors


def test_gate_catches_a_stale_anchor_pointing_past_end_of_file(tmp_path):
    root = _minimal_valid_root(tmp_path)
    _write(root / "src/dev_health_ops/workers/example_task.py", "# truncated\n")

    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    errors = checker.check(root, inventory_path)
    assert any("STALE ANCHOR" in e for e in errors), errors


def test_discovery_skips_docstring_examples(tmp_path):
    """A `@celery_app.task` example embedded in a docstring must not be
    discovered as a real surface (regression guard for the docstring-aware
    scanner)."""
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/workers/documented.py",
        '"""Module doc.\n'
        "\n"
        "Usage::\n"
        "\n"
        "    @celery_app.task(bind=True)\n"
        "    def example(self):\n"
        "        pass\n"
        '"""\n',
    )
    surfaces = checker.discover_celery_tasks(root)
    assert surfaces == []


def test_discovery_skips_docstring_call_site_prose(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/example_module.py",
        '"""Docs.\n'
        "\n"
        "Schedules ``some_task.apply_async(countdown=5)`` eventually.\n"
        '"""\n',
    )
    literal, getattr_indirection = checker.discover_call_sites(root)
    assert literal == []
    assert getattr_indirection == []

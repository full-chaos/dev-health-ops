"""CUT-01: transitional workload inventory contract + CI enforcement.

Proves two things:

1. The real, checked-in inventory (contracts/jobs/v1/transitional-inventory.json)
   is currently consistent with independent code discovery on this tree --
   i.e. the CI gate (ci/check_transitional_inventory.py) passes today.
2. The gate actually *works*: for every discovery class (Celery task, Beat
   entry -- unconditional and conditional, literal dispatch, getattr
   indirection, celery-canvas import and bare invocation, bound-method/
   partial aliasing, REST and GraphQL API triggers including the
   shared-helper fan-in shape, registry kind, transport route, stream
   surface), a synthetic unowned surface is caught; plus duplicate-exclusive-
   ownership, missing target owner, unknown target_kind_id (closed
   vocabulary), and stale/content-drifted anchors. All fixtures are synthetic
   trees written to tmp_path -- no real unowned surface is ever committed.
"""

from __future__ import annotations

import ast
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
    # 147 Wave-0 audit rows + 5 added in CUT-01 round-2 hardening (Codex
    # HIGH-1): the missed chord(...)() dispatch, plus a fail-closed row for
    # each celery-canvas (chain/chord/group) import in the tree, + 1 added
    # in round-3 hardening (an ordinary two-hop API trigger the call-graph
    # fallback missed: billing/router.py's Stripe webhook), + 1 added in
    # round-4 hardening (a separately deployed second FastAPI app,
    # billing_edge.py, forwarding to that same handler cross-file), + 2
    # added for CHAOS-3404 (the new ask_dev_retention celery task and its
    # ask-dev-retention-sweep Beat entry).
    assert inventory["row_count"] == 146


def test_retired_beat_entries_are_evidenced_and_absent_from_source():
    inventory = checker.load_inventory(_INVENTORY_PATH)
    retired = inventory["retired_beat_entries"]

    assert retired == [
        {
            "name": "dispatch-scheduled-metrics",
            "cadence": "300s",
            "reason": (
                "No production ScheduledJob writer creates job_type='metrics'; the legacy "
                "dispatcher could only create arbitrary-config Celery work, which the durable "
                "Go daily-metrics contract cannot safely emulate."
            ),
            "evidence": (
                "CHAOS-3128 retirement decision: source audit found zero production writers "
                "and a local feature-stack PostgreSQL read-only audit found zero scheduled_jobs "
                "rows with job_type='metrics'."
            ),
        }
    ]
    assert (
        checker.validate_retired_beat_entries(
            inventory, checker.discover_all(_REPO_ROOT)
        )
        == []
    )


def test_retired_beat_reintroduction_fails_the_inventory_gate():
    inventory = {
        "retired_beat_entries": [
            {
                "name": "dispatch-scheduled-metrics",
                "cadence": "300s",
                "reason": "audited retirement",
                "evidence": "reviewed evidence",
            }
        ]
    }
    errors = checker.validate_retired_beat_entries(
        inventory,
        [
            checker.Surface(
                checker.CLASS_BEAT_ENTRY,
                "src/dev_health_ops/workers/config.py",
                1,
                "dispatch-scheduled-metrics",
            )
        ],
    )
    assert errors == [
        "RETIRED BEAT REINTRODUCED: 'dispatch-scheduled-metrics' appears in source and must "
        "either be removed again or deleted from retired_beat_entries after review"
    ]


def test_discovered_keys_and_row_keys_are_identical_sets():
    """The acceptance criterion is that the inventory *equals* independent
    code discovery -- not merely that totals match. Comparing only
    `Counter(class)` totals (or total row counts) lets a missed surface in
    one spot net against an unrelated scanner phantom in the same class and
    silently pass (Codex round-2 MED-3: e.g. a missed qualified canvas call
    balanced by a false-positive literal match elsewhere). Compare the
    actual (class, file, line) key SETS in both directions instead: nothing
    discovered may be missing from the inventory, and no inventory row may
    reference a (class, file, line) that discovery didn't independently
    find there."""
    inventory = checker.load_inventory(_INVENTORY_PATH)
    discovered = checker.discover_all(_REPO_ROOT)

    discovered_keys = {s.key() for s in discovered}
    row_keys = {
        (r["class"], r["source"]["file"], r["source"]["line"])
        for r in inventory["rows"]
    }

    missed = discovered_keys - row_keys
    phantom = row_keys - discovered_keys
    assert missed == set(), f"discovered but not inventoried: {missed}"
    assert phantom == set(), f"inventoried but not (re)discovered: {phantom}"
    assert discovered_keys == row_keys
    assert len(discovered) == inventory["row_count"]


def test_every_row_has_a_target_owner_and_acceptance_test():
    inventory = checker.load_inventory(_INVENTORY_PATH)
    for row in inventory["rows"]:
        assert row["target_owner"]["value"], row["id"]
        assert row["acceptance_test_id"], row["id"]


def _test_function_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text())
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name.startswith("test_")
    }


def test_every_acceptance_test_id_points_at_a_real_test_function():
    """`acceptance_test_id` is more than a truthy string: when it names a
    specific test (`path.py::test_name`), that test must actually exist in
    that file -- catches typos, renames, and (per Codex MED-3) a row copied
    from another row's default without being retargeted."""
    inventory = checker.load_inventory(_INVENTORY_PATH)
    file_function_cache: dict[str, set[str]] = {}
    for row in inventory["rows"]:
        acceptance = row["acceptance_test_id"]
        file_part, sep, func_part = acceptance.partition("::")
        path = _REPO_ROOT / file_part
        assert path.is_file(), f"{row['id']}: acceptance test file missing: {file_part}"
        if not sep:
            continue
        if file_part not in file_function_cache:
            file_function_cache[file_part] = _test_function_names(path)
        assert func_part in file_function_cache[file_part], (
            f"{row['id']}: {func_part!r} not defined in {file_part}"
        )


def test_billing_rows_point_at_the_billing_bridge_test():
    """Regression guard for a Codex MED-3 finding: the billing dispatch rows
    previously pointed at unrelated tests (e.g. tests/test_sync_units.py for
    the API call site row). The billing-related rows whose surface text
    names "billing" (the Celery task, its API call site, its registry kind,
    and the round-4 billing_edge.py cross-file forwarding row) must all
    point at the real operational-bridge billing test."""
    inventory = checker.load_inventory(_INVENTORY_PATH)
    billing_rows = [r for r in inventory["rows"] if "billing" in r["surface"].lower()]
    assert len(billing_rows) == 4, billing_rows
    for row in billing_rows:
        assert row["acceptance_test_id"] == (
            "tests/api/test_worker_operational_bridge.py"
            "::test_internal_bridge_passes_only_durable_billing_reference"
        ), row


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


def _make_row(
    surface: str,
    cls: str,
    file: str,
    line: int,
    *,
    owner_role: str = "contributor",
    target_owner: dict | None = None,
    target_kind_id: str | None = None,
    current_implementation_state: str = "celery_only",
    route_mount_prefix: str = "",
) -> dict:
    """A minimal-but-complete inventory row for end-to-end fixture tests --
    every required field filled with an inert placeholder except the ones
    the caller supplies, so a synthesized surface can be turned into a
    passing row without repeating the full schema each time."""
    return {
        "id": f"{cls}:{file}:{line}",
        "surface": surface,
        "class": cls,
        "source": {"file": file, "line": line},
        "dispatch_mechanism": "n/a",
        "owner_role": owner_role,
        "target_owner": target_owner or {"type": "native_process", "value": "n/a"},
        "target_kind_id": target_kind_id,
        "current_implementation_state": current_implementation_state,
        "verification_status": "verified",
        "compatibility_dependency": None,
        "deletion_evidence_requirement": "n/a (fixture)",
        "acceptance_test_id": "tests/workers/test_transitional_inventory_contract.py",
        "notes": "",
        "route_mount_prefix": route_mount_prefix,
    }


def _inventory_root_with_rows(base_root: Path, rows: list[dict]) -> Path:
    inventory = {"schema_version": 1, "row_count": len(rows), "rows": rows}
    _write(
        base_root / "contracts/jobs/v1/transitional-inventory.json",
        json.dumps(inventory, indent=2),
    )
    return base_root


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
        '@celery_app.task(bind=True, name="dev_health_ops.workers.tasks.health_check")\n'
        "def health_check(self):\n"
        "    return {}\n",
    )
    inventory = {
        "schema_version": 1,
        "row_count": 1,
        "rows": [
            {
                "id": "celery_task:src/dev_health_ops/workers/example_task.py:6",
                "surface": "health_check",
                "class": "celery_task",
                "source": {
                    "file": "src/dev_health_ops/workers/example_task.py",
                    "line": 6,
                },
                "dispatch_mechanism": "celery_task_decorator",
                "owner_role": "primary",
                "target_owner": {
                    "type": "native_process",
                    "value": "Go worker consuming the webhooks queue",
                },
                "target_kind_id": "task:health_check",
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


def test_gate_catches_a_stale_anchor_with_content_drift(tmp_path):
    """The anchored line existing and being in-range isn't enough -- its
    content must still look like the row's declared class (Codex MED-1):
    the file keeps the same length, but line 6 is no longer a Celery task
    decorator, so only the content check (not existence/EOF) can catch it."""
    root = _minimal_valid_root(tmp_path)
    _write(
        root / "src/dev_health_ops/workers/example_task.py",
        '"""Example worker module."""\n'
        "\n"
        "from dev_health_ops.workers.celery_app import celery_app\n"
        "\n"
        "\n"
        "# the decorator that used to live here was deleted\n"
        "def health_check(self):\n"
        "    return {}\n",
    )
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    errors = checker.check(root, inventory_path)
    assert any("STALE ANCHOR" in e and "content drift" in e for e in errors), errors


def test_gate_catches_an_unknown_target_kind_id(tmp_path):
    """A row can't dodge duplicate-primary detection by renaming its
    target_kind_id to a variant that was never actually discovered in
    source (Codex MED-2 -- closed-vocabulary check)."""
    root = _minimal_valid_root(tmp_path)
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    inventory = checker.load_inventory(inventory_path)
    inventory["rows"][0]["target_kind_id"] = "kind:not-a-real-registry-kind"
    inventory["rows"][0]["target_owner"] = {
        "type": "native_kind",
        "value": "not-a-real-registry-kind",
    }
    _write(inventory_path, json.dumps(inventory, indent=2))

    errors = checker.check(root, inventory_path)
    assert any("UNKNOWN target_kind_id" in e for e in errors), errors


def test_gate_accepts_a_real_target_kind_id():
    """Sanity check that the closed-vocabulary check doesn't false-positive
    on the real inventory's own target_kind_id values."""
    inventory = checker.load_inventory(_INVENTORY_PATH)
    discovered = checker.discover_all(_REPO_ROOT)
    vocabulary = checker._closed_vocabulary(discovered)
    for row in inventory["rows"]:
        tkid = row.get("target_kind_id")
        if not tkid:
            continue
        prefix, _, name = tkid.partition(":")
        allowed = vocabulary.get(prefix)
        if allowed is not None:
            assert name in allowed, (row["id"], tkid)


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


def test_discovery_skips_a_trailing_comment_dispatch_lookalike(tmp_path):
    """`value = 1  # task.apply_async()` must not be mistaken for a real
    dispatch call site (Codex LOW)."""
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/example_module.py",
        '"""Use task.delay() here."""\nvalue = 1  # task.apply_async()\n',
    )
    literal, getattr_indirection = checker.discover_call_sites(root)
    assert literal == []
    assert getattr_indirection == []


# ---------------------------------------------------------------------------
# Positive fixtures: one per discovery class, proving the scanner finds the
# real thing (not just that it ignores prose). Codex MED-3.
# ---------------------------------------------------------------------------


def test_discovery_finds_a_literal_dispatch_call_site(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/somewhere.py",
        "from dev_health_ops.workers.tasks import some_task\n"
        "\n"
        "def trigger():\n"
        "    some_task.apply_async(args=(1,))\n",
    )
    literal, getattr_indirection = checker.discover_call_sites(root)
    assert [s.line for s in literal] == [4]
    assert getattr_indirection == []


def test_discovery_finds_a_getattr_indirection_call_site(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/somewhere.py",
        "from dev_health_ops.workers.tasks import some_task\n"
        "\n"
        "def trigger():\n"
        '    getattr(some_task, "apply_async")(args=(1,))\n',
    )
    literal, getattr_indirection = checker.discover_call_sites(root)
    assert literal == []
    assert [s.line for s in getattr_indirection] == [4]


def test_discovery_finds_a_bare_canvas_invocation(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/somewhere.py",
        "from celery import chord\n"
        "\n"
        "def trigger():\n"
        "    chord(\n"
        "        [x.s() for x in ()],\n"
        "        y.s(),\n"
        "    )()\n",
    )
    literal, _ = checker.discover_call_sites(root)
    assert [s.line for s in literal] == [4]


def test_discovery_finds_a_bound_method_alias(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/somewhere.py",
        "from dev_health_ops.workers.tasks import some_task\n"
        "\n"
        "enqueue = some_task.apply_async\n",
    )
    literal, _ = checker.discover_call_sites(root)
    assert [s.line for s in literal] == [3]


def test_discovery_finds_a_functools_partial_alias(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/somewhere.py",
        "from functools import partial\n"
        "from dev_health_ops.workers.tasks import some_task\n"
        "\n"
        'enqueue = partial(some_task.apply_async, queue="sync")\n',
    )
    literal, _ = checker.discover_call_sites(root)
    assert [s.line for s in literal] == [4]


def test_discovery_finds_a_celery_canvas_import(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/somewhere.py", "from celery import chain, chord\n"
    )
    surfaces = checker.discover_celery_canvas_imports(root)
    assert [s.line for s in surfaces] == [1]


def test_discovery_ignores_a_non_canvas_celery_import(tmp_path):
    root = tmp_path / "repo"
    _write(root / "src/dev_health_ops/somewhere.py", "from celery import Celery\n")
    surfaces = checker.discover_celery_canvas_imports(root)
    assert surfaces == []


def test_discovery_finds_a_rest_api_trigger_endpoint(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/api/routers/example.py",
        "from dev_health_ops.workers.tasks import some_task\n"
        "\n"
        "\n"
        '@router.post("/example")\n'
        "async def trigger_example():\n"
        "    some_task.apply_async()\n",
    )
    literal, getattr_indirection = checker.discover_call_sites(root)
    endpoints = checker.discover_api_trigger_endpoints(
        root, literal + getattr_indirection
    )
    assert [(s.file, s.line) for s in endpoints] == [
        ("src/dev_health_ops/api/routers/example.py", 4)
    ]


def test_discovery_finds_a_rest_api_trigger_with_multiline_decorator(tmp_path):
    """Regression guard: a decorator whose argument list spans multiple
    lines must not defeat the walk-back to find it (Codex HIGH-2)."""
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/api/routers/example.py",
        "from dev_health_ops.workers.tasks import some_task\n"
        "\n"
        "\n"
        "@router.post(\n"
        '    "/example",\n'
        "    status_code=202,\n"
        ")\n"
        "async def trigger_example():\n"
        "    some_task.apply_async()\n",
    )
    literal, getattr_indirection = checker.discover_call_sites(root)
    endpoints = checker.discover_api_trigger_endpoints(
        root, literal + getattr_indirection
    )
    assert [(s.file, s.line) for s in endpoints] == [
        ("src/dev_health_ops/api/routers/example.py", 4)
    ]


def test_discovery_finds_a_graphql_resolver_trigger(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/api/graphql/resolvers/example.py",
        "from dev_health_ops.workers.tasks import some_task\n"
        "\n"
        "\n"
        "async def resolve_example():\n"
        "    some_task.apply_async()\n",
    )
    literal, getattr_indirection = checker.discover_call_sites(root)
    endpoints = checker.discover_api_trigger_endpoints(
        root, literal + getattr_indirection
    )
    assert [(s.file, s.line) for s in endpoints] == [
        ("src/dev_health_ops/api/graphql/resolvers/example.py", 5)
    ]


def test_discovery_finds_the_shared_helper_fan_in_endpoint(tmp_path):
    """A dispatch call sitting in an undecorated helper called by several
    @router-decorated endpoints is represented by its lowest-line caller
    (Codex HIGH-2 -- webhooks/router.py's three provider routes funnel
    through one shared `_dispatch_webhook_task` helper this way)."""
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/api/routers/example.py",
        "from dev_health_ops.workers.tasks import some_task\n"
        "\n"
        "\n"
        "def _dispatch():\n"
        "    some_task.apply_async()\n"
        "\n"
        "\n"
        '@router.post("/a")\n'
        "async def route_a():\n"
        "    _dispatch()\n"
        "\n"
        "\n"
        '@router.post("/b")\n'
        "async def route_b():\n"
        "    _dispatch()\n",
    )
    literal, getattr_indirection = checker.discover_call_sites(root)
    endpoints = checker.discover_api_trigger_endpoints(
        root, literal + getattr_indirection
    )
    assert [(s.file, s.line) for s in endpoints] == [
        ("src/dev_health_ops/api/routers/example.py", 8)
    ]


def test_discovery_finds_a_conditional_beat_entry(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/workers/config.py",
        "beat_schedule = {\n"
        '    "always-on-entry": {\n'
        '        "task": "dev_health_ops.workers.tasks.always_on",\n'
        "    },\n"
        "}\n"
        "\n"
        'if env_flag("SOME_FLAG", default=False):\n'
        '    beat_schedule["conditional-entry"] = {\n'
        '        "task": "dev_health_ops.workers.tasks.conditional_task",\n'
        "    }\n",
    )
    surfaces = checker.discover_beat_entries(root)
    unconditional = [s for s in surfaces if s.cls == checker.CLASS_BEAT_ENTRY]
    conditional = [s for s in surfaces if s.cls == checker.CLASS_BEAT_ENTRY_CONDITIONAL]
    assert [(s.line, s.name) for s in unconditional] == [(2, "always-on-entry")]
    assert [(s.line, s.name) for s in conditional] == [(8, "conditional-entry")]


def test_discovery_finds_a_registry_kind(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "contracts/jobs/v1/registry.json",
        json.dumps({"jobs": [{"kind": "example.kind"}]}, indent=2),
    )
    surfaces = checker.discover_json_kinds(
        root, "contracts/jobs/v1/registry.json", checker.CLASS_REGISTRY_KIND
    )
    assert [s.name for s in surfaces] == ["example.kind"]


def test_discovery_finds_a_transport_route(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "contracts/sync-dispatch/v1/transport-routes.json",
        json.dumps({"routes": [{"kind": "example_route"}]}, indent=2),
    )
    surfaces = checker.discover_json_kinds(
        root,
        "contracts/sync-dispatch/v1/transport-routes.json",
        checker.CLASS_TRANSPORT_ROUTE,
    )
    assert [s.name for s in surfaces] == ["example_route"]


def test_discovery_finds_a_stream_surface(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/api/example/streams.py",
        'CONSUMER_GROUP = "example-consumers"\n',
    )
    surfaces = checker.discover_stream_surfaces(root)
    assert [(s.line, s.name) for s in surfaces] == [(1, "example-consumers")]


def test_discovery_finds_the_pagerduty_stream_special_case(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/api/webhooks/pagerduty.py",
        "\n\ndef _enqueue_event(*, binding_id):\n    pass\n",
    )
    surfaces = checker.discover_stream_surfaces(root)
    assert [(s.line, s.name) for s in surfaces] == [(3, checker.PAGERDUTY_STREAM_NAME)]


def test_discovery_finds_an_aliased_celery_task_decorator(tmp_path):
    """`@shared_task`/`@app.task(` aliases of the Celery task decorator must
    be discovered too, not just `@celery_app.task(`."""
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/workers/aliased.py",
        "from celery import shared_task\n"
        "\n"
        "\n"
        '@shared_task(name="dev_health_ops.workers.tasks.aliased_task")\n'
        "def aliased_task():\n"
        "    return None\n",
    )
    surfaces = checker.discover_celery_tasks(root)
    assert [(s.line, s.name) for s in surfaces] == [(4, "aliased_task")]


# ---------------------------------------------------------------------------
# Round-2 hardening (Codex round-2 review): qualified/aliased celery-canvas
# forms, router-alias/add_api_route API registration, the phantom-row half
# of bidirectional parity, the hard-error unknown-prefix closed-vocabulary
# check, and the curated (not "any discovered task") task: vocabulary.
# ---------------------------------------------------------------------------


def test_discovery_finds_import_celery_canvas_as_alias(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/somewhere.py",
        "import celery.canvas as canvas\n"
        "\n"
        "def trigger():\n"
        "    canvas.chord(\n"
        "        [x.s() for x in ()],\n"
        "        y.s(),\n"
        "    )()\n",
    )
    imports = checker.discover_celery_canvas_imports(root)
    assert [s.line for s in imports] == [1]
    literal, _ = checker.discover_call_sites(root)
    assert [s.line for s in literal] == [4]


def test_discovery_finds_from_celery_import_canvas_module(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/somewhere.py",
        "from celery import canvas\n\ndef trigger():\n    canvas.chain(a, b)()\n",
    )
    imports = checker.discover_celery_canvas_imports(root)
    assert [s.line for s in imports] == [1]
    literal, _ = checker.discover_call_sites(root)
    assert [s.line for s in literal] == [4]


def test_discovery_finds_importlib_import_module_celery_canvas(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/somewhere.py",
        "import importlib\n\n"
        'canvas = importlib.import_module("celery.canvas")\n\n'
        "def trigger():\n"
        "    canvas.group(\n"
        "        a,\n"
        "        b,\n"
        "    )()\n",
    )
    imports = checker.discover_celery_canvas_imports(root)
    assert [s.line for s in imports] == [3]
    literal, _ = checker.discover_call_sites(root)
    assert [s.line for s in literal] == [6]


def test_discovery_finds_a_parenthesized_multiline_canvas_import(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/somewhere.py",
        "from celery import (\n    chain,\n    chord,\n)\n",
    )
    imports = checker.discover_celery_canvas_imports(root)
    assert [s.line for s in imports] == [1]


def test_discovery_finds_an_aliased_bare_canvas_invocation(tmp_path):
    """`c = chord` then a later bare `c(...)()` -- a second invocation in an
    already-inventoried module must still be independently discoverable
    (Codex round-2 HIGH-1)."""
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/somewhere.py",
        "from celery import chord\n"
        "\n"
        "c = chord\n"
        "\n"
        "def trigger():\n"
        "    c(\n"
        "        [x.s() for x in ()],\n"
        "        y.s(),\n"
        "    )()\n",
    )
    literal, _ = checker.discover_call_sites(root)
    assert [s.line for s in literal] == [6]


def test_discovery_finds_a_router_alias_decorator(tmp_path):
    """`r = router; @r.post(...)` must be discovered, not just the literal
    `router` name (Codex round-2 HIGH-2)."""
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/api/routers/example.py",
        "from dev_health_ops.workers.tasks import some_task\n"
        "\n"
        "r = router\n"
        "\n"
        "\n"
        '@r.post("/example")\n'
        "async def trigger_example():\n"
        "    some_task.apply_async()\n",
    )
    literal, getattr_indirection = checker.discover_call_sites(root)
    endpoints = checker.discover_api_trigger_endpoints(
        root, literal + getattr_indirection
    )
    assert [(s.file, s.line) for s in endpoints] == [
        ("src/dev_health_ops/api/routers/example.py", 6)
    ]


def test_discovery_finds_an_add_api_route_registration(tmp_path):
    """`router.add_api_route(path, helper)` registers an endpoint with no
    decorator at all -- must be its own discovered surface even when the
    helper it registers has no other detected dispatch call site in the
    same module (Codex round-2 HIGH-2)."""
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/api/routers/example.py",
        "async def helper():\n"
        "    return None\n"
        "\n"
        "\n"
        'router.add_api_route("/example", helper, methods=["POST"])\n',
    )
    endpoints = checker.discover_api_trigger_endpoints(root, [])
    assert [(s.file, s.line) for s in endpoints] == [
        ("src/dev_health_ops/api/routers/example.py", 5)
    ]


def test_discovery_finds_an_add_route_alias_registration(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/api/routers/example.py",
        "r = router\n"
        "\n"
        "\n"
        "async def helper():\n"
        "    return None\n"
        "\n"
        "\n"
        'r.add_route("/example", helper, methods=["POST"])\n',
    )
    endpoints = checker.discover_api_trigger_endpoints(root, [])
    assert [(s.file, s.line) for s in endpoints] == [
        ("src/dev_health_ops/api/routers/example.py", 8)
    ]


def test_gate_catches_a_phantom_row(tmp_path):
    """A row referencing a (class, file, line) that independent discovery
    does NOT find there must fail -- the reverse direction of parity, so a
    missed surface elsewhere can't net against a phantom row and pass
    (Codex round-2 MED-3)."""
    root = _minimal_valid_root(tmp_path)
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    inventory = checker.load_inventory(inventory_path)
    # Point the row at a line that has no Celery task decorator at all.
    inventory["rows"][0]["source"]["line"] = 3
    _write(inventory_path, json.dumps(inventory, indent=2))

    errors = checker.check(root, inventory_path)
    assert any("PHANTOM ROW" in e for e in errors), errors


def test_gate_treats_an_unrecognized_target_kind_id_prefix_as_an_error(tmp_path):
    """An unrecognized target_kind_id namespace must be a hard error, not
    silently skipped (Codex round-2 MED-1 -- `vocabulary.get(prefix)`
    returning None used to let `bogus:anything` pass)."""
    root = _minimal_valid_root(tmp_path)
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    inventory = checker.load_inventory(inventory_path)
    inventory["rows"][0]["target_kind_id"] = "bogus:anything"
    _write(inventory_path, json.dumps(inventory, indent=2))

    errors = checker.check(root, inventory_path)
    assert any("UNKNOWN target_kind_id PREFIX" in e for e in errors), errors


def test_gate_rejects_an_unclaimed_discovered_task_as_a_target(tmp_path):
    """The `task:` vocabulary is curated (TRD_MAPPED_TASK_TARGETS), not
    "every discovered Celery task name" -- an unrelated but real, discovered
    task must NOT self-validate as a target just because some other row
    (or a renamed version of this one) also exists in the tree (Codex
    round-2 MED-1)."""
    root = _minimal_valid_root(tmp_path)
    _write(
        root / "src/dev_health_ops/workers/rogue_task.py",
        "from dev_health_ops.workers.celery_app import celery_app\n"
        "\n"
        "\n"
        '@celery_app.task(name="dev_health_ops.workers.tasks.some_other_task")\n'
        "def some_other_task():\n"
        "    return None\n",
    )
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    inventory = checker.load_inventory(inventory_path)
    # Retarget the existing primary row at the newly-discovered (but
    # unclaimed/uncurated) task name instead of the curated health_check.
    inventory["rows"][0]["target_kind_id"] = "task:some_other_task"
    inventory["rows"].append(
        {
            "id": "celery_task:src/dev_health_ops/workers/rogue_task.py:4",
            "surface": "some_other_task",
            "class": "celery_task",
            "source": {
                "file": "src/dev_health_ops/workers/rogue_task.py",
                "line": 4,
            },
            "dispatch_mechanism": "celery_task_decorator",
            "owner_role": "contributor",
            "target_owner": {"type": "native_process", "value": "n/a"},
            "target_kind_id": None,
            "current_implementation_state": "celery_only",
            "verification_status": "verified",
            "compatibility_dependency": None,
            "deletion_evidence_requirement": "n/a (fixture)",
            "acceptance_test_id": "tests/workers/test_transitional_inventory_contract.py",
            "notes": "",
        }
    )
    inventory["row_count"] = len(inventory["rows"])
    _write(inventory_path, json.dumps(inventory, indent=2))

    errors = checker.check(root, inventory_path)
    assert any("UNKNOWN target_kind_id" in e for e in errors), errors


def test_gate_catches_content_drift_when_the_dispatched_task_is_swapped(tmp_path):
    """Replacing the dispatched task at an anchored call-site line with a
    *different* task's dispatch must fail even though the line still has the
    right shape (`.apply_async(`) -- content drift must check the specific
    recorded name, not only the class shape (Codex round-2 MED-2)."""
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/api/billing/router.py",
        "\n" * 130 + "    a_completely_different_task.apply_async()\n",
    )
    inventory = {
        "schema_version": 1,
        "row_count": 1,
        "rows": [
            {
                "id": "call_site_literal:src/dev_health_ops/api/billing/router.py:131",
                "surface": "send_billing_notification.delay",
                "class": "call_site_literal",
                "source": {
                    "file": "src/dev_health_ops/api/billing/router.py",
                    "line": 131,
                },
                "dispatch_mechanism": "literal_dispatch_call",
                "owner_role": "contributor",
                "target_owner": {
                    "type": "native_process",
                    "value": "Go worker consuming the webhooks queue",
                },
                "target_kind_id": None,
                "current_implementation_state": "celery_only",
                "verification_status": "verified",
                "compatibility_dependency": None,
                "deletion_evidence_requirement": "n/a (fixture)",
                "acceptance_test_id": "tests/workers/test_transitional_inventory_contract.py",
                "notes": "",
            }
        ],
    }
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    _write(inventory_path, json.dumps(inventory, indent=2))

    errors = checker.check(root, inventory_path)
    assert any("STALE ANCHOR" in e and "content drift" in e for e in errors), errors


def test_gate_catches_content_drift_when_a_beat_entry_key_is_swapped(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/workers/config.py",
        "beat_schedule = {\n"
        '    "a-totally-different-entry": {\n'
        '        "task": "dev_health_ops.workers.tasks.something_else",\n'
        "    },\n"
        "}\n",
    )
    inventory = {
        "schema_version": 1,
        "row_count": 1,
        "rows": [
            {
                "id": "beat_entry:src/dev_health_ops/workers/config.py:2",
                "surface": "dispatch-scheduled-syncs",
                "class": "beat_entry",
                "source": {
                    "file": "src/dev_health_ops/workers/config.py",
                    "line": 2,
                },
                "dispatch_mechanism": "beat_schedule_entry",
                "owner_role": "primary",
                "target_owner": {
                    "type": "native_process",
                    "value": "Go scheduler + sync planner",
                },
                "target_kind_id": "beat:dispatch-scheduled-syncs",
                "current_implementation_state": "celery_only",
                "verification_status": "verified",
                "compatibility_dependency": None,
                "deletion_evidence_requirement": "n/a (fixture)",
                "acceptance_test_id": "tests/workers/test_transitional_inventory_contract.py",
                "notes": "",
            }
        ],
    }
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    _write(inventory_path, json.dumps(inventory, indent=2))

    errors = checker.check(root, inventory_path)
    assert any("STALE ANCHOR" in e and "content drift" in e for e in errors), errors


def test_gate_catches_content_drift_when_a_celery_task_name_is_swapped(tmp_path):
    """Replacing the decorated function's name at an anchored celery_task
    row (while keeping the `@celery_app.task(` decorator itself intact)
    must fail (Codex round-2 MED-2)."""
    root = _minimal_valid_root(tmp_path)
    _write(
        root / "src/dev_health_ops/workers/example_task.py",
        '"""Example worker module."""\n'
        "\n"
        "from dev_health_ops.workers.celery_app import celery_app\n"
        "\n"
        "\n"
        '@celery_app.task(bind=True, name="dev_health_ops.workers.tasks.renamed")\n'
        "def a_completely_different_function_name(self):\n"
        "    return {}\n",
    )
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    errors = checker.check(root, inventory_path)
    assert any("STALE ANCHOR" in e and "content drift" in e for e in errors), errors


# ---------------------------------------------------------------------------
# Round-4 hardening (Codex round-3 review): a real same-file two-hop API
# trigger, a comment/window content-drift false-pass, discovery/re-
# verification table drift for canvas/router forms, and REST-path /
# canvas-import-name content-drift. Per Codex's explicit ask, each
# discovery fix below has a full end-to-end test: synthesize the surface,
# add the row the gate demands, and assert the gate then passes -- not just
# that discovery finds it.
# ---------------------------------------------------------------------------


def test_discovery_finds_a_two_hop_api_trigger(tmp_path):
    """A dispatch reached through TWO helper calls (not one) from its
    @router-decorated endpoint -- the real shape of
    billing/router.py's stripe_webhook -> _process_subscription_event ->
    _enqueue_billing_notification chain (Codex round-3 HIGH-1: the one-hop
    fallback missed this ordinary, not exotic, pattern)."""
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/api/routers/example.py",
        "from dev_health_ops.workers.tasks import some_task\n"
        "\n"
        "\n"
        "async def _dispatch():\n"
        "    some_task.apply_async()\n"
        "\n"
        "\n"
        "async def _process_event():\n"
        "    await _dispatch()\n"
        "\n"
        "\n"
        '@router.post("/webhooks/example")\n'
        "async def example_webhook():\n"
        "    await _process_event()\n",
    )
    literal, getattr_indirection = checker.discover_call_sites(root)
    endpoints = checker.discover_api_trigger_endpoints(
        root, literal + getattr_indirection
    )
    assert [(s.file, s.line) for s in endpoints] == [
        ("src/dev_health_ops/api/routers/example.py", 12)
    ]


def test_gate_catches_content_drift_with_a_misleading_comment(tmp_path):
    """The exact Codex round-3 HIGH-2 repro: a genuinely swapped dispatch
    (`other_task.apply_async()`) must fail content-drift even when a
    trailing comment happens to still mention the OLD task name -- the
    unrestricted forward window used to search past the statement into
    the comment and false-pass."""
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/api/billing/router.py",
        "\n" * 130 + "    other_task.apply_async()  # was send_billing_notification\n",
    )
    row = _make_row(
        "send_billing_notification.delay",
        "call_site_literal",
        "src/dev_health_ops/api/billing/router.py",
        131,
    )
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    _inventory_root_with_rows(root, [row])

    errors = checker.check(root, inventory_path)
    assert any("STALE ANCHOR" in e and "content drift" in e for e in errors), errors


def test_end_to_end_two_hop_api_trigger_can_be_inventoried(tmp_path):
    """Synthesize the two-hop surface, add exactly the row the gate demands
    for it, and assert the gate then passes end to end (Codex round-3
    HIGH-3's standard: tests must verify successful inventory acceptance,
    not only that discovery finds something)."""
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/api/routers/example.py",
        "from dev_health_ops.workers.tasks import some_task\n"
        "\n"
        "\n"
        "async def _dispatch():\n"
        "    some_task.apply_async()\n"
        "\n"
        "\n"
        "async def _process_event():\n"
        "    await _dispatch()\n"
        "\n"
        "\n"
        '@router.post("/webhooks/example")\n'
        "async def example_webhook():\n"
        "    await _process_event()\n",
    )
    literal, getattr_indirection = checker.discover_call_sites(root)
    rows = [
        _make_row(
            "some_task.apply_async",
            "call_site_literal",
            "src/dev_health_ops/api/routers/example.py",
            5,
        ),
        *[
            _make_row(
                "POST /webhooks/example",
                "api_trigger_endpoint",
                s.file,
                s.line,
            )
            for s in checker.discover_api_trigger_endpoints(
                root, literal + getattr_indirection
            )
        ],
    ]
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    _inventory_root_with_rows(root, rows)

    errors = checker.check(root, inventory_path)
    assert errors == [], errors


def test_end_to_end_qualified_canvas_invocation_can_be_inventoried(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/somewhere.py",
        "import celery.canvas as canvas\n"
        "\n"
        "def trigger():\n"
        "    canvas.chord(\n"
        "        [x.s() for x in ()],\n"
        "        y.s(),\n"
        "    )()\n",
    )
    imports = checker.discover_celery_canvas_imports(root)
    literal, _ = checker.discover_call_sites(root)
    rows = [
        _make_row(
            "import celery.canvas as canvas", "celery_canvas_import", s.file, s.line
        )
        for s in imports
    ] + [
        _make_row("chord(...)() [qualified]", "call_site_literal", s.file, s.line)
        for s in literal
    ]
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    _inventory_root_with_rows(root, rows)

    errors = checker.check(root, inventory_path)
    assert errors == [], errors


def test_end_to_end_aliased_canvas_invocation_can_be_inventoried(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/somewhere.py",
        "from celery import chord\n"
        "\n"
        "c = chord\n"
        "\n"
        "def trigger():\n"
        "    c(\n"
        "        [x.s() for x in ()],\n"
        "        y.s(),\n"
        "    )()\n",
    )
    imports = checker.discover_celery_canvas_imports(root)
    literal, _ = checker.discover_call_sites(root)
    rows = [
        _make_row("from celery import chord", "celery_canvas_import", s.file, s.line)
        for s in imports
    ] + [
        _make_row("chord(...)() [aliased]", "call_site_literal", s.file, s.line)
        for s in literal
    ]
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    _inventory_root_with_rows(root, rows)

    errors = checker.check(root, inventory_path)
    assert errors == [], errors


def test_end_to_end_router_alias_decorator_can_be_inventoried(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/api/routers/example.py",
        "from dev_health_ops.workers.tasks import some_task\n"
        "\n"
        "r = router\n"
        "\n"
        "\n"
        '@r.post("/example")\n'
        "async def trigger_example():\n"
        "    some_task.apply_async()\n",
    )
    literal, getattr_indirection = checker.discover_call_sites(root)
    endpoints = checker.discover_api_trigger_endpoints(
        root, literal + getattr_indirection
    )
    rows = [
        _make_row("some_task.apply_async", "call_site_literal", s.file, s.line)
        for s in literal
    ] + [
        _make_row("POST /example", "api_trigger_endpoint", s.file, s.line)
        for s in endpoints
    ]
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    _inventory_root_with_rows(root, rows)

    errors = checker.check(root, inventory_path)
    assert errors == [], errors


def test_end_to_end_add_api_route_can_be_inventoried(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/api/routers/example.py",
        "from dev_health_ops.workers.tasks import some_task\n"
        "\n"
        "\n"
        "async def helper():\n"
        "    some_task.apply_async()\n"
        "\n"
        "\n"
        'router.add_api_route("/example", helper, methods=["POST"])\n',
    )
    literal, getattr_indirection = checker.discover_call_sites(root)
    endpoints = checker.discover_api_trigger_endpoints(
        root, literal + getattr_indirection
    )
    rows = [
        _make_row("some_task.apply_async", "call_site_literal", s.file, s.line)
        for s in literal
    ] + [
        _make_row("POST /example", "api_trigger_endpoint", s.file, s.line)
        for s in endpoints
    ]
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    _inventory_root_with_rows(root, rows)

    errors = checker.check(root, inventory_path)
    assert errors == [], errors


def test_gate_catches_a_canvas_import_name_swap(tmp_path):
    """Replacing `from celery import chain, chord` with
    `from celery import group` must fail content-drift -- Codex round-3
    MED: the old check only asked "is this still some canvas import", not
    "does it still import the SAME names"."""
    root = tmp_path / "repo"
    _write(root / "src/dev_health_ops/somewhere.py", "from celery import group\n")
    row = _make_row(
        "from celery import chain, chord",
        "celery_canvas_import",
        "src/dev_health_ops/somewhere.py",
        1,
    )
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    _inventory_root_with_rows(root, [row])

    errors = checker.check(root, inventory_path)
    assert any("STALE ANCHOR" in e and "content drift" in e for e in errors), errors


def test_gate_catches_a_rest_path_swap(tmp_path):
    """Editing a REST route's path string must fail content-drift -- Codex
    round-3 MED: only the decorator/method shape was checked before."""
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/api/routers/example.py",
        '@router.post("/completely/different/path")\n'
        "async def trigger_example():\n"
        "    pass\n",
    )
    row = _make_row(
        "POST /example",
        "api_trigger_endpoint",
        "src/dev_health_ops/api/routers/example.py",
        1,
    )
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    _inventory_root_with_rows(root, [row])

    errors = checker.check(root, inventory_path)
    assert any("STALE ANCHOR" in e and "content drift" in e for e in errors), errors


def test_gate_honors_an_explicit_route_mount_prefix(tmp_path):
    """A route decorator whose local path argument is only the tail of the
    row's recorded full effective path -- exactly the real pagerduty.py
    shape: `@router.post("/{binding_id}")` under a router conceptually
    mounted ahead of it -- must NOT be flagged as content drift PROVIDED the
    row records the matching `route_mount_prefix` explicitly (Codex round-4
    HIGH-2: this is no longer an arbitrary suffix-tolerance rule -- the
    prefix must be recorded on the row, not inferred)."""
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/api/routers/example.py",
        "from dev_health_ops.workers.tasks import some_task\n"
        "\n"
        "\n"
        '@router.post("/{binding_id}", status_code=202)\n'
        "async def trigger_example(binding_id: str):\n"
        "    some_task.apply_async()\n",
    )
    row = _make_row(
        "some_task.apply_async",
        "call_site_literal",
        "src/dev_health_ops/api/routers/example.py",
        6,
    )
    endpoint_row = _make_row(
        "POST /webhooks/pagerduty/{binding_id}",
        "api_trigger_endpoint",
        "src/dev_health_ops/api/routers/example.py",
        4,
        route_mount_prefix="/webhooks/pagerduty",
    )
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    _inventory_root_with_rows(root, [row, endpoint_row])

    errors = checker.check(root, inventory_path)
    assert errors == [], errors


def test_gate_catches_an_unrelated_same_tail_path(tmp_path):
    """The exact Codex round-4 HIGH-2 repro: a row for `POST /sync` must NOT
    accept `@router.post("/other/sync")` just because the tail matches --
    the old symmetric-suffix comparison false-passed this."""
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/api/routers/example.py",
        '@router.post("/other/sync")\nasync def trigger_example():\n    pass\n',
    )
    row = _make_row(
        "POST /sync",
        "api_trigger_endpoint",
        "src/dev_health_ops/api/routers/example.py",
        1,
    )
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    _inventory_root_with_rows(root, [row])

    errors = checker.check(root, inventory_path)
    assert any("STALE ANCHOR" in e and "content drift" in e for e in errors), errors


def test_gate_catches_an_http_method_swap(tmp_path):
    """Changing `@router.post(...)` to `@router.get(...)` while keeping the
    same path must fail content-drift -- Codex round-4 HIGH-2: the old
    check never compared the HTTP method at all."""
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/api/routers/example.py",
        '@router.get("/example")\nasync def trigger_example():\n    pass\n',
    )
    row = _make_row(
        "POST /example",
        "api_trigger_endpoint",
        "src/dev_health_ops/api/routers/example.py",
        1,
    )
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    _inventory_root_with_rows(root, [row])

    errors = checker.check(root, inventory_path)
    assert any("STALE ANCHOR" in e and "content drift" in e for e in errors), errors


def test_gate_catches_a_versioned_task_name_substring_swap(tmp_path):
    """The exact Codex round-4 MED-2 repro: `send_billing_notification_v2`
    must NOT satisfy the `send_billing_notification` row just because the
    old name is a substring of the new one."""
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/somewhere.py",
        "send_billing_notification_v2.delay()\n",
    )
    row = _make_row(
        "send_billing_notification.delay",
        "call_site_literal",
        "src/dev_health_ops/somewhere.py",
        1,
    )
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    _inventory_root_with_rows(root, [row])

    errors = checker.check(root, inventory_path)
    assert any("STALE ANCHOR" in e and "content drift" in e for e in errors), errors


def test_discovery_finds_a_cross_file_forwarding_endpoint(tmp_path):
    """A route in a module with its OWN FastAPI() app instance whose
    handler forwards to an imported symbol that's already a known
    dispatch-relevant endpoint elsewhere is discoverable, without needing a
    local dispatch call site in the same file (Codex round-4 HIGH-1 --
    exactly billing_edge.py's shape: a separately deployed edge app calling
    the imported billing/router.py stripe_webhook handler)."""
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/api/billing/router.py",
        "\n\n\n"
        '@router.post("/webhooks/stripe")\n'
        "async def stripe_webhook(request):\n"
        "    some_task.apply_async()\n",
    )
    _write(
        root / "src/dev_health_ops/api/billing_edge.py",
        "from fastapi import FastAPI\n"
        "\n"
        "from dev_health_ops.api.billing.router import stripe_webhook\n"
        "\n"
        "app = FastAPI(\n"
        '    title="Billing Edge",\n'
        ")\n"
        "\n"
        "\n"
        '@app.post("/api/v1/billing/webhooks/stripe")\n'
        "async def stripe_webhook_public(request):\n"
        "    return await stripe_webhook(request)\n",
    )
    literal, getattr_indirection = checker.discover_call_sites(root)
    primary = checker.discover_api_trigger_endpoints(
        root, literal + getattr_indirection
    )
    cross_file = checker.discover_cross_file_forwarding_endpoints(root, primary)
    assert [(s.file, s.line) for s in cross_file] == [
        ("src/dev_health_ops/api/billing_edge.py", 10)
    ]


def test_end_to_end_cross_file_forwarding_endpoint_can_be_inventoried(tmp_path):
    root = tmp_path / "repo"
    _write(
        root / "src/dev_health_ops/api/billing/router.py",
        "\n\n\n"
        '@router.post("/webhooks/stripe")\n'
        "async def stripe_webhook(request):\n"
        "    some_task.apply_async()\n",
    )
    _write(
        root / "src/dev_health_ops/api/billing_edge.py",
        "from fastapi import FastAPI\n"
        "\n"
        "from dev_health_ops.api.billing.router import stripe_webhook\n"
        "\n"
        "app = FastAPI(\n"
        '    title="Billing Edge",\n'
        ")\n"
        "\n"
        "\n"
        '@app.post("/api/v1/billing/webhooks/stripe")\n'
        "async def stripe_webhook_public(request):\n"
        "    return await stripe_webhook(request)\n",
    )
    literal, getattr_indirection = checker.discover_call_sites(root)
    primary = checker.discover_api_trigger_endpoints(
        root, literal + getattr_indirection
    )
    cross_file = checker.discover_cross_file_forwarding_endpoints(root, primary)

    rows = (
        [
            _make_row("some_task.apply_async", "call_site_literal", s.file, s.line)
            for s in literal
        ]
        + [
            _make_row("POST /webhooks/stripe", "api_trigger_endpoint", s.file, s.line)
            for s in primary
        ]
        + [
            _make_row(
                "POST /api/v1/billing/webhooks/stripe",
                "api_trigger_endpoint",
                s.file,
                s.line,
            )
            for s in cross_file
        ]
    )
    inventory_path = root / "contracts/jobs/v1/transitional-inventory.json"
    _inventory_root_with_rows(root, rows)

    errors = checker.check(root, inventory_path)
    assert errors == [], errors

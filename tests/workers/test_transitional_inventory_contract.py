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
    # each celery-canvas (chain/chord/group) import in the tree.
    assert inventory["row_count"] == 152


def test_discovered_surface_count_equals_inventory_row_count():
    """The acceptance criterion is that the inventory *equals* independent
    code discovery -- not merely that every discovered surface has a row.
    Assert exact parity per class so an intentionally-consolidated row (e.g.
    one API-trigger row representing three sibling routes that share a
    dispatch helper) can't silently mask a real gap in a different class."""
    inventory = checker.load_inventory(_INVENTORY_PATH)
    discovered = checker.discover_all(_REPO_ROOT)
    from collections import Counter

    discovered_counts = Counter(s.cls for s in discovered)
    inventory_counts = Counter(r["class"] for r in inventory["rows"])
    assert discovered_counts == inventory_counts
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
    the API call site row). All three billing-related rows (the Celery task,
    its API call site, and its registry kind) must point at the real
    operational-bridge billing test."""
    inventory = checker.load_inventory(_INVENTORY_PATH)
    billing_rows = [r for r in inventory["rows"] if "billing" in r["surface"].lower()]
    assert len(billing_rows) == 3, billing_rows
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
        "def owned_task(self):\n"
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

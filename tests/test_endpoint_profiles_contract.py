"""CHAOS-3273 L3: ops endpoint-profile inventory contract + CI enforcement.

Proves two things, same standard as tests/workers/test_transitional_inventory_contract.py:

1. The real, checked-in inventory (contracts/auth/v1/endpoint-profiles.ops.json)
   is currently consistent with independent code discovery on this tree --
   i.e. the CI gate (ci/check_endpoint_profiles.py) passes today.
2. The gate actually *works*: for every failure class it is supposed to
   catch (unowned surface, phantom/stale row, duplicate id, closed-vocabulary
   violation, anchor drift, schema violation, unstated null), a synthetic
   violation is seeded in a tmp_path fixture tree and the gate is asserted to
   reject it. No real violation is ever committed.

Fixture trees reuse the REAL schema, credential-classes vocabulary, and
discovery script (copied verbatim into tmp_path) so a fixture test is
exercising the actual gate mechanism, not a stand-in.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_CHECKER_PATH = _REPO_ROOT / "ci" / "check_endpoint_profiles.py"
_DISCOVERER_PATH = _REPO_ROOT / "ci" / "discover_ops_routes.py"
_INVENTORY_PATH = (
    _REPO_ROOT / "contracts" / "auth" / "v1" / "endpoint-profiles.ops.json"
)
_SCHEMA_PATH = _REPO_ROOT / "contracts" / "auth" / "v1" / "endpoint-profile.schema.json"
_CREDENTIAL_CLASSES_PATH = (
    _REPO_ROOT / "contracts" / "auth" / "v1" / "credential-classes.json"
)
_CREDENTIAL_CLASSES_SCHEMA_PATH = (
    _REPO_ROOT / "contracts" / "auth" / "v1" / "credential-classes.schema.json"
)


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


checker = _load_module(_CHECKER_PATH, "check_endpoint_profiles")


def test_real_tree_passes_the_gate():
    """The committed inventory must be consistent with real discovery today."""
    errors = checker.check(
        _REPO_ROOT, _INVENTORY_PATH, _SCHEMA_PATH, _CREDENTIAL_CLASSES_PATH
    )
    assert errors == [], "\n".join(errors)


_GRAPHQL_KINDS = ("graphql_field", "graphql_mutation", "graphql_subscription")


def test_inventory_row_count_matches_the_baseline():
    """370 rows = 311 REST + 59 GraphQL. A different number here is a finding
    to reconcile, not an adjustment to make quietly.

    Was 361 (303 + 58) under source-text discovery. The move to enumerating
    the served objects (CHAOS-4761) changed it by nine, and every one of the
    nine is a defect the old count concealed:
      +8 REST -- four fastapi doc routes, /metrics, and GET/POST/WEBSOCKET
         /graphql. All served by the main app, none written in ops source, so
         no source-text pattern could ever have matched them.
      +3 GraphQL -- the three @strawberry.subscription resolvers the old
         decorator list did not include.
      -2 GraphQL -- `metrics` and `update_setting`, which are
         `@strawberry.field` examples inside a DOCSTRING and are not fields
         on the served schema at all.
    """
    inventory = checker.load_json(_INVENTORY_PATH)
    rows = inventory["rows"]
    rest = [r for r in rows if r["surface_kind"] == "rest"]
    graphql = [r for r in rows if r["surface_kind"] in _GRAPHQL_KINDS]
    assert len(rest) == 311, len(rest)
    assert len(graphql) == 59, len(graphql)
    assert len(rows) == 370, len(rows)


def test_the_three_subscriptions_are_profiled():
    """They were invisible to the old discoverer and excluded from the count
    it produced (CHAOS-4761)."""
    inventory = checker.load_json(_INVENTORY_PATH)
    subs = {
        r["graphql_field_name"]
        for r in inventory["rows"]
        if r["surface_kind"] == "graphql_subscription"
    }
    assert subs == {"metrics_updated", "task_status", "sync_progress"}, subs


def test_classification_summary_matches_the_baseline():
    inventory = checker.load_json(_INVENTORY_PATH)
    rows = inventory["rows"]
    protected = [r for r in rows if r["classification"] == "protected"]
    public = [r for r in rows if r["classification"] == "public"]
    # 339 + 3 subscriptions + 3 /graphql transport rows - 2 docstring phantoms.
    assert len(protected) == 343, len(protected)
    # 22 + the four fastapi doc routes + /metrics.
    assert len(public) == 27, len(public)
    assert len(protected) + len(public) == len(rows)


def test_served_surface_set_and_row_set_are_identical_in_both_directions():
    """The structural test, and the reason this gate is worth anything.

    Not totals -- the actual SETS, keyed on the deployed identity discovery
    produces from the served app/schema objects, so a missed surface cannot
    net against an unrelated phantom row and pass. Both directions are
    asserted separately: a surface with no row, and a row naming no surface.
    """
    inventory = checker.load_json(_INVENTORY_PATH)
    discoverer = checker._load_module(_DISCOVERER_PATH, "discover_ops_routes_direct")
    discovered = discoverer.discover(_REPO_ROOT)

    served, map_errors = checker._live_surface_map(discovered)
    assert map_errors == [], map_errors
    row_keys = {
        key
        for key in (checker._row_surface_key(r) for r in inventory["rows"])
        if key is not None
    }

    missed = served.keys() - row_keys
    phantom = row_keys - served.keys()
    assert missed == set(), f"served but not inventoried: {sorted(missed)}"
    assert phantom == set(), f"inventoried but not served: {sorted(phantom)}"


def test_every_row_key_is_well_formed():
    """_row_surface_key returning None means the row could not name a surface
    at all; such a row is skipped by the parity check above, so a tree full of
    them would pass it vacuously."""
    inventory = checker.load_json(_INVENTORY_PATH)
    unkeyed = [
        r["id"] for r in inventory["rows"] if checker._row_surface_key(r) is None
    ]
    assert unkeyed == [], unkeyed


def test_discovery_defaults_are_the_real_deployed_objects():
    """`discover()` takes the app/schema table as parameters so the fixture
    tests can point it at a purpose-built package. This pins the DEFAULTS, so
    that flexibility can never quietly become a narrower production config."""
    discoverer = checker._load_module(_DISCOVERER_PATH, "discover_ops_routes_defaults")
    assert discoverer.DEPLOYED_APPS == (
        ("dev_health_ops.api.main", "app"),
        ("dev_health_ops.api.billing_edge", "app"),
    )
    assert discoverer.GRAPHQL_SCHEMA == (
        "dev_health_ops.api.graphql.schema",
        "schema",
    )
    # Every app the discoverer walks must be attributable to a service, or
    # rows for it cannot be checked at all.
    for module, attr in discoverer.DEPLOYED_APPS:
        assert f"{module}::{attr}" in checker._APP_ROOT_SERVICE


def test_multi_mount_yields_one_surface_per_mount(tmp_path):
    """CHAOS-4760, closed by construction.

    One router included at two prefixes is two served surfaces. Under the old
    mount arithmetic it collapsed to `prefixes[0]`, which produced BOTH a
    false negative (a profile covering one mount passed) and a false positive
    (two correct profiles tripped duplicate-surface, because identity was the
    shared file:line). Enumerating app.routes gives two route objects with two
    paths, and identity is (service, method, path), so both halves go away.
    """
    root = _minimal_valid_root(tmp_path)
    _write(
        root / _MAIN_FILE,
        "from fastapi import FastAPI\n"
        "\n"
        "from .routers.example import router as example_router\n"
        "\n"
        "app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)\n"
        "app.include_router(example_router)\n"
        'app.include_router(example_router, prefix="/v2")\n',
    )
    inventory_path, schema_path, cc_path = _paths(root)

    # 1. both mounts are discovered
    discoverer = checker._load_module(
        _DISCOVERER_PATH, "discover_ops_routes_multimount"
    )
    paths = {r["path"] for r in discoverer.discover(root)["routes"]}
    assert paths == {"/example", "/v2/example"}, paths

    # 2. covering only one of them FAILS, naming the missing mount
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("UNOWNED SURFACE" in e and "/v2/example" in e for e in errors), errors

    # 3. two correct rows for the two mounts BOTH pass -- no duplicate-surface
    #    error, even though they share one file:line anchor.
    second = _minimal_valid_row(
        id="GET /v2/example [dev-health-ops-api]", route="/v2/example"
    )
    _write_inventory(root, [_minimal_valid_row(), second])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert errors == [], errors


def test_a_dynamically_included_router_is_discovered(tmp_path):
    """The case the retired static walk had to fail closed on (UNVERIFIED
    ROUTE + its reviewed allowlist). A router reached through a computed
    import is in app.routes like any other, so there is nothing to vouch for.
    """
    root = _minimal_valid_root(tmp_path)
    _write(
        root / _MAIN_FILE,
        "import importlib\n"
        "\n"
        "from fastapi import FastAPI\n"
        "\n"
        "app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)\n"
        "_name = '.'.join(['dev_health_ops', 'api', 'routers', 'example'])\n"
        "app.include_router(getattr(importlib.import_module(_name), 'router'))\n",
    )
    discoverer = checker._load_module(_DISCOVERER_PATH, "discover_ops_routes_dynamic")
    paths = {r["path"] for r in discoverer.discover(root)["routes"]}
    assert paths == {"/example"}, paths
    inventory_path, schema_path, cc_path = _paths(root)
    assert checker.check(root, inventory_path, schema_path, cc_path) == []


def test_gate_fails_loudly_when_the_app_cannot_be_imported(tmp_path):
    """An unimportable app must raise, never yield an empty surface set --
    an empty set makes every cross-check pass while checking nothing."""
    root = _minimal_valid_root(tmp_path)
    _write(root / _MAIN_FILE, "raise RuntimeError('boom')\n")
    inventory_path, schema_path, cc_path = _paths(root)
    with pytest.raises(RuntimeError) as exc:
        checker.check(root, inventory_path, schema_path, cc_path)
    assert "dev_health_ops.api.main" in str(exc.value)


def test_gate_refuses_to_run_without_a_date_time_format_checker(tmp_path, monkeypatch):
    """`format_checker=` alone is inert: jsonschema only checks "date-time"
    when a backing implementation (rfc3339-validator) is importable, so
    `generated_at: "not-a-date"` would validate cleanly and the fix would look
    applied while changing nothing (CHAOS-4761)."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    checkers = dict(checker.jsonschema.Draft202012Validator.FORMAT_CHECKER.checkers)
    checkers.pop("date-time", None)
    monkeypatch.setattr(
        checker.jsonschema.Draft202012Validator.FORMAT_CHECKER, "checkers", checkers
    )
    with pytest.raises(RuntimeError, match="date-time"):
        checker.check(root, inventory_path, schema_path, cc_path)


def test_gate_rejects_a_malformed_generated_at(tmp_path):
    """The format check is real, not declared: red before the
    rfc3339-validator dependency was added, green after."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    inventory = json.loads(inventory_path.read_text())
    inventory["generated_at"] = "not-a-date"
    _write(inventory_path, json.dumps(inventory, indent=2))
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("generated_at" in e and "date-time" in e for e in errors), errors


def test_every_accepted_credential_class_is_real():
    inventory = checker.load_json(_INVENTORY_PATH)
    credential_classes = checker.load_json(_CREDENTIAL_CLASSES_PATH)
    vocab = checker.credential_class_vocabulary(credential_classes)
    for row in inventory["rows"]:
        for cls in row.get("accepted_credential_classes") or []:
            assert cls in vocab, (row["id"], cls)


def test_schema_accepts_server_action_surface_kind_dynamically():
    """The schema (edited by auth-cp for the web lane's Next.js Server
    Action ruling, uncommitted at the time this lane picked it up) added
    'server_action' to surface_kind's enum. The gate reads this enum LIVE
    from the schema file rather than hardcoding it, so it must already
    accept the new value with zero checker code change -- prove that by
    reading it back, not by asserting on checker source."""
    schema = checker.load_json(_SCHEMA_PATH)
    vocab = checker._schema_enum(schema, "surface_kind")
    assert vocab == {
        "rest",
        "graphql_field",
        "graphql_mutation",
        "graphql_subscription",
        "server_action",
    }


# ---------------------------------------------------------------------------
# Fixture-based proofs that the gate actually catches violations. Fixture
# trees reuse the real schema/credential-classes/discoverer (copied
# verbatim) -- only the source tree + inventory are synthetic.
# ---------------------------------------------------------------------------


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def _seed_shared_fixtures(root: Path) -> None:
    _write(root / "ci" / "discover_ops_routes.py", _DISCOVERER_PATH.read_text())
    _write(
        root / "contracts" / "auth" / "v1" / "endpoint-profile.schema.json",
        _SCHEMA_PATH.read_text(),
    )
    _write(
        root / "contracts" / "auth" / "v1" / "credential-classes.json",
        _CREDENTIAL_CLASSES_PATH.read_text(),
    )
    _write(
        root / "contracts" / "auth" / "v1" / "credential-classes.schema.json",
        _CREDENTIAL_CLASSES_SCHEMA_PATH.read_text(),
    )


_ROUTER_FILE = "src/dev_health_ops/api/routers/example.py"
_MAIN_FILE = "src/dev_health_ops/api/main.py"
_SCHEMA_FILE = "src/dev_health_ops/api/graphql/schema.py"
_BILLING_EDGE_FILE = "src/dev_health_ops/api/billing_edge.py"

# CHAOS-4761: discovery IMPORTS the app and the schema, so a fixture tree is
# no longer a few files that happen to contain the right decorators -- it is a
# real, importable package that really serves what it claims. That is the
# point: a fixture the discoverer only PARSES can drift from a fixture the
# framework would actually dispatch, and a gate proven against the first says
# nothing about the second. `ci/discover_ops_routes.py` swaps
# `dev_health_ops` in `sys.modules` for the root under test and restores the
# real one afterwards, so these fixtures and `test_real_tree_passes_the_gate`
# coexist in one pytest process.
#
# Note the app is built with docs_url/redoc_url/openapi_url=None. A default
# FastAPI() also serves /docs, /docs/oauth2-redirect, /redoc and
# /openapi.json, and under this gate those are four real surfaces that would
# need four real rows -- which is exactly how the four of them were found
# missing from the committed inventory in the first place.
_DEFAULT_ROUTER_BODY = (
    "from fastapi import APIRouter\n"
    "\n"
    "router = APIRouter()\n"
    "\n"
    "\n"
    '@router.get("/example")\n'
    "async def get_example():\n"
    "    return {}\n"
)

# `example` is a plain attribute, not a resolver, so the minimal tree has
# ZERO GraphQL surfaces -- a field with no resolver runs none of our code and
# is read off an already-resolved parent. Tests that need a GraphQL surface
# opt in with _schema_with_resolver() below.
_DEFAULT_SCHEMA_BODY = (
    "import strawberry\n"
    "\n"
    "\n"
    "@strawberry.type\n"
    "class Query:\n"
    "    example: str\n"
    "\n"
    "\n"
    "schema = strawberry.Schema(query=Query)\n"
)


def _schema_with_resolver(python_name: str) -> str:
    """A served schema with exactly one resolver-bearing root field.

    The resolver's `@strawberry.field` decorator lands on line 6, which is
    what `inspect.getsourcelines` reports and therefore what a row must
    anchor to.
    """
    return (
        "import strawberry\n"
        "\n"
        "\n"
        "@strawberry.type\n"
        "class Query:\n"
        "    @strawberry.field\n"
        f"    async def {python_name}(self) -> str:\n"
        '        return ""\n'
        "\n"
        "\n"
        "schema = strawberry.Schema(query=Query)\n"
    )


_RESOLVER_ANCHOR_LINE = 6


def _write_package_init(root: Path, *dotted_dirs: str) -> None:
    for rel in dotted_dirs:
        _write(root / rel / "__init__.py", "")


def _seed_source_tree(
    root: Path,
    router_body: str = _DEFAULT_ROUTER_BODY,
    schema_body: str = _DEFAULT_SCHEMA_BODY,
) -> None:
    _write_package_init(
        root,
        "src/dev_health_ops",
        "src/dev_health_ops/api",
        "src/dev_health_ops/api/routers",
        "src/dev_health_ops/api/graphql",
    )
    _write(
        root / _MAIN_FILE,
        "from fastapi import FastAPI\n"
        "\n"
        "from .routers.example import router as example_router\n"
        "\n"
        "app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)\n"
        "app.include_router(example_router)\n",
    )
    _write(root / _ROUTER_FILE, router_body)
    _write(root / _SCHEMA_FILE, schema_body)
    # billing_edge is in DEPLOYED_APPS, so every fixture tree needs one or
    # discovery cannot import it. Empty by default; the billing-edge tests
    # below overwrite it with a real second app.
    _write(
        root / _BILLING_EDGE_FILE,
        "from fastapi import FastAPI\n"
        "\n"
        "app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)\n",
    )


def _minimal_valid_row(**overrides) -> dict:
    row: dict = {
        "id": "GET /example [dev-health-ops-api]",
        "surface_kind": "rest",
        "method": "GET",
        "route": "/example",
        "graphql_field_name": None,
        "service": "dev-health-ops-api",
        "source": {"file": _ROUTER_FILE, "line": 6},
        "classification": "public",
        "public_rationale": "test fixture, no credential required",
        "accepted_credential_classes": [],
        "gaps": [],
    }
    row.update(overrides)
    return row


def _write_inventory(root: Path, rows: list[dict]) -> Path:
    inventory = {
        "schema_version": "endpoint-profile.v1",
        "generated_at": "2026-09-01T00:00:00Z",
        "source_commit": "0000000000000000000000000000000000000",
        "credential_class_source": "contracts/auth/v1/credential-classes.json",
        "rows": rows,
    }
    path = root / "contracts" / "auth" / "v1" / "endpoint-profiles.ops.json"
    _write(path, json.dumps(inventory, indent=2))
    return path


def _minimal_valid_root(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    _seed_shared_fixtures(root)
    _seed_source_tree(root)
    _write_inventory(root, [_minimal_valid_row()])
    return root


def _paths(root: Path):
    return (
        root / "contracts" / "auth" / "v1" / "endpoint-profiles.ops.json",
        root / "contracts" / "auth" / "v1" / "endpoint-profile.schema.json",
        root / "contracts" / "auth" / "v1" / "credential-classes.json",
    )


def test_gate_passes_on_a_minimal_fully_owned_fixture_tree(tmp_path):
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert errors == [], errors


def test_gate_catches_a_synthetic_unowned_rest_route(tmp_path):
    """A second @router-decorated route with no inventory row must fail."""
    root = _minimal_valid_root(tmp_path)
    _write(
        root / _ROUTER_FILE,
        "from fastapi import APIRouter\n"
        "\n"
        "router = APIRouter()\n"
        "\n"
        "\n"
        '@router.get("/example")\n'
        "async def get_example():\n"
        "    return {}\n"
        "\n"
        "\n"
        '@router.post("/rogue")\n'
        "async def rogue_route():\n"
        "    return {}\n",
    )
    inventory_path, schema_path, cc_path = _paths(root)
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("UNOWNED SURFACE" in e and f"{_ROUTER_FILE}:11" in e for e in errors), (
        errors
    )


def test_gate_catches_a_synthetic_unowned_graphql_resolver(tmp_path):
    root = _minimal_valid_root(tmp_path)
    _write(root / _SCHEMA_FILE, _schema_with_resolver("resolve_rogue"))
    inventory_path, schema_path, cc_path = _paths(root)
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("UNOWNED SURFACE" in e and "resolve_rogue" in e for e in errors), errors


def test_gate_catches_a_resolver_that_exists_only_in_a_docstring(tmp_path):
    """The defect that put two rows in the committed inventory for surfaces
    that never existed (CHAOS-4761).

    `api/graphql/authz.py:25` and `:30` are `@strawberry.field` EXAMPLES inside
    `require_permission`'s docstring. Source-text discovery matched them, so
    the inventory gained rows for `metrics` and `update_setting` -- two
    profiles of documentation. Enumerating the served schema cannot reproduce
    that: a docstring defines no field. Red-on-baseline for this is the
    committed inventory itself at the parent commit, which contained both rows
    and passed.
    """
    root = _minimal_valid_root(tmp_path)
    _write(
        root / "src/dev_health_ops/api/graphql/authz.py",
        "def require_permission(*permissions):\n"
        '    """Decorator to require permissions for GraphQL resolvers.\n'
        "\n"
        "    Usage:\n"
        "        @strawberry.field\n"
        "        async def metrics(self, info) -> list[str]:\n"
        "            ...\n"
        '    """\n'
        "    return lambda f: f\n",
    )
    inventory_path, schema_path, cc_path = _paths(root)
    # The tree is otherwise fully owned, so a docstring-derived surface would
    # show up as the ONLY new error.
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert errors == [], errors
    # And a row claiming that docstring "surface" is rejected as a phantom.
    row = _minimal_valid_row(
        id="graphql:field:metrics",
        surface_kind="graphql_field",
        method=None,
        route=None,
        graphql_field_name="metrics",
        source={"file": "src/dev_health_ops/api/graphql/authz.py", "line": 5},
    )
    _write_inventory(root, [_minimal_valid_row(), row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("PHANTOM ROW" in e and "metrics" in e for e in errors), errors


def test_gate_catches_a_row_anchored_away_from_its_endpoint(tmp_path):
    """The row names a surface the app really serves, but anchors it at a
    line the endpoint is not defined on.

    Identity and anchor are separate concerns since CHAOS-4761: this row
    matches a served surface (so it is not a phantom) and is caught on the
    anchor alone, checked against where the served callable actually is."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(
        source={"file": _ROUTER_FILE, "line": 3}
    )  # not the decorator line
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("STALE ANCHOR" in e and "content drift" in e for e in errors), errors


def test_gate_catches_a_row_naming_a_route_the_app_does_not_serve(tmp_path):
    """Bidirectional parity: the fabricated row is a phantom AND the real
    route it displaced is unowned. Both are asserted, because a check that
    only counted would let the two net out."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(
        id="GET /not-served [dev-health-ops-api]", route="/not-served"
    )
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("PHANTOM ROW" in e and "/not-served" in e for e in errors), errors
    assert any("UNOWNED SURFACE" in e and "/example" in e for e in errors), errors


def test_gate_catches_a_duplicate_id(tmp_path):
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row_a = _minimal_valid_row()
    row_b = _minimal_valid_row()  # same id, same source -- also exercises this path
    _write_inventory(root, [row_a, row_b])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("DUPLICATE ID" in e for e in errors), errors


def test_gate_catches_an_unknown_accepted_credential_class(tmp_path):
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(
        classification="protected",
        public_rationale=None,
        accepted_credential_classes=["not_a_real_credential_class"],
    )
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("UNKNOWN accepted_credential_class" in e for e in errors), errors


def test_gate_catches_an_unknown_service(tmp_path):
    """Guardrail G-26: an unknown service must fail validation, never be
    silently accepted. Enforced by the real Draft 2020-12 validator now
    (service is a schema enum) rather than a hand-rolled check, so the
    message is the schema's own, not a hand-authored "UNKNOWN service"
    string."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(service="totally-unregistered-app")
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any(
        "JSON SCHEMA VIOLATION" in e and "totally-unregistered-app" in e for e in errors
    ), errors


def test_gate_catches_a_protected_row_with_no_accepted_credential_classes(tmp_path):
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(
        classification="protected",
        public_rationale=None,
        accepted_credential_classes=[],
    )
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("EMPTY accepted_credential_classes" in e for e in errors), errors


def test_gate_catches_a_public_row_with_no_rationale(tmp_path):
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(public_rationale=None)
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("MISSING public_rationale" in e for e in errors), errors


def test_gate_catches_a_row_claiming_a_method_the_app_does_not_serve(tmp_path):
    """The app serves GET /example, not POST. Under identity-by-(service,
    method, path) that is a row naming nothing plus a real route left
    unowned -- both reported."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(method="POST", id="POST /example [dev-health-ops-api]")
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("PHANTOM ROW" in e and "POST" in e for e in errors), errors
    assert any("UNOWNED SURFACE" in e and "GET /example" in e for e in errors), errors


def test_gate_catches_a_row_claiming_a_path_the_app_does_not_serve(tmp_path):
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(route="/a-completely-different-path")
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any(
        "PHANTOM ROW" in e and "/a-completely-different-path" in e for e in errors
    ), errors
    assert any("UNOWNED SURFACE" in e and "/example" in e for e in errors), errors


def test_gate_catches_a_graphql_row_naming_a_field_the_schema_does_not_serve(
    tmp_path,
):
    root = _minimal_valid_root(tmp_path)
    _write(root / _SCHEMA_FILE, _schema_with_resolver("resolve_real_name"))
    row = _minimal_valid_row(
        id="graphql:field:a_different_name",
        surface_kind="graphql_field",
        method=None,
        route=None,
        graphql_field_name="a_different_name",
        source={"file": _SCHEMA_FILE, "line": _RESOLVER_ANCHOR_LINE},
    )
    inventory_path, schema_path, cc_path = _paths(root)
    _write_inventory(root, [_minimal_valid_row(), row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("PHANTOM ROW" in e and "a_different_name" in e for e in errors), errors
    assert any("UNOWNED SURFACE" in e and "resolve_real_name" in e for e in errors), (
        errors
    )


def test_gate_catches_a_graphql_row_anchored_at_the_wrong_line(tmp_path):
    """The row names a field the schema really serves, but anchors it
    somewhere the resolver is not defined."""
    root = _minimal_valid_root(tmp_path)
    _write(root / _SCHEMA_FILE, _schema_with_resolver("resolve_real_name"))
    row = _minimal_valid_row(
        id="graphql:field:resolve_real_name",
        surface_kind="graphql_field",
        method=None,
        route=None,
        graphql_field_name="resolve_real_name",
        source={"file": _SCHEMA_FILE, "line": 1},
    )
    inventory_path, schema_path, cc_path = _paths(root)
    _write_inventory(root, [_minimal_valid_row(), row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("STALE ANCHOR" in e and "content drift" in e for e in errors), errors


def test_gate_catches_a_stray_top_level_key(tmp_path):
    """Schema violation regression guard -- the exact defect class the lane
    brief calls out (a stale extra top-level key in the web/acr files,
    removed at e749efad4/fd338340)."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    inventory = checker.load_json(inventory_path)
    inventory["schema_deviation_note"] = "this key should not exist"
    _write(inventory_path, json.dumps(inventory, indent=2))
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any(
        "SCHEMA VIOLATION" in e and "schema_deviation_note" in e for e in errors
    ), errors


def test_gate_catches_a_stray_row_level_key(tmp_path):
    """Codex round-2 P2, EXECUTED repro: additionalProperties: false was
    declared at the top level only, not on $defs.endpointProfile -- so a
    typo'd/extra ROW-level key (the repro used 'primary_validtor') returned
    errors=[]. Fixed by adding additionalProperties: false to
    endpointProfile itself (verified safe against both the real ops and
    acr inventories: every key either currently uses is already declared)."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(this_key_does_not_exist_in_the_schema="typo")
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any(
        "JSON SCHEMA VIOLATION" in e and "this_key_does_not_exist_in_the_schema" in e
        for e in errors
    ), errors


def test_gate_catches_a_row_missing_a_required_field(tmp_path):
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row()
    del row["classification"]
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("SCHEMA VIOLATION" in e and "classification" in e for e in errors), (
        errors
    )


def test_gate_catches_issued_credential_null_with_no_gaps_entry(tmp_path):
    """issued_credential: null MUST be paired with a gaps entry, exactly
    like a null anchor -- do not collapse the four-valued contract."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(issued_credential=None, gaps=[])
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("UNSTATED NULL" in e and "issued_credential" in e for e in errors), (
        errors
    )


def test_gate_accepts_issued_credential_null_with_a_gaps_entry(tmp_path):
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(
        issued_credential=None,
        gaps=["issued_credential not determined this pass"],
    )
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert errors == [], errors


def test_gate_accepts_issued_credential_empty_array_with_no_gaps_entry(tmp_path):
    """[] means assessed-and-mints-nothing -- valid on its own, no gaps
    entry required (must not be conflated with null)."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(issued_credential=[], gaps=[])
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert errors == [], errors


def test_gate_catches_an_unknown_issued_credential_class_id(tmp_path):
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(
        issued_credential=[
            {
                "class_id": "not_a_real_class",
                "direction": "returned_to_caller",
                "anchor": {"path": _ROUTER_FILE, "line": 6},
            }
        ],
        gaps=[],
    )
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("UNKNOWN issued_credential class_id" in e for e in errors), errors


def test_gate_catches_an_issued_credential_anchor_pointing_at_a_missing_file(tmp_path):
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(
        issued_credential=[
            {
                "class_id": "ops_access_token_hs256",
                "direction": "returned_to_caller",
                "anchor": {"path": "src/dev_health_ops/does/not/exist.py", "line": 1},
            }
        ],
        gaps=[],
    )
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("STALE ANCHOR" in e and "issued_credential" in e for e in errors), errors


def test_gate_accepts_a_fully_populated_issued_credential_row(tmp_path):
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(
        issued_credential=[
            {
                "class_id": "ops_access_token_hs256",
                "direction": "returned_to_caller",
                # line 6 is the decorator; get_example is declared 1 line
                # below -- anchor.note names it for the content check.
                "anchor": {
                    "path": _ROUTER_FILE,
                    "line": 6,
                    "note": "get_example handler",
                },
                "issuer": "ops",
                "audience": None,
                "algorithm": "HS256",
                "lifetime_seconds": 900,
                "key_source": "JWT_SECRET_KEY",
                "verified_by": "ops:src/dev_health_ops/api/services/auth.py:285",
            }
        ],
        gaps=[],
    )
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert errors == [], errors


def test_gate_catches_exposure_null_with_no_gaps_entry(tmp_path):
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(exposure=None, gaps=[])
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("UNSTATED NULL" in e and "exposure" in e for e in errors), errors


def test_gate_accepts_exposure_null_with_a_gaps_entry(tmp_path):
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(
        exposure=None, gaps=["exposure/edge reachability not determined this pass"]
    )
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert errors == [], errors


def test_gate_catches_exposure_unknown_reachability_with_no_gaps_entry(tmp_path):
    """'unknown' is a legitimate value but must stay an honest declaration,
    not a silent default -- MUST be paired with a gaps entry."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(
        exposure={"reachability": "unknown", "source": "edge path-map not consulted"},
        gaps=[],
    )
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("UNSTATED NULL" in e and "exposure" in e for e in errors), errors


def test_gate_catches_exposure_missing_source(tmp_path):
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(
        exposure={"reachability": "private_network_only", "source": ""}, gaps=[]
    )
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("MISSING exposure.source" in e for e in errors), errors


def test_gate_accepts_a_fully_populated_exposure_row(tmp_path):
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(
        exposure={
            "reachability": "private_network_only",
            "source": "edge ingress path-map, reviewed 2026-09-01",
            "observed_at": "2026-09-01T00:00:00Z",
            "note": "internal-only route",
        },
        gaps=[],
    )
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert errors == [], errors


def test_gate_catches_a_protected_row_with_a_null_primary_validator_anchor(tmp_path):
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(
        classification="protected",
        public_rationale=None,
        accepted_credential_classes=["ops_access_token_hs256"],
        primary_validator={"description": "unresolved this pass", "anchor": None},
        gaps=[],
    )
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("UNSTATED NULL" in e and "primary_validator" in e for e in errors), (
        errors
    )


def test_gate_accepts_a_public_row_with_a_null_primary_validator(tmp_path):
    """A genuinely public route legitimately has no validator at all --
    primary_validator: null there is correct, not a gap."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(primary_validator=None)
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert errors == [], errors


def test_gate_catches_a_public_row_with_a_null_primary_validator_anchor(tmp_path):
    """Codex-verified gap (round 1): this rule used to be scoped to
    classification=='protected' only, so a PUBLIC row could set
    primary_validator to a present object with anchor=null and gaps=[] and
    pass. The schema's anchor $def rule ("null MUST be paired with a gaps
    entry") does not carve out public rows -- it's about the anchor being
    unresolved, not about who has to have a validator at all (that's a
    separate, correctly-public-scoped rule: see
    test_gate_accepts_a_public_row_with_a_null_primary_validator, where
    primary_validator ITSELF is null)."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(
        primary_validator={"description": "unresolved this pass", "anchor": None},
        gaps=[],
    )
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("UNSTATED NULL" in e and "primary_validator" in e for e in errors), (
        errors
    )


def test_gate_rejects_a_row_with_the_wrong_field_type(tmp_path):
    """Codex round-1 P1, EXECUTED repro: primary_validator: 17 (the wrong
    type entirely -- schema says object|null) previously returned
    CHECK_ERRORS=[]. Full Draft 2020-12 validation over the whole document
    catches this class categorically, not just this one field."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(primary_validator=17)
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("JSON SCHEMA VIOLATION" in e for e in errors), errors


def test_gate_reads_issued_credential_direction_and_exposure_reachability_live(
    tmp_path,
):
    """Codex round-1 P2: these two enums were hardcoded Python sets instead
    of read from the schema like every other enum -- so a legitimate future
    schema addition to either would have been rejected as UNKNOWN. Prove
    the live-schema contract holds for BOTH: add a new enum value to a
    fixture schema copy and confirm a row using it is accepted, the same
    standard test_schema_accepts_server_action_surface_kind_dynamically
    already holds surface_kind to."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    schema = checker.load_json(schema_path)
    schema["$defs"]["issuedCredential"]["properties"]["direction"]["enum"].append(
        "minted_to_broker"
    )
    schema["$defs"]["endpointProfile"]["properties"]["exposure"]["properties"][
        "reachability"
    ]["enum"].append("edge_and_direct")
    _write(schema_path, json.dumps(schema, indent=2))
    row = _minimal_valid_row(
        classification="protected",
        public_rationale=None,
        accepted_credential_classes=["ops_access_token_hs256"],
        issued_credential=[
            {
                "class_id": "ops_access_token_hs256",
                "direction": "minted_to_broker",
                "anchor": {
                    "path": _ROUTER_FILE,
                    "line": 6,
                    "note": "get_example mints it",
                },
            }
        ],
        exposure={"reachability": "edge_and_direct", "source": "fixture"},
        gaps=[],
    )
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert errors == [], errors


def test_gate_catches_duplicate_surface_ownership(tmp_path):
    """Codex round-1 P2, EXECUTED repro: two rows with DIFFERENT ids (so
    the plain duplicate-id check doesn't fire) both anchored at the SAME
    discovered (file, line) surface, with conflicting classifications, used
    to return OK -- worse than a missing row, since both look registered."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row_a = _minimal_valid_row(id="GET /example [dev-health-ops-api] (a)")
    row_b = _minimal_valid_row(
        id="GET /example [dev-health-ops-api] (b)",
        classification="protected",
        public_rationale=None,
        accepted_credential_classes=["ops_access_token_hs256"],
    )
    _write_inventory(root, [row_a, row_b])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any(
        "DUPLICATE SURFACE OWNERSHIP" in e and "GET /example" in e for e in errors
    ), errors


def test_gate_catches_a_trivial_issued_credential_anchor(tmp_path):
    """Codex round-1 P2, EXECUTED repro: an issued_credential.anchor
    pointing at an existing `return {}` line (real file, in-bounds line --
    passes the existence/bounds check) was accepted as a mint site. Anchor
    checks must reject an obviously-trivial/placeholder line, not just
    verify the file exists and the line is in range."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(
        classification="protected",
        public_rationale=None,
        accepted_credential_classes=["ops_access_token_hs256"],
        issued_credential=[
            {
                "class_id": "ops_access_token_hs256",
                "direction": "returned_to_caller",
                # line 8 of _ROUTER_FILE's fixture body is "    return {}".
                "anchor": {"path": _ROUTER_FILE, "line": 8},
            }
        ],
        gaps=[],
    )
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("TRIVIAL ANCHOR" in e and "issued_credential" in e for e in errors), (
        errors
    )


def test_gate_catches_the_real_committed_off_by_one_anchor_bug(tmp_path):
    """Coordinator-verified real defect (2026-09-01, acr inventory): a row
    anchored its primary_validator one line off the real call, at a bare
    closing bracket the (round-1) denylist did not cover. Fixed in acr's
    data; this is the regression guard against the same bug SHAPE, using a
    synthetic fixture (ops has no such bug -- this proves the mechanism)."""
    root = tmp_path / "repo"
    _seed_shared_fixtures(root)
    _seed_source_tree(
        root,
        router_body=(
            "from fastapi import APIRouter\n"
            "\n"
            "router = APIRouter()\n"
            "\n"
            "\n"
            "def build_handler():\n"
            "    inner = (\n"
            "        lambda: {}\n"
            "    )\n"
            "    return protected(inner)\n"
        ),
    )
    row = _minimal_valid_row(
        primary_validator={
            "description": "wraps itself in protected()",
            # The bug shape: anchored at the closing "    )" of the inner
            # lambda, one line above the real "return protected(inner)".
            "anchor": {"path": _ROUTER_FILE, "line": 9},
        },
        source={"file": _ROUTER_FILE, "line": 999},  # unrelated to this row's own check
    )
    inventory_path, schema_path, cc_path = _paths(root)
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("TRIVIAL ANCHOR" in e and "primary_validator" in e for e in errors), (
        errors
    )


def test_gate_catches_an_issued_credential_anchor_with_no_extractable_function_name(
    tmp_path,
):
    """Coordinator ruling (2026-09-01): "where [content] cannot be
    established, say so in the message rather than passing." An anchor
    landing on a line with no function/method declaration at or near it
    must be reported, not silently accepted just because the file exists
    and the line is in bounds."""
    root = tmp_path / "repo"
    _seed_shared_fixtures(root)
    _seed_source_tree(
        root,
        router_body="from fastapi import APIRouter\n\nrouter = APIRouter()\n",
    )
    row = _minimal_valid_row(
        classification="protected",
        public_rationale=None,
        accepted_credential_classes=["ops_access_token_hs256"],
        issued_credential=[
            {
                "class_id": "ops_access_token_hs256",
                "direction": "returned_to_caller",
                "anchor": {"path": _ROUTER_FILE, "line": 3},
            }
        ],
        gaps=[],
        source={"file": _ROUTER_FILE, "line": 999},
    )
    inventory_path, schema_path, cc_path = _paths(root)
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any(
        "ANCHOR CONTENT UNVERIFIED" in e and "issued_credential" in e for e in errors
    ), errors


def test_gate_catches_an_issued_credential_anchor_whose_note_names_the_wrong_function(
    tmp_path,
):
    """The core of the coordinator's fix: existence + in-bounds + non-trivial
    is not enough. An anchor pointing at a REAL function declaration that
    the row's own note/issuer never mentions is exactly the shape of the
    real committed bug (an anchor near, but not at, the right site)."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(
        classification="protected",
        public_rationale=None,
        accepted_credential_classes=["ops_access_token_hs256"],
        issued_credential=[
            {
                "class_id": "ops_access_token_hs256",
                "direction": "returned_to_caller",
                # line 6/7 is get_example -- a real function, but the note
                # below names something else.
                "anchor": {
                    "path": _ROUTER_FILE,
                    "line": 6,
                    "note": "signs the token in mint_credential",
                },
            }
        ],
        gaps=[],
    )
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("ANCHOR CONTENT MISMATCH" in e and "get_example" in e for e in errors), (
        errors
    )


def test_gate_accepts_an_issued_credential_anchor_whose_issuer_field_names_the_function(
    tmp_path,
):
    """The content check accepts a match via EITHER anchor.note or
    entry.issuer -- not note exclusively."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(
        classification="protected",
        public_rationale=None,
        accepted_credential_classes=["ops_access_token_hs256"],
        issued_credential=[
            {
                "class_id": "ops_access_token_hs256",
                "direction": "returned_to_caller",
                "anchor": {"path": _ROUTER_FILE, "line": 6},
                "issuer": "ops get_example",
            }
        ],
        gaps=[],
    )
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert errors == [], errors


def test_ops_owned_contract_files_are_jq_dash_S_stable():
    """acr's CI pins these two files via a sparse checkout at a fixed ops
    commit (contracts/auth/v1/endpoint-profile.schema.json and
    credential-classes.json -- see acr's ci/ops-contract.pin). Every commit
    that changes either file makes acr's pin worth reconsidering, so a
    PURELY cosmetic reformat (different indent, different key order) must
    never be able to land here: it would force acr to weigh a pin bump for
    a diff that carries no actual contract change. Enforcing that both
    files are always kept in their canonical `jq -S` (sorted-key) form means
    every future diff to them IS a content diff -- protects the pin from
    churn.
    """
    for path in (_SCHEMA_PATH, _CREDENTIAL_CLASSES_PATH):
        raw = path.read_text()
        normalized = subprocess.run(
            ["jq", "-S", "."], input=raw, capture_output=True, text=True, check=True
        ).stdout
        assert raw == normalized, (
            f"{path.name} is not in canonical `jq -S` form -- run "
            f"`jq -S . {path} > /tmp/x && mv /tmp/x {path}` before committing "
            "(guards acr's pin from formatting-only churn)"
        )


# ---------------------------------------------------------------------------
# `service` must match discovery's per-deployed-app attribution (merge-gate
# finding 1). `service` is a real schema enum with 5 legal values -- the
# schema validator alone accepts ANY of them, so relabelling a row to a
# DIFFERENT but still-legal value (e.g. dev-health-ops-api -> dev-health-web)
# previously passed with zero errors. The billing-edge fixture below is the
# interesting/adversarial direction: two rows genuinely share a path but are
# served by two different apps, and that must keep passing.
# ---------------------------------------------------------------------------

_BILLING_EDGE_FILE = "src/dev_health_ops/api/billing_edge.py"


def _seed_billing_edge_app(root: Path) -> None:
    """A second, separately-deployed FastAPI() root decorated directly on
    the `app` instance (never an APIRouter) -- mirrors the real
    src/dev_health_ops/api/billing_edge.py shape exactly, including a route
    at the SAME local path `/shared` that main.py's example router also
    serves, the real billing-edge scenario (POST /api/v1/billing/webhooks/
    stripe, served by both apps as two independent rows)."""
    _write(
        root / _BILLING_EDGE_FILE,
        "from fastapi import FastAPI\n"
        "\n"
        "app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)\n"
        "\n"
        "\n"
        '@app.post("/shared")\n'
        "async def shared_webhook():\n"
        "    return {}\n",
    )


def test_gate_catches_a_service_relabelled_to_a_different_deployed_app(tmp_path):
    """Codex's exact merge-gate repro: GET /example (served by main.py,
    app_root dev_health_ops.api.main::app) relabelled to the ALSO-VALID
    enum value dev-health-web, which nothing but discovery attribution can
    catch -- the schema's own enum check happily accepts dev-health-web,
    since it's a real vocabulary member, just not the app that serves this
    route."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(service="dev-health-web")
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any(
        "SERVICE MISMATCH" in e and "dev-health-web" in e and "dev-health-ops-api" in e
        for e in errors
    ), errors


def test_gate_passes_two_rows_sharing_a_path_served_by_different_apps(tmp_path):
    """The billing-edge false-positive guard: /shared is a real route on
    BOTH the main app and the billing-edge app, each correctly attributing
    its own row to its own service. Two rows, two different (file, line)
    anchors, two different services -- must pass cleanly."""
    root = _minimal_valid_root(tmp_path)
    _seed_billing_edge_app(root)
    inventory_path, schema_path, cc_path = _paths(root)
    rows = [
        _minimal_valid_row(),
        _minimal_valid_row(
            id="POST /shared [dev-health-ops-billing-edge]",
            method="POST",
            route="/shared",
            service="dev-health-ops-billing-edge",
            source={"file": _BILLING_EDGE_FILE, "line": 6},
        ),
    ]
    _write_inventory(root, rows)
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert errors == [], errors


def test_gate_catches_a_billing_edge_row_mislabelled_as_the_main_app(tmp_path):
    """The inverse of the pass-case above: the billing-edge row claims
    dev-health-ops-api instead of dev-health-ops-billing-edge. This matters
    specifically because reachable_validators is [] for billing-edge rows
    (that app shares no middleware with the main app) -- a row attributed
    to the wrong app silently invalidates that whole line of reasoning."""
    root = _minimal_valid_root(tmp_path)
    _seed_billing_edge_app(root)
    inventory_path, schema_path, cc_path = _paths(root)
    rows = [
        _minimal_valid_row(),
        _minimal_valid_row(
            id="POST /shared [dev-health-ops-billing-edge]",
            method="POST",
            route="/shared",
            service="dev-health-ops-api",  # wrong -- this route is billing-edge
            source={"file": _BILLING_EDGE_FILE, "line": 6},
        ),
    ]
    _write_inventory(root, rows)
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any(
        "SERVICE MISMATCH" in e
        and "dev-health-ops-billing-edge" in e
        and "/shared" in e
        for e in errors
    ), errors


# ---------------------------------------------------------------------------
# credential-classes.json validated against its own schema (merge-gate
# finding 2). Closing the vocabulary (test_every_accepted_credential_class_
# is_real, above) says nothing about whether each class in it is actually
# well-formed -- previously nothing did.
# ---------------------------------------------------------------------------


def test_gate_catches_an_under_specified_credential_class(tmp_path):
    """credential-classes.schema.json requires display_name/status/
    transport/issuer/validators/backing_store/principal_type/
    audience_and_scope/lifecycle/consumers/bootstrap/gaps on every class
    (contracts/auth/v1/credential-classes.schema.json:53-68) -- a class
    reduced to just class_id must fail, even though the CLOSED VOCABULARY
    check (class_id membership) never would, since class_id is the one
    field that IS present."""
    root = _minimal_valid_root(tmp_path)
    _write(
        root / "contracts" / "auth" / "v1" / "credential-classes.json",
        json.dumps(
            {
                "schema_version": "credential-classes.v1",
                "generated_at": "2026-09-01T00:00:00Z",
                "source_commits": {"ops": "0" * 40, "web": "0" * 40, "acr": "0" * 40},
                "classes": [{"class_id": "under_specified_class"}],
            }
        ),
    )
    inventory_path, schema_path, cc_path = _paths(root)
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any(
        "CREDENTIAL CLASS SCHEMA VIOLATION" in e and "required" in e for e in errors
    ), errors


# ---------------------------------------------------------------------------
# surface_kind is read live from the schema, never hardcoded (merge-gate
# finding 3). test_schema_accepts_server_action_surface_kind_dynamically
# (above) only unit-tests _schema_enum -- it never drives a server_action
# row through checker.check() itself, so a regression that reintroduced a
# hand-rolled surface_kind allowlist (e.g. `if kind not in {"rest",
# "graphql_field", "graphql_mutation"}: reject`) would leave that test
# green while check() wrongly rejected every legitimate server_action row
# with an "unknown/invalid surface_kind" complaint.
# ---------------------------------------------------------------------------


def test_gate_accepts_server_action_surface_kind_through_check_end_to_end(tmp_path):
    """Drive a schema-valid server_action row through check() end to end.
    ops's own discover_ops_routes.py has no server_action discovery at all
    (ops is a Python FastAPI backend, not Next.js -- server_action is in
    the shared schema for the WEB lane's checker) -- so this row can only
    ever fail here as an UNCHECKABLE ROW -- ops serves no Server Actions, so
    this gate cannot attribute the row to anything -- and that must be the
    ONLY complaint: never one naming surface_kind/enum/vocabulary, which is
    what a reintroduced hardcoded allowlist would add."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(
        id="server_action:src/app/actions.ts#doThing [dev-health-ops-api]",
        surface_kind="server_action",
        method=None,
        route=None,
        source={"file": "src/app/actions.ts", "line": 3},
    )
    _write_inventory(root, [_minimal_valid_row(), row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("UNCHECKABLE ROW" in e for e in errors), errors
    assert not any(
        ("enum" in e.lower() or "vocabulary" in e.lower())
        and "UNCHECKABLE ROW" not in e
        for e in errors
    ), errors


# ---------------------------------------------------------------------------
# DISCLOSURE-HOLD reporting: report-only, never a check() failure.
# ---------------------------------------------------------------------------


def test_disclosure_hold_marker_is_reported_not_rejected(tmp_path):
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    row = _minimal_valid_row(gaps=["DISCLOSURE-HOLD: pending fix for CHAOS-9999"])
    _write_inventory(root, [row])
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert errors == [], errors
    held = checker.find_disclosure_hold_rows([row])
    assert held == [row["id"]]


def test_disclosure_hold_marker_absent_reports_nothing():
    held = checker.find_disclosure_hold_rows(
        [
            {
                "id": "GET /example [dev-health-ops-api]",
                "gaps": ["ordinary undetermined field"],
            }
        ]
    )
    assert held == []


def test_disclosure_hold_marker_found_anywhere_in_row_not_only_gaps():
    """The marker can appear in any prose field on the row, not only gaps --
    a note, a description, a nested anchor note."""
    row = {
        "id": "POST /example [dev-health-ops-api]",
        "gaps": [],
        "primary_validator": {
            "description": "ok",
            "anchor": {
                "path": "x.py",
                "line": 1,
                "note": "DISCLOSURE-HOLD pending CHAOS-1234",
            },
        },
    }
    held = checker.find_disclosure_hold_rows([row])
    assert held == [row["id"]]


def test_gate_fails_loudly_when_jsonschema_is_unavailable(tmp_path, monkeypatch):
    """Coordinator ruling (2026-09-01): if the jsonschema package is not
    importable, the gate must FAIL LOUDLY (a clear message naming the
    missing package and how to install it), never silently skip
    validation and report success. Simulate the import having failed by
    monkeypatching the module's own sentinel, exactly what a real missing
    dependency leaves behind."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    monkeypatch.setattr(checker, "jsonschema", None)
    monkeypatch.setattr(
        checker,
        "_JSONSCHEMA_IMPORT_ERROR",
        ImportError("simulated: no module named jsonschema"),
    )
    with pytest.raises(RuntimeError) as exc_info:
        checker.check(root, inventory_path, schema_path, cc_path)
    message = str(exc_info.value)
    assert "jsonschema" in message
    assert "pip install" in message or "uv sync" in message


def test_cli_fails_loudly_when_jsonschema_is_unavailable(tmp_path, monkeypatch, capsys):
    root = _minimal_valid_root(tmp_path)
    monkeypatch.setattr(checker, "jsonschema", None)
    monkeypatch.setattr(
        checker,
        "_JSONSCHEMA_IMPORT_ERROR",
        ImportError("simulated: no module named jsonschema"),
    )
    exit_code = checker.main(["--root", str(root)])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert "FAIL" in captured.err
    assert "jsonschema" in captured.err


def test_cli_prints_disclosure_hold_even_when_jsonschema_is_unavailable(
    tmp_path, monkeypatch, capsys
):
    """Codex round-2 P3: the missing-jsonschema path returned before
    printing any DISCLOSURE-HOLD line at all, so "unconditional" was not
    actually true. DISCLOSURE-HOLD must print even on this failure path."""
    root = _minimal_valid_root(tmp_path)
    monkeypatch.setattr(checker, "jsonschema", None)
    monkeypatch.setattr(
        checker,
        "_JSONSCHEMA_IMPORT_ERROR",
        ImportError("simulated: no module named jsonschema"),
    )
    exit_code = checker.main(["--root", str(root)])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert "DISCLOSURE-HOLD" in captured.out


def test_cli_does_not_crash_on_a_malformed_rows_field(tmp_path, capsys):
    """Codex round-2 P3, EXECUTED repro: main() passed `rows` straight to
    find_disclosure_hold_rows, so a malformed `rows: 17` raised
    `TypeError: 'int' object is not iterable' BEFORE the real JSON SCHEMA
    VIOLATION was ever printed -- the gate crashed instead of reporting the
    actual problem."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, _, _ = _paths(root)
    inventory = checker.load_json(inventory_path)
    inventory["rows"] = 17
    _write(inventory_path, json.dumps(inventory))
    exit_code = checker.main(["--root", str(root)])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert "DISCLOSURE-HOLD" in captured.out
    assert "FAIL" in captured.err
    assert "JSON SCHEMA VIOLATION" in captured.err


def test_cli_does_not_crash_on_a_malformed_row_inside_the_list(tmp_path, capsys):
    """Codex round-3 P3, EXECUTED repro: the scalar `rows: 17` form above
    was fixed, but a malformed row INSIDE an otherwise-list `rows` (e.g.
    `rows: [17]`) still crashed -- the bidirectional-parity loop at
    check_endpoint_profiles.py:447 called row.get() without the same
    non-dict guard the per-row semantic-checks loop already had at :276.
    `AttributeError: 'int' object has no attribute 'get'` raised, and the
    JSON SCHEMA VIOLATION at rows/0 the full-document validator had ALREADY
    computed was lost to a traceback instead of being reported. This cannot
    produce a false pass (the gate still exits non-zero either way) -- it
    is a diagnostics defect, not a correctness one, but the whole value of
    this gate is telling you exactly what is wrong."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, _, _ = _paths(root)
    inventory = checker.load_json(inventory_path)
    inventory["rows"] = [17]
    _write(inventory_path, json.dumps(inventory))
    exit_code = checker.main(["--root", str(root)])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert "DISCLOSURE-HOLD" in captured.out
    assert "FAIL" in captured.err
    assert "JSON SCHEMA VIOLATION" in captured.err
    assert "rows/0" in captured.err


# ---------------------------------------------------------------------------
# Second merge-gate review (2026-09-01): three new findings, closed here.
#
# 1. DUPLICATE class_id -- JSON Schema cannot express cross-array id
#    uniqueness, so credential_class_vocabulary()'s collapse to a set
#    silently absorbed a duplicate/conflicting class_id.
# 2. Two crash-before-report cases: a scalar/list top-level inventory
#    document, and a malformed top-level credential-classes document, each
#    raised before the computed schema violation ever printed.
# 3. UNVERIFIED ROUTE -- retired by CHAOS-4761 along with its allowlist;
#    see the note at the end of this file.
# ---------------------------------------------------------------------------


def test_gate_catches_a_duplicate_class_id(tmp_path):
    """Merge-gate P1, EXECUTED repro: duplicating a REAL class (same
    class_id, conflicting display_name) in credential-classes.json
    previously passed with errors == [] -- credential_class_vocabulary()
    collapses the classes array into a set[str] of ids, which cannot tell
    two conflicting definitions apart from one. The duplicate check runs on
    the raw array, before that collapse."""
    root = _minimal_valid_root(tmp_path)
    cc = json.loads(_CREDENTIAL_CLASSES_PATH.read_text())
    real_class_id = cc["classes"][0]["class_id"]
    duplicate = json.loads(json.dumps(cc["classes"][0]))
    duplicate["display_name"] = "CONFLICTING DUPLICATE " + duplicate["display_name"]
    cc["classes"] = cc["classes"] + [duplicate]
    _write(
        root / "contracts" / "auth" / "v1" / "credential-classes.json", json.dumps(cc)
    )
    inventory_path, schema_path, cc_path = _paths(root)
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("DUPLICATE class_id" in e and real_class_id in e for e in errors), errors


def test_check_does_not_crash_on_a_scalar_top_level_inventory(tmp_path):
    """Merge-gate P2, EXECUTED repro: a top-level inventory document that
    is a JSON array (not an object at all -- a step further than the
    already-fixed `rows: 17` / `rows: [17]` cases) raised
    `AttributeError: 'list' object has no attribute 'get'` on
    `inventory.get("rows", [])` inside check() itself, discarding the JSON
    SCHEMA VIOLATION errors already computed a few lines above."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    _write(inventory_path, json.dumps([]))
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("JSON SCHEMA VIOLATION" in e for e in errors), errors


def test_cli_does_not_crash_on_a_scalar_top_level_inventory(tmp_path, capsys):
    """Same case as above, driven through main()'s CLI entrypoint --
    EXECUTED repro: `raw_inventory.get("rows", [])` in main()'s
    DISCLOSURE-HOLD preamble crashed with the same AttributeError BEFORE
    check() was ever even called, so no DISCLOSURE-HOLD line and no
    JSON SCHEMA VIOLATION printed at all -- just a traceback."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, _, _ = _paths(root)
    _write(inventory_path, json.dumps([]))
    exit_code = checker.main(["--root", str(root)])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert "DISCLOSURE-HOLD" in captured.out
    assert "FAIL" in captured.err
    assert "JSON SCHEMA VIOLATION" in captured.err


def test_check_does_not_crash_on_a_malformed_top_level_credential_classes(tmp_path):
    """Merge-gate P2, EXECUTED repro: a top-level credential-classes.json
    that is a JSON array (not an object) raised
    `TypeError: list indices must be integers or slices, not str` on
    `credential_classes["classes"]` inside credential_class_vocabulary(),
    BEFORE the CREDENTIAL CLASS SCHEMA VIOLATION (top level must be an
    object) that check() computes a few lines later ever printed."""
    root = _minimal_valid_root(tmp_path)
    inventory_path, schema_path, cc_path = _paths(root)
    _write(cc_path, json.dumps([]))
    errors = checker.check(root, inventory_path, schema_path, cc_path)
    assert any("CREDENTIAL CLASS SCHEMA VIOLATION" in e for e in errors), errors


# ---------------------------------------------------------------------------
# The UNVERIFIED ROUTE tests and their unresolved-route-allowlist fixtures
# were REMOVED with CHAOS-4761, not weakened. They proved a fail-closed
# escape hatch for routes a static source walk could not resolve to a mount
# path. Enumerating app.routes has nothing to resolve -- a route the app
# serves is in that list however it was included -- so the failure mode and
# the allowlist that excused it no longer exist. The replacement coverage is
# test_a_dynamically_included_router_is_discovered (the case the allowlist
# existed for, now simply discovered) and
# test_gate_fails_loudly_when_the_app_cannot_be_imported (the failure that
# replaced it).
# ---------------------------------------------------------------------------

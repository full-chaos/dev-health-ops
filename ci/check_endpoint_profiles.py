#!/usr/bin/env python3
"""CHAOS-3273 L3 CI gate: enforce the ops endpoint-authentication-profile
inventory contract (guardrail G-1: a route without a registered profile
fails CI and may not ship).

Modelled on ``ci/check_transitional_inventory.py`` (CUT-01) -- same shape:
independent re-discovery (never trusts
``contracts/auth/v1/endpoint-profiles.ops.json`` itself for what surfaces
exist), a discovery/inventory cross-check, and staleness/content-drift
validation on every anchor. Discovery is delegated to
``ci/discover_ops_routes.py`` rather than re-implemented here.

CHAOS-4761 changed what "independent" means here, and it is the single most
important property of this gate. Discovery used to re-derive the surface set
by matching decorators and ``include_router`` calls in SOURCE TEXT. The
inventory was built from the same patterns, so the two shared a blind spot:
they agreed with each other while saying nothing about anything neither
looked at. Discovery now enumerates from the SERVED objects -- ``app.routes``
on each deployed FastAPI application and every resolver-bearing field on the
served ``strawberry.Schema`` -- so the set is what the frameworks will
dispatch, not what a pattern remembered to match. Three defects that survived
the old cross-check and cannot survive this one: three ``@strawberry.
subscription`` resolvers that no pattern matched; a router mounted twice,
collapsed to its first mount; and two ``@strawberry.field`` examples inside a
DOCSTRING, profiled as if they were live surfaces.

A row is matched to a surface by its DEPLOYED IDENTITY -- ``(service, method,
route)`` for REST, the resolver's python name for GraphQL -- not by its source
anchor. The anchor is then verified against where the served callable is
actually defined. Identity-by-anchor was what made a legitimate two-mount
pair look like a duplicate (CHAOS-4760).

Every check EXPRESSIBLE as a plain JSON Schema constraint (required fields,
field types, enums, ``additionalProperties``, nested ``$defs`` shapes like
``issuedCredential.direction``) runs as REAL Draft 2020-12 validation via
the ``jsonschema`` library against ``endpoint-profile.schema.json`` --
never hand-re-derived. An earlier version of this file hand-rolled a
top-level-only shape check plus a handful of per-field enum checks (two of
which -- ``issued_credential.direction``, ``exposure.reachability`` -- were
hardcoded Python sets instead of read from the schema); a row with
``primary_validator: 17`` (the wrong type entirely) passed silently because
nothing checked row-level types at all. Full-document validation closes
that whole class at once and can never miss a schema-declared rule this
file forgot to re-derive. What's left below is exactly what a JSON Schema
document cannot express: closed vocabularies keyed to a SEPARATE file,
anchors (need a filesystem read), cross-row uniqueness, the discovery
cross-check, and the "null must carry a gaps entry" business rule.

Fails (exit 1, with a human-readable report) when:
  1. UNOWNED SURFACE -- a REST route or GraphQL resolver the application
     actually serves has no row in the inventory.
  2. PHANTOM ROW -- a row names a surface the served application/schema does
     not expose: either it was removed, or it never existed.
  3. DUPLICATE ID -- two rows share the same ``id``; or DUPLICATE SURFACE
     OWNERSHIP -- two rows with DIFFERENT ids both claim the same served
     surface (worse than a missing row: both look registered, possibly with
     contradicting classifications). Keyed on the deployed identity, so the
     two correct rows for one router mounted on two apps are NOT a duplicate.
  4. CLOSED-VOCABULARY VIOLATION -- an ``accepted_credential_classes`` or
     ``issued_credential[].class_id`` entry absent from
     ``contracts/auth/v1/credential-classes.json`` (the one closed
     vocabulary a JSON Schema document cannot itself express, since it
     lives in a separate file); or a JSON SCHEMA VIOLATION when an
     enum-typed field (``surface_kind``, ``service``, ``classification``,
     ``tenant_requirement``, ``impersonation_policy``,
     ``issued_credential[].direction``, ``exposure.reachability``) uses a
     value outside the schema's own enum -- read live by the real
     validator, so a schema-level vocabulary addition (e.g.
     ``server_action``) is accepted without a checker code change.
  5. STALE ANCHOR -- a matched row's ``source`` file:line is not where the
     served endpoint/resolver is defined, or (GraphQL) its ``surface_kind``
     disagrees with what the served schema says the field is; or
     EXTERNAL SURFACE WITHOUT PROVENANCE -- a row owning a surface whose
     handler comes from a third-party package (fastapi's ``/docs`` and
     ``/openapi.json``, strawberry's GraphQL router, the prometheus
     instrumentator's ``/metrics``) does not name that package in ``gaps``,
     so it reads as describing code in this repository when it does not;
     or TRIVIAL ANCHOR -- a ``primary_validator``/``reachable_validators``/
     ``issued_credential`` anchor whose line is an obviously-trivial
     placeholder (``return {}``, a bare ``}``, ...) rather than real
     validator/mint-site code. Existence and line-bounds are the floor, not
     the check: this defeats the concrete failure of pointing an anchor at
     unrelated dead code, though it is not a claim that a non-trivial line
     IS the correct site -- same documented-limitation posture as
     ``verified_by`` below.
  6. JSON SCHEMA VIOLATION -- the inventory document does not validate
     against ``endpoint-profile.schema.json`` under Draft 2020-12 (top-level
     shape, every row's required fields and field types/enums, including
     nested ``$defs``). Real structural validation, not a hand-rolled
     re-derivation of the schema's own rules.
  7. UNSTATED NULL -- a field whose null value the schema explicitly ties to
     a required ``gaps`` entry (the ``anchor`` $def, used by
     ``primary_validator.anchor``/``reachable_validators[].anchor``/
     ``issued_credential[].anchor``; the top-level ``issued_credential``
     array itself when JSON ``null`` rather than absent; and the new
     ``exposure`` object when JSON ``null`` or when present with
     ``reachability: "unknown"``) has no row ``gaps`` entry explaining it.

     Deliberately NOT enforced here: a null value in one of the plain
     free-text advisory fields (``action``, ``resource_resolver``,
     ``current_state_cache_behavior``, ``entitlement_requirement``,
     ``disclosure_behavior``, ``token_shape``). Their schema descriptions
     say "null + gaps note" as contributor guidance, but that is not a hard
     MUST the way the ``anchor``/``issued_credential``/``exposure`` null
     rules are (those are the only places the schema text uses the word
     "MUST"). The real, checked-in inventory has hundreds of rows where
     these advisory fields are null with no gaps entry because null there
     legitimately means "not applicable" (e.g. a public health-check route
     has no ``action``) rather than "undetermined" -- there is no way for
     this gate to tell those two meanings apart from the JSON alone without
     guessing, and guessing is the exact failure mode this inventory exists
     to prevent. Scoping to the schema's own explicit MUST spots is the
     verifiable half of that rule.
  8. SERVICE MISMATCH -- a row's ``service`` does not match the deployed
     app discovery attributes the surface to (REST: discovery's own
     ``app_root``, mapped via ``_APP_ROOT_SERVICE``; GraphQL: the single app
     that mounts the schema, ``_GRAPHQL_SERVICE``); or UNKNOWN APP ROOT --
     discovery resolved a REST route to an ``app_root`` this file has no
     service mapping for (a newly deployed app not yet added to
     ``_APP_ROOT_SERVICE``). ``service`` is per DEPLOYED APP, not per path
     -- the same path can be served by two apps with genuinely different
     middleware stacks (see the billing-edge rows), so a row attributed to
     the wrong app silently invalidates its own reasoning. This was
     previously never cross-checked at all: only the closed-vocabulary
     enum was validated (see 6 above), so relabelling a row to any OTHER
     valid ``service`` enum value passed silently.
  9. CREDENTIAL CLASS SCHEMA VIOLATION -- ``credential-classes.json`` does
     not validate against ``credential-classes.schema.json`` under Draft
     2020-12, the same way the inventory is validated against its own
     schema (see 6). Previously this file only ever read
     ``credential-classes.json`` to extract ``class_id``s for the closed
     vocabulary (see 4) -- the vocabulary was closed, but the CONTENTS of
     each class (issuer, validators, backing_store, lifecycle, ...) were
     never validated, so an under-specified class could be added and this
     gate would bless it.
  10. DUPLICATE class_id -- two entries in ``credential-classes.json``'s
      ``classes`` array share the same ``class_id`` with possibly
      CONFLICTING definitions. Draft 2020-12 JSON Schema has no constraint
      for "these array objects must have distinct <field>" (only
      ``uniqueItems``, which compares whole-object equality, not one key),
      so this file checks it directly, before
      ``credential_class_vocabulary()`` collapses the array into a
      ``set[str]`` of ids -- a collapse that silently absorbs a duplicate
      id and keeps whichever definition Python's set construction happened
      to keep. Merge-gate-verified gap: duplicating a real class (same
      ``class_id``, different ``display_name``) previously passed with
      ``errors == []``.
  11. DISCOVERY UNAVAILABLE -- the deployed apps or the served GraphQL
      schema could not be imported, or the format checker backing
      ``"format": "date-time"`` is not installed. Both raise rather than
      returning an error list, and ``main()`` turns the raise into a non-zero
      exit with a message naming the fix. Neither degrades to a partial or
      unchecked run: a discovery that cannot see the application would make
      every cross-check above pass while checking nothing, which is the exact
      defect class this gate exists to close.

      This REPLACES the former ``UNVERIFIED ROUTE`` failure and its
      ``unresolved-route-allowlist.json`` escape hatch, both removed with
      CHAOS-4761. They existed because a static walk could not follow a
      dynamic ``include_router`` and had to fail closed on the routes it
      could not resolve. Enumerating ``app.routes`` has nothing to resolve:
      a route the app serves is in that list however it got there, so the
      set is complete by construction and there is nothing left for a human
      to vouch for.

  12. STALE SOURCE COMMIT -- the inventory's ``source_commit`` is not a
      40-hex sha, names a commit absent from this repository, or names one
      this HEAD does not descend from. An inventory anchored off this history
      describes a tree nobody will read it against (acr's
      ``ci/ops-contract.pin`` held a commit that was never going to land, and
      nothing said so). In a SHALLOW clone the object is simply absent and the
      stale case cannot be told from the truncated one: that is reported as
      ``SOURCE COMMIT UNVERIFIED`` next to the verdict, never failed on --
      GitHub's default checkout is depth 1, so failing there would be a false
      red on every run. See ``check_source_commit``.

``issued_credential`` is deliberately kept three/four-valued, never
collapsed: non-empty array (mints these -- every class_id validated, every
anchor validated/drift-checked), ``[]`` (assessed, mints nothing -- valid,
no further check), ``null`` (undetermined -- MUST have a gaps entry), absent
(pass predates the field -- valid, not checked at all). Same four states for
the new ``exposure`` field (object / null / absent), with reachability
additionally gated: ``unknown`` MUST have a gaps entry so it stays an
honest declaration rather than a silent default.

``verified_by`` inside ``issued_credential`` entries is a repo-qualified
cross-repo citation (e.g. ``acr:internal/auth/web_assertion.go:90``) and is
deliberately NOT resolved as a local path here -- only its shape (a
non-empty string) is checked.

What this gate does NOT guarantee (read before trusting a green run for
anything this list doesn't cover):
  * The surface set is the set THIS PROCESS'S environment produces. Discovery
    imports the apps; a router mounted only under some runtime configuration
    is enumerated only when that configuration is present. On this tree every
    ``include_router`` reachable from either app root is unconditional and the
    ``/metrics`` mount is backed by a hard dependency -- a property of the
    tree, not a guarantee of the mechanism. A route whose presence is
    configuration-dependent must say so in its row's ``gaps``.
  * A router that is never mounted on either app root is not reported. It
    serves no request, so it is not an auth surface -- but neither is it
    audited here, and dead code that a later commit mounts arrives as a new
    UNOWNED SURFACE rather than as a row already thought through.
  * A GraphQL row is keyed on the resolver's python name alone; the inventory
    has no field for the resolver's parent type. A name that resolves on two
    types is reported (AMBIGUOUS RESOLVER NAME) rather than absorbed, but the
    schema would need a new field to profile both.
  * ``exposure`` remains an ASSERTED boundary. No repo in this gate's read set
    contains the edge path-map, so nothing here can verify whether a surface
    the app mounts is actually reachable from outside. ``reachability:
    "unknown"`` plus a gaps entry is the honest state, and the gate enforces
    that it is stated rather than defaulted.
  * A row's reasoning fields (``classification``, ``accepted_credential_
    classes``, ``primary_validator`` and the rest) are checked for
    consistency, vocabulary and anchor validity -- never for being RIGHT
    about what the code does. The set is now verified; the judgement in each
    row is still human work.
This file makes no claim of complete or exhaustive coverage anywhere else
either: every check above is scoped to what it can independently verify
from the served objects and the schemas, nothing more.

Usage:
    python3 ci/check_endpoint_profiles.py [--root PATH] [--inventory PATH]
        [--schema PATH] [--credential-classes PATH]
        [--credential-classes-schema PATH]
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path
from types import ModuleType

# The schema declares Draft 2020-12 ("$schema":
# "https://json-schema.org/draft/2020-12/schema"); `jsonschema` (PyPI)
# supports it via Draft202012Validator. Declared in pyproject.toml
# ("jsonschema>=4.23.0"), so CI (which installs from pyproject.toml) always
# has it -- but a LOCAL interpreter that predates the dependency (or wasn't
# re-synced) will not. Guarded rather than a bare top-level `import
# jsonschema`, which would die with a raw ModuleNotFoundError traceback
# instead of a message naming the fix. Never made optional: coordinator
# ruling (2026-09-01) -- a `try/except ImportError: skip validation` gate
# would report success while validating nothing, the exact defect this file
# exists to fix. See check()'s use of _JSONSCHEMA_IMPORT_ERROR: the gate
# FAILS LOUDLY when the validator is unavailable, it never silently passes.
# The two names are annotated as Optional up front: without that, mypy infers
# each from its first binding (Module / ImportError) and then rejects the None
# assignment in the other branch. Annotating is the honest fix -- widening the
# declared type to what the code actually produces -- rather than silencing it.
_JSONSCHEMA_IMPORT_ERROR: ImportError | None
jsonschema: ModuleType | None
try:
    import jsonschema
except ImportError as _exc:  # pragma: no cover -- environment-dependent
    jsonschema = None
    _JSONSCHEMA_IMPORT_ERROR = _exc
else:
    _JSONSCHEMA_IMPORT_ERROR = None

DEFAULT_INVENTORY = "contracts/auth/v1/endpoint-profiles.ops.json"
DEFAULT_SCHEMA = "contracts/auth/v1/endpoint-profile.schema.json"
DEFAULT_CREDENTIAL_CLASSES = "contracts/auth/v1/credential-classes.json"
DEFAULT_CREDENTIAL_CLASSES_SCHEMA = "contracts/auth/v1/credential-classes.schema.json"
DEFAULT_DISCOVERER = "ci/discover_ops_routes.py"

# Deployed FastAPI() app roots discover_ops_routes.py resolves ops REST
# routes to (its `app_root` field, a "module::varname" router-def key --
# see discover_ops_routes.py:558 / _app_root_for), mapped to the `service`
# enum value each one corresponds to (docs/reference/auth/endpoint-profiles.md
# "Two deployed apps"; the schema's own `service` description). `service` is
# per DEPLOYED APP, not per path -- reachable_validators is `[]` for
# billing-edge rows precisely because that app shares no middleware with the
# main app, so a row attributed to the wrong app silently invalidates its
# own reasoning. Never previously checked at all: Codex relabelled
# `GET /api/v1/meta` from `dev-health-ops-api` to the ALSO-VALID enum value
# `dev-health-web` (schema validation alone can't catch this -- dev-health-web
# is a real vocabulary member, just not one this discoverer's app_root ever
# resolves to) and the gate returned zero errors. Adding a third deployed
# ops app means adding it here first -- an app_root this dict doesn't know
# is a hard failure below (UNKNOWN APP ROOT), never a silent pass.
_APP_ROOT_SERVICE: dict[str, str] = {
    "dev_health_ops.api.main::app": "dev-health-ops-api",
    "dev_health_ops.api.billing_edge::app": "dev-health-ops-billing-edge",
}

# The GraphQL schema is mounted from exactly one deployed app -- main.py's
# `app.include_router(graphql_app, prefix="/graphql")`; billing_edge.py
# never imports or mounts it (verified: `rg "include_router\(graphql_app"
# src/dev_health_ops/api` -- one hit, main.py). discover_ops_routes.py has
# no app_root concept for GraphQL resolvers at all (they are bare decorated
# functions, never reached via an include_router edge, so there is no mount
# graph to walk the way REST routes' app_root is derived) -- a constant
# rather than a discovery-derived value. Documented limitation, same
# posture as this file's other known-narrow checks (anchor denylist,
# verified_by): true as long as ops has exactly one GraphQL-mounting app,
# which is the whole of ops's current architecture.
_GRAPHQL_SERVICE = "dev-health-ops-api"

# Required-field / top-level-shape / enum vocabulary rules formerly lived
# here as hand-rolled constants (TOP_LEVEL_REQUIRED, ROW_REQUIRED). They are
# now enforced by real Draft 2020-12 validation against the schema itself
# (see check()) -- keeping a parallel hand-rolled list invites exactly the
# drift Codex found: a schema-declared rule (e.g. primary_validator must be
# object/null) that this file never re-derived.


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_json(path: Path) -> dict:
    with path.open() as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Live-read closed vocabularies -- pulled from the schema/credential-classes
# files themselves at check time, never hardcoded, so a schema-level
# vocabulary addition (e.g. surface_kind gaining "server_action") is accepted
# without a checker code change.
# ---------------------------------------------------------------------------


def _schema_enum(schema: dict, prop: str) -> set[str] | None:
    props = schema["$defs"]["endpointProfile"]["properties"]
    node = props.get(prop, {})
    enum = node.get("enum")
    return set(enum) if enum is not None else None


def credential_class_vocabulary(credential_classes: dict) -> set[str]:
    # Guarded rather than a bare `credential_classes["classes"]`: a
    # malformed top-level credential-classes document (e.g. the whole file
    # is a JSON array or a scalar, not an object) previously raised a raw
    # TypeError here -- BEFORE the CREDENTIAL CLASS SCHEMA VIOLATION that
    # check() computes a few lines later ever printed. Merge-gate-verified
    # repro: a top-level `[]` document raised `TypeError: list indices must
    # be integers or slices, not str`. Falling back to an empty vocabulary
    # lets the real Draft 2020-12 validator (which handles any JSON shape)
    # report the actual, located error instead of a traceback -- this
    # function's job is only "what ids exist", not "is this well-formed".
    if not isinstance(credential_classes, dict):
        return set()
    classes = credential_classes.get("classes")
    if not isinstance(classes, list):
        return set()
    return {
        c["class_id"]
        for c in classes
        if isinstance(c, dict) and isinstance(c.get("class_id"), str)
    }


def _check_duplicate_class_ids(credential_classes: dict) -> list[str]:
    """JSON Schema (even Draft 2020-12) has no constraint for "these array
    objects must have distinct <field>" -- `uniqueItems` compares whole-item
    equality, not one key -- so `credential-classes.schema.json` cannot
    itself reject two classes sharing a `class_id`. Worse,
    `credential_class_vocabulary()` above collapses the `classes` array into
    a `set[str]` of ids, which silently absorbs a duplicate and keeps
    whichever definition Python's set construction happens to retain --
    the closed-vocabulary check (4) and CREDENTIAL CLASS SCHEMA VIOLATION
    check (9) both operate downstream of that collapse and never see the
    conflict. Must run on the raw `classes` array, before any collapse.
    Merge-gate-verified repro: duplicating a real class (same class_id,
    different display_name) previously passed with errors == []."""
    errors: list[str] = []
    if not isinstance(credential_classes, dict):
        return errors  # reported by the credential-classes schema validation
    classes = credential_classes.get("classes")
    if not isinstance(classes, list):
        return errors  # reported by the credential-classes schema validation
    seen: dict[str, int] = {}
    for idx, c in enumerate(classes):
        if not isinstance(c, dict):
            continue  # reported by the credential-classes schema validation
        cid = c.get("class_id")
        if not isinstance(cid, str):
            continue  # reported by the credential-classes schema validation
        if cid in seen:
            errors.append(
                f"DUPLICATE class_id: {cid!r} is used by more than one class "
                f"in credential-classes.json (first seen at classes[{seen[cid]}], "
                f"again at classes[{idx}]) -- JSON Schema cannot express "
                "cross-array id uniqueness, so two conflicting definitions for "
                "the same class_id would otherwise collapse into a single "
                "vocabulary entry and pass silently"
            )
        else:
            seen[cid] = idx
    return errors


# ---------------------------------------------------------------------------
# Discovery cross-check
# ---------------------------------------------------------------------------


_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


def _git(root: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def check_source_commit(root: Path, inventory: dict) -> tuple[list[str], str | None]:
    """``source_commit`` must name a commit this repository's HEAD descends from.

    The inventory declares the tree its rows were derived against. Nothing
    stopped that from being a commit that does not exist here -- acr's
    ``ci/ops-contract.pin`` held ``e57ca829f0ec...``, a commit that was never
    going to land on ``origin/main``, and nothing said so. An inventory
    anchored to a commit outside this history is decayed by construction: the
    tree it describes is not the tree anyone will read it against.

    Three outcomes, all explicit, none of them a silent pass:

    * the commit exists and HEAD descends from it -- OK;
    * the commit exists and HEAD does NOT descend from it, or it is not a
      40-hex sha at all -- ``STALE SOURCE COMMIT``, a hard failure;
    * the object is absent because this is a SHALLOW clone -- returned as the
      second element, a NOTE the caller prints, never an error. GitHub's default checkout is depth 1, so failing here would
      be a false red on every CI run rather than a finding. This is a real
      narrowing and it is the one hole in this check: in a shallow clone the
      stale case is indistinguishable from the truncated case. Everywhere the
      history exists -- local runs, and any job checked out with
      ``fetch-depth: 0`` -- it is a hard check.

    The set-level guarantee does not depend on any of this: the gate re-derives
    every surface from the tree it is run against, so a green run is a
    statement about THAT tree whatever ``source_commit`` says. What this adds
    is that the provenance line cannot quietly lie.

    Returns ``(errors, note)``. The two are separate return values rather than
    one list precisely so an unverifiable case can never be mistaken for a
    verified one by a caller that only counts errors.
    """
    errors: list[str] = []
    commit = inventory.get("source_commit") if isinstance(inventory, dict) else None
    if not isinstance(commit, str) or not _SHA_RE.match(commit):
        errors.append(
            f"STALE SOURCE COMMIT: source_commit={commit!r} is not a 40-character "
            "hex commit sha. It must name the exact ops commit these rows were "
            "derived against (a placeholder or short sha cannot be verified)."
        )
        return errors, None

    if _git(root, "rev-parse", "--git-dir").returncode != 0:
        # Not a git repository at all -- a fixture tree, or an exported
        # tarball. Nothing to check against; say so rather than implying a
        # verification happened.
        return [], (
            "SOURCE COMMIT UNVERIFIED: not a git repository, ancestry not checked"
        )

    if _git(root, "cat-file", "-e", f"{commit}^{{commit}}").returncode != 0:
        shallow = _git(root, "rev-parse", "--is-shallow-repository").stdout.strip()
        if shallow == "true":
            return [], (
                "SOURCE COMMIT UNVERIFIED: shallow clone, "
                f"{commit[:12]} is not present, ancestry not checked"
            )
        errors.append(
            f"STALE SOURCE COMMIT: {commit[:12]} is not a commit in this "
            "repository. The inventory claims to have been derived against a "
            "tree that does not exist here."
        )
        return errors, None

    if _git(root, "merge-base", "--is-ancestor", commit, "HEAD").returncode != 0:
        errors.append(
            f"STALE SOURCE COMMIT: HEAD does not descend from {commit[:12]}. The "
            "inventory is anchored to a commit off this history (an abandoned or "
            "rebased branch) -- re-derive the rows and re-stamp source_commit."
        )
    return errors, None


def _live_surface_map(discovered: dict) -> tuple[dict[tuple, dict], list[str]]:
    """Every surface the deployed apps and the served GraphQL schema actually
    serve, keyed by its DEPLOYED IDENTITY, plus any errors raised while
    building the map.

    The key was ``(file, line)`` until CHAOS-4761. That was the defect behind
    CHAOS-4760, not merely adjacent to it: one router mounted at two prefixes
    is TWO served surfaces with two different paths and possibly two different
    middleware stacks, but only ONE ``(file, line)``, so the two correct rows
    describing them collided under the duplicate-surface rule while a row
    covering only one of them looked complete. Keying on what the framework
    dispatches -- ``(service, method, path)`` for REST, the resolver's python
    name for GraphQL -- makes mount multiplicity fall out of the object graph.
    The anchor is still checked; it is an ATTRIBUTE of the surface now rather
    than its identity.
    """
    errors: list[str] = []
    out: dict[tuple, dict] = {}

    for r in discovered["routes"]:
        app_root = r.get("app_root")
        service = _APP_ROOT_SERVICE.get(app_root) if isinstance(app_root, str) else None
        if service is None:
            errors.append(
                f"UNKNOWN APP ROOT: {r.get('method')} {r.get('path')} is served by "
                f"app_root {app_root!r}, which is not in _APP_ROOT_SERVICE -- a "
                "newly deployed FastAPI() app must be added there (and to "
                "DEPLOYED_APPS in ci/discover_ops_routes.py) before this gate can "
                "attribute rows to it"
            )
            continue
        key: tuple = ("rest", service, r["method"], r["path"])
        if key in out:
            errors.append(
                f"AMBIGUOUS LIVE SURFACE: {service} serves {r['method']} "
                f"{r['path']} from more than one route object -- discovery "
                "cannot attribute a row to one of them"
            )
            continue
        out[key] = {"_surface_type": "rest", "service": service, **r}

    for r in discovered["graphql"]:
        python_name = r.get("python_name")
        if not python_name:
            errors.append(
                f"UNNAMED RESOLVER: {r.get('parent_type')}.{r.get('name')} has no "
                "python_name -- the inventory keys GraphQL rows on it"
            )
            continue
        key = ("graphql", python_name)
        if key in out:
            # The inventory has no field for the resolver's parent type, so a
            # python_name collision between (say) a root field and a nested
            # one would silently let one row stand for two surfaces. Reported
            # rather than absorbed; the fix would be a schema field for the
            # parent type, which this tree does not need yet.
            errors.append(
                f"AMBIGUOUS RESOLVER NAME: {python_name!r} resolves on more than "
                f"one type ({out[key].get('parent_type')} and {r.get('parent_type')}) "
                "-- inventory rows key on python_name alone and cannot tell them "
                "apart"
            )
            continue
        out[key] = {"_surface_type": "graphql", **r}

    return out, errors


def _row_surface_key(row: dict) -> tuple | None:
    """The deployed identity a row claims, in the same shape
    ``_live_surface_map`` keys on. ``None`` when the row does not carry
    enough to name a surface (already reported by schema validation)."""
    kind = row.get("surface_kind")
    if kind == "rest":
        service, method, route = row.get("service"), row.get("method"), row.get("route")
        if not (
            isinstance(service, str)
            and isinstance(method, str)
            and isinstance(route, str)
        ):
            return None
        return ("rest", service, method, route)
    if kind in ("graphql_field", "graphql_mutation", "graphql_subscription"):
        name = row.get("graphql_field_name")
        if not isinstance(name, str):
            return None
        return ("graphql", name)
    return None


def _describe_key(key: tuple) -> str:
    if key[0] == "rest":
        return f"REST {key[2]} {key[3]} [{key[1]}]"
    return f"GraphQL resolver {key[1]!r}"


def check(
    root: Path,
    inventory_path: Path,
    schema_path: Path,
    credential_classes_path: Path,
    credential_classes_schema_path: Path | None = None,
) -> list[str]:
    if jsonschema is None:
        raise RuntimeError(
            "the 'jsonschema' package is required (declared in "
            "pyproject.toml as 'jsonschema>=4.23.0') but is not importable "
            "in this interpreter -- refusing to run with validation "
            "silently skipped, which would report success while checking "
            "nothing. Install it (uv sync in an environment where that "
            "works, or `pip install jsonschema` for a quick local check) "
            f"and retry. Original import error: {_JSONSCHEMA_IMPORT_ERROR}"
        )

    # A `format_checker` is not enough on its own. `jsonschema` only checks
    # "date-time" when a backing implementation is importable; without
    # `rfc3339-validator` it registers no checker for that format and
    # `generated_at: "not-a-date"` validates cleanly -- the fix would look
    # applied while changing nothing, which is the exact defect class this
    # gate exists to close. Declared in pyproject.toml as `rfc3339-validator`;
    # verified here rather than assumed, and fatal rather than degraded.
    if "date-time" not in jsonschema.Draft202012Validator.FORMAT_CHECKER.checkers:
        raise RuntimeError(
            "jsonschema has no 'date-time' format checker registered, so "
            '`"format": "date-time"` in the schemas would validate ANY '
            "string -- refusing to run with a format check that silently "
            "checks nothing. Install the backing implementation "
            "('rfc3339-validator', declared in pyproject.toml) and retry."
        )

    errors: list[str] = []

    inventory = load_json(inventory_path)
    schema = load_json(schema_path)
    credential_classes = load_json(credential_classes_path)

    # Provenance: the commit these rows claim to have been derived against
    # must be one this HEAD descends from. The note (shallow clone, or not a
    # git repo) is deliberately NOT folded into `errors` -- see
    # check_source_commit's docstring -- and is printed by main().
    source_commit_errors, _source_commit_note = check_source_commit(root, inventory)
    errors.extend(source_commit_errors)

    class_vocab = credential_class_vocabulary(credential_classes)
    errors.extend(_check_duplicate_class_ids(credential_classes))

    if credential_classes_schema_path is None:
        credential_classes_schema_path = credential_classes_path.with_name(
            "credential-classes.schema.json"
        )
    credential_classes_schema = load_json(credential_classes_schema_path)

    # Discovery IMPORTS the deployed apps and the served GraphQL schema
    # (CHAOS-4761). An import failure is fatal here for the same reason a
    # missing `jsonschema` is: the alternative is an empty or partial surface
    # set, which makes every cross-check below pass while checking nothing.
    # Never caught and downgraded to a warning.
    discoverer = _load_module(root / DEFAULT_DISCOVERER, "discover_ops_routes")
    discovered = discoverer.discover(root)
    surface_map, surface_map_errors = _live_surface_map(discovered)
    errors.extend(surface_map_errors)
    discovered_keys = set(surface_map)

    # --- 6. FULL Draft 2020-12 JSON Schema validation ------------------
    # Real structural validation over the WHOLE document (top-level shape,
    # every row's required fields, every field's declared type/enum,
    # including nested $defs like issuedCredential.direction and
    # exposure.reachability) -- not a hand-rolled re-derivation of the
    # schema's own rules, which drifts the moment the schema gains a field
    # this file doesn't know to re-check (Codex-verified gap: a row with
    # primary_validator: 17 -- the wrong type entirely -- previously passed
    # silently; and issued_credential.direction/exposure.reachability were
    # hardcoded Python sets here instead of being read from the schema like
    # every other enum, so a legitimate future schema addition to either
    # would have been rejected as UNKNOWN). Errors are sorted by JSON path
    # for stable, diffable output.
    validator = jsonschema.Draft202012Validator(
        schema, format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER
    )
    for err in sorted(
        validator.iter_errors(inventory), key=lambda e: list(map(str, e.path))
    ):
        loc = "/".join(str(p) for p in err.path) or "<root>"
        errors.append(f"JSON SCHEMA VIOLATION at {loc}: {err.message}")

    # --- credential-classes.json validated against ITS OWN schema --------
    # Previously this file only ever read credential-classes.json to pull
    # out class_ids (credential_class_vocabulary() above) -- the vocabulary
    # was closed, but nothing validated that each class in it actually has
    # the shape credential-classes.schema.json requires (issuer, validators,
    # backing_store, lifecycle, ... -- see that schema's `required` list).
    # Codex-verified gap: reducing every class down to just `class_id` still
    # returned zero errors, so a new class could be added under-specified
    # and the gate would bless it -- defeating the "every class has an
    # issuer, validator, lifecycle authority and allowed route set"
    # guarantee this vocabulary exists to enforce. Validated the same way
    # the inventory is validated above: real Draft 2020-12 structural
    # validation, not a hand-rolled re-derivation of the schema's rules.
    cc_validator = jsonschema.Draft202012Validator(
        credential_classes_schema,
        format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER,
    )
    for err in sorted(
        cc_validator.iter_errors(credential_classes),
        key=lambda e: list(map(str, e.path)),
    ):
        loc = "/".join(str(p) for p in err.path) or "<root>"
        errors.append(f"CREDENTIAL CLASS SCHEMA VIOLATION at {loc}: {err.message}")

    # Guarded rather than a bare `inventory.get(...)`: a scalar/list
    # top-level inventory document (not an object at all) previously raised
    # a raw AttributeError here -- AFTER the JSON SCHEMA VIOLATION above was
    # already appended to `errors`, but that list was then lost to the
    # traceback instead of ever being returned/printed. Merge-gate-verified
    # repro: a top-level `[]` document raised `AttributeError: 'list'
    # object has no attribute 'get'` before main() ever got to print
    # anything but the (also crashing, see main()'s own guard)
    # DISCLOSURE-HOLD line.
    rows = inventory.get("rows", []) if isinstance(inventory, dict) else []
    if not isinstance(rows, list):
        return errors

    # --- per-row semantic checks the schema CANNOT express --------------
    # (closed vocabularies keyed to a separate file; anchors, which need a
    # filesystem read; cross-row uniqueness; discovery cross-checks; the
    # "null must carry a gaps entry" business rule). Every check a plain
    # JSON Schema document CAN express (required fields, types, enums,
    # additionalProperties) is handled by the real validator above, not
    # re-derived here -- see this function's own history for the bug class
    # that produces.
    ids_seen: dict[str, str] = {}
    row_keys: set[tuple] = set()
    # Every DEPLOYED IDENTITY claimed by a row, mapped to every row id that
    # claims it -- a served surface must be owned by EXACTLY one row. Two
    # distinct ids on the same surface (Codex-verified gap: possible with
    # conflicting classifications, e.g. one row says public/no-creds and
    # another says protected for the identical route) is worse than a missing
    # row: both look registered, so neither reviewer nor gate has a reason to
    # look closer. Not expressible as a JSON Schema constraint -- it's a
    # cross-row uniqueness rule.
    #
    # Keyed on (service, method, path) / resolver name since CHAOS-4761, NOT
    # on the source anchor: two rows legitimately share one `file:line` when
    # one router is mounted on two apps, and rejecting that pair was the false
    # positive half of CHAOS-4760.
    surface_owners: dict[tuple, list[str]] = {}
    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            # Already reported by the full schema validation above; nothing
            # further can be checked on a non-object row.
            continue
        rid = row.get("id", f"<row {idx}, no id>")

        # 3. duplicate id
        if "id" in row:
            if row["id"] in ids_seen:
                errors.append(
                    f"DUPLICATE ID: {row['id']!r} used by more than one row "
                    f"(first seen at index {ids_seen[row['id']]}, again at {idx})"
                )
            else:
                ids_seen[row["id"]] = str(idx)

        # public/protected pairing
        if row.get("classification") == "public" and not row.get("public_rationale"):
            errors.append(
                f"MISSING public_rationale: row {rid!r} is classification=public "
                "but public_rationale is null/empty"
            )
        if row.get("classification") == "protected":
            if not row.get("accepted_credential_classes"):
                errors.append(
                    f"EMPTY accepted_credential_classes: row {rid!r} is "
                    "classification=protected but lists no accepted credential class"
                )

        # accepted_credential_classes closed vocabulary
        for cls in row.get("accepted_credential_classes") or []:
            if cls not in class_vocab:
                errors.append(
                    f"UNKNOWN accepted_credential_class: row {rid!r} claims "
                    f"{cls!r}, not in {credential_classes_path.name}'s closed vocabulary"
                )

        # --- issued_credential: four-valued, never collapsed -----------
        if "issued_credential" in row:
            ic = row["issued_credential"]
            if ic is None:
                if not _gaps_mentions(row, "issued_credential"):
                    errors.append(
                        f"UNSTATED NULL: row {rid!r} has issued_credential=null "
                        "(undetermined) with no gaps entry explaining it"
                    )
            elif isinstance(ic, list):
                for entry_idx, entry in enumerate(ic):
                    if not isinstance(entry, dict):
                        continue  # reported by the full schema validation above
                    class_id = entry.get("class_id")
                    if class_id not in class_vocab:
                        errors.append(
                            f"UNKNOWN issued_credential class_id: row {rid!r} "
                            f"entry {entry_idx} claims {class_id!r}, not in the "
                            "closed vocabulary"
                        )
                    anchor = entry.get("anchor")
                    if anchor is None:
                        if not _gaps_mentions(row, "issued_credential"):
                            errors.append(
                                f"UNSTATED NULL: row {rid!r} issued_credential "
                                f"entry {entry_idx} has anchor=null with no gaps "
                                "entry explaining it"
                            )
                    else:
                        _check_anchor_exists(
                            root, rid, anchor, errors, label="issued_credential anchor"
                        )
                        _check_issued_credential_anchor_identity(
                            root, rid, entry_idx, anchor, entry, errors
                        )
            # else: not a list -- reported by the full schema validation above.

        # --- exposure: absent / null / object, reachability=unknown gated
        if "exposure" in row:
            exposure = row["exposure"]
            if exposure is None:
                if not _gaps_mentions(row, "exposure"):
                    errors.append(
                        f"UNSTATED NULL: row {rid!r} has exposure=null (undetermined) "
                        "with no gaps entry explaining it"
                    )
            elif isinstance(exposure, dict):
                reachability = exposure.get("reachability")
                if not exposure.get("source"):
                    errors.append(
                        f"MISSING exposure.source: row {rid!r} has an exposure "
                        "claim with no source artifact cited"
                    )
                if reachability == "unknown" and not _gaps_mentions(row, "exposure"):
                    errors.append(
                        f"UNSTATED NULL: row {rid!r} has exposure.reachability="
                        "'unknown' with no gaps entry explaining it"
                    )
            # else: not an object -- reported by the full schema validation above.

        # --- primary_validator anchor null ------------------------------
        # NOTE: this is about primary_validator.anchor being null while
        # primary_validator itself is a present object (an unresolved
        # anchor on a validator the row DOES claim to have) -- distinct
        # from primary_validator itself being null, which is schema-legal
        # for a genuinely public route with no validator at all and is
        # handled by falling through the isinstance(pv, dict) guard below.
        # Codex-verified gap: this used to be scoped to classification ==
        # "protected" only, so a public row could set an anchor object with
        # anchor=null and no gaps entry and pass -- but the schema's anchor
        # $def rule ("null MUST be paired with a gaps entry") is
        # unconditional on classification.
        pv = row.get("primary_validator")
        if isinstance(pv, dict):
            anchor = pv.get("anchor")
            if anchor is None:
                if not _gaps_mentions(row, "primary_validator"):
                    errors.append(
                        f"UNSTATED NULL: row {rid!r} has primary_validator.anchor=null "
                        "with no gaps entry explaining it"
                    )
            else:
                _check_anchor_exists(
                    root, rid, anchor, errors, label="primary_validator anchor"
                )

        # --- reachable_validators anchors --------------------------------
        for rv_idx, rv in enumerate(row.get("reachable_validators") or []):
            anchor = rv.get("anchor")
            if anchor is None:
                if not _gaps_mentions(row, "reachable_validator"):
                    errors.append(
                        f"UNSTATED NULL: row {rid!r} reachable_validators[{rv_idx}] "
                        "has anchor=null and no gaps entry explaining it"
                    )
            else:
                _check_anchor_exists(
                    root,
                    rid,
                    anchor,
                    errors,
                    label=f"reachable_validators[{rv_idx}] anchor",
                )

        # --- the deployed identity this row claims ----------------------
        surface_key = _row_surface_key(row)
        if surface_key is not None:
            row_keys.add(surface_key)
            surface_owners.setdefault(surface_key, []).append(rid)

    # 3b. duplicate surface ownership: two DIFFERENT ids both claiming the
    # same served surface. Worse than a missing row -- both look registered,
    # possibly with contradicting classifications, and nothing else here would
    # notice (the plain duplicate-id check above only catches identical ids;
    # the parity cross-check below only notices a surface with ZERO owners,
    # not two).
    for key, owners in surface_owners.items():
        if len(owners) > 1:
            errors.append(
                f"DUPLICATE SURFACE OWNERSHIP: rows {sorted(owners)!r} all claim "
                f"{_describe_key(key)} -- exactly one row may own a served surface"
            )

    # --- 1 & 2. bidirectional surface/row parity ------------------------
    # Both directions, against the SERVED set: a surface the apps serve with
    # no row fails, and a row naming a surface the apps do not serve fails.
    # Neither direction can be satisfied by agreeing with a source-text
    # pattern, which is what made the old cross-check self-consistent while
    # proving nothing (CHAOS-4761).
    for key, surface in surface_map.items():
        if key not in row_keys:
            # For a third-party handler the definition site is inside a
            # virtualenv, which is noise in a CI log and unreviewable in a
            # diff -- name the providing module instead.
            in_ops = surface.get(
                "endpoint_in_ops_source", surface.get("resolver_in_ops_source")
            )
            if in_ops and surface.get("file"):
                anchor = f" ({surface['file']}:{surface['line']})"
            elif surface.get("endpoint_module"):
                anchor = f" (handler from {surface['endpoint_module']})"
            else:
                anchor = ""
            errors.append(
                f"UNOWNED SURFACE: {_describe_key(key)}{anchor} is served by the "
                f"application but has no row in {inventory_path.name}. Add an "
                "owning row (guardrail G-1)."
            )

    for row in rows:
        if not isinstance(row, dict):
            continue  # reported by the full schema validation above
        src = row.get("source")
        if not isinstance(src, dict) or "file" not in src or "line" not in src:
            continue
        rid = row.get("id", "<no id>")
        row_key = _row_surface_key(row)
        if row_key is None:
            if row.get("surface_kind") == "server_action":
                # Schema-valid (ruling: server_action IS a surface kind) but
                # not something ops serves -- ops is a Python FastAPI backend,
                # and Next.js Server Actions live in web, checked by web's own
                # gate. Reported rather than skipped: a row this gate cannot
                # key is a row it cannot verify, and silently passing it would
                # be a registered-looking row nothing checked.
                errors.append(
                    f"UNCHECKABLE ROW: {row.get('id', '<no id>')!r} has "
                    "surface_kind='server_action', which this gate cannot "
                    "attribute to any ops-served surface. Server Actions "
                    "belong in web's inventory and are checked by web's gate."
                )
            continue  # otherwise reported by the full schema validation above
        if row_key not in discovered_keys:
            # Before calling it a phantom: is this exact method+path served by
            # a DIFFERENT app? That is the more specific and more dangerous
            # defect -- `service` is per DEPLOYED APP, and the same path can be
            # served by two apps with genuinely different middleware stacks
            # (the billing-edge pair), so a row on the wrong app silently
            # invalidates its own reasoning while still describing a real
            # route. Codex found this by relabelling GET /api/v1/meta to the
            # also-valid enum value `dev-health-web`; schema validation cannot
            # catch it, because that IS a real vocabulary member.
            if row_key[0] == "rest":
                elsewhere = sorted(
                    k[1]
                    for k in discovered_keys
                    if k[0] == "rest" and k[2:] == row_key[2:]
                )
                if elsewhere:
                    errors.append(
                        f"SERVICE MISMATCH: row {rid!r} claims service={row_key[1]!r} "
                        f"for {row_key[2]} {row_key[3]}, but that route is served by "
                        f"{', '.join(repr(s) for s in elsewhere)} instead "
                        "(service is per DEPLOYED APP, not per path)"
                    )
                    continue
            errors.append(
                f"PHANTOM ROW: row {rid!r} claims {_describe_key(row_key)}, which the "
                "served application/schema does not expose (stale row -- "
                "re-anchor or remove). Two ways this happens: the surface was "
                "removed, or it never existed and the row was derived from "
                "something that only looked like one."
            )
            continue

        # --- 5. anchor drift: matched row vs the live surface -----------
        # `surface_kind`, `method`, `route` and `service` are the identity the
        # row was matched ON, so they agree by construction. What still needs
        # checking is the ANCHOR: the row's file:line must be where the served
        # endpoint/resolver actually is.
        surface = surface_map[row_key]
        live_file, live_line = surface.get("file"), surface.get("line")
        in_ops_source = surface.get(
            "endpoint_in_ops_source", surface.get("resolver_in_ops_source")
        )
        if in_ops_source:
            if (src["file"], src["line"]) != (live_file, live_line):
                errors.append(
                    f"STALE ANCHOR: row {rid!r} anchors {_describe_key(row_key)} at "
                    f"{src['file']}:{src['line']}, but the served "
                    f"endpoint/resolver is defined at {live_file}:{live_line} "
                    "(content drift -- re-anchor the row)"
                )
        else:
            # The endpoint is provided by a third-party package (fastapi's own
            # /docs and /openapi.json, strawberry's GraphQL router, the
            # prometheus instrumentator's /metrics), so there is no ops-source
            # definition to anchor to and the anchor cannot be verified by
            # equality. The row must instead anchor at an ops-source line that
            # EXISTS and must NAME the providing module in `gaps`, so a reader
            # can tell "we registered someone else's handler here" from "we
            # wrote this" -- rather than the row silently reading like the
            # latter.
            # _check_anchor_exists speaks the `anchor` $def's row_key name
            # ("path"); a row's `source` calls the same thing "file".
            _check_anchor_exists(
                root,
                rid,
                {"path": src["file"], "line": src["line"]},
                errors,
                label="source anchor",
            )
            module = (surface.get("endpoint_module") or "").split(".")[0]
            if module and not _gaps_mentions(row, module):
                errors.append(
                    f"EXTERNAL SURFACE WITHOUT PROVENANCE: row {rid!r} owns "
                    f"{_describe_key(row_key)}, whose handler is provided by "
                    f"{surface.get('endpoint_module')!r} and not by ops source. "
                    f"Its gaps must name {module!r} so the row is not read as "
                    "describing code in this repository."
                )
        # --- surface_kind must match what the object actually is --------
        # Not implied by the identity match: a GraphQL row is matched on its
        # resolver name alone, so a row could call a subscription a plain
        # field and still match. REST rows are matched on surface_kind
        # already (see _row_surface_key).
        if surface["_surface_type"] == "graphql":
            expected_kind = surface["kind"]
            if row.get("surface_kind") != expected_kind:
                errors.append(
                    f"STALE ANCHOR: row {rid!r} claims surface_kind="
                    f"{row.get('surface_kind')!r} but {surface['python_name']!r} "
                    f"is a {expected_kind} on the served schema (content drift)"
                )
            # The whole GraphQL schema is mounted from exactly one app (see
            # _GRAPHQL_SERVICE), so any GraphQL row not claiming that app is
            # a mismatch.
            if row.get("service") != _GRAPHQL_SERVICE:
                errors.append(
                    f"SERVICE MISMATCH: row {rid!r} claims service="
                    f"{row.get('service')!r} but GraphQL resolvers are only "
                    f"ever served by {_GRAPHQL_SERVICE!r} (the only ops app "
                    "that mounts the GraphQL schema)"
                )

    return errors


def _gaps_mentions(row: dict, needle: str) -> bool:
    gaps = row.get("gaps") or []
    needle_lower = needle.lower()
    return any(needle_lower in str(g).lower() for g in gaps)


# Lines an anchor pointing at a real validator/mint site should never
# collapse to -- an obviously-trivial or placeholder body, not the actual
# check/creation logic. This is a DENYLIST, not a positive "looks like real
# code" test: an earlier positive-signal design (require a function call or
# assignment on the line) was tried and rejected -- verified against the
# real inventory (361 rows at the time; 370 now), it produced 602 false
# positives, because this
# dataset's own anchoring convention is to point at a decorator/def
# DECLARATION line (e.g. "async def require_admin(", "@app.api_route(")
# rather than a line that itself performs work. A denylist of known-trivial
# shapes is there for, verified zero false positives against the real
# inventory both before and after this expansion.
#
# NOT a claim that a non-denylisted line IS the correct site (that would
# need real semantic understanding of "this specific line signs/validates a
# credential", which this checker does not attempt -- same
# documented-limitation posture as `verified_by`, validated for shape only,
# never resolved). Codex round 1 demonstrated `return {}`; round 2
# demonstrated `else:`, `...`, and `return []` still passing -- expanded
# below, but this remains fundamentally bypassable by any trivial shape not
# yet enumerated. A denylist can only ever close the shapes it knows about.
_TRIVIAL_ANCHOR_LINES = frozenset(
    {
        "{",
        "}",
        "pass",
        "return",
        "return {}",
        "return none",
        "return []",
        "return ()",
        "return nil",
        "return true",
        "return false",
        "return 0",
        "break",
        "continue",
        "else",
        "else:",
        "...",
        "default:",
        "fallthrough",
        "raise",
        ")",
        "),",
        "})",
        "}),",
    }
)


def _is_trivial_anchor_line(line: str) -> bool:
    stripped = line.strip().rstrip(";").lower()
    if stripped in _TRIVIAL_ANCHOR_LINES:
        return True
    return stripped.startswith("#") or stripped.startswith("//")


def _check_anchor_exists(
    root: Path, rid: str, anchor: dict, errors: list[str], *, label: str
) -> None:
    if not isinstance(anchor, dict):
        errors.append(
            f"SCHEMA VIOLATION: row {rid!r} {label} must be an object or null"
        )
        return
    path = root / anchor.get("path", "")
    line = anchor.get("line")
    if not anchor.get("path") or not path.exists():
        errors.append(
            f"STALE ANCHOR: row {rid!r} {label} references missing file {anchor.get('path')!r}"
        )
        return
    lines = path.read_text().splitlines()
    if not isinstance(line, int) or line < 1 or line > len(lines):
        errors.append(
            f"STALE ANCHOR: row {rid!r} {label} references {anchor.get('path')}:{line} "
            f"but the file only has {len(lines)} lines"
        )
        return
    if _is_trivial_anchor_line(lines[line - 1]):
        errors.append(
            f"TRIVIAL ANCHOR: row {rid!r} {label} references "
            f"{anchor.get('path')}:{line}, which is a placeholder/no-op line, "
            "not a real validator or mint site"
        )


_DEF_NAME_RE = re.compile(r"^\s*(?:async\s+def|def)\s+(\w+)")
_GO_FUNC_NAME_RE = re.compile(r"^\s*func\s+(?:\([^)]*\)\s*)?(\w+)\s*\(")


_FUNC_NAME_FORWARD_BUFFER = 3


def _func_name_near(lines: list[str], line: int, line_end: int | None) -> str | None:
    """The name of the function/method declared AT or within [line,
    line_end] (padded a few lines forward -- an anchor commonly points at a
    decorator, with the def itself 1-2 lines below), falling back to the
    nearest ENCLOSING declaration found by walking backward from line --
    the same "what does this specific line actually belong to" question the
    source anchor's content-drift check already answers for method+path,
    applied here to mint-site anchors."""
    end = max(line, line_end or line) + _FUNC_NAME_FORWARD_BUFFER
    end = min(end, len(lines))
    for i in range(line - 1, end):
        m = _DEF_NAME_RE.match(lines[i]) or _GO_FUNC_NAME_RE.match(lines[i])
        if m:
            return m.group(1)
    for i in range(line - 1, -1, -1):
        m = _DEF_NAME_RE.match(lines[i]) or _GO_FUNC_NAME_RE.match(lines[i])
        if m:
            return m.group(1)
    return None


def _mentions_word(text: str, name: str) -> bool:
    if not name:
        return False
    return re.search(r"\b" + re.escape(name) + r"\b", text, re.IGNORECASE) is not None


def _check_issued_credential_anchor_identity(
    root: Path, rid: str, entry_idx: int, anchor: dict, entry: dict, errors: list[str]
) -> None:
    """Cross-checks an issued_credential anchor's CONTENT against what the
    row itself claims (anchor.note, issuer), rather than only verifying the
    anchor exists and is in bounds. Coordinator ruling (2026-09-01): a
    denylist of trivial line shapes cannot establish an anchor is
    meaningful, only rule out the shapes someone thought of -- proven true
    by a real committed bug this same review found in acr's inventory (an
    anchor one line off the real call, at a bare closing bracket the
    denylist did not yet cover).

    Scoped to issued_credential specifically (not primary_validator/
    reachable_validators). Two DIFFERENT candidate designs were measured
    against the real ops inventory (1054 total primary_validator +
    reachable_validators anchors, 340 + 714) before choosing this one --
    both numbers below are reproducible by running _func_name_near and
    _mentions_word (this file's own shipped functions) against the
    checked-in inventory, exactly as Codex's round-3 review did
    independently and got the same figures:

    1. THIS design (name-match this function's own name against the row's
       free-text): reachable_validators misses 41.6% (297/714) -- those
       descriptions are written as BEHAVIORAL summaries of a whole
       middleware flow, not literal citations of every function name in
       the anchored range (e.g. "OrgIdMiddleware.__call__ decodes ... and
       resolves/validates X-Org-Id" genuinely describes what
       get_authenticated_user_from_headers does, without ever naming it).
       primary_validator is much cleaner at only 2.1% (7/340) -- those
       rows more often narrate a single named function directly. Extending
       this check to primary_validator alone might be viable on the
       numbers; kept out for consistency with reachable_validators rather
       than split the design per-field, and because a real Wave-0 frozen
       row failing on this basis is not this lane's data to rewrite.
    2. An EARLIER, cruder, and already-abandoned design (require the
       anchor LINE ITSELF -- not the enclosing function name -- to contain
       a call-with-args, assignment, or raise/throw/panic) was tried
       first and rejected before the name-match design was even written:
       602/1054 (57.1%) of the SAME anchors would have been wrongly
       rejected, because real anchors routinely point at a bare
       decorator/def DECLARATION line, which never itself "does work".
       This is a different design testing a different property than (1)
       above -- cited here only because an earlier draft of this comment
       conflated the two numbers as if they measured the same thing,
       which they do not.

    issued_credential notes, by contrast, consistently name the actual
    mint function -- a narrow "this is the mint site" citation, not a
    prose summary -- making a real, precision check possible here
    specifically. No ops row carries issued_credential yet (this pass
    predates any ops backfill), so this exercises identically on acr's
    real data (where the equivalent per-field measurement instead argues
    the check could safely extend further -- see acr's own
    checkIssuedCredentialAnchorIdentity comment) and is exercised for ops
    entirely via fixture tests, ready the moment a real ops row adds one.

    Reports (never silently passes) when no function/method name can be
    established at all, and when one is found but named nowhere in the
    row's own text -- "say so in the message rather than passing."
    """
    path_str = anchor.get("path")
    line = anchor.get("line")
    line_end = anchor.get("line_end")
    if not path_str or not isinstance(line, int) or line < 1:
        return  # reported elsewhere
    path = root / path_str
    if not path.exists():
        return  # reported elsewhere (_check_anchor_exists)
    lines = path.read_text().splitlines()
    if line > len(lines):
        return  # reported elsewhere
    name = _func_name_near(lines, line, line_end)
    note = (anchor.get("note") or "") + " " + (entry.get("issuer") or "")
    if name is None:
        errors.append(
            f"ANCHOR CONTENT UNVERIFIED: row {rid!r} issued_credential entry "
            f"{entry_idx} anchors {path_str}:{line}, but no function/method "
            "declaration could be found there or nearby -- cannot confirm "
            "this is the mint site"
        )
        return
    if not _mentions_word(note, name):
        errors.append(
            f"ANCHOR CONTENT MISMATCH: row {rid!r} issued_credential entry "
            f"{entry_idx} anchors {path_str}:{line} (function {name!r}), but "
            "neither anchor.note nor issuer names it -- re-anchor to the "
            "real mint site or update the note"
        )


# ---------------------------------------------------------------------------
# DISCLOSURE-HOLD reporting -- REPORT ONLY, never a check() error. Other
# lanes mark content that documents a currently-unfixed weakness with the
# literal string "DISCLOSURE-HOLD" (in a gaps entry, or elsewhere in a row's
# prose) so it can be found and withheld from a public push. A held row is a
# CORRECT row -- the marker records that publishing it is gated on a fix
# landing, not that the row is wrong. Failing on it would pressure someone
# into deleting the finding just to get green, the opposite of the intent,
# so this never touches `errors`; main() prints it as a separate, always-on
# report line regardless of pass/fail.
# ---------------------------------------------------------------------------

DISCLOSURE_HOLD_MARKER = "DISCLOSURE-HOLD"


def _row_contains_marker(value, marker: str) -> bool:
    if isinstance(value, str):
        return marker in value
    if isinstance(value, dict):
        return any(_row_contains_marker(v, marker) for v in value.values())
    if isinstance(value, list):
        return any(_row_contains_marker(v, marker) for v in value)
    return False


def find_disclosure_hold_rows(rows: list[dict]) -> list[str]:
    """Every row id whose content (recursively, any string field -- gaps
    entries and any other prose alike) contains the literal DISCLOSURE-HOLD
    marker, sorted for stable output."""
    return sorted(
        row.get("id", "<no id>")
        for row in rows
        if _row_contains_marker(row, DISCLOSURE_HOLD_MARKER)
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".", help="repository root")
    parser.add_argument(
        "--inventory", default=DEFAULT_INVENTORY, help="inventory JSON path"
    )
    parser.add_argument(
        "--schema", default=DEFAULT_SCHEMA, help="endpoint-profile schema JSON path"
    )
    parser.add_argument(
        "--credential-classes",
        default=DEFAULT_CREDENTIAL_CLASSES,
        help="credential-classes JSON path",
    )
    parser.add_argument(
        "--credential-classes-schema",
        default=DEFAULT_CREDENTIAL_CLASSES_SCHEMA,
        help="credential-classes schema JSON path",
    )
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    inventory_path = root / args.inventory
    schema_path = root / args.schema
    credential_classes_path = root / args.credential_classes
    credential_classes_schema_path = root / args.credential_classes_schema

    # DISCLOSURE-HOLD: report only, printed FIRST and unconditionally --
    # before check() runs at all, so it survives every other failure mode
    # (a malformed inventory, jsonschema being unavailable) rather than
    # only the ones that happen to leave `errors` defined. Coordinator
    # ruling (2026-09-01): two robustness holes found by Codex -- main()
    # passing a malformed `rows` (e.g. `rows: 17`) straight to
    # find_disclosure_hold_rows crashed with a raw TypeError before the
    # real schema error ever printed; and the missing-jsonschema path
    # returned before printing any DISCLOSURE-HOLD line at all, so
    # "unconditional" was not actually true. Both fixed by computing this
    # from the raw JSON directly (never via check(), which can legitimately
    # raise) and tolerating a non-list `rows` (schema validation reports
    # that as its own, real error; this line only decides whether to also
    # print a DISCLOSURE-HOLD count, never suppresses or duplicates that).
    # (2026-09-01 merge-gate: a scalar/list top-level inventory document --
    # not even an object -- raised a raw AttributeError on
    # `raw_inventory.get(...)` right here, before check() was ever called
    # and before this DISCLOSURE-HOLD line printed at all. Guarded the same
    # way as the non-list `rows` case just below: an inventory that isn't a
    # dict simply has no rows to scan for the marker; the real JSON SCHEMA
    # VIOLATION for "top level must be an object" is still reported by
    # check() a few lines down.)
    try:
        raw_inventory = load_json(inventory_path)
        raw_rows = (
            raw_inventory.get("rows", []) if isinstance(raw_inventory, dict) else []
        )
        if not isinstance(raw_rows, list):
            raw_rows = []
        held_rows = find_disclosure_hold_rows(raw_rows)
    except (OSError, json.JSONDecodeError):
        held_rows = []  # inventory itself is unreadable -- check() reports why
        raw_inventory = {}
    if held_rows:
        print(
            f"DISCLOSURE-HOLD: {len(held_rows)} row(s) marked: {', '.join(held_rows)}"
        )
    else:
        print("DISCLOSURE-HOLD: 0 rows marked")

    # Printed unconditionally, next to the DISCLOSURE-HOLD line and before the
    # verdict: an inventory whose provenance could not be checked must SAY so
    # in the same output a reader takes the verdict from, or "OK" reads as a
    # stronger claim than it is.
    try:
        _, source_commit_note = check_source_commit(
            root, raw_inventory if isinstance(raw_inventory, dict) else {}
        )
    except (OSError, subprocess.SubprocessError):
        source_commit_note = "SOURCE COMMIT UNVERIFIED: git unavailable"
    if source_commit_note:
        print(source_commit_note)

    try:
        errors = check(
            root,
            inventory_path,
            schema_path,
            credential_classes_path,
            credential_classes_schema_path,
        )
    except RuntimeError as exc:
        # jsonschema unavailable (see check()'s own guard) or another
        # infra-level failure -- fail loudly with a clean message, never a
        # raw traceback, and never silently treat it as "no violations".
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    if errors:
        print(f"FAIL: {len(errors)} endpoint-profile violation(s):", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1
    print("OK: ops endpoint-profile inventory is consistent with discovery.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

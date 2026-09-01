#!/usr/bin/env python3
"""CHAOS-3273 L3 CI gate: enforce the ops endpoint-authentication-profile
inventory contract (guardrail G-1: a route without a registered profile
fails CI and may not ship).

Modelled on ``ci/check_transitional_inventory.py`` (CUT-01) -- same shape:
independent re-discovery from source (never trusts
``contracts/auth/v1/endpoint-profiles.ops.json`` itself for what surfaces
exist), a discovery/inventory cross-check, and staleness/content-drift
validation on every anchor. Discovery is delegated to
``ci/discover_ops_routes.py`` (already built by lane auth-cp/L1 as this
gate's discovery half) rather than re-implemented here.

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
  1. UNOWNED SURFACE -- a discovered REST route or GraphQL resolver has no
     row in the inventory.
  2. PHANTOM ROW -- a row's ``source`` anchor does not correspond to any
     independently-discovered surface (stale row).
  3. DUPLICATE ID -- two rows share the same ``id``; or DUPLICATE SURFACE
     OWNERSHIP -- two rows with DIFFERENT ids both claim the same
     discovered ``(file, line)`` surface (worse than a missing row: both
     look registered, possibly with contradicting classifications).
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
  5. ANCHOR DRIFT -- a matched row's ``method``/``route``
     (REST) or ``graphql_field_name``/``surface_kind`` (GraphQL) no longer
     agrees with what independent discovery finds at that same file:line;
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

Usage:
    python3 ci/check_endpoint_profiles.py [--root PATH] [--inventory PATH]
        [--schema PATH] [--credential-classes PATH]
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
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
DEFAULT_DISCOVERER = "ci/discover_ops_routes.py"

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
    return {c["class_id"] for c in credential_classes["classes"]}


# ---------------------------------------------------------------------------
# Discovery cross-check
# ---------------------------------------------------------------------------


def _discovered_surface_map(discovered: dict) -> dict[tuple[str, int], dict]:
    """Every discovered REST route + GraphQL resolver keyed by (file, line),
    tagged with a ``_surface_type`` ('rest'/'graphql') distinct from the
    GraphQL resolver's own ``kind`` field ('field'/'mutation'), so a matched
    row's content can be checked against the right shape without either
    clobbering the other."""
    out: dict[tuple[str, int], dict] = {}
    for r in discovered["routes"]:
        out[(r["file"], r["line"])] = {"_surface_type": "rest", **r}
    for r in discovered["graphql"]:
        out[(r["file"], r["line"])] = {"_surface_type": "graphql", **r}
    return out


def check(
    root: Path,
    inventory_path: Path,
    schema_path: Path,
    credential_classes_path: Path,
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

    errors: list[str] = []

    inventory = load_json(inventory_path)
    schema = load_json(schema_path)
    credential_classes = load_json(credential_classes_path)
    class_vocab = credential_class_vocabulary(credential_classes)

    discoverer = _load_module(root / DEFAULT_DISCOVERER, "discover_ops_routes")
    discovered = discoverer.discover(root)
    surface_map = _discovered_surface_map(discovered)
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
    validator = jsonschema.Draft202012Validator(schema)
    for err in sorted(
        validator.iter_errors(inventory), key=lambda e: list(map(str, e.path))
    ):
        loc = "/".join(str(p) for p in err.path) or "<root>"
        errors.append(f"JSON SCHEMA VIOLATION at {loc}: {err.message}")

    rows = inventory.get("rows", [])
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
    row_keys: set[tuple[str, int]] = set()
    # Every (file, line) claimed as a row's `source`, mapped to every row
    # id that claims it -- a discovered surface must be owned by EXACTLY
    # one row. Two distinct ids anchored at the same surface (Codex-verified
    # gap: possible with conflicting classifications, e.g. one row says
    # public/no-creds and another says protected for the identical route)
    # is worse than a missing row: both look registered, so neither reviewer
    # nor gate has a reason to look closer. Not expressible as a JSON Schema
    # constraint -- it's a cross-row uniqueness rule.
    surface_owners: dict[tuple[str, int], list[str]] = {}
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

        # --- source anchor: required, existence + content-drift ---------
        src = row.get("source")
        if isinstance(src, dict) and "file" in src and "line" in src:
            key = (src["file"], src["line"])
            row_keys.add(key)
            surface_owners.setdefault(key, []).append(rid)

    # 3b. duplicate surface ownership: two DIFFERENT ids both claiming the
    # same discovered (file, line) surface. Worse than a missing row -- both
    # look registered, possibly with contradicting classifications, and
    # nothing else here would notice (the plain duplicate-id check above
    # only catches identical ids; the discovery cross-check below only
    # notices a surface with ZERO owners, not two).
    for key, owners in surface_owners.items():
        if len(owners) > 1:
            file, line = key
            errors.append(
                f"DUPLICATE SURFACE OWNERSHIP: rows {sorted(owners)!r} all claim "
                f"{file}:{line} -- exactly one row may own a discovered surface"
            )

    # --- 1 & 2. bidirectional surface/row parity ------------------------
    for key, surface in surface_map.items():
        if key not in row_keys:
            file, line = key
            errors.append(
                f"UNOWNED SURFACE: {surface['_surface_type']} at {file}:{line} has no row in "
                f"{inventory_path.name}. Add an owning row (guardrail G-1)."
            )

    for row in rows:
        if not isinstance(row, dict):
            continue  # reported by the full schema validation above
        src = row.get("source")
        if not isinstance(src, dict) or "file" not in src or "line" not in src:
            continue
        key = (src["file"], src["line"])
        rid = row.get("id", "<no id>")
        if key not in discovered_keys:
            errors.append(
                f"PHANTOM ROW: row {rid!r} references {src['file']}:{src['line']} "
                "which independent discovery did not find there (stale row -- "
                "re-anchor or remove)"
            )
            continue

        # --- 5. content/anchor drift: matched row vs discovered surface -
        surface = surface_map[key]
        if surface["_surface_type"] == "rest":
            if row.get("surface_kind") not in ("rest",):
                errors.append(
                    f"STALE ANCHOR: row {rid!r} claims surface_kind={row.get('surface_kind')!r} "
                    f"but {src['file']}:{src['line']} is a REST route (content drift)"
                )
            if row.get("method") != surface["method"]:
                errors.append(
                    f"STALE ANCHOR: row {rid!r} claims method={row.get('method')!r} "
                    f"but discovery finds {surface['method']!r} at "
                    f"{src['file']}:{src['line']} (content drift)"
                )
            if surface.get("resolution") == "OK" and row.get("route") != surface.get(
                "full_path"
            ):
                errors.append(
                    f"STALE ANCHOR: row {rid!r} claims route={row.get('route')!r} "
                    f"but discovery resolves {surface.get('full_path')!r} at "
                    f"{src['file']}:{src['line']} (content drift)"
                )
        else:  # graphql
            expected_kind = (
                "graphql_field" if surface["kind"] == "field" else "graphql_mutation"
            )
            if row.get("surface_kind") != expected_kind:
                errors.append(
                    f"STALE ANCHOR: row {rid!r} claims surface_kind={row.get('surface_kind')!r} "
                    f"but {src['file']}:{src['line']} is a {expected_kind} resolver (content drift)"
                )
            if row.get("graphql_field_name") != surface["name"]:
                errors.append(
                    f"STALE ANCHOR: row {rid!r} claims graphql_field_name="
                    f"{row.get('graphql_field_name')!r} but discovery finds "
                    f"{surface['name']!r} at {src['file']}:{src['line']} (content drift)"
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
# real 361-row inventory, it produced 602 false positives, because this
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
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    inventory_path = root / args.inventory
    schema_path = root / args.schema
    credential_classes_path = root / args.credential_classes

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
    try:
        raw_inventory = load_json(inventory_path)
        raw_rows = raw_inventory.get("rows", [])
        if not isinstance(raw_rows, list):
            raw_rows = []
        held_rows = find_disclosure_hold_rows(raw_rows)
    except (OSError, json.JSONDecodeError):
        held_rows = []  # inventory itself is unreadable -- check() reports why
    if held_rows:
        print(
            f"DISCLOSURE-HOLD: {len(held_rows)} row(s) marked: {', '.join(held_rows)}"
        )
    else:
        print("DISCLOSURE-HOLD: 0 rows marked")

    try:
        errors = check(root, inventory_path, schema_path, credential_classes_path)
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

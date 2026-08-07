"""Oracle pair `status/mapping/normalize` -- the two normalizer boundaries.

THE REFLECTOR IS THE POINT. `normalize_type` returns a bare string, so there is
no dataclass or dict literal to reflect and the obvious move -- writing
`frozenset({"type"})` by hand -- is exactly the silently-narrowing pair that
codex finding #1 and this registry exist to prevent: it would keep reporting a
perfect match while an entire method went uncompared.

Instead the field set is parsed out of StatusMapping's OWN source: every method
named `normalize_*` becomes a required row key. That is two-directional. Add a
`normalize_priority` upstream and every case here fails until the Go port covers
it. Defer one of the two methods and the deferral cannot be silent -- it has to
be written down as an `excluded_fields` entry with a reason. Both engines are
therefore exercised by every single case.

Helpers are imported from status_mapping_load rather than copied so that both
pairs resolve a config NAME to the same file by construction; two private copies
of that rule could drift and let the pairs read different configs while both
looked green.
"""

from __future__ import annotations

import ast
import contextlib
import io
import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.oracle_pairs.status_mapping_load import (
    MIRRORED_EXCEPTIONS,
    load_mapping_for_case,
)

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_STATUS_MAPPING_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/status_mapping.py"

_CLASS_NAME = "StatusMapping"
_METHOD_PREFIX = "normalize_"


def _reflected_fields() -> frozenset[str]:
    """Every `normalize_*` method StatusMapping declares, parsed from its source.

    Static parse, no import and no execution, matching field_reflection.py's
    constraint: the set describes what the production class can emit without a
    second hand-maintained list that could start incomplete.
    """
    tree = ast.parse(_STATUS_MAPPING_SOURCE.read_text())
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef) or node.name != _CLASS_NAME:
            continue
        names = {
            statement.name
            for statement in node.body
            if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef))
            and statement.name.startswith(_METHOD_PREFIX)
        }
        if not names:
            raise ValueError(
                f"{_CLASS_NAME} declares no {_METHOD_PREFIX}* methods -- either the "
                "class was renamed or the normalizers moved; a pair that cannot "
                "find its own boundary must fail, not compare nothing"
            )
        return frozenset(names)
    raise ValueError(
        f"no class named {_CLASS_NAME!r} in {_STATUS_MAPPING_SOURCE} -- has it "
        "been renamed or moved?"
    )


def _labels(case: dict[str, Any]) -> list[str]:
    """Python str()s every label; the Go harness must not SKIP non-strings.

    `normalize_*` does `_norm_key(str(label))`, so a numeric or boolean label is
    a real, indexable value rather than something to drop. The Go side mirrors
    this in pythonStrOfCaseValue.
    """
    return [str(label) for label in case.get("labels") or []]


def _optional(case: dict[str, Any], key: str) -> str | None:
    value = case.get(key)
    return None if value is None else str(value)


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    """Build one row, carrying the PHASE and exception type when Python raises.

    LOAD and NORMALIZE are separate phases on purpose: a config that fails to
    load and an input that fails to normalize are different defects, and a row
    that recorded only "it raised" would let one silently move into the other.
    """
    try:
        mapping = load_mapping_for_case(case)
    except MIRRORED_EXCEPTIONS as exc:
        return {
            "outcome": f"load:{type(exc).__name__}",
            "normalize_status": None,
            "normalize_type": None,
        }
    provider = case["provider"]
    labels = _labels(case)
    sink = io.StringIO()
    try:
        with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
            return {
                "outcome": "ok",
                "normalize_status": mapping.normalize_status(
                    provider=provider,
                    status_raw=_optional(case, "status_raw"),
                    labels=labels,
                    state=_optional(case, "state"),
                ),
                "normalize_type": mapping.normalize_type(
                    provider=provider,
                    type_raw=_optional(case, "type_raw"),
                    labels=labels,
                ),
            }
    except MIRRORED_EXCEPTIONS as exc:
        return {
            "outcome": f"normalize:{type(exc).__name__}",
            "normalize_status": None,
            "normalize_type": None,
        }


oracle_registry.register(
    oracle_registry.PairSpec(
        id="status/mapping/normalize",
        build_row=_build_row,
        reflected_fields=_reflected_fields,
    )
)

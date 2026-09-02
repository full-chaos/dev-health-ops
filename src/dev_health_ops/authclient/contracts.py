"""Load and validate documents against the Auth Control Plane v1 wire contracts.

The JSON Schema documents under ``contracts/auth/v1/jsonschema/`` are the
source of truth for the wire format. This module is the Python half of the
three-language validation surface CHAOS-4884 requires; the Go half lives in
``internal/contracts/v1`` and the TypeScript half in dev-health-web. All three
read the SAME schema files and the SAME golden fixtures -- a language that
maintains its own copy is the drift class the cross-language goldens exist to
catch.

Two behaviours here are deliberately fatal rather than degraded, both copied
from ``ci/check_endpoint_profiles.py``'s precedent for the same hazards:

1. A missing ``jsonschema`` import refuses to run rather than skipping
   validation, because "validated nothing" and "validated cleanly" are
   indistinguishable from the exit code.
2. A missing ``date-time`` format checker refuses to run. Draft 2020-12 treats
   ``format`` as an ANNOTATION unless the validator opts in, and ``jsonschema``
   only registers a ``date-time`` checker when a backing implementation
   (``rfc3339-validator``, declared in ``pyproject.toml``) is importable.
   Without it, ``"expires_at": "not-a-date"`` validates cleanly -- the check
   looks applied while checking nothing. Verified here rather than assumed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any

import jsonschema

#: Repo-relative location of the v1 wire schemas. Resolved from this file's
#: own path rather than from a working directory, so a caller's cwd cannot
#: change which contract is validated against.
_CONTRACTS_SUBPATH = Path("contracts") / "auth" / "v1"


class ContractError(RuntimeError):
    """The contract tooling cannot run, or a document violates its contract."""


def repo_root() -> Path:
    """Return the ops repo root that owns the contracts directory.

    Walks up from this module until a directory containing
    ``contracts/auth/v1/jsonschema`` is found. Raises rather than guessing:
    a wrong root would validate against a contract that is not this tree's.
    """
    for candidate in (Path(__file__).resolve(), *Path(__file__).resolve().parents):
        if (candidate / _CONTRACTS_SUBPATH / "jsonschema").is_dir():
            return candidate
    raise ContractError(
        "could not locate the ops repo root above "
        f"{Path(__file__).resolve()} -- no ancestor contains "
        f"{_CONTRACTS_SUBPATH / 'jsonschema'}. Refusing to guess a root, "
        "because validating against the wrong contracts directory reports "
        "success while checking a different contract."
    )


def contracts_dir(root: Path | None = None) -> Path:
    """Return ``contracts/auth/v1`` beneath *root* (default: :func:`repo_root`)."""
    return (root or repo_root()) / _CONTRACTS_SUBPATH


def _require_format_assertion() -> None:
    checkers = jsonschema.Draft202012Validator.FORMAT_CHECKER.checkers
    if "date-time" not in checkers:
        raise ContractError(
            "jsonschema has no 'date-time' format checker registered, so "
            '`"format": "date-time"` in the wire schemas would validate ANY '
            "string -- refusing to run with a format check that silently "
            "checks nothing. Install the backing implementation "
            "('rfc3339-validator', declared in pyproject.toml) and retry."
        )


def load_json(path: Path) -> Any:
    """Read one JSON document, reporting the path on a parse failure."""
    try:
        with path.open(encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise ContractError(f"{path}: {exc}") from exc


@cache
def _validator_for(schema_path_str: str) -> jsonschema.Draft202012Validator:
    _require_format_assertion()
    schema = load_json(Path(schema_path_str))
    # check_schema first: an invalid schema otherwise fails per-instance in
    # ways that read like instance errors, and a schema this validator cannot
    # interpret must be a hard stop, never a skip.
    jsonschema.Draft202012Validator.check_schema(schema)
    return jsonschema.Draft202012Validator(
        schema,
        format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER,
    )


def validator_for(
    surface: str, root: Path | None = None
) -> jsonschema.Draft202012Validator:
    """Return the Draft 2020-12 validator for one wire surface.

    *surface* is the schema's stem, e.g. ``"principal.v1"`` for
    ``contracts/auth/v1/jsonschema/principal.v1.schema.json``.
    """
    path = contracts_dir(root) / "jsonschema" / f"{surface}.schema.json"
    if not path.is_file():
        raise ContractError(f"no wire schema for surface {surface!r} at {path}")
    return _validator_for(str(path))


@dataclass(frozen=True, slots=True)
class Violation:
    """One schema violation, located by JSON Pointer and keyword.

    The pointer and keyword are carried separately, not folded into the
    message, so a test can assert WHICH rule rejected a document. A negative
    fixture that fails for the wrong reason is not a passing rejection test --
    it is a fixture that has stopped testing what it claims to test.
    """

    instance_location: str
    keyword: str
    message: str

    def __str__(self) -> str:
        return f"{self.instance_location} {self.keyword}: {self.message}"


def violations(
    surface: str, document: Any, root: Path | None = None
) -> list[Violation]:
    """Return every violation of *surface*'s schema in *document*, sorted.

    Empty list means the document validates. Returns ALL errors rather than
    the first, so a caller sees the whole disagreement rather than repairing
    one field at a time.
    """
    validator = validator_for(surface, root)
    found = []
    for error in validator.iter_errors(document):
        pointer = "/" + "/".join(str(part) for part in error.absolute_path)
        found.append(
            Violation(
                instance_location=pointer if list(error.absolute_path) else "/",
                keyword=str(error.validator),
                message=error.message,
            )
        )
    return sorted(found, key=lambda v: (v.instance_location, v.keyword))


def validate(surface: str, document: Any, root: Path | None = None) -> None:
    """Raise :class:`ContractError` unless *document* satisfies *surface*."""
    found = violations(surface, document, root)
    if found:
        joined = "; ".join(str(v) for v in found)
        raise ContractError(f"{surface}: {joined}")

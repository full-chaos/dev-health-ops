"""Load and validate documents against the Auth Control Plane v1 wire contracts.

The JSON Schema documents under ``contracts/auth/v1/jsonschema/`` are the
source of truth for the wire format. This module is the Python half of the
three-language validation surface CHAOS-4884 requires; the Go half lives in
``internal/auth/contracts`` and the TypeScript half in dev-health-web. All three
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
import re
from collections.abc import Iterator
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any

import jsonschema
import jsonschema.exceptions
import jsonschema.protocols
import jsonschema.validators

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


#: Cache of pattern -> strictly-anchored compiled regex, keyed by the pattern
#: text exactly as it appears in the schema.
_STRICT_PATTERNS: dict[str, re.Pattern[str]] = {}


def _unescaped_dollar_positions(pattern: str) -> list[int]:
    """Indices of every ``$`` that is a regex anchor rather than a literal."""
    positions = []
    for index, char in enumerate(pattern):
        if char != "$":
            continue
        backslashes = 0
        walk = index - 1
        while walk >= 0 and pattern[walk] == "\\":
            backslashes += 1
            walk -= 1
        if backslashes % 2 == 0:
            positions.append(index)
    return positions


def strictly_anchored(pattern: str) -> re.Pattern[str]:
    r"""Compile *pattern* so ``$`` means end-of-input, as Go RE2 and ECMA-262 do.

    THE DIVERGENCE, measured on three engines with one document:

        python  ^prn_[A-Za-z0-9_-]+$  vs  "prn_EXAMPLE0000000000000001\n"  -> MATCH
        go RE2  same pattern, same input                                    -> no match
        node    same pattern, same input (ECMA-262, which ajv implements)   -> false

    Python's ``$`` matches at the end of the string OR immediately before a
    single trailing newline. Go RE2 and ECMA-262 match only at true end. So a
    principal id with a trailing newline was ACCEPTED by this validator and
    REJECTED by the other two -- Python the sole outlier, 1 of 3, exactly as
    with the Unicode ``\d`` divergence. Found by codex round 1; it is a
    genuine oracle attack, because the golden corpus and the schema dialect
    guard both pass while it is present.

    The schema cannot fix this. ``\Z`` is Python-only and ``\z`` is RE2-only;
    ECMA-262 has neither, so no single portable pattern expresses "true end".
    The fix therefore belongs in the divergent runtime, and this is it: rewrite
    a trailing anchor to ``\Z``, which Python does implement as absolute end.

    FAILS CLOSED. A pattern carrying an unescaped ``$`` anywhere other than its
    final character is REFUSED rather than passed through untransformed, even
    though such a pattern is legal and might well be harmless. The alternative
    -- transform what is recognised and silently accept the rest -- is the
    "default unrecognised shapes to safe" mistake, and here the unrecognised
    shape is precisely the one whose semantics are in question.
    """
    cached = _STRICT_PATTERNS.get(pattern)
    if cached is not None:
        return cached

    anchors = _unescaped_dollar_positions(pattern)
    if anchors and anchors != [len(pattern) - 1]:
        raise ContractError(
            f"pattern {pattern!r} contains an unescaped '$' that is not the final "
            "character. This validator rewrites a TRAILING '$' to Python's '\\Z' so "
            "that '$' means end-of-input as it does in Go RE2 and ECMA-262; it "
            "refuses any other placement rather than guessing, because a '$' whose "
            "semantics differ between the three validators is the exact defect this "
            "rewrite exists to close. Rewrite the pattern, or extend this function "
            "deliberately with a test."
        )

    source = pattern[:-1] + r"\Z" if anchors else pattern
    compiled = re.compile(source)
    _STRICT_PATTERNS[pattern] = compiled
    return compiled


def _strict_pattern_keyword(
    validator: Any, patrn: str, instance: Any, schema: Any
) -> Iterator[jsonschema.exceptions.ValidationError]:
    """The ``pattern`` keyword, with Go/ECMA end-of-input semantics."""
    if not isinstance(instance, str):
        return
    if strictly_anchored(patrn).search(instance) is None:
        yield jsonschema.exceptions.ValidationError(
            f"{instance!r} does not match {patrn!r}"
        )


#: Draft 2020-12 with the one keyword whose Python semantics diverge from the
#: other two validators replaced. Everything else is stock.
StrictDraft202012Validator = jsonschema.validators.extend(
    jsonschema.Draft202012Validator, {"pattern": _strict_pattern_keyword}
)


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
def _validator_for(schema_path_str: str) -> jsonschema.protocols.Validator:
    _require_format_assertion()
    schema = load_json(Path(schema_path_str))
    # check_schema first: an invalid schema otherwise fails per-instance in
    # ways that read like instance errors, and a schema this validator cannot
    # interpret must be a hard stop, never a skip.
    jsonschema.Draft202012Validator.check_schema(schema)
    # Compile every pattern up front so a shape strictly_anchored refuses is a
    # LOAD-time failure, not a per-instance surprise that only fires when some
    # document happens to reach that field.
    for pattern in _every_pattern(schema):
        strictly_anchored(pattern)
    return StrictDraft202012Validator(
        schema,
        format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER,
    )


def _every_pattern(node: Any) -> Iterator[str]:
    """Yield every ``pattern`` string anywhere in a schema document."""
    if isinstance(node, dict):
        found = node.get("pattern")
        if isinstance(found, str):
            yield found
        for value in node.values():
            yield from _every_pattern(value)
    elif isinstance(node, list):
        for value in node:
            yield from _every_pattern(value)


def validator_for(
    surface: str, root: Path | None = None
) -> jsonschema.protocols.Validator:
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

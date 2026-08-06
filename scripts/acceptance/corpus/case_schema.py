"""CHAOS-3219 Wave 4 Phase 2 Lane 2a: corpus case + resolution-profile
loading and validation.

Case JSON schema is Lane 2b's authoritative spec (``case-schema.md``,
authored alongside ``tests/acceptance/world/ask-dev-world.v1/corpus/*.json``);
the interface decision fixed with the team-lead (2026-08-05) is: ``id`` /
``question`` / ``subject_class`` / invariant-first ``invariants`` +
``resolution_profile_ref``. This loader validates defensively -- it rejects
a case MISSING one of those fields (Lane 2a's runner cannot execute or
classify a case without them), but never rejects an unknown EXTRA field, so
it does not have to change in lockstep with every schema addition Lane 2b's
case authoring needs for its own bookkeeping.

Matcher-specific expected outcomes never live on the case itself -- they
live in a versioned ``resolution-profile`` file
(``tests/acceptance/world/ask-dev-world.v1/resolution-profiles/<ref>.json``),
keyed by case id. This is the pluggable seam CHAOS-3389's fold-in requires:
when the Question Understanding Agent flips a case's actual resolution
behavior (e.g. from clarification to a direct resolved answer), only the
profile changes -- a new ``resolution-profiles/qua-v1.json`` -- never the
case file, never this loader, never the runner.

``status: "declared-blocked"`` (team-lead direction, 2026-08-06, folding in
2b's codex round-1 finding): a corpus case blocked on an external ticket
(e.g. a `[BLOCKED on CHAOS-3393]` registry entry) loads successfully with
ZERO invariants -- the world-manifest discipline already established
elsewhere in this project (``world.json``'s own ``declared-blocked``/
``blocked_by`` fields for ``cross_generation_digest_status``; ``sources.json``
truncated-workgraph's identical status/blocked_by pair) applied here so a
legitimately-blocked case can be loaded and LOUDLY, COUNTABLY reported as
blocked (never silently dropped, never crashing the whole corpus load,
never mistaken for a passing case) instead of forcing placeholder invariants
that would assert nothing real. See :func:`load_corpus_case`'s docstring for
the exact field contract.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

__all__ = [
    "ACTIVE_STATUS",
    "DECLARED_BLOCKED_STATUS",
    "CaseSchemaError",
    "CorpusCase",
    "ResolutionProfile",
    "load_corpus_case",
    "load_corpus_cases",
    "load_resolution_profile",
    "resolve_case_expectations",
]

_CASE_REQUIRED_FIELDS = ("id", "question", "subject_class")
_PROFILE_SCHEMA_VERSION_PREFIX = "resolution-profile."

#: The only two case statuses this loader understands. Mirrors the
#: ``declared-blocked``/``blocked_by`` convention already established in
#: ``world.json``/``sources.json`` (CHAOS-3428/CHAOS-3432's own status
#: fields) -- one discipline, applied consistently across the fixture world
#: and the corpus case layer.
ACTIVE_STATUS = "active"
DECLARED_BLOCKED_STATUS = "declared-blocked"
_KNOWN_STATUSES = frozenset({ACTIVE_STATUS, DECLARED_BLOCKED_STATUS})

#: Codex round-3 finding (MEDIUM, confirmed): a non-empty string like
#: "not-a-ticket" satisfied the earlier ``blocked_by`` check, letting a
#: case suppress its own coverage with no traceable blocker despite the
#: documented ticket-reference contract. Matches this repo's own real
#: convention (``world.json``'s ``"CHAOS-3432 concurrent ClickHouse ..."``
#: -- a leading ``CHAOS-<number>`` token, optionally followed by free-text
#: description) rather than requiring an exact bare ``CHAOS-<number>`` --
#: anchored at the start only, so descriptive suffixes stay allowed.
_BLOCKED_BY_TICKET_PATTERN = re.compile(r"^CHAOS-\d+\b")


class CaseSchemaError(Exception):
    """A corpus case or resolution-profile file fails structural validation.

    Raised at load time, never at assertion time -- a malformed case must
    fail loud before any HTTP call is made, not surface as a confusing
    downstream assertion failure that looks like a product defect.
    """


@dataclass(frozen=True, slots=True)
class CorpusCase:
    id: str
    question: str
    subject_class: str
    invariants: tuple[Mapping[str, Any], ...]
    resolution_profile_ref: str | None
    status: str
    blocked_by: str | None
    source_path: Path
    raw: Mapping[str, Any] = field(repr=False)

    @property
    def is_declared_blocked(self) -> bool:
        return self.status == DECLARED_BLOCKED_STATUS


@dataclass(frozen=True, slots=True)
class ResolutionProfile:
    profile_id: str
    schema_version: str
    cases: Mapping[str, Mapping[str, Any]]
    source_path: Path


def _require_str(payload: Mapping[str, Any], field_name: str, *, where: Path) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise CaseSchemaError(
            f"{where}: required field {field_name!r} must be a non-empty string, "
            f"got {value!r}"
        )
    return value


def load_corpus_case(path: Path) -> CorpusCase:
    """Load and structurally validate one corpus case file.

    Missing/mistyped required fields raise :class:`CaseSchemaError`
    immediately -- this function never returns a partially-valid case for a
    caller to trip over later.
    """

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise CaseSchemaError(f"{path}: not valid JSON ({exc})") from exc
    if not isinstance(raw, Mapping):
        raise CaseSchemaError(f"{path}: top-level JSON value must be an object")

    missing = [f for f in _CASE_REQUIRED_FIELDS if f not in raw]
    if missing:
        raise CaseSchemaError(
            f"{path}: missing required field(s) {missing} -- Lane 2a's runner "
            "cannot execute or classify a case without every one of "
            f"{_CASE_REQUIRED_FIELDS}"
        )

    case_id = _require_str(raw, "id", where=path)
    question = _require_str(raw, "question", where=path)
    subject_class = _require_str(raw, "subject_class", where=path)

    status = raw.get("status", ACTIVE_STATUS)
    if not isinstance(status, str) or status not in _KNOWN_STATUSES:
        raise CaseSchemaError(
            f"{path}: {case_id!r} 'status' must be one of {sorted(_KNOWN_STATUSES)!r} "
            f"(or absent, defaulting to {ACTIVE_STATUS!r}), got {status!r}"
        )
    is_blocked = status == DECLARED_BLOCKED_STATUS

    blocked_by = raw.get("blocked_by")
    if is_blocked:
        if not isinstance(blocked_by, str) or not blocked_by.strip():
            raise CaseSchemaError(
                f"{path}: {case_id!r} status={DECLARED_BLOCKED_STATUS!r} requires "
                "a non-empty 'blocked_by' ticket reference (e.g. 'CHAOS-3393') -- "
                "a blocked case must be loudly, traceably blocked, never a bare "
                "status flag with no way to know what unblocks it"
            )
        if not _BLOCKED_BY_TICKET_PATTERN.match(blocked_by.strip()):
            raise CaseSchemaError(
                f"{path}: {case_id!r} 'blocked_by' {blocked_by!r} does not start "
                "with a real ticket reference (e.g. 'CHAOS-3393' or 'CHAOS-3393 "
                "some description') -- a value like 'not-a-ticket' would let "
                "coverage be suppressed with no traceable, actionable blocker"
            )
    elif blocked_by is not None:
        raise CaseSchemaError(
            f"{path}: {case_id!r} 'blocked_by' is only valid when "
            f"status={DECLARED_BLOCKED_STATUS!r}, got status={status!r} with "
            f"blocked_by={blocked_by!r}"
        )

    invariants_raw = raw.get("invariants", [])
    if not isinstance(invariants_raw, list):
        raise CaseSchemaError(f"{path}: {case_id!r} 'invariants' must be a list")
    if not is_blocked and not invariants_raw:
        raise CaseSchemaError(
            f"{path}: {case_id!r} 'invariants' must be a non-empty list -- a "
            "case with zero invariant assertions would let the "
            "assertion_count>0 false-green guard silently pass on a case "
            "that never asserted anything (a case with nothing to assert "
            f"yet belongs under status={DECLARED_BLOCKED_STATUS!r}, not an "
            "empty invariants list on an 'active' case)"
        )
    for index, entry in enumerate(invariants_raw):
        if (
            not isinstance(entry, Mapping)
            or "category" not in entry
            or "check" not in entry
        ):
            raise CaseSchemaError(
                f"{path}: {case_id!r} invariants[{index}] must be an object "
                "with at least 'category' and 'check' fields, got "
                f"{entry!r}"
            )

    resolution_profile_ref = raw.get("resolution_profile_ref")
    if resolution_profile_ref is not None and (
        not isinstance(resolution_profile_ref, str)
        or not resolution_profile_ref.strip()
    ):
        raise CaseSchemaError(
            f"{path}: {case_id!r} 'resolution_profile_ref' must be a "
            f"non-empty string or absent/null, got {resolution_profile_ref!r}"
        )

    if case_id != path.stem and not path.stem.endswith(case_id):
        # Not fatal on its own -- the id inside the file is authoritative,
        # matching the frozen-registry convention where filenames are a
        # human convenience, not the identity. Surfaced only as part of the
        # duplicate-id check in load_corpus_cases, which is where a real
        # naming collision would actually cause silent data loss.
        pass

    return CorpusCase(
        id=case_id,
        question=question,
        subject_class=subject_class,
        invariants=tuple(invariants_raw),
        resolution_profile_ref=resolution_profile_ref,
        status=status,
        blocked_by=blocked_by,
        source_path=path,
        raw=raw,
    )


def load_corpus_cases(
    directory: Path, *, pattern: str = "case-*.json"
) -> list[CorpusCase]:
    """Load every corpus case file under ``directory`` matching ``pattern``.

    Returns an empty list for a directory that does not exist or has no
    matching files -- this is a deliberate, documented non-failure: Lane 2b
    lands case content on its own schedule (merge order is 2a before 2b),
    and a live-armed test session is what enforces "zero cases collected is
    a failure", not this loader (see
    ``tests/acceptance/test_wave4_corpus_runner_live.py``'s session guard).
    A loader that raised on an absent directory would make Lane 2a's own
    merge fail before Lane 2b exists at all.

    Raises :class:`CaseSchemaError` on any malformed file (fail loud, don't
    skip) or on a duplicate case id across files (silently keeping "the
    last one glob happened to return" would make corpus content nondeterministic).
    """

    if not directory.is_dir():
        return []
    cases: dict[str, CorpusCase] = {}
    duplicates: dict[str, list[Path]] = {}
    for path in sorted(directory.glob(pattern)):
        case = load_corpus_case(path)
        if case.id in cases:
            duplicates.setdefault(case.id, [cases[case.id].source_path]).append(path)
        else:
            cases[case.id] = case
    if duplicates:
        raise CaseSchemaError(
            f"{directory}: duplicate case id(s) across files: {duplicates!r}"
        )
    return sorted(cases.values(), key=lambda case: case.id)


def load_resolution_profile(path: Path) -> ResolutionProfile:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise CaseSchemaError(f"{path}: not valid JSON ({exc})") from exc
    if not isinstance(raw, Mapping):
        raise CaseSchemaError(f"{path}: top-level JSON value must be an object")

    schema_version = _require_str(raw, "schema_version", where=path)
    if not schema_version.startswith(_PROFILE_SCHEMA_VERSION_PREFIX):
        raise CaseSchemaError(
            f"{path}: schema_version {schema_version!r} does not start with "
            f"{_PROFILE_SCHEMA_VERSION_PREFIX!r} -- refusing to load a file "
            "that may not actually be a resolution-profile document"
        )
    profile_id = _require_str(raw, "profile_id", where=path)
    cases_raw = raw.get("cases")
    if not isinstance(cases_raw, Mapping):
        raise CaseSchemaError(f"{path}: 'cases' must be an object keyed by case id")
    for case_id, expectations in cases_raw.items():
        if not isinstance(expectations, Mapping):
            raise CaseSchemaError(
                f"{path}: cases[{case_id!r}] must be an object of expected "
                f"values, got {expectations!r}"
            )

    return ResolutionProfile(
        profile_id=profile_id,
        schema_version=schema_version,
        cases=dict(cases_raw),
        source_path=path,
    )


def resolve_case_expectations(
    case: CorpusCase, profiles: Mapping[str, ResolutionProfile]
) -> Mapping[str, Any]:
    """The matcher-specific expected-outcome block for one case.

    A case with no ``resolution_profile_ref`` gets an empty block -- it
    asserts invariants only, by design (e.g. an adversarial or
    non-subject-shaped case with nothing matcher-specific to pin). A case
    that DOES cite a profile ref, but whose id has no entry in that
    profile's ``cases`` map, fails loud here -- exactly the same
    "cites-but-missing" contract as the script-inventory preflight, and for
    the identical reason: silently treating it as "no matcher-specific
    expectations" would let a profile-authoring gap masquerade as an
    intentionally invariant-only case.
    """

    if case.resolution_profile_ref is None:
        return {}
    profile = profiles.get(case.resolution_profile_ref)
    if profile is None:
        raise CaseSchemaError(
            f"case {case.id!r} cites resolution_profile_ref "
            f"{case.resolution_profile_ref!r}, which was not loaded (known "
            f"profiles: {sorted(profiles)!r})"
        )
    expectations = profile.cases.get(case.id)
    if expectations is None:
        raise CaseSchemaError(
            f"case {case.id!r} cites resolution profile "
            f"{case.resolution_profile_ref!r}, but that profile has no "
            f"'cases[{case.id!r}]' entry -- a case cannot cite a profile and "
            "then run with no matcher-specific expectations silently "
            "assumed"
        )
    return expectations

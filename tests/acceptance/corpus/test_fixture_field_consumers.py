"""CHAOS-3546: a fixture field that reads as CONFIGURATION must be consumed.

Four confirmed instances of the same class (the ticket named three; a
fourth, ``kill_switches``, was found by the Phase 4d lane while this guard
was in flight and folded in here rather than filed separately), and their
disposition:

* ``ASK_DEV_SCRIPTED_PROVIDER_SCRIPTS_DIR`` -- set nowhere, read only from
  scrub lists, while the container silently fell back to a relative path
  that did not exist. Cost a full armed run: 140 receipts, reported green,
  19 scripted cases exercised nothing. Fixed in ops #1567, whose own guard
  (``provider_scripts.role_script_identity_digest``) proves the CONTAINER
  loaded the intended script -- but that guard is narrow to one env var and
  one digest, not a general property of the fixture surface. WIRED (already,
  by #1567); kept in :data:`FIXTURE_FIELD_REGISTRY` as the positive-control
  precedent this module generalizes.
* ``fault_ref`` -- declared on 18 corpus cases, read by zero code paths
  (fault selection is entirely by ``question_fingerprint``). Investigation
  found it carried strictly zero information even where present: its value
  equalled the case's OWN ``id`` in all 18 cases, with no exceptions -- not
  a pointer to something else, a redundant marker of "I am scripted"
  reconstructible from the id alone. RETIRED: the field itself is deleted
  from the 18 cases that declared it (CASE-SCHEMA.v1.md's row removed too)
  rather than wired, and the real invariant it gestured at -- "a scripted
  provider-scripts entry corresponds to a real corpus case" -- is enforced
  below as a DERIVED cross-check that needs no field on either side, reading
  provider-scripts' own registry instead. See ``_scripted_registry_violations``.
* ``provider_profile_override`` -- declared on two world.json users,
  referenced nowhere. RETIRED (not wired): wiring it would mean inventing a
  whole new production feature -- a per-user provider-override concept that
  exists nowhere today (no DB column, no migration, no settings-table write
  in the acceptance world-builder) -- rather than connecting an existing
  seam, a product-shape decision out of this ticket's scope. The field is
  deleted from world.json. THREE corpus cases it drove
  (``deg.provider.unsupported``, ``readiness.capabilities.unsupported-model``,
  and ``readiness.capabilities.degraded`` -- the third found while editing
  its two siblings, not in the ticket's original two-case count) are
  flipped ``status: "declared-blocked"`` on CHAOS-3588 (the follow-up that
  must choose a real per-user or per-org denial mechanism and re-author
  them) rather than left silently green on a mechanism that never fires --
  all three were PASSING while proving nothing about unsupported-model/
  degraded-readiness handling, the exact false-coverage shape this ticket
  exists to close.
* ``kill_switches`` -- declared per-org in world.json (global/org/provider/
  role/surface/contextual_entry, every fixture org's copy hardcoded
  ``"enabled"``), referenced nowhere in ``src/``. RETIRED: CHAOS-3458 (Done)
  already ruled the whole granular kill-switch concept superseded -- the
  single real mechanism is ``emergency_disabled``
  (``org_policy.ASK_DEV_EMERGENCY_DISABLED_KEY``, a settings-table boolean,
  actually read by ``router.py``/``ask_dev.py``). That ticket's own 6 named
  ``ops.kill-switch.*`` corpus ids confirm zero production enforcement ever
  existed for this fixture; the 2 that were actually authored as case files
  are declared-blocked for unrelated reasons already (CHAOS-3454, CHAOS-3549)
  and neither references ``kill_switches`` by name, so retiring the field has
  no corpus dependent to flip -- unlike ``provider_profile_override``, a
  clean deletion. Removed from all 3 world.json org entries.

Each instance made a scenario look configured when nothing configured it,
and every test that "covered" it asserted nothing about it. This module is
the generalization the ticket asks for: a registry of every fixture field
this repo has DECLARED operationally meaningful (selects or drives
behavior -- never a field CASE-SCHEMA.v1.md itself marks purely
documentary, like ``notes``/``proves``/an invariant's own ``assert``), each
paired with the evidence that something outside the fixture files and their
own schema doc actually reads it. A field added to this registry with no
real consumer fails here, not three measured instances and a fourth still
free to appear.

**What "consumed" means here, precisely.** Not "the string appears
somewhere" -- that would pass on the field's OWN name showing up in a
comment, or in this very module's own registry entry for it.
``field_has_external_consumer`` requires the literal field name, as a whole
identifier (``\\b``-bounded), inside a ``.py`` file that is neither the
fixture directory nor this guard module itself -- enough to separate a real
``.get(...)``/attribute read from prose that merely mentions the field, and
to stop a field from certifying itself by appearing in its own registry row.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
_WORLD_DIR = _REPO_ROOT / "tests" / "acceptance" / "world" / "ask-dev-world.v1"
_CORPUS_DIR = _WORLD_DIR / "corpus"

#: Every directory a real consumer may live in. Deliberately excludes
#: ``tests/acceptance/world`` (the fixture files themselves -- a field
#: obviously "contains" its own name) but INCLUDES the rest of ``tests/``:
#: a dedicated test asserting something concrete about a field (like this
#: module's own ``fault_ref`` check below) is exactly as real a consumer as
#: production code, and excluding it would make the guard unable to ever
#: certify a field whose only correctness value is a build-time assertion
#: rather than a runtime branch.
_CONSUMER_ROOTS = (
    _REPO_ROOT / "src",
    _REPO_ROOT / "scripts",
    _REPO_ROOT / "tests",
)

#: The fixture/doc paths a hit must never come from, even though they live
#: under ``tests/`` -- that would make a field consume itself.
_EXCLUDED_PATHS = (
    _REPO_ROOT / "tests" / "acceptance" / "world",
    Path(__file__).resolve(),
)


def _iter_py_files() -> list[Path]:
    files: list[Path] = []
    for root in _CONSUMER_ROOTS:
        if not root.is_dir():
            continue
        files.extend(root.rglob("*.py"))
    return files


def _is_excluded(path: Path) -> bool:
    return any(
        path == excluded or excluded in path.parents for excluded in _EXCLUDED_PATHS
    )


def field_has_external_consumer(field_name: str) -> bool:
    """Whether ``field_name`` appears, as a whole identifier, in some
    ``.py`` file outside the fixture directory and this guard module.

    Deliberately a plain substring/regex scan, not an AST walk -- the class
    of defect this closes (a field nothing reads AT ALL) does not need
    call-graph precision to catch, and a regex scan is auditable by anyone
    without importing this module's internals.
    """

    pattern = re.compile(rf"\b{re.escape(field_name)}\b")
    for path in _iter_py_files():
        if _is_excluded(path):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        if pattern.search(text):
            return True
    return False


# ---------------------------------------------------------------------------
# fault_ref -- RETIRED (CHAOS-3546). Replaced by a derived cross-check that
# needs no field on either side: the real invariant was always "a scripted
# provider-scripts entry corresponds to a real corpus case", readable
# straight off the registry without a marker on the case at all.
# ---------------------------------------------------------------------------


def _scripted_registry_entries(cases: Mapping[str, Any]) -> dict[str, str]:
    """Every provider-scripts case KEY whose entry is genuinely SCRIPTED --
    kind ``"fault"`` or ``"decisions"`` -- excluding the ``"delegate_default"``
    string sentinel, which is not a per-case script at all.

    Takes the registry's own ``cases`` mapping as a parameter rather than
    loading ``role-legacy_agent.json`` itself, so the cross-check below can
    be proven against a synthetic mapping without editing the real file --
    that file's identity digest is pinned by the in-flight armed run for the
    duration of this ticket (see the PR notes) and must not be touched,
    planted defect or not.
    """

    scripted: dict[str, str] = {}
    for case_id, entry in cases.items():
        kind = entry if isinstance(entry, str) else entry.kind
        if kind in ("fault", "decisions"):
            scripted[case_id] = kind
    return scripted


def _corpus_case_ids() -> frozenset[str]:
    import json

    return frozenset(
        json.loads(path.read_text(encoding="utf-8"))["id"]
        for path in sorted(_CORPUS_DIR.glob("case-*.json"))
    )


def _scripted_registry_violations(
    cases: Mapping[str, Any], corpus_ids: frozenset[str]
) -> list[str]:
    """CHAOS-3546's derived replacement for the retired ``fault_ref`` field.

    ``fault_ref`` asked, per case, "if I am declared scripted, does the
    registry actually have an entry for me" -- redundant, because the
    field's value was always the case's own ``id`` (measured: true in all 18
    cases that declared it, no exceptions), so that question and "does the
    registry have an entry named <this case's id>" were the same question
    asked twice through an extra field. This asks the only question that
    needed asking, from the side that actually holds authoritative
    information -- the registry -- with no field on the case at all: for
    every provider-scripts key that is genuinely scripted, does a corpus
    case with that id exist?

    A registry entry with no corresponding corpus case is dead script: the
    fault/decision it encodes can never fire in production, because nothing
    ever asks the engine a question that fingerprints to it
    (``provider_scripts.question_fingerprint`` is the only routing
    mechanism, driven entirely by corpus case question text).
    """

    return [
        f"{case_id}: provider-scripts has a {kind!r}-kind scripted entry for "
        f"this id, but no corpus case declares it -- dead script, can never "
        f"fire"
        for case_id, kind in sorted(_scripted_registry_entries(cases).items())
        if case_id not in corpus_ids
    ]


def _live_role_script_cases() -> Mapping[str, Any]:
    from dev_health_ops.llm.agent.provider_scripts import load_role_script

    return load_role_script("legacy_agent").cases


def test_scripted_registry_cross_check_catches_a_planted_mismatch() -> None:
    """RED-first proof the cross-check is not vacuously green: a synthetic
    registry entry naming a case id that does not exist must be caught.
    Uses a FAKE mapping, never the real ``role-legacy_agent.json`` -- see
    ``_scripted_registry_entries`` for why that file stays untouched here.
    """

    fake_cases = {"chaos_3546_dead_script_entry_no_such_case": "fault"}
    violations = _scripted_registry_violations(fake_cases, _corpus_case_ids())
    assert violations == [
        "chaos_3546_dead_script_entry_no_such_case: provider-scripts has a "
        "'fault'-kind scripted entry for this id, but no corpus case "
        "declares it -- dead script, can never fire"
    ]


def test_scripted_registry_population_floor() -> None:
    """Sanity floor for the test below: if this drops to zero, that test
    would pass VACUOUSLY. CHAOS-3546 measured 20 real fault/decisions-kind
    entries (10 fault, 10 decisions -- 2 more decisions-kind entries than
    the 18 cases that used to declare the now-retired ``fault_ref``, because
    that field was never completely applied by authors even while it
    existed -- ``pers.clarification-persistence`` and ``scope.ambiguous``
    were always scripted without ever declaring it). Asserting >= 20 rather
    than == 20 so a newly-scripted case does not fail this guard for the
    unrelated reason of growing the population.
    """

    assert len(_scripted_registry_entries(_live_role_script_cases())) >= 20


def test_every_scripted_registry_entry_names_a_real_corpus_case() -> None:
    violations = _scripted_registry_violations(
        _live_role_script_cases(), _corpus_case_ids()
    )
    assert violations == [], (
        f"provider-scripts declares {len(violations)} scripted entry(ies) "
        "with no corresponding corpus case:\n" + "\n".join(violations)
    )


# ---------------------------------------------------------------------------
# The general registry (CHAOS-3546's own ask: generalize #1567's guard)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FixtureField:
    name: str
    intended_purpose: str
    #: Human-pointer to the evidence, shown in a failure message -- not
    #: executed, just so a reader does not have to re-derive where the
    #: consumer lives from a bare boolean.
    consumer_location: str
    consumer_check: Callable[[], bool]


#: Every fixture field this repo has declared operationally meaningful,
#: closed and explicit -- adding a field here without a real
#: ``consumer_check`` is exactly the failure mode this module exists to
#: catch, so there is no default/permissive fallback: an entry with no
#: verifiable consumer FAILS, on purpose, until it is wired or removed from
#: the fixture and this table together.
#: ``fault_ref`` is deliberately NOT here -- it is retired, not wired (see
#: the module docstring and the derived cross-check above); a field with no
#: field left to check does not belong in a registry of fields.
FIXTURE_FIELD_REGISTRY: tuple[FixtureField, ...] = (
    FixtureField(
        name="ASK_DEV_SCRIPTED_PROVIDER_SCRIPTS_DIR",
        intended_purpose=(
            "env var selecting the provider-scripts directory the scripted "
            "engine loads at runtime"
        ),
        consumer_location="src/dev_health_ops/llm/agent/provider_scripts.py::_scripts_dir",
        consumer_check=lambda: field_has_external_consumer(
            "ASK_DEV_SCRIPTED_PROVIDER_SCRIPTS_DIR"
        ),
    ),
    # Positive controls (CHAOS-3546 codex-round pattern: a guard proven only
    # by fields it FAILS on proves less than one also proven by fields it
    # PASSES on for a real, checked reason) -- fields CASE-SCHEMA.v1.md
    # documents as load-bearing, with their own real readers.
    FixtureField(
        name="resolution_profile_ref",
        intended_purpose="selects which resolution-profiles/*.json block supplies the case's expected outcome",
        consumer_location="scripts/acceptance/corpus/case_schema.py::resolve_case_expectations",
        consumer_check=lambda: field_has_external_consumer("resolution_profile_ref"),
    ),
    FixtureField(
        name="org_alias",
        intended_purpose="selects which world.json org the runner authenticates the case's principal against",
        consumer_location="scripts/acceptance/corpus/principals.py",
        consumer_check=lambda: field_has_external_consumer("org_alias"),
    ),
)


@pytest.mark.parametrize("field", FIXTURE_FIELD_REGISTRY, ids=lambda field: field.name)
def test_declared_configuration_field_has_a_consumer(field: FixtureField) -> None:
    assert field.consumer_check(), (
        f"{field.name!r} ({field.intended_purpose}) has no consumer outside "
        f"the fixture files -- expected one at {field.consumer_location}. "
        "A declared configuration field with no reader makes a scenario "
        "look configured when nothing configures it (CHAOS-3546)."
    )


def test_registry_names_are_unique() -> None:
    names = [field.name for field in FIXTURE_FIELD_REGISTRY]
    assert len(names) == len(set(names))


def test_field_has_external_consumer_is_not_vacuously_true() -> None:
    """Negative control: a name that names nothing must fail the check --
    proves ``field_has_external_consumer`` can say no, not only yes. Also
    pins the anti-self-reference guard directly: this module's OWN source
    contains the literal needle below (right here, in this string), and the
    check must still return ``False`` because ``_EXCLUDED_PATHS`` excludes
    this file from counting as its own consumer.
    """

    needle = "chaos_3546_field_that_names_nothing_real"
    assert field_has_external_consumer(needle) is False
    assert needle in Path(__file__).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# provider_profile_override -- RETIRED (CHAOS-3546). Removed from world.json
# outright rather than merely left unconsumed: wiring it means inventing a
# per-user provider-override concept this repo has nowhere today (no DB
# column, no migration, no settings-table write in the acceptance
# world-builder) -- a product-shape decision parked on CHAOS-3588, not a
# plumbing fix.
# ---------------------------------------------------------------------------


def test_provider_profile_override_is_retired_from_world_json() -> None:
    """Deliberately NOT in :data:`FIXTURE_FIELD_REGISTRY` -- there is no
    field left to check a consumer for. Asserts absence directly (the field
    is gone from world.json, not merely unread) so a future reintroduction
    is caught precisely, rather than re-running the generic external-
    consumer scan on a field that no longer exists to scan for.
    """

    import json

    world = json.loads((_WORLD_DIR / "world.json").read_text(encoding="utf-8"))
    offenders = [
        user["alias"]
        for user in world.get("users", [])
        if "provider_profile_override" in user
    ]
    assert offenders == [], (
        f"provider_profile_override reappeared on user(s) {offenders} -- "
        "either wire it for real (move it into FIXTURE_FIELD_REGISTRY with "
        "a consumer_check) or keep it retired"
    )


def test_the_provider_profile_override_cases_are_declared_blocked() -> None:
    """The other half of retiring ``provider_profile_override``: every
    corpus case it used to drive must not stay silently ``active`` and
    green on a mechanism that no longer exists (and, before this ticket,
    never existed in production either). CHAOS-3546's initial scope named
    two -- ``deg.provider.unsupported`` and ``readiness.capabilities.
    unsupported-model`` -- both found PASSING while proving nothing about
    unsupported-model handling; a THIRD, ``readiness.capabilities.degraded``
    (the ``degraded-profile`` companion, via ``primary.degraded-readiness-
    user``), was found the same way while editing its siblings and is
    covered here too rather than left as a residual gap.
    ``declared-blocked`` on CHAOS-3588 (the ticket that must build a real
    denial mechanism and re-author all three) is the honest terminal state,
    not a silent pass.
    """

    import json

    for case_id in (
        "deg.provider.unsupported",
        "readiness.capabilities.unsupported-model",
        "readiness.capabilities.degraded",
    ):
        path = _CORPUS_DIR / f"case-{case_id}.json"
        raw = json.loads(path.read_text(encoding="utf-8"))
        assert raw["status"] == "declared-blocked", (
            f"{case_id}: status={raw['status']!r}, expected declared-blocked "
            "-- its mechanism (provider_profile_override) was retired by "
            "CHAOS-3546 with nothing yet replacing it (CHAOS-3588)"
        )
        assert raw["blocked_by"] == "CHAOS-3588", (
            f"{case_id}: blocked_by={raw['blocked_by']!r}, expected 'CHAOS-3588'"
        )


# ---------------------------------------------------------------------------
# kill_switches -- RETIRED (CHAOS-3546, found by the Phase 4d lane). CHAOS-3458
# (Done) already ruled the granular kill-switch concept superseded by the
# single emergency_disabled mechanism; no corpus case references this field,
# so retiring it is a clean deletion with no dependent to flip.
# ---------------------------------------------------------------------------


def test_kill_switches_is_retired_from_world_json() -> None:
    """Asserts absence directly (the field is gone from every world.json org
    entry, not merely unread) so a future reintroduction is caught precisely.
    See the module docstring for the CHAOS-3458 citation.
    """

    import json

    world = json.loads((_WORLD_DIR / "world.json").read_text(encoding="utf-8"))
    offenders = [
        org["alias"] for org in world.get("orgs", []) if "kill_switches" in org
    ]
    assert offenders == [], (
        f"kill_switches reappeared on org(s) {offenders} -- CHAOS-3458 ruled "
        "the granular kill-switch concept superseded by emergency_disabled; "
        "either wire this for real (move it into FIXTURE_FIELD_REGISTRY with "
        "a consumer_check) or keep it retired"
    )

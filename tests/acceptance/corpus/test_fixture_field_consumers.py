"""CHAOS-3546: a fixture field that reads as CONFIGURATION must be consumed.

Three confirmed instances of the same class (the ticket's own count):

* ``ASK_DEV_SCRIPTED_PROVIDER_SCRIPTS_DIR`` -- set nowhere, read only from
  scrub lists, while the container silently fell back to a relative path
  that did not exist. Cost a full armed run: 140 receipts, reported green,
  19 scripted cases exercised nothing. Fixed in ops #1567, whose own guard
  (``provider_scripts.role_script_identity_digest``) proves the CONTAINER
  loaded the intended script -- but that guard is narrow to one env var and
  one digest, not a general property of the fixture surface.
* ``fault_ref`` -- declared on 18 corpus cases, read by zero code paths
  (fault selection is entirely by ``question_fingerprint``). Worse,
  internally inconsistent: 10 of the 18 point at a ``kind: "fault"``
  provider-scripts entry, the other 8 at ``kind: "decisions"``.
* ``provider_profile_override`` -- declared on two world.json users,
  referenced nowhere. Tracked as an open decision (see the module docstring
  note near the bottom); not yet in :data:`FIXTURE_FIELD_REGISTRY` because
  wiring it means inventing a new per-user override concept this repo does
  not have anywhere today (no DB column, no fixture-seeding code even
  writes it) -- a product-shape call, not a plumbing fix, and out of this
  guard's scope until that call is made.

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
``.get("fault_ref")``/attribute read from prose that merely mentions the
field, and to stop a field from certifying itself by appearing in its own
registry row. ``fault_ref`` is the one exception, and deliberately so: its
real consumer (the correctness check below) lives IN this excluded module,
so its ``consumer_check`` calls that function directly instead of grepping
for it -- see the comment on its registry entry.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

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
# fault_ref -- the wire decision (CHAOS-3546)
# ---------------------------------------------------------------------------


def _fault_ref_cases() -> list[dict]:
    import json

    cases = []
    for path in sorted(_CORPUS_DIR.glob("case-*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        if raw.get("fault_ref") is not None:
            cases.append(raw)
    return cases


def _fault_ref_violations() -> list[str]:
    """Every way a declared ``fault_ref`` can fail to name a real,
    correctly-shaped scripted-engine entry.

    This IS ``fault_ref``'s consumer (CHAOS-3546's wire decision): the field
    was never meant to drive request-time routing -- that is
    ``question_fingerprint``'s job, unconditionally, and changing it is a
    much larger, riskier change than this ticket is about. What was missing
    is any assertion that the field TELLS THE TRUTH about the case it is
    declared on. Three things must hold for every non-null ``fault_ref``:

    1. its value equals the case's own ``id`` -- true today in all 18 cases,
       with no exceptions; a future case that declares something else is a
       real authoring bug this catches;
    2. a ``provider-scripts/role-legacy_agent.json`` entry exists for that
       id;
    3. that entry's ``kind`` is ``"fault"`` or ``"decisions"`` -- widened
       from the schema doc's original "fault case only" wording, because 8
       of the 18 (every ``adv.injection-request.*`` plus
       ``scope.prohibited-write``/``scope.unsupported-request``) are
       legitimately decision-scripted refusals, not fault injections, and
       always have been; the field's job is "this case is scripted, not
       left to the unscripted default heuristic", which both kinds satisfy.
    """

    from dev_health_ops.llm.agent.provider_scripts import load_role_script

    role_script = load_role_script("legacy_agent")
    violations: list[str] = []
    for raw in _fault_ref_cases():
        case_id = raw["id"]
        fault_ref = raw["fault_ref"]
        if fault_ref != case_id:
            violations.append(
                f"{case_id}: fault_ref={fault_ref!r} does not equal the case's own id"
            )
            continue
        entry = role_script.cases.get(case_id)
        if entry is None:
            violations.append(
                f"{case_id}: fault_ref is set but role-legacy_agent.json has no "
                f"entry for this case id"
            )
            continue
        kind = "delegate_default" if isinstance(entry, str) else entry.kind
        if kind not in ("fault", "decisions"):
            violations.append(
                f"{case_id}: fault_ref is set but the scripted-engine entry's "
                f"kind is {kind!r}, not 'fault' or 'decisions' -- this case "
                f"would run against the unscripted default heuristic despite "
                f"declaring a fault_ref"
            )
    return violations


def test_fault_ref_is_declared_on_at_least_the_measured_population() -> None:
    """Sanity floor for the two tests below: if this drops to zero, they
    would pass VACUOUSLY (an empty violation list from an empty case list),
    which would read as "fault_ref is fine" while actually meaning "fault_ref
    was never checked". CHAOS-3546 measured exactly 18; asserting >= 18
    rather than == 18 so a newly-authored fault/decisions case does not fail
    this guard for the unrelated reason of growing the population.
    """

    assert len(_fault_ref_cases()) >= 18


def test_every_declared_fault_ref_names_a_real_scripted_engine_entry() -> None:
    violations = _fault_ref_violations()
    assert violations == [], (
        "fault_ref is declared but does not correctly name a scripted-engine "
        f"entry for {len(violations)} case(s):\n" + "\n".join(violations)
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
FIXTURE_FIELD_REGISTRY: tuple[FixtureField, ...] = (
    FixtureField(
        name="fault_ref",
        intended_purpose=(
            "corpus case field naming the provider-scripts entry that makes "
            "this a scripted fault/decisions case rather than the unscripted "
            "default heuristic"
        ),
        consumer_location=(
            "tests/acceptance/corpus/test_fixture_field_consumers.py::"
            "_fault_ref_violations (this module)"
        ),
        # Not `field_has_external_consumer`: this field's own consumer lives
        # IN this module (`_fault_ref_violations`, tested above), which
        # `_EXCLUDED_PATHS` deliberately excludes from the generic scan so a
        # field cannot certify itself merely by being named in its own
        # registry entry below. The real proof for `fault_ref` specifically
        # is that the dedicated consistency check runs and finds nothing
        # wrong -- which is exactly what
        # `test_every_declared_fault_ref_names_a_real_scripted_engine_entry`
        # already asserts; re-running it here keeps `fault_ref` visible in
        # the SAME registry every other tracked field is enumerated from,
        # rather than being a special case a reader has to know to look for
        # elsewhere.
        consumer_check=lambda: _fault_ref_violations() == [],
    ),
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
    """Negative control: a name that names nothing must fail the check the
    same way ``provider_profile_override`` currently does (see the module
    docstring) -- proves ``field_has_external_consumer`` can say no, not
    only yes. Also pins the anti-self-reference guard directly: this
    module's OWN source contains the literal needle below (right here, in
    this string), and the check must still return ``False`` because
    ``_EXCLUDED_PATHS`` excludes this file from counting as its own
    consumer.
    """

    needle = "chaos_3546_field_that_names_nothing_real"
    assert field_has_external_consumer(needle) is False
    assert needle in Path(__file__).read_text(encoding="utf-8")


def test_provider_profile_override_is_confirmed_still_unconsumed() -> None:
    """CHAOS-3546's third field, deliberately NOT in
    :data:`FIXTURE_FIELD_REGISTRY` -- see the module docstring for why
    (wiring it means inventing a per-user override concept this repo does
    not have anywhere, a product-shape call pending an explicit decision,
    not a plumbing fix). This test exists so that call is never made
    silently: it fails the day someone adds a real reader (which is the
    correct, loud signal to move the field into the registry above with a
    consumer_check pointing at it), and until then it pins the exact,
    current, measured state rather than leaving the gap undocumented in
    executable form.
    """

    assert field_has_external_consumer("provider_profile_override") is False

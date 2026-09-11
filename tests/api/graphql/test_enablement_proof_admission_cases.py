"""The Python half of the shared enablement-admission pin (CHAOS-5484).

``internal/goapiproof/enablement_predicate_integration_test.go`` is the
other half. Both drive their own PRODUCTION predicate over
``tests/fixtures/enablement_proof_admission_cases.json``.

Why a shared table rather than a text comparison of the two statements.
The two readers implement the same rule in deliberately different SQL:
this side composes SQLAlchemy column expressions and matches the
operation/document pairs as a tuple-OR; the Go side writes raw SQL and
matches them with ``JOIN unnest(...)``. Neither shape is incidental -- a
cross-product ``IN (...) AND ... IN (...)`` would admit a proof recorded
against a DIFFERENT registered document -- so the statements can never be
byte-equal, and pinning their text would pin a proxy. Pinning behaviour
also catches a divergence that identical text could still produce, since
an ``ARRAY`` predicate does not necessarily behave the same through the
ORM's type coercion as through raw SQL.

Neither test restates the rule, so neither can pass by agreeing with
itself. A case the two answer differently fails on whichever side is
wrong, and adding a case to the fixture changes the rule for both.

Requires ``DEV_HEALTH_POSTGRES_TEST_URI``: ``cardinality()`` over a
``text[]`` and the 4-column composite FK are PostgreSQL, and a fake
session would exercise the call shape while skipping every property that
matters. This file skipping silently is exactly the trap that let a whole
class of hardcoded-alembic-head failures reach CI, so the run that proves
it must be a real-Postgres one.
"""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.graphql.go_api_routing_admin import (
    operations_with_enablement_proof,
)
from dev_health_ops.models.go_api_registry import CandidateBuild, ProofRun

POSTGRES_TEST_URI = os.environ.get("DEV_HEALTH_POSTGRES_TEST_URI")

#: Applied to the DATABASE-BACKED tests only, by decorating them.
#:
#: It used to be ``pytestmark``, i.e. module-level, which took the
#: data-only assertions down with them: on a runner without the URI the
#: whole cross-language guarantee evaporated with a green run of 25 skips.
#: The fixture-shape checks read a JSON file and need no
#: database, so they now run everywhere -- a malformed or unexplained case
#: is caught on any runner, and only the rows-against-Postgres half is
#: conditional.
requires_postgres = pytest.mark.skipif(
    not POSTGRES_TEST_URI,
    reason=(
        "Requires DEV_HEALTH_POSTGRES_TEST_URI: the admission predicate uses "
        "cardinality() over a Postgres text[] and a 4-column composite FK, "
        "neither of which has an in-memory substitute."
    ),
)

_FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "enablement_proof_admission_cases.json"
)


def _load() -> dict[str, Any]:
    with _FIXTURE.open() as handle:
        return json.load(handle)


def _merged_receipt(document: dict[str, Any], case: dict[str, Any]) -> dict[str, Any]:
    """Case values over fixture defaults.

    A case that says ``"measurement_route": null`` MEANS null -- that is
    the pre-0128 row shape -- so presence of the key decides, never
    truthiness of the value.
    """
    merged = dict(document["receipt_defaults"])
    merged.update(case.get("receipt", {}))
    return merged


def _merged_key(document: dict[str, Any], case: dict[str, Any]) -> dict[str, Any]:
    merged = dict(document["key"])
    merged.update(case.get("key_override") or {})
    return merged


def _asked_key(document: dict[str, Any], case: dict[str, Any]) -> dict[str, Any]:
    """The key the predicate is ASKED about.

    The fixture key, unless the case sets ``ask_with_overridden_key``: then
    the question uses the overridden key too. Without that, a key override
    can only test "a receipt for a different key is not proof" -- refused by
    equality -- and a clause about the key's own VALUE (a blank
    ``candidate_build``) is never reached.
    """
    if case.get("ask_with_overridden_key"):
        return _merged_key(document, case)
    return dict(document["key"])


def _cases() -> list[dict[str, Any]]:
    return _load()["cases"]


def _control_of(case: dict[str, Any]) -> dict[str, Any]:
    """The refused ``case`` with exactly its stated reason removed.

    ``match_route_null_canary_refused`` carried a NULL
    ``build_binding`` beside the NULL route it is named for. Once the
    binding joined the rule, the binding alone refused it, so the case
    pinned nothing about the route -- the canary NULL-route clause could be
    replaced with ``sa.true()`` and every Python test stayed green. A
    table-level "the base is admissible" control cannot see that; the base
    WAS admissible. So every refused case names what removes its reason,
    and this control -- the case with exactly that removed -- must be
    ADMITTED. A hidden second reason then fails in the case's own name.

    ``control.receipt`` patches the case's receipt. ``control.key_override``,
    when present, REPLACES the case's override (``null`` = none).
    """
    control = case["control"]
    derived = dict(case)
    del derived["control"]
    derived["name"] = case["name"] + "/control"
    derived["admits"] = True
    derived["why"] = (
        "CONTROL: the case with its stated refusal reason removed must be "
        "ADMITTED; refused means the case is refused for a SECOND reason and "
        "does not pin the one it names. Case why: " + case["why"]
    )
    receipt = dict(case.get("receipt", {}))
    receipt.update(control.get("receipt", {}))
    derived["receipt"] = receipt
    if "key_override" in control:
        if control["key_override"] is None:
            derived.pop("key_override", None)
        else:
            derived["key_override"] = control["key_override"]
    return derived


def _runs() -> list[dict[str, Any]]:
    """Every case, plus the control of every refused case."""
    runs: list[dict[str, Any]] = []
    for case in _cases():
        runs.append(case)
        if not case["admits"]:
            runs.append(_control_of(case))
    return runs


def _case_ids() -> list[str]:
    return [case["name"] for case in _runs()]


async def _seed(
    session: AsyncSession, key: dict[str, Any], receipt: dict[str, Any]
) -> None:
    """Insert DIRECTLY, not through ``record_proof_run``.

    Deliberate: rows with a NULL ``measurement_route`` or NULL
    ``build_binding`` exist in this table -- every row written before
    alembic 0128/0129 has them -- and the predicate's job is to read the
    table as it really is, not as a current writer would leave it.
    """
    await session.execute(
        pg_insert(CandidateBuild)
        .values(
            schema_digest=key["schema_digest"],
            document_digest=key["document_digest"],
            selected_operation=key["selected_operation"],
            candidate_build=key["candidate_build"],
        )
        .on_conflict_do_nothing()
    )
    await session.execute(
        sa.insert(ProofRun).values(
            id=uuid.uuid4(),
            schema_digest=key["schema_digest"],
            document_digest=key["document_digest"],
            selected_operation=key["selected_operation"],
            candidate_build=key["candidate_build"],
            request_identity=receipt["request_identity"],
            stage=receipt["stage"],
            terminal_state=receipt["terminal_state"],
            measurement_route=receipt["measurement_route"],
            build_binding=receipt["build_binding"],
            baseline_defect=receipt["baseline_defect"],
            differences_outside_baseline_defect=receipt[
                "differences_outside_baseline_defect"
            ],
        )
    )
    await session.commit()


@requires_postgres
@pytest.mark.asyncio
@pytest.mark.parametrize("case", _runs(), ids=_case_ids())
async def test_predicate_matches_the_shared_admission_table(
    session: AsyncSession, case: dict[str, Any]
) -> None:
    document = _load()
    key = _merged_key(document, case)
    await _seed(session, key, _merged_receipt(document, case))

    wanted = _asked_key(document, case)
    admitted = await operations_with_enablement_proof(
        session,
        schema_digest=wanted["schema_digest"],
        candidate_build=wanted["candidate_build"],
        operations={wanted["selected_operation"]: wanted["document_digest"]},
        target_mode=case["target_mode"],
    )
    got = wanted["selected_operation"] in admitted
    assert got == case["admits"], (
        f"case {case['name']!r}: predicate returned admits={got}, fixture "
        f"says {case['admits']}.\nwhy: {case['why']}"
    )


def test_the_shared_table_asserts_in_both_directions() -> None:
    """A one-sided table passes against a predicate that is constant.

    Admissions only would pass against one that admits everything;
    refusals only against one that admits nothing. Requiring both is what
    stops this pin holding vacuously -- and it runs without a database, so
    it still fails in the environments where the parametrized tests skip.
    """
    cases = _cases()
    admits = sum(1 for case in cases if case["admits"])
    refuses = len(cases) - admits
    assert admits and refuses, (
        f"the shared admission table must contain both admitted and refused "
        f"cases (admits={admits} refuses={refuses})"
    )


def test_every_case_states_why() -> None:
    """A case with no stated reason is a case nobody can review.

    The failure message quotes ``why`` verbatim, so an empty one turns a
    real regression into an unexplained boolean.
    """
    for case in _cases():
        assert case.get("why", "").strip(), (
            f"case {case['name']!r} has no 'why': the assertion message is "
            "the only place a future reader learns what the rule is for"
        )


def test_every_refused_case_names_the_control_that_removes_its_reason() -> None:
    """A refused case without a control can be refused for the wrong reason.

    Runs without a database, so the SHAPE is enforced on every runner even
    where the database-backed half skips. The control itself is executed
    by ``test_predicate_matches_the_shared_admission_table`` (as
    ``<case>/control``) and by the Go half.
    """
    for case in _cases():
        if case["admits"]:
            assert "control" not in case, (
                f"case {case['name']!r} is ADMITTED and declares a control: a "
                "control removes a refusal reason, and an admitted case has none"
            )
            continue
        control = case.get("control")
        assert isinstance(control, dict) and control, (
            f"refused case {case['name']!r} declares no control, so nothing shows "
            "it is refused for the reason it names rather than for another"
        )
        assert set(control) <= {"receipt", "key_override"}, (
            f"control of {case['name']!r} has unknown keys {sorted(control)}"
        )
        derived = _control_of(case)
        assert (derived.get("receipt"), derived.get("key_override")) != (
            case.get("receipt"),
            case.get("key_override"),
        ), f"control of {case['name']!r} changes nothing, so it cannot remove a reason"
        if case.get("ask_with_overridden_key"):
            assert case.get("key_override"), (
                f"case {case['name']!r} asks with an overridden key it does not declare"
            )


def test_the_cutset_is_the_one_the_shared_fixture_states() -> None:
    """The Python half of the one-definition pin.

    ``_BLANK_CUTSET`` is bound as a parameter, so it is version-independent
    -- but a rune added to Go's constant and not here would still split the
    engines. Both sides are asserted equal to the value the fixture states.
    """
    from dev_health_ops.api.graphql.go_api_routing_admin import _BLANK_CUTSET

    stated = _load().get("blank_cutset")
    assert stated is not None, "the shared fixture states no blank_cutset"
    assert _BLANK_CUTSET == stated, (
        f"_BLANK_CUTSET is {_BLANK_CUTSET!r} but the shared fixture states "
        f"{stated!r}: Go and Python would disagree about which citations name "
        "nothing"
    )

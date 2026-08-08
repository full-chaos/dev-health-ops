"""CHAOS-3536: the QUA schema PRODUCTION SENDS must satisfy OpenAI strict mode.

The QUA path was non-functional against every real OpenAI-compatible provider:
``schema_version`` was declared with ``const`` and no ``type``, and strict
structured-output mode rejects the request outright::

    BadRequestError: 400 - Invalid schema for response_format
    'dev_question_understanding': In context=('properties', 'schema_version'),
    schema must have a 'type' key.

CHAOS-3389's shadow therefore collected ZERO real proposal evidence (every
run recorded ``SKIPPED_PROVIDER_ERROR``/``invalid_request``), and
CHAOS-3525's commit mode could never fire. It fails closed, so the shipped
capability was inert rather than dangerous -- but inert.

WHY NO TEST CAUGHT IT, AND WHY THIS MODULE TESTS WHAT IT TESTS
--------------------------------------------------------------
``ScriptedAgentProvider`` accepts any ``response_schema`` without validating
it, so every QUA test -- including CHAOS-3389's never-widen proofs and
CHAOS-3525's commit tests -- passed against a schema no real provider would
accept. The schema's correctness was asserted nowhere.

The obvious repair is to assert on ``_response_schema``'s return value. That
would be the SAME MISTAKE ONE LEVEL UP, because **the generator's output is
not what production sends.** ``OpenAICompatibleAgentProvider.decide()``
nests our schema under ``properties.value`` via ``_decision_response_schema``
and then projects the whole tree through ``_structural_schema``, which drops
every key outside ``_STRUCTURAL_SCHEMA_KEYS`` and synthesizes
``additionalProperties``/``required``. A raw-dict assertion proves nothing
about the bytes that reach the provider.

So every test here goes through ``build_completion_request`` -- the single
producer ``decide()`` dispatches wholesale -- and asserts on the projected
result. That is the artifact strict mode judges.
"""

from __future__ import annotations

import copy
from typing import Any, cast

import pytest

from dev_health_ops.api.dev.qua_shadow import (
    QUAShadowConfig,
    QuestionUnderstandingShadow,
)
from dev_health_ops.llm.agent.openai_compatible import build_completion_request

#: Counts that bracket the interesting behaviour: none authorized (the
#: never-widen edge), one, a typical shortlist, and the per-mention cap.
_CANDIDATE_COUNTS = (0, 1, 3, 25)

_MODEL = "gpt-5-nano"


def _shadow() -> QuestionUnderstandingShadow:
    """A shadow built only to reach ``_response_schema``.

    The schema builder reads nothing off ``self``; the provider and scope
    service are never touched on this path, so passing ``None`` keeps the
    test honest about what it exercises rather than standing up a scope
    service that plays no part in the result.
    """

    return QuestionUnderstandingShadow(
        provider=None,
        scope_service=cast(Any, None),
        config=QUAShadowConfig(enabled=True),
    )


def _generated_schema(
    *, candidate_count: int, mention_count: int = 1
) -> dict[str, Any]:
    """What the QUA generator produces, BEFORE the provider projects it."""

    return _shadow()._response_schema(
        mention_count=mention_count, candidate_count=candidate_count
    )


def _wire_schema(*, candidate_count: int, mention_count: int = 1) -> dict[str, Any]:
    """The complete schema production puts on the wire for a QUA call.

    Assembled by the product's own request builder, not reconstructed here --
    a hand-built approximation could drift from the real one in exactly the
    way that would hide the next instance of this bug.
    """

    request = build_completion_request(
        model=_MODEL,
        messages=(),
        tools=(),
        response_schema=_generated_schema(
            candidate_count=candidate_count, mention_count=mention_count
        ),
        max_output_tokens=6000,
    )
    return cast(dict[str, Any], request["response_format"]["json_schema"]["schema"])


def _qua_branch(wire: dict[str, Any]) -> dict[str, Any]:
    """The QUA schema's own subtree inside the decision envelope."""

    return cast(dict[str, Any], wire["properties"]["value"]["anyOf"][0])


#: A node declares its type when it has ``type`` or defers via a combinator.
_TYPE_DECLARING_KEYS = frozenset({"type", "anyOf", "allOf", "oneOf", "$ref"})


def strict_mode_violations(schema: Any, path: str = "$") -> list[str]:
    """Every OpenAI strict-mode STRUCTURAL rule this schema breaks.

    The rules:

    1. every object node declares ``type: "object"``;
    2. every object node sets ``additionalProperties: false``;
    3. every object node's ``required`` names EVERY property -- strict mode
       has no optional properties, so optionality is expressed as a nullable
       type, never by omission from ``required``;
    4. every SCHEMA NODE declares a type (or defers to a combinator).
       ``const`` alone is not a type declaration, which is the entire bug.

    Review round 3 (medium, reproduced before fixing): rule 4 used to be
    applied only to entries under ``properties``. A node reached through
    ``items`` was recursed into but never checked itself, so deleting
    ``type`` from ``candidate_indices.items`` left this function returning
    ``[]`` -- the validator had a false-pass path of exactly the kind it
    exists to catch. It now checks every position a schema can occupy, and
    ``test_the_validator_detects_each_rule_it_claims_to_check`` plants a
    defect in an array item specifically.
    """

    violations: list[str] = []
    if not isinstance(schema, dict):
        return violations

    # Rule 4, applied to THIS node. The caller decides whether this position
    # requires a type at all -- only the positions visited below do, and the
    # root is entered via the object rules rather than as a bare value.
    properties = schema.get("properties")
    if isinstance(properties, dict):
        if schema.get("type") != "object":
            violations.append(f"{path}: has 'properties' but type is not 'object'")
        if schema.get("additionalProperties") is not False:
            violations.append(f"{path}: 'additionalProperties' is not false")
        declared = set(properties)
        required = set(schema.get("required") or ())
        if declared != required:
            violations.append(
                f"{path}: 'required' must name every property; "
                f"missing={sorted(declared - required)} "
                f"unknown={sorted(required - declared)}"
            )
        for name, definition in properties.items():
            violations.extend(
                _schema_position_violations(definition, f"{path}.properties.{name}")
            )

    if "items" in schema:
        violations.extend(_schema_position_violations(schema["items"], f"{path}.items"))
    for key in ("anyOf", "allOf", "oneOf"):
        branches = schema.get(key)
        if isinstance(branches, list):
            for index, branch in enumerate(branches):
                violations.extend(
                    _schema_position_violations(branch, f"{path}.{key}[{index}]")
                )
    defs = schema.get("$defs")
    if isinstance(defs, dict):
        for name, definition in defs.items():
            violations.extend(
                _schema_position_violations(definition, f"{path}.$defs.{name}")
            )
    return violations


def _schema_position_violations(node: Any, path: str) -> list[str]:
    """Check a node that occupies a schema position, then recurse into it.

    Every such position must declare a type. Split out from
    ``strict_mode_violations`` so that ``properties`` entries, array
    ``items`` and combinator branches are all held to the same rule --
    keeping the check inline for properties only is what let a typeless
    array item through.
    """

    violations: list[str] = []
    if isinstance(node, dict) and not (_TYPE_DECLARING_KEYS & set(node)):
        violations.append(
            f"{path}: no type key (has {sorted(node)}) -- "
            "strict mode requires one on every schema node"
        )
    violations.extend(strict_mode_violations(node, path))
    return violations


# ---------------------------------------------------------------------------
# The validator must be able to fail. Proven before it is trusted.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("mutate", "expected_fragment"),
    [
        pytest.param(
            lambda node: node.pop("type"),
            "type is not 'object'",
            id="object-without-type",
        ),
        pytest.param(
            lambda node: node.update({"additionalProperties": True}),
            "'additionalProperties' is not false",
            id="additional-properties-allowed",
        ),
        pytest.param(
            lambda node: node.update({"required": ["schema_version"]}),
            "must name every property",
            id="required-omits-a-property",
        ),
        pytest.param(
            lambda node: node["properties"]["intent_id"].pop("type"),
            "no type key",
            id="property-without-type",
        ),
        pytest.param(
            lambda node: node["properties"]["mentions"]["items"]["properties"][
                "candidate_indices"
            ]["items"].pop("type"),
            "no type key",
            id="array-item-without-type",
        ),
        # Deliberately NOT a case: popping ``items`` from ``mentions``. That
        # leaves ``{"type": "array"}``, which breaks no rule this validator
        # claims to check (arrays-must-declare-items is a real strict-mode
        # rule, but not one of the four documented above). Asserting a
        # violation there would record an INVALID mutation as a kill.
    ],
)
def test_the_validator_detects_each_rule_it_claims_to_check(
    mutate, expected_fragment: str
) -> None:
    """A validator that cannot fail would certify the bug as fixed.

    Each strict-mode rule is planted as a defect in an otherwise-valid wire
    schema, and the validator must name it. Without this, a typo in
    ``strict_mode_violations`` (an unreachable branch, a wrong key) would
    make every other test in this module vacuously green -- which is the
    precise failure mode CHAOS-3536 exists to correct, so repeating it here
    would be indefensible.
    """

    wire = _wire_schema(candidate_count=3)
    assert strict_mode_violations(wire) == [], (
        "the fixture must start clean or the mutation proves nothing"
    )

    mutated = copy.deepcopy(wire)
    mutate(_qua_branch(mutated))

    found = strict_mode_violations(mutated)
    assert found, f"the validator missed a planted {expected_fragment!r} defect"
    assert any(expected_fragment in violation for violation in found), (
        f"expected a {expected_fragment!r} violation, got {found}"
    )


# ---------------------------------------------------------------------------
# The defect itself.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("candidate_count", _CANDIDATE_COUNTS)
def test_the_schema_production_sends_satisfies_strict_mode(
    candidate_count: int,
) -> None:
    """The class, not the instance.

    RED before the fix at every candidate count with exactly one violation::

        $.properties.value.anyOf[0].properties.schema_version: no type key
        (has ['const']) -- strict mode requires one on every property

    Note the path: the violation is NESTED under ``value``, because that is
    where the provider puts our schema. The ticket's live reproduction
    reported a ROOT-level ``('properties', 'schema_version')`` context, which
    is how we know that probe sent the raw generated schema directly rather
    than going through ``decide()``.
    """

    violations = strict_mode_violations(_wire_schema(candidate_count=candidate_count))

    assert violations == [], (
        "the schema production sends must satisfy strict mode, or every QUA "
        f"call fails invalid_request: {violations}"
    )


@pytest.mark.parametrize("mention_count", (1, 2, 5))
def test_strict_mode_holds_across_mention_counts_too(mention_count: int) -> None:
    """The schema is generated per call from BOTH counts; vary the other one.

    A fix verified at a single shape is a fix verified by coincidence.
    """

    violations = strict_mode_violations(
        _wire_schema(candidate_count=3, mention_count=mention_count)
    )

    assert violations == [], str(violations)


def test_schema_version_is_pinned_by_value_and_not_merely_typed() -> None:
    """The fix must not weaken what the field means.

    Adding ``type`` to satisfy strict mode is trivially achievable by
    dropping ``const`` for a bare ``{"type": "string"}``, which would satisfy
    the validator above while silently letting any version string through.
    The version pin is what makes a parsed proposal a v1 proposal.
    """

    schema_version = _qua_branch(_wire_schema(candidate_count=3))["properties"][
        "schema_version"
    ]

    assert schema_version.get("type") == "string"
    assert schema_version.get("const") == "dev_question_understanding.v1", (
        "the version must still be pinned by value, not merely typed"
    )


# ---------------------------------------------------------------------------
# Never-widen, as it survives the projection.
# ---------------------------------------------------------------------------


def test_no_candidate_index_is_expressible_when_none_were_authorized() -> None:
    """The zero-candidate re-encoding, and the reason it had to change.

    The generator expressed "no candidate may be selected" as the empty
    integer range ``{"minimum": 0, "maximum": -1}``. That never reached a
    provider: ``_structural_schema`` drops both keywords, so the wire schema
    said ``{"type": ["integer", "null"]}`` -- **any** integer expressible,
    with nothing authorized.

    ``{"type": "null"}`` carries the same never-widen intent through the
    projection, and stays parseable because the contract field is
    ``_StrictIndex | None``.
    """

    selected = _qua_branch(_wire_schema(candidate_count=0))["properties"]["mentions"][
        "items"
    ]["properties"]["selected_candidate_index"]

    assert selected == {"type": "null"}, (
        "with nothing authorized, no integer index may be expressible on the "
        f"wire; got {selected}"
    )


def test_a_selectable_index_is_still_expressible_when_candidates_exist() -> None:
    """The control: the zero-candidate encoding must not leak into real calls.

    A change that returned ``{"type": "null"}`` unconditionally would pass
    the test above while making every QUA proposal impossible -- a quieter
    version of the outage being fixed.
    """

    selected = _qua_branch(_wire_schema(candidate_count=3))["properties"]["mentions"][
        "items"
    ]["properties"]["selected_candidate_index"]

    assert "integer" in selected["type"]
    assert "null" in selected["type"], "no_match must remain expressible"


def test_the_null_encoding_is_call_wide_and_not_per_mention() -> None:
    """The trap this encoding sets, pinned so the next reader does not fall in.

    ``_response_schema`` is built ONCE per call from the COMBINED shortlist,
    so every mention shares one index space. ``candidate_count == 0`` means
    "this CALL authorized nothing", not "this mention's slice is empty".

    A mention past ``max_total_candidates`` gets an empty ``[start, end)``
    slice from ``_combine_shortlists`` while the call-wide count stays
    non-zero -- so it still sees the full range in the schema, and ONLY
    ``_verify``'s per-mention check stops it selecting another mention's
    candidate.

    Written after review caught the author claiming the opposite in
    ``qua_promotion``'s truncation comment. The pre-existing comment there
    made the same error, so this is a mistake the code invites rather than
    one anybody made carelessly, which is exactly why it is worth a test.
    """

    wire = _qua_branch(_wire_schema(candidate_count=50, mention_count=3))
    selected = wire["properties"]["mentions"]["items"]["properties"][
        "selected_candidate_index"
    ]

    assert selected != {"type": "null"}, (
        "a multi-mention call with a non-empty combined shortlist must NOT "
        "get the zero-candidate encoding, however empty an individual "
        "mention's own slice may be"
    )
    assert "integer" in selected["type"]


# ---------------------------------------------------------------------------
# What this schema does NOT guarantee. Stated, because the module docstring
# used to claim the opposite.
# ---------------------------------------------------------------------------


def test_the_range_keywords_still_do_not_survive_the_projection() -> None:
    """Unchanged fact, and the reason CHAOS-3537 had to use ``enum``.

    ``minimum``/``maximum``/``minItems``/``maxItems``/``maxLength`` are still
    absent from ``_STRUCTURAL_SCHEMA_KEYS`` and still stripped before
    dispatch. CHAOS-3537 did not change that -- it changed which keyword the
    bound is written in.

    ``maxItems`` is still emitted by the generator as intent, so this asserts
    the round trip: written by the generator, gone on the wire. If someone
    later widens the allowlist instead, this fails and sends them to
    ``qua_shadow``'s docstring, which says why the enum was chosen over
    exactly that.
    """

    generated_mention = _generated_schema(candidate_count=3)["properties"]["mentions"][
        "items"
    ]["properties"]
    assert generated_mention["candidate_indices"]["maxItems"] == 25, (
        "the generator still states the intent..."
    )

    wire_mention = _qua_branch(_wire_schema(candidate_count=3))["properties"][
        "mentions"
    ]["items"]["properties"]
    assert "maxItems" not in wire_mention["candidate_indices"], (
        "...but it does not reach the wire, so nothing may be claimed on it"
    )
    for keyword in ("minimum", "maximum"):
        assert keyword not in wire_mention["selected_candidate_index"]


def test_candidate_indices_stay_unbounded_when_nothing_was_authorized() -> None:
    """The residual CHAOS-3536 documented is still open, on purpose.

    CHAOS-3537 bounded every index field with an ``enum`` for the non-empty
    case, and could have closed this one too with an EMPTY item enum --
    measured live against OpenAI and accepted. It was dropped on review:
    ``enum: []`` is unsatisfiable, and ``local``/``ollama``/``lmstudio`` are
    certified platform providers whose decoders were never probed. One that
    rejects it turns every zero-candidate call into a provider error, losing
    the no-match evidence the shadow exists to gather.

    ``_verify`` covers this case regardless -- with nothing authorized every
    mention slice is empty, so any index at all is rejected.
    """

    items = _qua_branch(_wire_schema(candidate_count=0))["properties"]["mentions"][
        "items"
    ]["properties"]["candidate_indices"]["items"]

    assert items == {"type": "integer"}, (
        "if this becomes structurally bounded, the certification evidence "
        "for every supported decoder must land with it"
    )

"""The shared wire-integer guards in ``authclient.contracts``."""

from __future__ import annotations

import json

import pytest

from dev_health_ops.authclient.contracts import (
    ContractError,
    refuse_bool,
    refuse_decimal_form,
    require_wire_int,
)

S = "test.v1"


def test_require_wire_int_returns_an_integer_and_none_for_absent_or_null() -> None:
    assert require_wire_int({"n": 403}, "n", S) == 403
    assert require_wire_int({}, "n", S) is None
    assert require_wire_int({"n": None}, "n", S) is None


@pytest.mark.parametrize("raw", ["403.0", "1e2", "0.0"])
def test_require_wire_int_refuses_a_decimal_token_json_hands_back_as_float(
    raw: str,
) -> None:
    document = json.loads(f'{{"n": {raw}}}')
    with pytest.raises(ContractError, match="decimal or exponent form"):
        require_wire_int(document, "n", S)


@pytest.mark.parametrize("value", [True, False])
def test_require_wire_int_refuses_a_boolean_before_the_int_check(value: bool) -> None:
    # bool subclasses int: with the order reversed True would return 1.
    with pytest.raises(ContractError, match="boolean, not an integer"):
        require_wire_int({"n": value}, "n", S)


@pytest.mark.parametrize("value", [True, False])
def test_refuse_bool_names_the_noun_it_was_given(value: bool) -> None:
    with pytest.raises(ContractError, match="boolean, not a revision"):
        refuse_bool(value, S, "n", "a revision")
    refuse_bool(1, S, "n", "a revision")
    refuse_bool(1.5, S, "n", "a revision")


DECIMAL_FORM_TOKENS = [
    "1.0",
    "1e0",
    "1E0",
    "1.0e0",
    "403.0",
    "9007199254740992.0",
    "9007199254740993.0",
    "1e300",
    "1.0000000000000000001",
    "9007199254740990.1",
    "9007199254740990.5",
    "1.0000000000000001",
    "1.5",
    "-1.0",
    "-0.0",
]


@pytest.mark.parametrize("token", DECIMAL_FORM_TOKENS)
def test_refuse_decimal_form_refuses_every_decimal_or_exponent_token(
    token: str,
) -> None:
    # json hands back a float for exactly the tokens with '.', 'e' or 'E'.
    with pytest.raises(ContractError, match="decimal or exponent form"):
        refuse_decimal_form(json.loads(token), S, "n")


@pytest.mark.parametrize("token", ["0", "1", "-1", "9007199254740993"])
def test_refuse_decimal_form_passes_integer_tokens(token: str) -> None:
    refuse_decimal_form(json.loads(token), S, "n")


@pytest.mark.parametrize("token", DECIMAL_FORM_TOKENS)
def test_principal_revision_parsing_refuses_every_decimal_form_token(
    token: str,
) -> None:
    from dev_health_ops.authclient.principal import _parse_revision

    with pytest.raises(ContractError, match="decimal or exponent form"):
        _parse_revision(json.loads(token), "membership_revision")


def test_principal_revision_parsing_composes_the_shared_guards() -> None:
    # A boolean never reaches _parse_revision through validate() (the schema
    # types revisions as integer), so this is the only test that exercises the
    # call site; without it the call can be deleted with the suite staying green.
    from dev_health_ops.authclient.principal import _parse_revision

    with pytest.raises(ContractError, match="boolean, not a revision"):
        _parse_revision(True, "membership_revision")
    # 2**53 + 1 as an integer token stays exact: the only accepted form.
    assert _parse_revision(json.loads("9007199254740993"), "n") == 9007199254740993

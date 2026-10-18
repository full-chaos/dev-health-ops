"""The shared wire-integer guards in ``authclient.contracts``."""

from __future__ import annotations

import json

import pytest

from dev_health_ops.authclient.contracts import (
    MAX_EXACT_FLOAT_INTEGER,
    ContractError,
    refuse_bool,
    refuse_inexact_decimal,
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
    with pytest.raises(ContractError, match="decimal, not an integer"):
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


def test_the_exact_float_bound_is_two_to_the_fifty_third() -> None:
    assert MAX_EXACT_FLOAT_INTEGER == 9007199254740992


@pytest.mark.parametrize(
    "raw",
    ["9007199254740992.0", "9007199254740993.0", "-9007199254740992.0", "1e300"],
)
def test_refuse_inexact_decimal_fires_at_and_past_the_bound(raw: str) -> None:
    # The first value is exactly 2**53: the comparison is >=, because 2**53 + 1
    # parses to the same double and a float cannot say which one arrived.
    with pytest.raises(ContractError, match="past 2\\*\\*53"):
        refuse_inexact_decimal(float(raw), S, "n")


@pytest.mark.parametrize("raw", ["9007199254740991.0", "0.0", "-9007199254740991.0"])
def test_refuse_inexact_decimal_passes_below_the_bound(raw: str) -> None:
    refuse_inexact_decimal(float(raw), S, "n")


def test_principal_revision_parsing_composes_the_shared_guards() -> None:
    # A boolean never reaches _parse_revision through validate() (the schema
    # types revisions as integer), so this is the only test that exercises the
    # call site; without it the call can be deleted with the suite staying green.
    from dev_health_ops.authclient.principal import _parse_revision

    with pytest.raises(ContractError, match="boolean, not a revision"):
        _parse_revision(True, "membership_revision")
    with pytest.raises(ContractError, match="past 2\\*\\*53"):
        _parse_revision(9007199254740992.0, "membership_revision")
    assert _parse_revision(7.0, "membership_revision") == 7

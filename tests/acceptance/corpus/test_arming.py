"""Unit coverage for ``scripts.acceptance.corpus.arming``."""

from __future__ import annotations

import pytest

from scripts.acceptance.corpus.arming import (
    ALLOW_ENV,
    ARM_ENV_VAR,
    ARMED_RUN_ENV_ALLOW_NAMES,
    ArmedButScrubbedError,
    NotArmedError,
    env_allow_value,
    require_armed,
)


class TestRequireArmed:
    def test_armed_is_a_silent_no_op(self) -> None:
        require_armed({"ASK_DEV_LIVE_ACCEPTANCE": "1"})

    def test_missing_raises(self) -> None:
        with pytest.raises(NotArmedError, match="ASK_DEV_LIVE_ACCEPTANCE=1"):
            require_armed({})

    def test_wrong_value_raises(self) -> None:
        with pytest.raises(NotArmedError):
            require_armed({"ASK_DEV_LIVE_ACCEPTANCE": "true"})

    def test_zero_raises(self) -> None:
        with pytest.raises(NotArmedError):
            require_armed({"ASK_DEV_LIVE_ACCEPTANCE": "0"})


class TestArmedButScrubbed:
    """CHAOS-3462 B1: "the shell WAS armed, the scrub ate it" is a third
    state, and it is the only one of the three that must fail loud.

    ``scrubbed_names`` is what ``tests/conftest.py``'s scrub actually
    REMOVED (a name only appears there if it was present and then deleted),
    so its presence is positive evidence the operator armed this run --
    evidence that survives the deletion of the variable itself.
    """

    def test_scrubbed_arming_var_raises_the_loud_error(self) -> None:
        with pytest.raises(ArmedButScrubbedError):
            require_armed({}, scrubbed_names=[ARM_ENV_VAR])

    def test_the_loud_error_is_not_a_not_armed_error(self) -> None:
        """Fail-safe by inheritance: a caller that only knows about
        ``NotArmedError`` (and therefore skips) must NOT accidentally
        swallow this one. If these were parent/child, the existing
        ``except NotArmedError: pytest.skip`` in the runner would keep
        producing the very false green this class exists to kill."""

        assert not issubclass(ArmedButScrubbedError, NotArmedError)
        with pytest.raises(ArmedButScrubbedError):
            try:
                require_armed({}, scrubbed_names=[ARM_ENV_VAR])
            except NotArmedError:  # pragma: no cover - must not be reached
                pytest.fail("ArmedButScrubbedError was caught as NotArmedError")

    def test_the_error_names_the_documented_remedy(self) -> None:
        with pytest.raises(ArmedButScrubbedError, match=ALLOW_ENV):
            require_armed({}, scrubbed_names=[ARM_ENV_VAR])

    def test_unrelated_scrubbed_names_do_not_imply_arming(self) -> None:
        """Only the ARMING variable's own scrub is evidence of intent. A
        run that merely had ``LOG_LEVEL`` scrubbed is an ordinary
        contributor's run, and must keep its legitimate skip."""

        with pytest.raises(NotArmedError):
            require_armed({}, scrubbed_names=["LOG_LEVEL", "OPENAI_API_KEY"])

    def test_a_genuinely_armed_run_is_unaffected_by_the_scrub_list(self) -> None:
        require_armed({ARM_ENV_VAR: "1"}, scrubbed_names=[ARM_ENV_VAR])

    def test_default_scrubbed_names_is_empty_so_callers_opt_in(self) -> None:
        with pytest.raises(NotArmedError):
            require_armed({})


class TestEnvAllowValue:
    """The launcher's own allow-list construction: never clobber a value the
    operator already set for their own debugging."""

    def test_builds_the_full_list_from_nothing(self) -> None:
        assert set(env_allow_value(None).split(",")) == set(ARMED_RUN_ENV_ALLOW_NAMES)

    def test_empty_string_is_treated_as_unset(self) -> None:
        assert set(env_allow_value("").split(",")) == set(ARMED_RUN_ENV_ALLOW_NAMES)

    def test_merges_with_an_operator_supplied_value(self) -> None:
        merged = set(env_allow_value("LOG_LEVEL").split(","))
        assert "LOG_LEVEL" in merged
        assert set(ARMED_RUN_ENV_ALLOW_NAMES) <= merged

    def test_is_deduplicated_and_stable(self) -> None:
        value = env_allow_value(f"{ARM_ENV_VAR}, LOG_LEVEL ,{ARM_ENV_VAR}")
        parts = value.split(",")
        assert len(parts) == len(set(parts))
        assert value == env_allow_value(f"LOG_LEVEL,{ARM_ENV_VAR}")

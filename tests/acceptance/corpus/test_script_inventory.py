"""Unit + real-file coverage for ``scripts.acceptance.corpus.script_inventory``."""

from __future__ import annotations

import pytest

from dev_health_ops.llm.agent.provider_scripts import (
    load_registry_ids,
    load_role_script,
)
from scripts.acceptance.corpus.script_inventory import (
    ScriptInventoryError,
    check_script_inventory,
    missing_scripted_cases,
)


class TestMissingScriptedCases:
    def test_no_missing_when_every_case_id_is_scripted(self) -> None:
        assert missing_scripted_cases(["a", "b"], ["a", "b", "c"]) == []

    def test_reports_every_missing_id_sorted(self) -> None:
        assert missing_scripted_cases(["b", "a", "c"], ["a"]) == ["b", "c"]


class TestCheckScriptInventory:
    def test_passes_silently_when_nothing_is_missing(self) -> None:
        check_script_inventory(["a"], ["a", "b"], role="legacy_agent")

    def test_raises_loud_when_a_case_has_no_script(self) -> None:
        with pytest.raises(ScriptInventoryError, match="scripted.provider"):
            check_script_inventory(["a", "missing-one"], ["a"], role="legacy_agent")

    def test_error_names_every_missing_case_id(self) -> None:
        with pytest.raises(ScriptInventoryError) as excinfo:
            check_script_inventory(
                ["missing-one", "missing-two"], [], role="legacy_agent"
            )
        assert "missing-one" in str(excinfo.value)
        assert "missing-two" in str(excinfo.value)


class TestAgainstTheRealCheckedInRoleFile:
    """The exact regression this module exists to catch: prove it against
    the REAL ``registry-ids.v1.json`` and ``role-legacy_agent.json`` this
    repo ships, not just fabricated id lists."""

    def test_the_real_registry_has_cases_with_no_script_yet(self) -> None:
        """Setup control: as of this writing, Lane 2b has authored zero
        corpus case files, so ``role-legacy_agent.json`` scripts only a
        handful of the 134 registry ids -- proving the real files really do
        exercise the missing-script branch, not an artificial fixture that
        happens to always pass.
        """

        registry_ids = load_registry_ids()
        role_script = load_role_script("legacy_agent")
        missing = missing_scripted_cases(registry_ids, role_script.cases)
        assert missing, (
            "expected the checked-in role-legacy_agent.json to leave at "
            "least one registry id unscripted today"
        )

    def test_a_scripted_registry_id_is_never_reported_missing(self) -> None:
        role_script = load_role_script("legacy_agent")
        scripted_ids = list(role_script.cases)
        assert scripted_ids, "role-legacy_agent.json should script at least one case"
        assert missing_scripted_cases(scripted_ids, role_script.cases) == []

    def test_check_raises_for_a_corpus_case_id_the_real_role_file_never_scripted(
        self,
    ) -> None:
        role_script = load_role_script("legacy_agent")
        with pytest.raises(ScriptInventoryError):
            check_script_inventory(
                ["runner-selftest.definitely-unscripted"],
                role_script.cases,
                role="legacy_agent",
            )

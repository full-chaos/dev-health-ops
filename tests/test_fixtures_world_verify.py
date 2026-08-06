"""CHAOS-3219 Codex adversarial review (HIGH-2, 2026-08-05):

Manifest validation was string-set membership only -- a bare
``{"class": "deleted"}`` subjects.json row passed every existing check.
This file proves the fix two ways:

1. Typed per-entry schema (``validate_subject_entry_schema``/
   ``validate_source_entry_schema``): the LITERAL codex example
   (``{"class": "deleted"}``) is used as the RED test, plus one negative
   case per subject class/source-state generic requirement.
2. Live production-path verification stubs: the acronym-alias check runs
   the REAL ``alias_matching.alias_forms`` (no DB needed -- a pure
   function), and the DataHealthState/catalog-existence checks are proven
   against a fully-controlled stub ClickHouse client covering both the
   "claim matches reality" and "claim does NOT match reality" shapes.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from dev_health_ops.fixtures import world_verify
from dev_health_ops.fixtures.world import load_world_manifest

_WORLD_DIR = (
    Path(__file__).resolve().parents[1]
    / "tests"
    / "acceptance"
    / "world"
    / "ask-dev-world.v1"
)


class TestValidateSubjectEntrySchema:
    def test_red_bare_deleted_row_from_codex_finding(self) -> None:
        """The EXACT example codex's finding names: {"class": "deleted"}
        with nothing else must be rejected."""
        with pytest.raises(world_verify.WorldSchemaError):
            world_verify.validate_subject_entry_schema({"class": "deleted"})

    def test_red_exact_missing_repo_full_name(self) -> None:
        with pytest.raises(world_verify.WorldSchemaError, match="repo_full_name"):
            world_verify.validate_subject_entry_schema(
                {
                    "id": "s1",
                    "class": "exact",
                    "org_alias": "primary",
                    "mentions": ["x"],
                    "expected_terminal_resolution": "EXACT",
                }
            )

    def test_red_ambiguous_single_candidate(self) -> None:
        with pytest.raises(world_verify.WorldSchemaError, match="ambiguous"):
            world_verify.validate_subject_entry_schema(
                {
                    "id": "s2",
                    "class": "ambiguous",
                    "org_alias": "primary",
                    "candidates": ["only-one"],
                    "mentions": ["x"],
                    "expected_terminal_resolution": "AMBIGUOUS",
                }
            )

    def test_red_bounded_set_single_member(self) -> None:
        with pytest.raises(world_verify.WorldSchemaError, match="bounded-set"):
            world_verify.validate_subject_entry_schema(
                {
                    "id": "s3",
                    "class": "bounded-set",
                    "org_alias": "primary",
                    "members": ["only-one"],
                    "mentions": ["x"],
                    "expected_terminal_resolution": "X",
                }
            )

    def test_red_acronym_alias_missing_both_project_and_team(self) -> None:
        with pytest.raises(world_verify.WorldSchemaError, match="acronym-alias"):
            world_verify.validate_subject_entry_schema(
                {
                    "id": "s4",
                    "class": "acronym-alias",
                    "org_alias": "primary",
                    "alias_form": "acronym",
                    "mentions": ["MWA"],
                    "expected_terminal_resolution": "X",
                }
            )

    def test_green_well_formed_exact(self) -> None:
        world_verify.validate_subject_entry_schema(
            {
                "id": "s5",
                "class": "exact",
                "org_alias": "primary",
                "repo_full_name": "meridian/web-app",
                "mentions": ["meridian/web-app"],
                "expected_terminal_resolution": "EXACT",
            }
        )

    def test_green_no_match_needs_no_org_alias(self) -> None:
        world_verify.validate_subject_entry_schema(
            {
                "id": "s6",
                "class": "no-match",
                "mentions": ["the Ask Dev project"],
                "expected_terminal_resolution": "NOT_FOUND",
            }
        )

    def test_red_unknown_class(self) -> None:
        with pytest.raises(world_verify.WorldSchemaError, match="unknown class"):
            world_verify.validate_subject_entry_schema(
                {
                    "id": "s7",
                    "class": "totally-made-up",
                    "org_alias": "primary",
                    "mentions": ["x"],
                    "expected_terminal_resolution": "X",
                }
            )

    _PARTIALLY_RESOLVED_BASE = {
        "id": "s8",
        "class": "partially-resolved",
        "org_alias": "primary",
        "team_id": "platform",
        "mentions": ["x"],
        "expected_terminal_resolution": "X",
    }

    def test_red_unverified_live_without_ticket_reference(self) -> None:
        with pytest.raises(world_verify.WorldSchemaError, match="tracked_by"):
            world_verify.validate_subject_entry_schema(
                {
                    **self._PARTIALLY_RESOLVED_BASE,
                    "verification_status": "realized-but-unverified-live",
                }
            )

    def test_red_unknown_verification_status(self) -> None:
        with pytest.raises(
            world_verify.WorldSchemaError, match="unknown verification_status"
        ):
            world_verify.validate_subject_entry_schema(
                {**self._PARTIALLY_RESOLVED_BASE, "verification_status": "wontfix"}
            )

    def test_green_unverified_live_with_ticket_reference(self) -> None:
        world_verify.validate_subject_entry_schema(
            {
                **self._PARTIALLY_RESOLVED_BASE,
                "verification_status": "realized-but-unverified-live",
                "tracked_by": "CHAOS-3429 team-catalog live verification path",
            }
        )

    def test_green_no_verification_status_is_the_default(self) -> None:
        world_verify.validate_subject_entry_schema(self._PARTIALLY_RESOLVED_BASE)


class TestValidateSourceEntrySchema:
    def test_red_bare_state_only(self) -> None:
        with pytest.raises(world_verify.WorldSchemaError):
            world_verify.validate_source_entry_schema({"state": "stale"})

    def test_red_realized_by_missing_keys(self) -> None:
        with pytest.raises(world_verify.WorldSchemaError, match="realized_by"):
            world_verify.validate_source_entry_schema(
                {
                    "state": "stale",
                    "data_health_state": "DataHealthState.STALE",
                    "mechanism": "aged watermark",
                    "realized_by": {"org_alias": "primary"},  # missing repo_full_name
                    "source_classes": ["commits"],
                }
            )

    def test_red_missing_source_classes(self) -> None:
        with pytest.raises(world_verify.WorldSchemaError, match="source_classes"):
            world_verify.validate_source_entry_schema(
                {
                    "state": "stale",
                    "data_health_state": "DataHealthState.STALE",
                    "mechanism": "aged watermark",
                    "realized_by": {
                        "org_alias": "primary",
                        "repo_full_name": "probe/source-stale",
                    },
                }
            )

    def test_green_null_realized_by_values_allowed(self) -> None:
        """The documented not-applicable case: both keys present, null-valued."""
        world_verify.validate_source_entry_schema(
            {
                "state": "not-applicable",
                "data_health_state": "n/a",
                "mechanism": "hardcoded acr special-case, no fixture needed",
                "realized_by": {"org_alias": None, "repo_full_name": None},
                "source_classes": ["acr"],
            }
        )

    def test_green_well_formed(self) -> None:
        world_verify.validate_source_entry_schema(
            {
                "state": "current",
                "data_health_state": "DataHealthState.COMPLETE",
                "mechanism": "fresh watermark",
                "realized_by": {
                    "org_alias": "primary",
                    "repo_full_name": "probe/source-current",
                },
                "source_classes": ["commits"],
            }
        )


class TestDeclaredBlockedStatus:
    """2026-08-05: the honest third option between "realized and verified"
    and "verified elsewhere" -- see DECLARED_BLOCKED_STATUS's own docstring
    for the full history (the 'truncated' state's mechanism text turned out
    to be a false 'verified empirically' claim, the first time this round's
    new live check actually ran)."""

    _BASE = {
        "state": "truncated",
        "data_health_state": "n/a",
        "mechanism": "cannot currently be produced by any volume knob",
        "realized_by": {
            "org_alias": "primary",
            "repo_full_name": "probe/source-truncated-workgraph",
        },
        "source_classes": ["work_graph"],
    }

    def test_red_declared_blocked_without_ticket_reference(self) -> None:
        with pytest.raises(world_verify.WorldSchemaError, match="blocked_by"):
            world_verify.validate_source_entry_schema(
                {**self._BASE, "status": "declared-blocked"}
            )

    def test_red_declared_blocked_with_empty_ticket_reference(self) -> None:
        with pytest.raises(world_verify.WorldSchemaError, match="blocked_by"):
            world_verify.validate_source_entry_schema(
                {**self._BASE, "status": "declared-blocked", "blocked_by": ""}
            )

    def test_red_unknown_status_value(self) -> None:
        with pytest.raises(world_verify.WorldSchemaError, match="unknown status"):
            world_verify.validate_source_entry_schema(
                {**self._BASE, "status": "wontfix"}
            )

    def test_green_declared_blocked_with_ticket_reference(self) -> None:
        world_verify.validate_source_entry_schema(
            {
                **self._BASE,
                "status": "declared-blocked",
                "blocked_by": "CHAOS-TBD generator fan-out clustering",
            }
        )

    def test_green_no_status_is_the_default_realized_shape(self) -> None:
        """An entry with no 'status' field at all (every OTHER sources.json
        entry) must keep passing -- declared-blocked is additive, not a
        new requirement on every entry."""
        world_verify.validate_source_entry_schema(self._BASE)

    def test_is_declared_blocked_true_for_blocked_entry(self) -> None:
        entry = {
            **self._BASE,
            "status": "declared-blocked",
            "blocked_by": "CHAOS-TBD generator fan-out clustering",
        }
        assert world_verify.is_declared_blocked(entry) is True

    def test_is_declared_blocked_false_for_realized_entry(self) -> None:
        assert world_verify.is_declared_blocked(self._BASE) is False

    def test_checked_in_truncated_entry_is_declared_blocked(self) -> None:
        """End-to-end: the actual checked-in sources.json entry this whole
        mechanism was built for."""
        sources = json.loads((_WORLD_DIR / "sources.json").read_text())
        entry = next(e for e in sources["matrix"] if e["state"] == "truncated")
        world_verify.validate_source_entry_schema(entry)
        assert world_verify.is_declared_blocked(entry) is True
        assert entry.get("blocked_by")


class TestManifestValidationRejectsCodexExample:
    """End-to-end: a subjects.json file containing the literal codex
    example must fail load_world_manifest, not just the unit-level
    validator function."""

    def test_bare_deleted_row_fails_manifest_load(self, tmp_path: Path) -> None:
        world = json.loads((_WORLD_DIR / "world.json").read_text())
        subjects = json.loads((_WORLD_DIR / "subjects.json").read_text())
        sources = json.loads((_WORLD_DIR / "sources.json").read_text())

        subjects["subjects"].append({"class": "deleted"})

        (tmp_path / "world.json").write_text(json.dumps(world))
        (tmp_path / "subjects.json").write_text(json.dumps(subjects))
        (tmp_path / "sources.json").write_text(json.dumps(sources))

        with pytest.raises(Exception, match="missing required field"):
            load_world_manifest(tmp_path / "world.json")

    def test_checked_in_manifest_still_passes(self) -> None:
        """The real, current subjects.json/sources.json must satisfy the
        new typed schema -- proves the stricter check isn't itself broken
        against real content."""
        load_world_manifest(_WORLD_DIR / "world.json")


class TestVerifyAcronymAliasSubject:
    """Runs the REAL alias_matching.alias_forms (pure function)."""

    def test_green_acronym_matches(self) -> None:
        world_verify.verify_acronym_alias_subject(
            {
                "id": "a1",
                "project_display_name": "Meridian Web Application",
                "mentions": ["MWA"],
            }
        )

    def test_green_literal_parenthetical_matches(self) -> None:
        world_verify.verify_acronym_alias_subject(
            {
                "id": "a2",
                "team_display_name": "Platform Reliability (Ground Control)",
                "mentions": ["Ground Control"],
            }
        )

    def test_red_mention_does_not_resolve(self) -> None:
        with pytest.raises(
            world_verify.WorldVerificationError, match="does not resolve"
        ):
            world_verify.verify_acronym_alias_subject(
                {
                    "id": "a3",
                    "project_display_name": "Meridian Web Application",
                    "mentions": ["ZZZ"],
                }
            )

    def test_red_no_display_name(self) -> None:
        with pytest.raises(world_verify.WorldVerificationError, match="has no"):
            world_verify.verify_acronym_alias_subject({"id": "a4", "mentions": ["MWA"]})


class _StubResult:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.result_rows = rows


class _StubClient:
    def __init__(
        self, rows_by_query_substring: dict[str, list[tuple[Any, ...]]]
    ) -> None:
        self.rows_by_query_substring = rows_by_query_substring
        self.queries: list[str] = []

    def query(self, query: str, parameters: dict[str, Any]) -> _StubResult:
        self.queries.append(query)
        for substring, rows in self.rows_by_query_substring.items():
            if substring in query:
                return _StubResult(rows)
        raise AssertionError(f"no stub rows configured for query: {query}")


def _minimal_manifest() -> Any:
    from pathlib import Path as _Path

    from dev_health_ops.fixtures.world import WorldManifest

    return WorldManifest(
        manifest_path=_Path("/dev/null"),
        world={
            "master_seed": 1,
            "orgs": [{"alias": "primary", "id_seed": "org:primary"}],
            "users": [],
        },
        subjects={},
        sources={},
    )


@pytest.mark.asyncio
class TestVerifySubjectsAgainstProductionUnverifiedLiveGap:
    """CHAOS-3429 (2026-08-05): the `if not repo_names: continue` gap that
    used to silently swallow `partially-resolved` (and any future subject
    with no repo/candidates/members) must now be loud either way -- a
    named, ticketed skip for an explicitly marked entry, or a hard failure
    for an unmarked one."""

    async def test_red_unmarked_gap_raises(self) -> None:
        """An entry with no repo_names and NO verification_status marker
        must fail loudly -- this is what proves a FUTURE unverifiable
        entry can't silently join the same blind spot the pre-3429 code
        had for `partially-resolved`."""

        manifest = _minimal_manifest()
        manifest.subjects["subjects"] = [
            {
                "id": "subject.partially-resolved.example",
                "class": "partially-resolved",
                "org_alias": "primary",
                "team_id": "platform",
                "mentions": ["x"],
                "expected_terminal_resolution": "X",
            }
        ]
        client = _StubClient({})
        with pytest.raises(
            world_verify.WorldVerificationError, match="unmarked verification gap"
        ):
            await world_verify.verify_subjects_against_production(
                client=client, manifest=manifest
            )

    async def test_green_marked_gap_logs_and_skips_without_raising(self) -> None:
        import logging

        manifest = _minimal_manifest()
        manifest.subjects["subjects"] = [
            {
                "id": "subject.partially-resolved.example",
                "class": "partially-resolved",
                "org_alias": "primary",
                "team_id": "platform",
                "mentions": ["x"],
                "expected_terminal_resolution": "X",
                "verification_status": "realized-but-unverified-live",
                "tracked_by": "CHAOS-3429 team-catalog live verification path",
            }
        ]
        client = _StubClient({})
        logged: list[str] = []

        class _CapturingHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                logged.append(record.getMessage())

        handler = _CapturingHandler(level=logging.WARNING)
        # world_verify.py calls the module-level `logging.warning(...)`
        # convenience function, which logs to the ROOT logger -- attach
        # there, not to a named logger, to actually observe it.
        logger = logging.getLogger()
        logger.addHandler(handler)
        try:
            verified = await world_verify.verify_subjects_against_production(
                client=client, manifest=manifest
            )
        finally:
            logger.removeHandler(handler)
        assert verified == [], (
            "an unverified-live entry must NOT be reported as verified -- "
            "that would be a false coverage claim"
        )
        assert any("CHAOS-3429" in message for message in logged), (
            "the skip must be loud (a named, ticketed log line), not silent"
        )

    async def test_checked_in_partially_resolved_entry_is_marked(self) -> None:
        """End-to-end: the actual checked-in subjects.json entry this
        mechanism was built for skips loudly, not silently, and is not
        falsely reported as verified."""

        subjects = json.loads((_WORLD_DIR / "subjects.json").read_text())
        manifest = _minimal_manifest()
        manifest.subjects["subjects"] = [
            s for s in subjects["subjects"] if s["class"] == "partially-resolved"
        ]
        assert len(manifest.subjects["subjects"]) == 1
        client = _StubClient({})
        verified = await world_verify.verify_subjects_against_production(
            client=client, manifest=manifest
        )
        assert verified == []


@pytest.mark.asyncio
class TestVerifyConflictingCiRuns:
    async def test_green_two_distinct_statuses(self) -> None:
        client = _StubClient({"ci_pipeline_runs": [("success",), ("failed",)]})
        await world_verify.verify_conflicting_ci_runs(
            client, org_id="org-1", repo_id="repo-1"
        )

    async def test_red_only_one_status(self) -> None:
        client = _StubClient({"ci_pipeline_runs": [("success",)]})
        with pytest.raises(world_verify.WorldVerificationError, match="conflicting"):
            await world_verify.verify_conflicting_ci_runs(
                client, org_id="org-1", repo_id="repo-1"
            )

    async def test_red_no_rows(self) -> None:
        client = _StubClient({"ci_pipeline_runs": []})
        with pytest.raises(world_verify.WorldVerificationError):
            await world_verify.verify_conflicting_ci_runs(
                client, org_id="org-1", repo_id="repo-1"
            )


@pytest.mark.asyncio
class TestVerifyTruncatedWorkGraph:
    async def test_green_fanout_exceeds_max(self) -> None:
        client = _StubClient({"work_graph_issue_pr": [(30,)]})
        await world_verify.verify_truncated_work_graph(
            client, org_id="org-1", max_neighbors=25
        )

    async def test_red_fanout_within_limit(self) -> None:
        client = _StubClient({"work_graph_issue_pr": [(10,)]})
        with pytest.raises(world_verify.WorldVerificationError, match="truncated"):
            await world_verify.verify_truncated_work_graph(
                client, org_id="org-1", max_neighbors=25
            )

    async def test_red_no_rows(self) -> None:
        client = _StubClient({"work_graph_issue_pr": [(None,)]})
        with pytest.raises(world_verify.WorldVerificationError):
            await world_verify.verify_truncated_work_graph(
                client, org_id="org-1", max_neighbors=25
            )

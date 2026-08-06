"""Unit coverage for ``scripts.acceptance.corpus.resolution_path``.

Pure-logic module (no DB, no live infra) -- these are fast unit-tier tests.
A companion live/integration suite
(``tests/api/dev/test_wave4_resolution_path_live.py``) drives the REAL
``DevOrchestrator`` + a real (ephemeral) sqlite-backed ``PersistenceRunRecorder``
to prove this module classifies genuinely persisted ``dev_run_resolutions``
rows correctly, not just hand-built dataclass fixtures.
"""

from __future__ import annotations

import pytest

from scripts.acceptance.corpus.resolution_path import (
    ABSENCE_EMPTY_LEDGER,
    ABSENCE_RUN_ID_NOT_OBSERVED,
    ResolutionLedgerEntry,
    ResolutionPathError,
    absence_is_a_broken_measurement,
    attach_mention_texts,
    classify_match_kind,
    derive_resolution_path,
    resolution_path_absence_reason,
)


def _entry(
    outcome: str,
    *,
    mention_id: str = "m1",
    committed_label: str | None = None,
    committed_canonical_id: str | None = None,
    mention_text: str | None = None,
) -> ResolutionLedgerEntry:
    return ResolutionLedgerEntry(
        outcome=outcome,
        mention_id=mention_id,
        committed_label=committed_label,
        committed_canonical_id=committed_canonical_id,
        mention_text=mention_text,
    )


class TestClassifyMatchKind:
    def test_identical_text_is_exact(self) -> None:
        assert classify_match_kind("meridian/web-app", "meridian/web-app") == "exact"

    def test_case_insensitive(self) -> None:
        assert classify_match_kind("MERIDIAN/WEB-APP", "meridian/web-app") == "exact"

    def test_matches_canonical_id_directly(self) -> None:
        # Codex round-1, HIGH: production's exact() matches EITHER the
        # canonical id or the label -- a mention naming the id must classify
        # exact even though it shares no text with the label at all.
        assert (
            classify_match_kind(
                "project-ask-dev-uuid",
                "Meridian Web Application (MWA)",
                committed_canonical_id="project-ask-dev-uuid",
            )
            == "exact"
        )

    def test_canonical_id_match_is_case_insensitive(self) -> None:
        assert (
            classify_match_kind(
                "PROJECT-ASK-DEV-UUID",
                "Some Label",
                committed_canonical_id="project-ask-dev-uuid",
            )
            == "exact"
        )

    def test_a_mere_substring_is_never_exact(self) -> None:
        # Codex round-1, HIGH, confirmed by executing the old implementation:
        # bidirectional substring matching accepted "meridian/web-app extra"
        # as exact against "meridian/web-app" -- production's own exact()
        # never accepts a substring, only equality. A pure substring here is
        # not alias-reachable either, so it must raise.
        with pytest.raises(ResolutionPathError):
            classify_match_kind("meridian/web-app extra", "meridian/web-app")

    def test_a_shortened_substring_is_never_exact(self) -> None:
        with pytest.raises(ResolutionPathError):
            classify_match_kind("web-app", "meridian/web-app")

    def test_parenthetical_literal_alias_is_alias(self) -> None:
        assert classify_match_kind("MWA", "Meridian Web Application (MWA)") == "alias"

    def test_derived_acronym_is_alias(self) -> None:
        assert (
            classify_match_kind(
                "ACR", "Dev Health Agent Context Runtime (Context Fabric)"
            )
            == "alias"
        )

    def test_unrelated_text_raises(self) -> None:
        with pytest.raises(ResolutionPathError):
            classify_match_kind("Nightfall", "meridian/web-app")

    def test_empty_mention_text_raises(self) -> None:
        with pytest.raises(ResolutionPathError):
            classify_match_kind("", "meridian/web-app")


class TestDeriveResolutionPath:
    def test_empty_ledger_is_none(self) -> None:
        assert derive_resolution_path([]) is None

    def test_single_shot_direct_exact_match_is_deterministic_exact(self) -> None:
        entries = [
            _entry(
                "exact_match",
                committed_label="meridian/web-app",
                mention_text="meridian/web-app",
            )
        ]
        assert derive_resolution_path(entries) == "deterministic-exact"

    def test_single_shot_canonical_id_match_is_deterministic_exact(self) -> None:
        entries = [
            _entry(
                "exact_match",
                committed_label="Meridian Web Application (MWA)",
                committed_canonical_id="project-01",
                mention_text="project-01",
            )
        ]
        assert derive_resolution_path(entries) == "deterministic-exact"

    def test_single_shot_alias_reachable_label_is_deterministic_alias(self) -> None:
        entries = [
            _entry(
                "exact_match",
                committed_label="Meridian Web Application (MWA)",
                mention_text="MWA",
            )
        ]
        assert derive_resolution_path(entries) == "deterministic-alias"

    def test_final_entry_still_unresolved_is_miss_clarification(self) -> None:
        entries = [_entry("ambiguous_candidates")]
        assert derive_resolution_path(entries) == "miss-clarification"

    def test_no_authorized_match_is_miss_clarification(self) -> None:
        entries = [_entry("no_authorized_match")]
        assert derive_resolution_path(entries) == "miss-clarification"

    def test_mention_resolved_after_earlier_candidate_offer_is_deterministic_alias(
        self,
    ) -> None:
        """Two-turn disambiguation shape: the same mention_id first surfaces
        candidates (nothing committed), then a follow-up commits it exactly
        -- this is the alias-assisted convergence path, never a direct
        match, regardless of what the eventual committed label looks like.
        """

        entries = [
            _entry("ambiguous_candidates", mention_id="m1"),
            _entry(
                "exact_match",
                mention_id="m1",
                committed_label="Meridian Web Application",
                mention_text="Meridian Web Application",
            ),
        ]
        assert derive_resolution_path(entries) == "deterministic-alias"

    def test_transient_failure_then_exact_is_not_alias(self) -> None:
        """Codex round-1, MEDIUM, confirmed: `catalog_unavailable` and
        `no_authorized_match` are structurally forbidden from carrying
        candidates (``DevResolutionEntry.validate_outcome_payload``) -- a
        transient failure followed by a genuine direct match on retry must
        NEVER be mislabeled as alias-assisted convergence."""

        entries = [
            _entry("catalog_unavailable", mention_id="m1"),
            _entry(
                "exact_match",
                mention_id="m1",
                committed_label="meridian/web-app",
                mention_text="meridian/web-app",
            ),
        ]
        assert derive_resolution_path(entries) == "deterministic-exact"

    def test_no_authorized_match_then_exact_is_not_alias(self) -> None:
        entries = [
            _entry("no_authorized_match", mention_id="m1"),
            _entry(
                "exact_match",
                mention_id="m1",
                committed_label="meridian/web-app",
                mention_text="meridian/web-app",
            ),
        ]
        assert derive_resolution_path(entries) == "deterministic-exact"

    def test_multiple_mentions_any_alias_makes_the_whole_case_alias(self) -> None:
        entries = [
            _entry(
                "exact_match",
                mention_id="m1",
                committed_label="meridian/web-app",
                mention_text="meridian/web-app",
            ),
            _entry(
                "exact_match",
                mention_id="m2",
                committed_label="Meridian Web Application (MWA)",
                mention_text="MWA",
            ),
        ]
        assert derive_resolution_path(entries) == "deterministic-alias"

    def test_multiple_mentions_all_exact_is_deterministic_exact(self) -> None:
        entries = [
            _entry(
                "exact_match",
                mention_id="m1",
                committed_label="meridian/web-app",
                mention_text="meridian/web-app",
            ),
            _entry(
                "exact_match",
                mention_id="m2",
                committed_label="meridian/api-gateway",
                mention_text="meridian/api-gateway",
            ),
        ]
        assert derive_resolution_path(entries) == "deterministic-exact"

    def test_one_mention_unresolved_dominates_even_if_another_resolves(self) -> None:
        entries = [
            _entry(
                "exact_match",
                mention_id="m1",
                committed_label="meridian/web-app",
                mention_text="meridian/web-app",
            ),
            _entry("no_authorized_match", mention_id="m2"),
        ]
        assert derive_resolution_path(entries) == "miss-clarification"

    def test_single_shot_exact_match_missing_mention_text_raises(self) -> None:
        entries = [_entry("exact_match", committed_label="meridian/web-app")]
        with pytest.raises(ResolutionPathError):
            derive_resolution_path(entries)

    def test_unknown_outcome_value_raises(self) -> None:
        entries = [_entry("something_new")]
        with pytest.raises(ResolutionPathError):
            derive_resolution_path(entries)


class TestAttachMentionTexts:
    """CHAOS-3462 B6: thread the case-declared spans onto exec-plane entries.

    The exec plane cannot return them -- ``DevResolutionEntry`` never
    persists the mention span -- so before B6 every single-shot
    ``exact_match`` was unclassifiable and ``deterministic-exact`` was dead
    vocabulary for the entire corpus.
    """

    def test_single_mention_gets_its_span(self) -> None:
        entries = [
            ResolutionLedgerEntry(
                outcome="exact_match",
                mention_id="m1",
                committed_label="meridian/web-app",
            )
        ]
        attached = attach_mention_texts(entries, ["meridian/web-app"])
        assert attached[0].mention_text == "meridian/web-app"
        # And the whole point: it is now classifiable.
        assert derive_resolution_path(attached) == "deterministic-exact"

    def test_the_unattached_entry_is_unclassifiable(self) -> None:
        """The negative control -- this is the B6 defect itself. Without the
        span the same ledger raises, which is what made ~46 cases red."""

        entries = [
            ResolutionLedgerEntry(
                outcome="exact_match",
                mention_id="m1",
                committed_label="meridian/web-app",
            )
        ]
        with pytest.raises(ResolutionPathError):
            derive_resolution_path(entries)

    def test_spans_are_assigned_by_first_seen_mention_order(self) -> None:
        entries = [
            ResolutionLedgerEntry(outcome="ambiguous_candidates", mention_id="m1"),
            ResolutionLedgerEntry(
                outcome="exact_match", mention_id="m2", committed_label="second"
            ),
            ResolutionLedgerEntry(
                outcome="exact_match", mention_id="m1", committed_label="first"
            ),
        ]
        attached = attach_mention_texts(entries, ["alpha", "beta"])
        by_id = {(e.mention_id, e.mention_text) for e in attached}
        assert ("m1", "alpha") in by_id
        assert ("m2", "beta") in by_id

    def test_more_observed_mentions_than_declared_raises(self) -> None:
        """A short declaration would leave a real mention with no span, and
        positional mapping past the end is meaningless."""

        entries = [
            ResolutionLedgerEntry(outcome="exact_match", mention_id="m1"),
            ResolutionLedgerEntry(outcome="exact_match", mention_id="m2"),
        ]
        with pytest.raises(ResolutionPathError, match="drifted"):
            attach_mention_texts(entries, ["only-one"])

    def test_a_partial_terminating_ledger_does_not_raise(self) -> None:
        """The asymmetry, found by attacking this function directly.

        A two-mention case that TERMINATES ambiguous persists only the
        terminating entry, so the ledger carries one mention while the case
        declares two. That is legitimate -- a PROCEED always ledgers every
        mention, so a short ledger can only come from the TERMINATE path,
        which persists only an ambiguous_candidates entry. Raising would
        turn a correct, classifiable run RED and blame the case author for
        drift that did not happen.
        """

        partial = [
            ResolutionLedgerEntry(outcome="ambiguous_candidates", mention_id="m2")
        ]
        attached = attach_mention_texts(partial, ["span-one", "span-two"])
        # Nothing attached -- and nothing needed attaching, because a
        # non-exact_match final entry short-circuits before mention_text is
        # ever consulted.
        assert attached[0].mention_text is None
        assert derive_resolution_path(attached) == "miss-clarification"

    def test_the_partial_case_is_not_mispaired(self) -> None:
        """Attaching positionally in the short case would be worse than
        attaching nothing: the surviving entry is not necessarily the FIRST
        mention, so span-one could be pinned to mention two."""

        partial = [
            ResolutionLedgerEntry(outcome="ambiguous_candidates", mention_id="m2")
        ]
        assert attach_mention_texts(partial, ["span-one", "span-two"])[
            0
        ].mention_text != ("span-one")

    def test_an_already_populated_mention_text_is_not_overwritten(self) -> None:
        entries = [
            ResolutionLedgerEntry(
                outcome="exact_match",
                mention_id="m1",
                committed_label="x",
                mention_text="already-there",
            )
        ]
        assert attach_mention_texts(entries, ["declared"])[0].mention_text == (
            "already-there"
        )

    def test_empty_ledger_with_empty_declaration_is_a_no_op(self) -> None:
        assert attach_mention_texts([], []) == []


class TestHonestAbsence:
    """CHAOS-3219 Phase 2 exit: ``derive_resolution_path`` returns ``None``
    for BOTH "this run legitimately resolved no subject" and "this runner
    never got far enough to look", and exit run #3 wrote ``resolution_path:
    null`` into all 143 receipts without distinguishing them. A reader could
    not tell an honest absence from a broken measurement, which is what let
    a corpus that measured nothing look merely unlucky.
    """

    def test_a_derived_path_has_no_absence_reason(self) -> None:
        assert (
            resolution_path_absence_reason(run_id="r1", path="deterministic-exact")
            is None
        )

    def test_empty_ledger_on_a_real_run_is_an_honest_absence(self) -> None:
        """A zero-mention question really does append nothing -- absent, but
        HONESTLY absent, and not a broken measurement."""

        reason = resolution_path_absence_reason(run_id="r1", path=None)
        assert reason == ABSENCE_EMPTY_LEDGER
        assert not absence_is_a_broken_measurement(reason)

    def test_a_missing_run_id_is_a_BROKEN_measurement(self) -> None:
        """No run_id means the stream never yielded one, so the ledger was
        never even queried. That is a measurement that did not happen and
        must be distinguishable -- and loud."""

        reason = resolution_path_absence_reason(run_id=None, path=None)
        assert reason == ABSENCE_RUN_ID_NOT_OBSERVED
        assert absence_is_a_broken_measurement(reason)

"""
Tests for AI attribution models, sink, and resolved-view precedence logic.

Coverage:
  1. Model construction and serialization
  2. Signal → Record promotion
  3. Sink insert path (CH client mocked via _FakeSink.client)
  4. Deduplication: same (org, provider, subject_type, subject_id, source) → idempotent
  5. Supersession by MANUAL source
  6. Resolved-view precedence ordering (pure-Python simulation)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest

from dev_health_ops.metrics.sinks.clickhouse.ai_attribution import (
    _COLUMNS,
    AIAttributionMixin,
    _to_row,
)
from dev_health_ops.models.ai_attribution import (
    SOURCE_PRECEDENCE,
    AIAttributionKind,
    AIAttributionRecord,
    AIAttributionSignal,
    AIAttributionSource,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

ORG = uuid4()
REPO = uuid4()
NOW = datetime(2025, 5, 1, 12, 0, 0, tzinfo=timezone.utc)


def _make_record(
    source: AIAttributionSource = AIAttributionSource.PR_LABEL,
    kind: AIAttributionKind = AIAttributionKind.AI_ASSISTED,
    confidence: float = 0.9,
    subject_id: str = "pr-42",
    subject_type: str = "pull_request",
    superseded_by: UUID | None = None,
    actor: str | None = "copilot[bot]",
    evidence: dict[str, object] | None = None,
) -> AIAttributionRecord:
    return AIAttributionRecord(
        org_id=ORG,
        provider="github",
        subject_type=subject_type,  # type: ignore[arg-type]
        subject_id=subject_id,
        repo_id=REPO,
        kind=kind,
        source=source,
        confidence=confidence,
        actor=actor,
        evidence=evidence or {"label": "ai-assisted"},
        observed_at=NOW,
        superseded_by=superseded_by,
    )


def _row_as_dict(row_list: list[object]) -> dict[str, object]:
    """Convert a _to_row() list back to a dict keyed by _COLUMNS for easier assertions."""
    return dict(zip(_COLUMNS, row_list))


# ---------------------------------------------------------------------------
# 1. Model construction and serialization
# ---------------------------------------------------------------------------


class TestAIAttributionRecord:
    def test_defaults_populated(self) -> None:
        rec = _make_record()
        assert rec.ingested_at is not None
        assert rec.record_id is not None
        assert rec.superseded_by is None

    def test_evidence_json_roundtrip(self) -> None:
        rec = _make_record(evidence={"label": "ai-assisted", "count": 3})
        payload = rec.evidence_json()
        parsed = json.loads(payload)
        assert parsed == {"label": "ai-assisted", "count": 3}

    def test_evidence_json_with_non_string_values(self) -> None:
        rec = _make_record(evidence={"score": 0.95, "tags": ["ai", "bot"]})
        payload = rec.evidence_json()
        parsed = json.loads(payload)
        assert parsed["score"] == pytest.approx(0.95)
        assert parsed["tags"] == ["ai", "bot"]

    def test_record_id_unique_per_instance(self) -> None:
        r1 = _make_record()
        r2 = _make_record()
        assert r1.record_id != r2.record_id

    def test_str_enum_values(self) -> None:
        assert str(AIAttributionSource.PR_LABEL) == "pr_label"
        assert str(AIAttributionKind.AGENT_CREATED) == "agent_created"
        assert str(AIAttributionSource.MANUAL) == "manual"


# ---------------------------------------------------------------------------
# 2. Signal → Record promotion
# ---------------------------------------------------------------------------


class TestFromSignal:
    def test_promotes_signal_to_record(self) -> None:
        signal = AIAttributionSignal(
            kind=AIAttributionKind.AI_ASSISTED,
            source=AIAttributionSource.COMMIT_TRAILER,
            confidence=0.75,
            actor="claude-code[bot]",
            evidence={"trailer_key": "AI-Assisted-By", "value": "claude"},
        )
        rec = AIAttributionRecord.from_signal(
            signal,
            org_id=ORG,
            provider="github",
            subject_type="commit",
            subject_id="abc123",
            repo_id=REPO,
            observed_at=NOW,
        )
        assert rec.kind == AIAttributionKind.AI_ASSISTED
        assert rec.source == AIAttributionSource.COMMIT_TRAILER
        assert rec.confidence == pytest.approx(0.75)
        assert rec.actor == "claude-code[bot]"
        assert rec.org_id == ORG
        assert rec.provider == "github"
        assert rec.subject_type == "commit"
        assert rec.subject_id == "abc123"
        assert rec.superseded_by is None

    def test_ingested_at_set_on_promotion(self) -> None:
        signal = AIAttributionSignal(
            kind=AIAttributionKind.AI_REVIEW,
            source=AIAttributionSource.PR_LABEL,
            confidence=0.95,
            actor=None,
            evidence={},
        )
        rec = AIAttributionRecord.from_signal(
            signal,
            org_id=ORG,
            provider="github",
            subject_type="review",
            subject_id="rv-1",
            repo_id=None,
            observed_at=NOW,
        )
        assert rec.ingested_at is not None
        assert rec.ingested_at.tzinfo is not None


# ---------------------------------------------------------------------------
# 3. Sink insert path — _to_row() and write_ai_attribution()
# ---------------------------------------------------------------------------


class _FakeSink(AIAttributionMixin):
    """Minimal concrete sink — provides a mock CH client to capture insert calls."""

    def __init__(self) -> None:
        self.client = MagicMock()


class TestToRow:
    """Unit tests for the _to_row() conversion function."""

    def test_returns_correct_length(self) -> None:
        rec = _make_record()
        row = _to_row(rec)
        assert len(row) == len(_COLUMNS)

    def test_column_order_matches_columns_constant(self) -> None:
        rec = _make_record()
        d = _row_as_dict(_to_row(rec))
        assert set(d.keys()) == set(_COLUMNS)

    def test_uuids_serialized_as_strings(self) -> None:
        rec = _make_record()
        d = _row_as_dict(_to_row(rec))
        assert isinstance(d["org_id"], str)
        assert isinstance(d["record_id"], str)
        assert isinstance(d["repo_id"], str)

    def test_null_repo_id(self) -> None:
        rec = _make_record()
        rec.repo_id = None
        d = _row_as_dict(_to_row(rec))
        assert d["repo_id"] is None

    def test_null_actor(self) -> None:
        rec = _make_record(actor=None)
        d = _row_as_dict(_to_row(rec))
        assert d["actor"] is None

    def test_evidence_is_json_string(self) -> None:
        rec = _make_record(evidence={"label": "copilot", "score": 0.9})
        d = _row_as_dict(_to_row(rec))
        assert isinstance(d["evidence"], str)
        parsed = json.loads(str(d["evidence"]))
        assert parsed["label"] == "copilot"

    def test_kind_and_source_are_strings(self) -> None:
        rec = _make_record(
            kind=AIAttributionKind.AGENT_CREATED,
            source=AIAttributionSource.BOT_AUTHOR,
        )
        d = _row_as_dict(_to_row(rec))
        assert d["kind"] == "agent_created"
        assert d["source"] == "bot_author"

    def test_null_superseded_by(self) -> None:
        rec = _make_record()
        d = _row_as_dict(_to_row(rec))
        assert d["superseded_by"] is None

    def test_superseded_by_serialized_as_string(self) -> None:
        ref_id = uuid4()
        rec = _make_record(superseded_by=ref_id)
        d = _row_as_dict(_to_row(rec))
        assert d["superseded_by"] == str(ref_id)

    def test_confidence_is_float(self) -> None:
        rec = _make_record(confidence=0.75)
        d = _row_as_dict(_to_row(rec))
        assert isinstance(d["confidence"], float)
        assert d["confidence"] == pytest.approx(0.75)


class TestAIAttributionMixin:
    def test_write_empty_is_noop(self) -> None:
        sink = _FakeSink()
        sink.write_ai_attribution([])
        sink.client.insert.assert_not_called()

    def test_write_single_record_calls_client_insert(self) -> None:
        sink = _FakeSink()
        rec = _make_record()
        sink.write_ai_attribution([rec])

        sink.client.insert.assert_called_once()
        call_args = sink.client.insert.call_args
        table, matrix = call_args.args[0], call_args.args[1]
        assert table == "ai_attribution"
        assert len(matrix) == 1
        assert call_args.kwargs["column_names"] == _COLUMNS

    def test_write_multiple_records(self) -> None:
        sink = _FakeSink()
        records = [_make_record(subject_id=f"pr-{i}") for i in range(5)]
        sink.write_ai_attribution(records)
        sink.client.insert.assert_called_once()
        _, matrix = sink.client.insert.call_args.args
        assert len(matrix) == 5

    def test_batching_splits_into_chunks(self) -> None:
        sink = _FakeSink()
        records = [_make_record(subject_id=f"pr-{i}") for i in range(7)]
        sink.write_ai_attribution(records, batch_size=3)
        # 7 records, batch_size=3 → 3 insert calls: [3, 3, 1]
        assert sink.client.insert.call_count == 3
        total = sum(len(call.args[1]) for call in sink.client.insert.call_args_list)
        assert total == 7

    def test_each_row_has_correct_column_count(self) -> None:
        sink = _FakeSink()
        records = [_make_record(subject_id=f"pr-{i}") for i in range(3)]
        sink.write_ai_attribution(records)
        _, matrix = sink.client.insert.call_args.args
        for row in matrix:
            assert len(row) == len(_COLUMNS)


# ---------------------------------------------------------------------------
# 4. Deduplication: same natural key → idempotent re-insert
# ---------------------------------------------------------------------------


class TestDeduplication:
    def test_same_source_two_inserts_both_sent_to_client(self) -> None:
        """
        The sink sends both rows; ReplacingMergeTree(computed_at) handles
        dedup server-side — latest computed_at survives after OPTIMIZE.
        """
        sink = _FakeSink()
        rec1 = _make_record(confidence=0.7)
        rec2 = _make_record(confidence=0.9)  # same subject_id, same source
        sink.write_ai_attribution([rec1, rec2])

        _, matrix = sink.client.insert.call_args.args
        assert len(matrix) == 2
        # Both rows share the same natural key components
        d1, d2 = _row_as_dict(matrix[0]), _row_as_dict(matrix[1])
        assert d1["subject_id"] == d2["subject_id"]
        assert d1["source"] == d2["source"]

    def test_different_sources_same_subject_all_persisted(self) -> None:
        sink = _FakeSink()
        records = [
            _make_record(source=AIAttributionSource.PR_LABEL),
            _make_record(source=AIAttributionSource.COMMIT_TRAILER),
            _make_record(source=AIAttributionSource.BOT_AUTHOR),
        ]
        sink.write_ai_attribution(records)
        _, matrix = sink.client.insert.call_args.args
        sources = {_row_as_dict(row)["source"] for row in matrix}
        assert sources == {"pr_label", "commit_trailer", "bot_author"}


# ---------------------------------------------------------------------------
# 5. Supersession by MANUAL source
# ---------------------------------------------------------------------------


class TestSupersession:
    def test_manual_record_has_superseded_by_none(self) -> None:
        """MANUAL records themselves are never superseded."""
        manual = _make_record(source=AIAttributionSource.MANUAL)
        assert manual.superseded_by is None
        d = _row_as_dict(_to_row(manual))
        assert d["superseded_by"] is None

    def test_superseded_record_carries_manual_record_id(self) -> None:
        """When a MANUAL override is applied, the old record carries its ID."""
        manual = _make_record(source=AIAttributionSource.MANUAL)
        auto_record = _make_record(
            source=AIAttributionSource.PR_LABEL,
            superseded_by=manual.record_id,
        )
        d = _row_as_dict(_to_row(auto_record))
        assert d["superseded_by"] == str(manual.record_id)

    def test_superseded_source_excluded_from_resolved_logic(self) -> None:
        """
        Simulate the resolved-view filter:
        records with superseded_by set should not win even if higher priority.
        """
        manual = _make_record(source=AIAttributionSource.MANUAL, confidence=0.8)
        pr_label_superseded = _make_record(
            source=AIAttributionSource.PR_LABEL,
            confidence=0.95,
            superseded_by=manual.record_id,
        )
        # Simulate what the CH view does: filter superseded_by IS NULL first
        active = [r for r in [manual, pr_label_superseded] if r.superseded_by is None]
        assert len(active) == 1
        assert active[0].source == AIAttributionSource.MANUAL


# ---------------------------------------------------------------------------
# 6. Resolved-view precedence (pure-Python simulation)
# ---------------------------------------------------------------------------


def _resolve_effective(records: list[AIAttributionRecord]) -> AIAttributionRecord:
    """
    Pure-Python simulation of the ai_attribution_resolved view logic:
    1. Exclude superseded records (superseded_by IS NULL).
    2. Pick the record with the lowest SOURCE_PRECEDENCE value
       (tie-break: highest confidence).

    Mirrors the SQL:
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY org_id, subject_type, repo_id, subject_id
            ORDER BY _source_priority ASC, confidence DESC
        ) = 1

    These callers pass records that all share a single resolution partition,
    so this returns the single winner. Use :func:`_resolve_all` to simulate the
    full per-partition view output (CHAOS-2379 repo-scope, migration 043).
    """
    active = [r for r in records if r.superseded_by is None]
    if not active:
        raise ValueError("No active records to resolve")
    return min(
        active,
        key=lambda r: (SOURCE_PRECEDENCE.get(r.source, 99), -r.confidence),
    )


def _resolve_all(
    records: list[AIAttributionRecord],
) -> list[AIAttributionRecord]:
    """Per-partition resolution mirroring migration 043's view.

    Partition key: (org_id, subject_type, repo_id, subject_id). `repo_id` is
    part of the key because PR/MR subject ids are repo-local — two repos that
    share a subject id must each surface their own winning row, never collapse
    into one.
    """
    active = [r for r in records if r.superseded_by is None]
    partitions: dict[tuple[object, str, object, str], list[AIAttributionRecord]] = {}
    for r in active:
        key = (r.org_id, r.subject_type, r.repo_id, r.subject_id)
        partitions.setdefault(key, []).append(r)
    return [
        min(rows, key=lambda r: (SOURCE_PRECEDENCE.get(r.source, 99), -r.confidence))
        for rows in partitions.values()
    ]


class TestResolvedViewPrecedence:
    def test_manual_wins_all(self) -> None:
        records = [
            _make_record(source=AIAttributionSource.PR_LABEL, confidence=0.99),
            _make_record(source=AIAttributionSource.MANUAL, confidence=0.5),
            _make_record(source=AIAttributionSource.BOT_AUTHOR, confidence=0.9),
        ]
        winner = _resolve_effective(records)
        assert winner.source == AIAttributionSource.MANUAL

    def test_pr_label_beats_bot_author(self) -> None:
        records = [
            _make_record(source=AIAttributionSource.BOT_AUTHOR, confidence=0.99),
            _make_record(source=AIAttributionSource.PR_LABEL, confidence=0.8),
        ]
        winner = _resolve_effective(records)
        assert winner.source == AIAttributionSource.PR_LABEL

    def test_branch_name_beats_pr_body(self) -> None:
        records = [
            _make_record(source=AIAttributionSource.PR_BODY, confidence=0.9),
            _make_record(source=AIAttributionSource.BRANCH_NAME, confidence=0.4),
        ]
        winner = _resolve_effective(records)
        assert winner.source == AIAttributionSource.BRANCH_NAME

    def test_high_confidence_wins_same_source(self) -> None:
        # Two PR_LABEL records — higher confidence wins
        r_low = _make_record(source=AIAttributionSource.PR_LABEL, confidence=0.5)
        r_high = _make_record(source=AIAttributionSource.PR_LABEL, confidence=0.95)
        winner = _resolve_effective([r_low, r_high])
        assert winner.confidence == pytest.approx(0.95)

    def test_superseded_excluded_before_precedence(self) -> None:
        manual = _make_record(source=AIAttributionSource.MANUAL)
        pr_label = _make_record(
            source=AIAttributionSource.PR_LABEL,
            confidence=0.99,
            superseded_by=manual.record_id,
        )
        winner = _resolve_effective([manual, pr_label])
        assert winner.source == AIAttributionSource.MANUAL

    def test_all_superseded_raises(self) -> None:
        manual_id = uuid4()
        rec = _make_record(superseded_by=manual_id)
        with pytest.raises(ValueError, match="No active records"):
            _resolve_effective([rec])

    def test_full_precedence_chain_manual_wins(self) -> None:
        """All 7 sources present — MANUAL always wins."""
        sources = list(AIAttributionSource)
        records = [_make_record(source=s, confidence=0.5) for s in sources]
        winner = _resolve_effective(records)
        assert winner.source == AIAttributionSource.MANUAL

    def test_source_precedence_map_completeness(self) -> None:
        """Every source must appear in SOURCE_PRECEDENCE."""
        for src in AIAttributionSource:
            assert src in SOURCE_PRECEDENCE, f"{src} missing from SOURCE_PRECEDENCE"

    def test_precedence_values_are_unique(self) -> None:
        values = list(SOURCE_PRECEDENCE.values())
        assert len(values) == len(set(values)), "Duplicate precedence values found"


# ---------------------------------------------------------------------------
# 7. Repo-scoped resolution — cross-repo subject_id collision (CHAOS-2379)
# ---------------------------------------------------------------------------


class TestResolvedViewRepoScope:
    """Migration 043: PR/MR subject ids are repo-local, so two repos in one org
    that share the same subject_id (e.g. both have PR/MR #1) must each surface
    their own resolved row. The pre-043 partition (org, subject_type,
    subject_id) collapsed them to ONE, silently dropping the second repo's AI
    MR from governance coverage and impact. This simulates the view's partition
    key and proves both repos survive.
    """

    def test_two_repos_same_subject_id_both_survive(self) -> None:
        repo_a = uuid4()
        repo_b = uuid4()
        # Two GitLab repos in the SAME org both have MR !1 attributed as AI.
        rec_a = AIAttributionRecord(
            org_id=ORG,
            provider="gitlab",
            subject_type="pull_request",
            subject_id="1",
            repo_id=repo_a,
            kind=AIAttributionKind.AI_ASSISTED,
            source=AIAttributionSource.PR_LABEL,
            confidence=0.9,
            actor=None,
            evidence={"label": "ai-assisted"},
            observed_at=NOW,
        )
        rec_b = AIAttributionRecord(
            org_id=ORG,
            provider="gitlab",
            subject_type="pull_request",
            subject_id="1",
            repo_id=repo_b,
            kind=AIAttributionKind.AGENT_CREATED,
            source=AIAttributionSource.BOT_AUTHOR,
            confidence=0.85,
            actor="claude-code[bot]",
            evidence={"login": "claude-code[bot]"},
            observed_at=NOW,
        )
        resolved = _resolve_all([rec_a, rec_b])
        # BOTH repos' attribution rows survive — neither is collapsed away.
        survivors = {(r.repo_id, r.subject_id) for r in resolved}
        assert survivors == {(repo_a, "1"), (repo_b, "1")}
        assert len(resolved) == 2

    def test_same_repo_same_subject_still_resolves_to_one(self) -> None:
        # Within ONE repo, cross-source precedence still collapses to a single
        # winner — repo-scoping does NOT weaken intra-repo dedup.
        repo = uuid4()
        weak = AIAttributionRecord(
            org_id=ORG,
            provider="gitlab",
            subject_type="pull_request",
            subject_id="1",
            repo_id=repo,
            kind=AIAttributionKind.AI_ASSISTED,
            source=AIAttributionSource.PR_BODY,
            confidence=0.95,
            actor=None,
            evidence={},
            observed_at=NOW,
        )
        strong = AIAttributionRecord(
            org_id=ORG,
            provider="gitlab",
            subject_type="pull_request",
            subject_id="1",
            repo_id=repo,
            kind=AIAttributionKind.AI_ASSISTED,
            source=AIAttributionSource.PR_LABEL,
            confidence=0.5,
            actor=None,
            evidence={"label": "ai-assisted"},
            observed_at=NOW,
        )
        resolved = _resolve_all([weak, strong])
        assert len(resolved) == 1
        assert resolved[0].source == AIAttributionSource.PR_LABEL

    def test_repo_pinned_and_repo_less_do_not_collapse(self) -> None:
        # A repo-less (work-item-level) attribution and a repo-pinned one that
        # share a subject_id are DIFFERENT partitions (repo_id NULL vs a UUID),
        # so neither suppresses the other.
        repo = uuid4()
        pinned = AIAttributionRecord(
            org_id=ORG,
            provider="github",
            subject_type="pull_request",
            subject_id="1",
            repo_id=repo,
            kind=AIAttributionKind.AI_ASSISTED,
            source=AIAttributionSource.PR_LABEL,
            confidence=0.9,
            actor=None,
            evidence={},
            observed_at=NOW,
        )
        repo_less = AIAttributionRecord(
            org_id=ORG,
            provider="github",
            subject_type="pull_request",
            subject_id="1",
            repo_id=None,
            kind=AIAttributionKind.AI_ASSISTED,
            source=AIAttributionSource.MANUAL,
            confidence=0.99,
            actor=None,
            evidence={},
            observed_at=NOW,
        )
        resolved = _resolve_all([pinned, repo_less])
        repo_ids = {r.repo_id for r in resolved}
        assert repo_ids == {repo, None}
        assert len(resolved) == 2

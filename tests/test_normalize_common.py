"""Tests for shared provider normalization utilities."""

from datetime import datetime, timezone

import pytest

from dev_health_ops.models.work_items import WorkItemStatusTransition
from dev_health_ops.providers.normalize_common import (
    detect_reopen_events_from_transitions,
    parse_iso_datetime,
    parse_jira_datetime,
    priority_from_labels,
)


class TestParseIsoDatetime:
    def test_parse_github_rfc3339_with_z(self):
        result = parse_iso_datetime("2024-01-15T10:30:00Z")
        assert result == datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

    def test_parse_gitlab_iso_with_z(self):
        result = parse_iso_datetime("2024-01-15T10:30:00.123Z")
        assert result == datetime(2024, 1, 15, 10, 30, 0, 123000, tzinfo=timezone.utc)

    def test_parse_iso_with_offset(self):
        result = parse_iso_datetime("2024-01-15T10:30:00+05:00")
        expected = datetime(2024, 1, 15, 5, 30, 0, tzinfo=timezone.utc)
        assert result == expected

    def test_parse_none_returns_none(self):
        assert parse_iso_datetime(None) is None

    def test_parse_empty_string_returns_none(self):
        assert parse_iso_datetime("") is None

    def test_parse_invalid_format_returns_none(self):
        assert parse_iso_datetime("not-a-date") is None

    def test_parse_datetime_object_returns_utc(self):
        dt = datetime(2024, 1, 15, 10, 30, 0)
        result = parse_iso_datetime(dt)
        assert result == datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)


class TestParseJiraDatetime:
    def test_parse_jira_with_z(self):
        result = parse_jira_datetime("2024-01-15T10:30:00Z")
        assert result == datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

    def test_parse_jira_with_plus_offset_no_colon(self):
        result = parse_jira_datetime("2024-01-15T10:30:00+0000")
        assert result == datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

    def test_parse_jira_with_minus_offset_no_colon(self):
        result = parse_jira_datetime("2024-01-15T10:30:00-0500")
        expected = datetime(2024, 1, 15, 15, 30, 0, tzinfo=timezone.utc)
        assert result == expected

    def test_parse_jira_with_offset_with_colon(self):
        result = parse_jira_datetime("2024-01-15T10:30:00+05:00")
        expected = datetime(2024, 1, 15, 5, 30, 0, tzinfo=timezone.utc)
        assert result == expected

    def test_parse_none_returns_none(self):
        assert parse_jira_datetime(None) is None

    def test_parse_empty_string_returns_none(self):
        assert parse_jira_datetime("") is None

    def test_parse_invalid_format_returns_none(self):
        assert parse_jira_datetime("not-a-date") is None

    def test_parse_datetime_object_returns_utc(self):
        dt = datetime(2024, 1, 15, 10, 30, 0)
        result = parse_jira_datetime(dt)
        assert result == datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

    def test_parse_naive_datetime_adds_utc(self):
        dt = datetime(2024, 1, 15, 10, 30, 0)
        result = parse_jira_datetime(dt)
        assert result.tzinfo == timezone.utc


class TestPriorityFromLabels:
    def test_priority_critical_standard_format(self):
        priority, service_class = priority_from_labels(["priority::critical"])
        assert priority == "critical"
        assert service_class == "expedite"

    def test_priority_high_standard_format(self):
        priority, service_class = priority_from_labels(["priority::high"])
        assert priority == "high"
        assert service_class == "fixed_date"

    def test_priority_medium_standard_format(self):
        priority, service_class = priority_from_labels(["priority::medium"])
        assert priority == "medium"
        assert service_class == "standard"

    def test_priority_low_standard_format(self):
        priority, service_class = priority_from_labels(["priority::low"])
        assert priority == "low"
        assert service_class == "intangible"

    def test_priority_p0_notation(self):
        priority, service_class = priority_from_labels(["p0"])
        assert priority == "critical"
        assert service_class == "expedite"

    def test_priority_p1_notation(self):
        priority, service_class = priority_from_labels(["p1"])
        assert priority == "high"
        assert service_class == "fixed_date"

    def test_priority_p2_notation(self):
        priority, service_class = priority_from_labels(["p2"])
        assert priority == "medium"
        assert service_class == "standard"

    def test_priority_p3_notation(self):
        priority, service_class = priority_from_labels(["p3", "p4"])
        assert priority == "low"
        assert service_class == "intangible"

    def test_priority_short_forms(self):
        priority, service_class = priority_from_labels(["critical"])
        assert priority == "critical"
        assert service_class == "expedite"

        priority, service_class = priority_from_labels(["blocker"])
        assert priority == "critical"
        assert service_class == "expedite"

        priority, service_class = priority_from_labels(["urgent"])
        assert priority == "critical"
        assert service_class == "expedite"

    def test_priority_hyphenated_forms(self):
        priority, service_class = priority_from_labels(["priority-high"])
        assert priority == "high"
        assert service_class == "fixed_date"

        priority, service_class = priority_from_labels(["high-priority"])
        assert priority == "high"
        assert service_class == "fixed_date"

    def test_priority_case_insensitive(self):
        priority, service_class = priority_from_labels(["PRIORITY::CRITICAL"])
        assert priority == "critical"
        assert service_class == "expedite"

        priority, service_class = priority_from_labels(["P1"])
        assert priority == "high"
        assert service_class == "fixed_date"

    def test_priority_with_whitespace(self):
        priority, service_class = priority_from_labels(["  p2  "])
        assert priority == "medium"
        assert service_class == "standard"

    def test_priority_first_match_wins(self):
        priority, service_class = priority_from_labels(["p3", "p1", "p0"])
        assert priority == "low"
        assert service_class == "intangible"

    def test_no_priority_labels_returns_none(self):
        priority, service_class = priority_from_labels(["bug", "feature"])
        assert priority is None
        assert service_class is None

    def test_empty_labels_returns_none(self):
        priority, service_class = priority_from_labels([])
        assert priority is None
        assert service_class is None


class TestDetectReopenEventsFromTransitions:
    def test_detect_reopen_from_done_to_todo(self):
        transitions = [
            WorkItemStatusTransition(
                work_item_id="test-1",
                provider="github",
                occurred_at=datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
                from_status_raw="closed",
                to_status_raw="open",
                from_status="done",
                to_status="todo",
                actor="user1",
            )
        ]
        events = detect_reopen_events_from_transitions(
            work_item_id="test-1", transitions=transitions
        )
        assert len(events) == 1
        assert events[0].from_status == "done"
        assert events[0].to_status == "todo"
        assert events[0].actor == "user1"

    def test_detect_reopen_from_canceled_to_in_progress(self):
        transitions = [
            WorkItemStatusTransition(
                work_item_id="test-1",
                provider="gitlab",
                occurred_at=datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
                from_status_raw="closed",
                to_status_raw="reopened",
                from_status="canceled",
                to_status="in_progress",
                actor="user2",
            )
        ]
        events = detect_reopen_events_from_transitions(
            work_item_id="test-1", transitions=transitions
        )
        assert len(events) == 1
        assert events[0].from_status == "canceled"
        assert events[0].to_status == "in_progress"

    def test_no_reopen_for_normal_transitions(self):
        transitions = [
            WorkItemStatusTransition(
                work_item_id="test-1",
                provider="github",
                occurred_at=datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
                from_status_raw="open",
                to_status_raw="in_progress",
                from_status="todo",
                to_status="in_progress",
                actor="user1",
            ),
            WorkItemStatusTransition(
                work_item_id="test-1",
                provider="github",
                occurred_at=datetime(2024, 1, 15, 11, 0, 0, tzinfo=timezone.utc),
                from_status_raw="in_progress",
                to_status_raw="closed",
                from_status="in_progress",
                to_status="done",
                actor="user1",
            ),
        ]
        events = detect_reopen_events_from_transitions(
            work_item_id="test-1", transitions=transitions
        )
        assert len(events) == 0

    def test_no_reopen_for_done_to_canceled(self):
        transitions = [
            WorkItemStatusTransition(
                work_item_id="test-1",
                provider="jira",
                occurred_at=datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
                from_status_raw="Done",
                to_status_raw="Canceled",
                from_status="done",
                to_status="canceled",
                actor="user1",
            )
        ]
        events = detect_reopen_events_from_transitions(
            work_item_id="test-1", transitions=transitions
        )
        assert len(events) == 0

    def test_multiple_reopens_detected(self):
        transitions = [
            WorkItemStatusTransition(
                work_item_id="test-1",
                provider="github",
                occurred_at=datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
                from_status_raw="closed",
                to_status_raw="reopened",
                from_status="done",
                to_status="todo",
                actor="user1",
            ),
            WorkItemStatusTransition(
                work_item_id="test-1",
                provider="github",
                occurred_at=datetime(2024, 1, 15, 11, 0, 0, tzinfo=timezone.utc),
                from_status_raw="reopened",
                to_status_raw="closed",
                from_status="todo",
                to_status="done",
                actor="user1",
            ),
            WorkItemStatusTransition(
                work_item_id="test-1",
                provider="github",
                occurred_at=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
                from_status_raw="closed",
                to_status_raw="reopened",
                from_status="done",
                to_status="in_progress",
                actor="user2",
            ),
        ]
        events = detect_reopen_events_from_transitions(
            work_item_id="test-1", transitions=transitions
        )
        assert len(events) == 2
        assert events[0].occurred_at == datetime(
            2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc
        )
        assert events[1].occurred_at == datetime(
            2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc
        )

    def test_empty_transitions_returns_empty_list(self):
        events = detect_reopen_events_from_transitions(
            work_item_id="test-1", transitions=[]
        )
        assert len(events) == 0

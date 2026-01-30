"""
Unit tests for Linear provider and normalize functions.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from dev_health_ops.models.work_items import (
    Sprint,
    WorkItem,
    WorkItemInteractionEvent,
    WorkItemReopenEvent,
    WorkItemStatusTransition,
)
from dev_health_ops.providers.base import (
    IngestionContext,
    IngestionWindow,
    ProviderBatch,
    ProviderCapabilities,
)
from dev_health_ops.providers.identity import IdentityResolver
from dev_health_ops.providers.linear.normalize import (
    LINEAR_PRIORITY_MAP,
    LINEAR_STATE_TYPE_MAP,
    detect_linear_reopen_events,
    extract_linear_status_transitions,
    linear_comment_to_interaction_event,
    linear_cycle_to_sprint,
    linear_issue_to_work_item,
)
from dev_health_ops.providers.linear.provider import LinearProvider
from dev_health_ops.providers.registry import get_provider, is_registered
from dev_health_ops.providers.status_mapping import StatusMapping


@pytest.fixture
def mock_identity() -> IdentityResolver:
    identity = MagicMock(spec=IdentityResolver)
    identity.resolve.side_effect = lambda **kwargs: (
        f"user:{kwargs.get('email', kwargs.get('display_name', 'unknown'))}"
    )
    return identity


@pytest.fixture
def mock_status_mapping() -> StatusMapping:
    mapping = MagicMock(spec=StatusMapping)
    mapping.normalize_status.return_value = "unknown"
    mapping.normalize_type.return_value = "unknown"
    return mapping


def _mock_linear_issue(
    *,
    identifier: str = "ENG-123",
    title: str = "Test Issue",
    description: Optional[str] = None,
    priority: int = 2,
    estimate: Optional[float] = None,
    state_name: str = "In Progress",
    state_type: str = "started",
    labels: Optional[List[str]] = None,
    assignee_email: Optional[str] = "dev@example.com",
    assignee_name: Optional[str] = "Developer",
    creator_email: Optional[str] = "creator@example.com",
    creator_name: Optional[str] = "Creator",
    team_key: str = "ENG",
    project_name: Optional[str] = None,
    cycle_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
    parent_identifier: Optional[str] = None,
    created_at: str = "2025-01-01T10:00:00Z",
    updated_at: str = "2025-01-15T12:00:00Z",
    started_at: Optional[str] = None,
    completed_at: Optional[str] = None,
    canceled_at: Optional[str] = None,
    due_date: Optional[str] = None,
    url: str = "https://linear.app/team/issue/ENG-123",
) -> Dict[str, Any]:
    issue: Dict[str, Any] = {
        "id": f"issue-{identifier}",
        "identifier": identifier,
        "title": title,
        "description": description,
        "priority": priority,
        "estimate": estimate,
        "createdAt": created_at,
        "updatedAt": updated_at,
        "startedAt": started_at,
        "completedAt": completed_at,
        "canceledAt": canceled_at,
        "dueDate": due_date,
        "url": url,
        "state": {
            "id": "state-1",
            "name": state_name,
            "type": state_type,
        },
        "labels": {"nodes": [{"id": f"label-{l}", "name": l} for l in (labels or [])]},
        "team": {
            "id": "team-1",
            "key": team_key,
            "name": f"Team {team_key}",
        },
    }

    if assignee_email or assignee_name:
        issue["assignee"] = {
            "id": "user-assignee",
            "email": assignee_email,
            "name": assignee_name,
        }
    else:
        issue["assignee"] = None

    if creator_email or creator_name:
        issue["creator"] = {
            "id": "user-creator",
            "email": creator_email,
            "name": creator_name,
        }
    else:
        issue["creator"] = None

    if project_name:
        issue["project"] = {"id": "proj-1", "name": project_name}
    else:
        issue["project"] = None

    if cycle_id:
        issue["cycle"] = {
            "id": cycle_id,
            "number": 1,
            "name": cycle_name,
            "startsAt": "2025-01-01T00:00:00Z",
            "endsAt": "2025-01-14T23:59:59Z",
        }
    else:
        issue["cycle"] = None

    if parent_identifier:
        issue["parent"] = {"id": "parent-1", "identifier": parent_identifier}
    else:
        issue["parent"] = None

    return issue


def _mock_linear_cycle(
    *,
    cycle_id: str = "cycle-1",
    number: int = 1,
    name: Optional[str] = None,
    starts_at: str = "2025-01-01T00:00:00Z",
    ends_at: str = "2025-01-14T23:59:59Z",
    completed_at: Optional[str] = None,
    progress: float = 0.5,
) -> Dict[str, Any]:
    return {
        "id": cycle_id,
        "number": number,
        "name": name,
        "startsAt": starts_at,
        "endsAt": ends_at,
        "completedAt": completed_at,
        "progress": progress,
        "team": {"id": "team-1", "key": "ENG", "name": "Engineering"},
    }


def _mock_linear_comment(
    *,
    comment_id: str = "comment-1",
    body: str = "This is a comment",
    created_at: str = "2025-01-10T14:30:00Z",
    user_email: str = "commenter@example.com",
    user_name: str = "Commenter",
) -> Dict[str, Any]:
    return {
        "id": comment_id,
        "body": body,
        "createdAt": created_at,
        "user": {
            "id": "user-commenter",
            "email": user_email,
            "name": user_name,
        },
    }


def _mock_linear_history_entry(
    *,
    from_state_name: Optional[str] = "Todo",
    from_state_type: Optional[str] = "unstarted",
    to_state_name: str = "In Progress",
    to_state_type: str = "started",
    created_at: str = "2025-01-05T10:00:00Z",
    actor_email: str = "actor@example.com",
    actor_name: str = "Actor",
) -> Dict[str, Any]:
    entry: Dict[str, Any] = {
        "id": "history-1",
        "createdAt": created_at,
        "toState": {"id": "state-to", "name": to_state_name, "type": to_state_type},
        "actor": {"id": "user-actor", "email": actor_email, "name": actor_name},
    }
    if from_state_name and from_state_type:
        entry["fromState"] = {
            "id": "state-from",
            "name": from_state_name,
            "type": from_state_type,
        }
    else:
        entry["fromState"] = None
    return entry


class TestLinearPriorityMapping:
    def test_priority_0_no_priority(self) -> None:
        priority_raw, service_class = LINEAR_PRIORITY_MAP[0]
        assert priority_raw == "none"
        assert service_class == "intangible"

    def test_priority_1_urgent(self) -> None:
        priority_raw, service_class = LINEAR_PRIORITY_MAP[1]
        assert priority_raw == "urgent"
        assert service_class == "expedite"

    def test_priority_2_high(self) -> None:
        priority_raw, service_class = LINEAR_PRIORITY_MAP[2]
        assert priority_raw == "high"
        assert service_class == "fixed_date"

    def test_priority_3_medium(self) -> None:
        priority_raw, service_class = LINEAR_PRIORITY_MAP[3]
        assert priority_raw == "medium"
        assert service_class == "standard"

    def test_priority_4_low(self) -> None:
        priority_raw, service_class = LINEAR_PRIORITY_MAP[4]
        assert priority_raw == "low"
        assert service_class == "intangible"


class TestLinearStateTypeMapping:
    def test_backlog_state(self) -> None:
        assert LINEAR_STATE_TYPE_MAP["backlog"] == "backlog"

    def test_unstarted_state(self) -> None:
        assert LINEAR_STATE_TYPE_MAP["unstarted"] == "todo"

    def test_started_state(self) -> None:
        assert LINEAR_STATE_TYPE_MAP["started"] == "in_progress"

    def test_completed_state(self) -> None:
        assert LINEAR_STATE_TYPE_MAP["completed"] == "done"

    def test_canceled_state(self) -> None:
        assert LINEAR_STATE_TYPE_MAP["canceled"] == "canceled"
        assert LINEAR_STATE_TYPE_MAP["cancelled"] == "canceled"


class TestLinearIssueToWorkItem:
    def test_basic_issue(
        self, mock_identity: IdentityResolver, mock_status_mapping: StatusMapping
    ) -> None:
        issue = _mock_linear_issue(
            identifier="ENG-42",
            title="Test Issue",
            state_name="In Progress",
            state_type="started",
        )

        work_item, transitions = linear_issue_to_work_item(
            issue=issue,
            status_mapping=mock_status_mapping,
            identity=mock_identity,
        )

        assert work_item.work_item_id == "linear:ENG-42"
        assert work_item.provider == "linear"
        assert work_item.title == "Test Issue"
        assert work_item.status == "in_progress"
        assert work_item.project_key == "ENG"

    def test_issue_with_priority(
        self, mock_identity: IdentityResolver, mock_status_mapping: StatusMapping
    ) -> None:
        issue = _mock_linear_issue(priority=1)

        work_item, _ = linear_issue_to_work_item(
            issue=issue,
            status_mapping=mock_status_mapping,
            identity=mock_identity,
        )

        assert work_item.priority_raw == "urgent"
        assert work_item.service_class == "expedite"

    def test_issue_with_estimate(
        self, mock_identity: IdentityResolver, mock_status_mapping: StatusMapping
    ) -> None:
        issue = _mock_linear_issue(estimate=5.0)

        work_item, _ = linear_issue_to_work_item(
            issue=issue,
            status_mapping=mock_status_mapping,
            identity=mock_identity,
        )

        assert work_item.story_points == 5.0

    def test_issue_with_labels(
        self, mock_identity: IdentityResolver, mock_status_mapping: StatusMapping
    ) -> None:
        issue = _mock_linear_issue(labels=["bug", "urgent"])

        work_item, _ = linear_issue_to_work_item(
            issue=issue,
            status_mapping=mock_status_mapping,
            identity=mock_identity,
        )

        assert "bug" in work_item.labels
        assert "urgent" in work_item.labels

    def test_issue_with_cycle(
        self, mock_identity: IdentityResolver, mock_status_mapping: StatusMapping
    ) -> None:
        issue = _mock_linear_issue(cycle_id="cycle-123", cycle_name="Sprint 1")

        work_item, _ = linear_issue_to_work_item(
            issue=issue,
            status_mapping=mock_status_mapping,
            identity=mock_identity,
        )

        assert work_item.sprint_id == "linear:cycle:cycle-123"
        assert work_item.sprint_name == "Sprint 1"

    def test_issue_with_parent(
        self, mock_identity: IdentityResolver, mock_status_mapping: StatusMapping
    ) -> None:
        issue = _mock_linear_issue(parent_identifier="ENG-1")

        work_item, _ = linear_issue_to_work_item(
            issue=issue,
            status_mapping=mock_status_mapping,
            identity=mock_identity,
        )

        assert work_item.parent_id == "linear:ENG-1"

    def test_completed_issue(
        self, mock_identity: IdentityResolver, mock_status_mapping: StatusMapping
    ) -> None:
        issue = _mock_linear_issue(
            state_name="Done",
            state_type="completed",
            completed_at="2025-01-20T15:00:00Z",
        )

        work_item, _ = linear_issue_to_work_item(
            issue=issue,
            status_mapping=mock_status_mapping,
            identity=mock_identity,
        )

        assert work_item.status == "done"
        assert work_item.completed_at is not None
        assert work_item.closed_at is not None

    def test_canceled_issue(
        self, mock_identity: IdentityResolver, mock_status_mapping: StatusMapping
    ) -> None:
        issue = _mock_linear_issue(
            state_name="Canceled",
            state_type="canceled",
            canceled_at="2025-01-18T10:00:00Z",
        )

        work_item, _ = linear_issue_to_work_item(
            issue=issue,
            status_mapping=mock_status_mapping,
            identity=mock_identity,
        )

        assert work_item.status == "canceled"
        assert work_item.closed_at is not None

    def test_bug_type_from_labels(
        self, mock_identity: IdentityResolver, mock_status_mapping: StatusMapping
    ) -> None:
        issue = _mock_linear_issue(labels=["bug"])

        work_item, _ = linear_issue_to_work_item(
            issue=issue,
            status_mapping=mock_status_mapping,
            identity=mock_identity,
        )

        assert work_item.type == "bug"


class TestLinearCycleToSprint:
    def test_active_cycle(self) -> None:
        cycle = _mock_linear_cycle(
            cycle_id="cycle-100",
            number=5,
            name="Sprint 5",
            progress=0.5,
        )

        sprint = linear_cycle_to_sprint(cycle)

        assert sprint.sprint_id == "linear:cycle:cycle-100"
        assert sprint.provider == "linear"
        assert sprint.name == "Sprint 5"
        assert sprint.state == "active"
        assert sprint.started_at is not None
        assert sprint.ended_at is not None

    def test_completed_cycle(self) -> None:
        cycle = _mock_linear_cycle(
            completed_at="2025-01-14T23:59:59Z",
            progress=1.0,
        )

        sprint = linear_cycle_to_sprint(cycle)

        assert sprint.state == "closed"
        assert sprint.completed_at is not None

    def test_future_cycle(self) -> None:
        cycle = _mock_linear_cycle(progress=0.0)

        sprint = linear_cycle_to_sprint(cycle)

        assert sprint.state == "future"

    def test_cycle_without_name_uses_number(self) -> None:
        cycle = _mock_linear_cycle(number=10, name=None)

        sprint = linear_cycle_to_sprint(cycle)

        assert sprint.name == "Cycle 10"


class TestLinearCommentToInteractionEvent:
    def test_valid_comment(self, mock_identity: IdentityResolver) -> None:
        comment = _mock_linear_comment(
            body="This is a test comment with some content.",
            user_email="commenter@example.com",
        )

        event = linear_comment_to_interaction_event(
            comment=comment,
            work_item_id="linear:ENG-123",
            identity=mock_identity,
        )

        assert event is not None
        assert event.work_item_id == "linear:ENG-123"
        assert event.provider == "linear"
        assert event.interaction_type == "comment"
        assert event.body_length == len("This is a test comment with some content.")

    def test_empty_comment_returns_none(self, mock_identity: IdentityResolver) -> None:
        comment = _mock_linear_comment(body="")

        event = linear_comment_to_interaction_event(
            comment=comment,
            work_item_id="linear:ENG-123",
            identity=mock_identity,
        )

        assert event is None


class TestExtractLinearStatusTransitions:
    def test_single_transition(self, mock_identity: IdentityResolver) -> None:
        history = [
            _mock_linear_history_entry(
                from_state_name="Todo",
                from_state_type="unstarted",
                to_state_name="In Progress",
                to_state_type="started",
            )
        ]

        transitions = extract_linear_status_transitions(
            work_item_id="linear:ENG-123",
            history=history,
            identity=mock_identity,
        )

        assert len(transitions) == 1
        assert transitions[0].work_item_id == "linear:ENG-123"
        assert transitions[0].from_status_raw == "Todo"
        assert transitions[0].to_status_raw == "In Progress"
        assert transitions[0].from_status == "todo"
        assert transitions[0].to_status == "in_progress"

    def test_multiple_transitions(self, mock_identity: IdentityResolver) -> None:
        history = [
            _mock_linear_history_entry(
                from_state_name="Backlog",
                from_state_type="backlog",
                to_state_name="Todo",
                to_state_type="unstarted",
                created_at="2025-01-02T10:00:00Z",
            ),
            _mock_linear_history_entry(
                from_state_name="Todo",
                from_state_type="unstarted",
                to_state_name="In Progress",
                to_state_type="started",
                created_at="2025-01-03T10:00:00Z",
            ),
            _mock_linear_history_entry(
                from_state_name="In Progress",
                from_state_type="started",
                to_state_name="Done",
                to_state_type="completed",
                created_at="2025-01-05T10:00:00Z",
            ),
        ]

        transitions = extract_linear_status_transitions(
            work_item_id="linear:ENG-123",
            history=history,
            identity=mock_identity,
        )

        assert len(transitions) == 3

    def test_initial_transition_no_from_state(
        self, mock_identity: IdentityResolver
    ) -> None:
        history = [
            _mock_linear_history_entry(
                from_state_name=None,
                from_state_type=None,
                to_state_name="Backlog",
                to_state_type="backlog",
            )
        ]

        transitions = extract_linear_status_transitions(
            work_item_id="linear:ENG-123",
            history=history,
            identity=mock_identity,
        )

        assert len(transitions) == 1
        assert transitions[0].from_status == "unknown"


class TestDetectLinearReopenEvents:
    def test_reopen_detected(self, mock_identity: IdentityResolver) -> None:
        history = [
            _mock_linear_history_entry(
                from_state_name="Done",
                from_state_type="completed",
                to_state_name="In Progress",
                to_state_type="started",
                created_at="2025-01-10T10:00:00Z",
            )
        ]

        events = detect_linear_reopen_events(
            work_item_id="linear:ENG-123",
            history=history,
            identity=mock_identity,
        )

        assert len(events) == 1
        assert events[0].work_item_id == "linear:ENG-123"
        assert events[0].from_status == "done"
        assert events[0].to_status == "in_progress"

    def test_reopen_from_canceled(self, mock_identity: IdentityResolver) -> None:
        history = [
            _mock_linear_history_entry(
                from_state_name="Canceled",
                from_state_type="canceled",
                to_state_name="Todo",
                to_state_type="unstarted",
            )
        ]

        events = detect_linear_reopen_events(
            work_item_id="linear:ENG-123",
            history=history,
            identity=mock_identity,
        )

        assert len(events) == 1
        assert events[0].from_status == "canceled"
        assert events[0].to_status == "todo"

    def test_no_reopen_normal_progress(self, mock_identity: IdentityResolver) -> None:
        history = [
            _mock_linear_history_entry(
                from_state_name="Todo",
                from_state_type="unstarted",
                to_state_name="In Progress",
                to_state_type="started",
            ),
            _mock_linear_history_entry(
                from_state_name="In Progress",
                from_state_type="started",
                to_state_name="Done",
                to_state_type="completed",
            ),
        ]

        events = detect_linear_reopen_events(
            work_item_id="linear:ENG-123",
            history=history,
            identity=mock_identity,
        )

        assert len(events) == 0


class TestLinearProviderRegistration:
    def test_linear_registered(self) -> None:
        assert is_registered("linear")

    def test_linear_provider_instantiation(self) -> None:
        provider = get_provider("linear")
        assert provider.name == "linear"

    def test_linear_provider_capabilities(self) -> None:
        provider = LinearProvider()
        assert provider.capabilities.work_items is True
        assert provider.capabilities.status_transitions is True
        assert provider.capabilities.dependencies is False
        assert provider.capabilities.interactions is True
        assert provider.capabilities.sprints is True
        assert provider.capabilities.reopen_events is True
        assert provider.capabilities.priority is True


class TestLinearProviderIngest:
    @patch.dict(os.environ, {"LINEAR_API_KEY": "test-api-key"}, clear=False)
    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_ingest_all_teams(
        self,
        mock_client_class: MagicMock,
        mock_identity: IdentityResolver,
        mock_status_mapping: StatusMapping,
    ) -> None:
        mock_client = MagicMock()
        mock_client_class.from_env.return_value = mock_client

        mock_client.iter_teams.return_value = [
            {"id": "team-1", "key": "ENG", "name": "Engineering"}
        ]
        mock_client.iter_cycles.return_value = [_mock_linear_cycle()]
        mock_client.iter_issues.return_value = [_mock_linear_issue()]
        mock_client.get_issue_history.return_value = []
        mock_client.get_issue_comments.return_value = []

        provider = LinearProvider(
            status_mapping=mock_status_mapping,
            identity=mock_identity,
        )

        ctx = IngestionContext(
            window=IngestionWindow(),
            repo=None,
        )

        batch = provider.ingest(ctx)

        assert isinstance(batch, ProviderBatch)
        assert len(batch.work_items) == 1
        assert len(batch.sprints) == 1

    @patch.dict(os.environ, {"LINEAR_API_KEY": "test-api-key"}, clear=False)
    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_ingest_specific_team(
        self,
        mock_client_class: MagicMock,
        mock_identity: IdentityResolver,
        mock_status_mapping: StatusMapping,
    ) -> None:
        mock_client = MagicMock()
        mock_client_class.from_env.return_value = mock_client

        mock_client.iter_teams.return_value = [
            {"id": "team-1", "key": "ENG", "name": "Engineering"},
            {"id": "team-2", "key": "PROD", "name": "Product"},
        ]
        mock_client.iter_cycles.return_value = []
        mock_client.iter_issues.return_value = [_mock_linear_issue(team_key="ENG")]
        mock_client.get_issue_history.return_value = []
        mock_client.get_issue_comments.return_value = []

        provider = LinearProvider(
            status_mapping=mock_status_mapping,
            identity=mock_identity,
        )

        ctx = IngestionContext(
            window=IngestionWindow(),
            repo="ENG",
        )

        batch = provider.ingest(ctx)

        assert len(batch.work_items) == 1
        mock_client.iter_issues.assert_called_once()

    @patch.dict(os.environ, {"LINEAR_API_KEY": "test-api-key"}, clear=False)
    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_ingest_team_not_found(
        self,
        mock_client_class: MagicMock,
        mock_identity: IdentityResolver,
        mock_status_mapping: StatusMapping,
    ) -> None:
        mock_client = MagicMock()
        mock_client_class.from_env.return_value = mock_client

        mock_client.iter_teams.return_value = [
            {"id": "team-1", "key": "ENG", "name": "Engineering"}
        ]

        provider = LinearProvider(
            status_mapping=mock_status_mapping,
            identity=mock_identity,
        )

        ctx = IngestionContext(
            window=IngestionWindow(),
            repo="NONEXISTENT",
        )

        with pytest.raises(ValueError, match="not found"):
            provider.ingest(ctx)

    @patch.dict(os.environ, {}, clear=True)
    def test_ingest_requires_api_key(
        self,
        mock_identity: IdentityResolver,
        mock_status_mapping: StatusMapping,
    ) -> None:
        provider = LinearProvider(
            status_mapping=mock_status_mapping,
            identity=mock_identity,
        )

        ctx = IngestionContext(
            window=IngestionWindow(),
            repo=None,
        )

        with pytest.raises(ValueError, match="LINEAR_API_KEY"):
            provider.ingest(ctx)

    @patch.dict(os.environ, {"LINEAR_API_KEY": "test-api-key"}, clear=False)
    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_ingest_with_history_and_comments(
        self,
        mock_client_class: MagicMock,
        mock_identity: IdentityResolver,
        mock_status_mapping: StatusMapping,
    ) -> None:
        mock_client = MagicMock()
        mock_client_class.from_env.return_value = mock_client

        mock_client.iter_teams.return_value = [
            {"id": "team-1", "key": "ENG", "name": "Engineering"}
        ]
        mock_client.iter_cycles.return_value = []
        mock_client.iter_issues.return_value = [_mock_linear_issue()]
        mock_client.get_issue_history.return_value = [
            _mock_linear_history_entry(
                from_state_name="Done",
                from_state_type="completed",
                to_state_name="In Progress",
                to_state_type="started",
            )
        ]
        mock_client.get_issue_comments.return_value = [
            _mock_linear_comment(body="Test comment")
        ]

        provider = LinearProvider(
            status_mapping=mock_status_mapping,
            identity=mock_identity,
        )

        ctx = IngestionContext(
            window=IngestionWindow(),
            repo=None,
        )

        batch = provider.ingest(ctx)

        assert len(batch.work_items) == 1
        assert len(batch.status_transitions) > 0
        assert len(batch.reopen_events) == 1
        assert len(batch.interactions) == 1

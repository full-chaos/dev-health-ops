from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

import dev_health_ops.metrics.job_work_items as job
from dev_health_ops.metrics.work_items import DiscoveredRepo
from dev_health_ops.models.ai_attribution import AIAttributionRecord
from dev_health_ops.providers.github.client import GitHubAuth


@dataclass(frozen=True)
class _Classification:
    investment_area: str = "Maintenance / Tech Debt"
    project_stream: str = ""
    confidence: float = 1.0
    rule_id: str = "test"


class _Classifier:
    def __init__(self, *_args: object, **_kwargs: object) -> None:
        pass

    def classify(self, _payload: object) -> _Classification:
        return _Classification()


class _FakeClickHouseSink:
    def __init__(self, _dsn: str) -> None:
        self.org_id = ""
        self.work_items: list[object] = []
        self.transitions: list[object] = []
        self.dependencies: list[object] = []
        self.reopen_events: list[object] = []
        self.interactions: list[object] = []
        self.sprints: list[object] = []
        self.ai_attributions: list[AIAttributionRecord] = []
        self.metric_rows: list[object] = []

    def ensure_tables(self) -> None:
        return None

    def query_dicts(
        self, _query: str, _params: dict[str, object]
    ) -> list[dict[str, object]]:
        return []

    def write_work_items(self, rows: list[object]) -> None:
        self.work_items.extend(rows)

    def write_work_item_transitions(self, rows: list[object]) -> None:
        self.transitions.extend(rows)

    def write_work_item_dependencies(self, rows: list[object]) -> None:
        self.dependencies.extend(rows)

    def write_work_item_reopen_events(self, rows: list[object]) -> None:
        self.reopen_events.extend(rows)

    def write_work_item_interactions(self, rows: list[object]) -> None:
        self.interactions.extend(rows)

    def write_sprints(self, rows: list[object]) -> None:
        self.sprints.extend(rows)

    def write_ai_attribution(self, rows: list[AIAttributionRecord]) -> None:
        self.ai_attributions.extend(rows)

    def write_work_item_metrics(self, rows: list[object]) -> None:
        self.metric_rows.extend(rows)

    def write_work_item_user_metrics(self, rows: list[object]) -> None:
        self.metric_rows.extend(rows)

    def write_work_item_cycle_times(self, rows: list[object]) -> None:
        self.metric_rows.extend(rows)

    def write_work_item_state_durations(self, rows: list[object]) -> None:
        self.metric_rows.extend(rows)

    def write_issue_type_metrics(self, rows: list[object]) -> None:
        self.metric_rows.extend(rows)

    def write_investment_classifications(self, rows: list[object]) -> None:
        self.metric_rows.extend(rows)

    def write_investment_metrics(self, rows: list[object]) -> None:
        self.metric_rows.extend(rows)

    def close(self) -> None:
        return None


def _label(name: str) -> SimpleNamespace:
    return SimpleNamespace(name=name)


def _user(login: str, *, user_type: str = "User") -> SimpleNamespace:
    return SimpleNamespace(
        login=login,
        email=f"{login}@example.com",
        name=login,
        type=user_type,
        app_slug=None,
    )


def _issue(number: int) -> SimpleNamespace:
    created = datetime(2026, 5, 1, 12, tzinfo=timezone.utc)
    return SimpleNamespace(
        number=number,
        title=f"Issue {number}",
        state="open",
        body="normal non-AI work",
        created_at=created,
        updated_at=created,
        closed_at=None,
        html_url=f"https://github.com/fullchaos/dev-health/issues/{number}",
        pull_request=None,
        labels=[],
        assignees=[],
        user=_user("human-author"),
    )


def _pr(
    number: int,
    *,
    labels: list[str] | None = None,
    author: str = "human-author",
    body: str = "normal PR",
) -> SimpleNamespace:
    created = datetime(2026, 5, 1, 13, tzinfo=timezone.utc)
    return SimpleNamespace(
        number=number,
        title=f"PR {number}",
        state="open",
        merged=False,
        draft=False,
        body=body,
        created_at=created,
        updated_at=created,
        closed_at=None,
        merged_at=None,
        html_url=f"https://github.com/fullchaos/dev-health/pull/{number}",
        labels=[_label(name) for name in labels or []],
        assignees=[],
        user=_user(author, user_type="Bot" if author.endswith("[bot]") else "User"),
        head=SimpleNamespace(ref="feature/human-work"),
    )


@pytest.mark.usefixtures("monkeypatch")
def test_github_work_items_sync_writes_ai_attribution_with_org_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    org_id = uuid.uuid4()
    repo_id = uuid.uuid4()
    sink = _FakeClickHouseSink("clickhouse://test")

    monkeypatch.setattr(job, "ClickHouseMetricsSink", lambda _dsn: sink)
    monkeypatch.setattr(job, "InvestmentClassifier", _Classifier)
    monkeypatch.setattr(
        job, "compute_work_item_metrics_daily", lambda **_kwargs: ([], [], [])
    )
    monkeypatch.setattr(
        job, "compute_work_item_state_durations_daily", lambda **_kwargs: []
    )
    monkeypatch.setattr(job, "parse_github_projects_v2_env", lambda: [])
    monkeypatch.setattr(
        job,
        "_discover_repos",
        lambda **_kwargs: [
            DiscoveredRepo(
                repo_id=repo_id,
                full_name="fullchaos/dev-health",
                source="github",
                settings={},
            )
        ],
    )

    client = MagicMock()
    client.iter_repo_milestones.return_value = []
    client.iter_issues.return_value = [_issue(10)]
    client.iter_pull_requests.return_value = [
        _pr(11, labels=["ai-assisted"]),
        _pr(12, author="claude-code[bot]"),
        _pr(13, body="Implementation details\n\nAI-Assisted-By: Claude Code"),
        _pr(14, body="Reviewed and implemented by a human."),
    ]
    client.iter_issue_events.return_value = []
    client.iter_issue_comments.return_value = []
    client.iter_pr_comments_batch.return_value = []

    # Org-scoped runs resolve credentials org-first (ambient env no longer
    # preempts the org's DB credential, CHAOS-2292); inject the fake client
    # at the builder seam — client construction has its own dedicated tests.
    monkeypatch.setattr(job, "_build_github_work_client", lambda **_kwargs: client)

    run_job: Any = job.run_work_items_sync_job
    run_job(
        db_url="clickhouse://test",
        day=date(2026, 5, 2),
        backfill_days=1,
        provider="github",
        org_id=str(org_id),
    )

    assert sink.ai_attributions
    assert {row.org_id for row in sink.ai_attributions} == {org_id}
    assert {row.repo_id for row in sink.ai_attributions} == {repo_id}
    assert {str(row.source) for row in sink.ai_attributions} >= {
        "pr_label",
        "bot_author",
        "commit_trailer",
    }

    work_item_ids = {getattr(row, "work_item_id", "") for row in sink.work_items}
    assert "gh:fullchaos/dev-health#10" in work_item_ids
    assert "ghpr:fullchaos/dev-health#14" in work_item_ids
    assert not any(
        row.subject_id == "ghpr:fullchaos/dev-health#14" for row in sink.ai_attributions
    )


def _run_github_work_items_with_credentials(
    monkeypatch: pytest.MonkeyPatch,
    credentials: dict[str, object],
) -> GitHubAuth:
    org_id = uuid.uuid4()
    repo_id = uuid.uuid4()
    sink = _FakeClickHouseSink("clickhouse://test")
    captured_auth: list[GitHubAuth] = []

    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.setattr(job, "ClickHouseMetricsSink", lambda _dsn: sink)
    monkeypatch.setattr(job, "InvestmentClassifier", _Classifier)
    monkeypatch.setattr(
        job, "compute_work_item_metrics_daily", lambda **_kwargs: ([], [], [])
    )
    monkeypatch.setattr(
        job, "compute_work_item_state_durations_daily", lambda **_kwargs: []
    )
    monkeypatch.setattr(job, "parse_github_projects_v2_env", lambda: [])
    monkeypatch.setattr(
        job,
        "_discover_repos",
        lambda **_kwargs: [
            DiscoveredRepo(
                repo_id=repo_id,
                full_name="fullchaos/dev-health",
                source="github",
                settings={},
            )
        ],
    )

    from dev_health_ops.providers.github.client import GitHubWorkClient

    def capture_init(self: Any, *, auth: GitHubAuth, **_kwargs: object) -> None:
        captured_auth.append(auth)
        self.auth = auth

    monkeypatch.setattr(GitHubWorkClient, "__init__", capture_init)
    monkeypatch.setattr(GitHubWorkClient, "iter_repo_milestones", lambda *_, **__: [])
    monkeypatch.setattr(GitHubWorkClient, "iter_issues", lambda *_, **__: [])
    monkeypatch.setattr(GitHubWorkClient, "iter_pull_requests", lambda *_, **__: [])

    before = os.environ.get("GITHUB_TOKEN")
    run_job: Any = job.run_work_items_sync_job
    run_job(
        db_url="clickhouse://test",
        day=date(2026, 5, 2),
        backfill_days=1,
        provider="github",
        org_id=str(org_id),
        credentials=credentials,
    )

    assert os.environ.get("GITHUB_TOKEN") == before
    assert captured_auth
    return captured_auth[0]


def test_github_work_items_sync_uses_db_pat_without_env_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    auth = _run_github_work_items_with_credentials(
        monkeypatch,
        {"token": "ghp_database_token"},
    )

    assert auth.token == "ghp_database_token"
    assert auth.is_app_auth is False


def test_github_work_items_sync_uses_db_app_auth_without_env_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private_key = "\n".join(
        [
            "-----BEGIN" + " PRIVATE KEY-----",
            "test",
            "-----END" + " PRIVATE KEY-----",
        ]
    )
    auth = _run_github_work_items_with_credentials(
        monkeypatch,
        {
            "app_id": "12345",
            "private_key": private_key,
            "installation_id": "67890",
        },
    )

    assert auth.token is None
    assert auth.app_id == "12345"
    assert auth.private_key == private_key
    assert auth.installation_id == "67890"
    assert auth.is_app_auth is True


@pytest.mark.usefixtures("monkeypatch")
def test_github_attribution_subject_id_is_bare_pr_number_joins_pr_number(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Positive JOIN contract (CHAOS-2396).

    The AI governance loader (``audit.ai_governance.loaders``) resolves
    ``human_reviewed`` by joining ``ai_attribution.subject_id =
    toString(git_pull_requests.number)`` scoped by ``repo_id``. GitHub's
    ``git_pull_requests.number`` is the bare PR number (``int(pr.number)``),
    exactly as GitLab writes ``int(mr.iid)``. So the live work-items sync path
    MUST write ``subject_id == str(pr.number)`` (e.g. ``"11"``) and NEVER the
    prefixed work-item id (``ghpr:{repo}#{n}``) — otherwise every GitHub PR
    attribution would miss the join and the org would get zero AI
    governance/coverage while fabricating "missing review" violations.
    """
    org_id = uuid.uuid4()
    repo_id = uuid.uuid4()
    sink = _FakeClickHouseSink("clickhouse://test")

    monkeypatch.setattr(job, "ClickHouseMetricsSink", lambda _dsn: sink)
    monkeypatch.setattr(job, "InvestmentClassifier", _Classifier)
    monkeypatch.setattr(
        job, "compute_work_item_metrics_daily", lambda **_kwargs: ([], [], [])
    )
    monkeypatch.setattr(
        job, "compute_work_item_state_durations_daily", lambda **_kwargs: []
    )
    monkeypatch.setattr(job, "parse_github_projects_v2_env", lambda: [])
    monkeypatch.setattr(
        job,
        "_discover_repos",
        lambda **_kwargs: [
            DiscoveredRepo(
                repo_id=repo_id,
                full_name="fullchaos/dev-health",
                source="github",
                settings={},
            )
        ],
    )

    # One PR (#11) carries an explicit AI signal via the 'ai-assisted' label;
    # one PR (#14) is plain human work and must produce no attribution row.
    ai_pr = _pr(11, labels=["ai-assisted"])
    human_pr = _pr(14, body="Reviewed and implemented by a human.")

    client = MagicMock()
    client.iter_repo_milestones.return_value = []
    client.iter_issues.return_value = []
    client.iter_pull_requests.return_value = [ai_pr, human_pr]
    client.iter_issue_events.return_value = []
    client.iter_issue_comments.return_value = []
    client.iter_pr_comments_batch.return_value = []

    monkeypatch.setattr(job, "_build_github_work_client", lambda **_kwargs: client)

    run_job: Any = job.run_work_items_sync_job
    run_job(
        db_url="clickhouse://test",
        day=date(2026, 5, 2),
        backfill_days=1,
        provider="github",
        org_id=str(org_id),
    )

    assert sink.ai_attributions
    # The PR carrying the AI signal produced at least one attribution row.
    ai_rows = [r for r in sink.ai_attributions if r.subject_type == "pull_request"]
    assert ai_rows

    # Producer side: git_pull_requests.number == int(pr.number) for this PR.
    expected_subject_id = str(int(ai_pr.number))
    assert expected_subject_id == "11"

    # The loader join is `a.subject_id = toString(pr.number)` scoped by repo_id.
    for rec in ai_rows:
        assert rec.subject_id == expected_subject_id  # bare "11", joins pr.number
        assert rec.org_id == org_id
        assert rec.repo_id == repo_id  # join also scoped a.repo_id = pr.repo_id
        assert rec.provider == "github"

    # Negative guard: the prefixed work-item id shape would NOT join and must
    # never be written as subject_id (CHAOS-2396 regression).
    assert all(
        rec.subject_id != "ghpr:fullchaos/dev-health#11" for rec in sink.ai_attributions
    )
    # The plain human PR produced no attribution row.
    assert not any(rec.subject_id == "14" for rec in sink.ai_attributions)

from __future__ import annotations

import dataclasses
import uuid
from datetime import date, datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

# Import connectors FIRST to break the providers._base <-> connectors circular
# import that otherwise ERRORs at collection in isolated runs (see CHAOS-2370).
import dev_health_ops.connectors  # noqa: F401
import dev_health_ops.metrics.job_work_items as job
import dev_health_ops.metrics.work_items as work_items_module
from dev_health_ops.metrics.work_items import DiscoveredRepo
from dev_health_ops.models.ai_attribution import AIAttributionRecord
from dev_health_ops.providers.gitlab.normalize import (
    detect_mr_attributions,
    gitlab_mr_ai_attributions,
)


def _mr(
    *,
    iid: int = 7,
    labels: list[str] | None = None,
    author_username: str = "human-author",
    author_bot: bool = False,
    description: str = "normal MR",
    source_branch: str = "feature/human-work",
    created_at: datetime | None = None,
) -> SimpleNamespace:
    """Build a GitLab merge-request-like object for detection tests."""
    return SimpleNamespace(
        iid=iid,
        labels=list(labels or []),
        author=SimpleNamespace(
            username=author_username,
            name=author_username,
            bot=author_bot,
        ),
        description=description,
        source_branch=source_branch,
        created_at=created_at or datetime(2026, 5, 1, 13, tzinfo=timezone.utc),
        updated_at=created_at or datetime(2026, 5, 1, 13, tzinfo=timezone.utc),
    )


# ---------------------------------------------------------------------------
# Detector unit tests
# ---------------------------------------------------------------------------


def test_detect_mr_attributions_from_ai_label() -> None:
    signals = detect_mr_attributions(mr=_mr(labels=["ai-assisted"]))

    assert any(s.source.value == "pr_label" for s in signals)
    label_signal = next(s for s in signals if s.source.value == "pr_label")
    assert label_signal.kind.value == "ai_assisted"
    assert label_signal.evidence["label"] == "ai-assisted"


def test_detect_mr_attributions_from_commit_trailer() -> None:
    signals = detect_mr_attributions(
        mr=_mr(description="Implement feature\n\nAI-Assisted-By: Claude Code"),
    )

    assert any(s.source.value == "commit_trailer" for s in signals)
    trailer_signal = next(s for s in signals if s.source.value == "commit_trailer")
    assert trailer_signal.actor == "Claude Code"


def test_detect_mr_attributions_from_bot_author() -> None:
    signals = detect_mr_attributions(
        mr=_mr(author_username="claude-code[bot]", author_bot=True),
    )

    assert any(s.source.value == "bot_author" for s in signals)
    bot_signal = next(s for s in signals if s.source.value == "bot_author")
    assert bot_signal.kind.value == "agent_created"
    assert bot_signal.actor == "claude-code[bot]"


def test_detect_mr_attributions_from_source_branch() -> None:
    signals = detect_mr_attributions(
        mr=_mr(source_branch="copilot/fix-bug"),
    )

    assert any(s.source.value == "branch_name" for s in signals)


def test_detect_mr_attributions_non_ai_mr_emits_none() -> None:
    signals = detect_mr_attributions(
        mr=_mr(
            labels=["bug", "frontend"],
            author_username="alice",
            author_bot=False,
            description="Refactor the widget rendering pipeline.",
            source_branch="feature/widget-refactor",
        ),
    )

    assert signals == []


def test_detect_mr_attributions_ci_bot_author_emits_none() -> None:
    # CI automation bots are explicitly excluded — they are not AI.
    signals = detect_mr_attributions(
        mr=_mr(author_username="dependabot[bot]", author_bot=True),
    )

    assert not any(s.source.value == "bot_author" for s in signals)


# ---------------------------------------------------------------------------
# Record-promotion helper tests
# ---------------------------------------------------------------------------


def test_gitlab_mr_ai_attributions_uses_canonical_subject_and_real_timestamp() -> None:
    org_id = uuid.uuid4()
    repo_id = uuid.uuid4()
    created = datetime(2026, 4, 17, 9, 30, tzinfo=timezone.utc)

    records = gitlab_mr_ai_attributions(
        mr=_mr(iid=42, labels=["ai-assisted"], created_at=created),
        project_full_path="group/widget",
        org_id=org_id,
        repo_id=repo_id,
    )

    assert records
    rec = records[0]
    # Subject id matches the canonical gitlab_mr_to_work_item formula ("!" for MRs).
    assert rec.subject_id == "gitlab:group/widget!42"
    assert rec.org_id == org_id
    assert rec.repo_id == repo_id
    assert rec.provider == "gitlab"
    assert rec.subject_type == "pull_request"
    # observed_at is the MR's real created_at, never a fabricated ingest time.
    assert rec.observed_at == created


def test_gitlab_mr_ai_attributions_empty_for_non_ai_mr() -> None:
    records = gitlab_mr_ai_attributions(
        mr=_mr(),
        project_full_path="group/widget",
        org_id=uuid.uuid4(),
        repo_id=None,
    )
    assert records == []


# ---------------------------------------------------------------------------
# Live-path integration test
#
# Drives the ACTUAL scheduled work-items sync entrypoint
# (run_work_items_sync_job -> fetch_gitlab_work_items) with a fake GitLab
# client and asserts MR-derived AI attribution records reach
# write_ai_attribution() with the real org_id. This is the seam the prior
# attempt left broken (records were only emitted from the orphaned
# GitLabProvider.iter_ingest path, never the legacy GitLab branch).
# ---------------------------------------------------------------------------


class _FakeGitLabClient:
    """Minimal stand-in for GitLabWorkClient covering the live fetch path."""

    def __init__(self, *, issues: list[Any], mrs: list[Any]) -> None:
        self._issues = issues
        self._mrs = mrs

    def iter_project_issues(
        self, *, project_id_or_path: str, state: str = "all", **_kwargs: Any
    ) -> list[Any]:
        return self._issues

    def iter_project_merge_requests(
        self, *, project_id_or_path: str, state: str = "all", **_kwargs: Any
    ) -> list[Any]:
        return self._mrs


def _gitlab_issue(iid: int) -> SimpleNamespace:
    created = datetime(2026, 5, 1, 12, tzinfo=timezone.utc)
    return SimpleNamespace(
        iid=iid,
        title=f"Issue {iid}",
        description="normal non-AI work",
        state="opened",
        created_at=created,
        updated_at=created,
        closed_at=None,
        labels=[],
        assignees=[],
        author=SimpleNamespace(username="human", name="human", email=None),
        web_url=f"https://gitlab.com/group/widget/-/issues/{iid}",
    )


@dataclasses.dataclass(frozen=True)
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


def _patch_common(
    monkeypatch: pytest.MonkeyPatch,
    *,
    sink: _FakeClickHouseSink,
    repo_id: uuid.UUID,
    fake_client: _FakeGitLabClient,
) -> None:
    monkeypatch.setattr(job, "ClickHouseMetricsSink", lambda _dsn: sink)
    monkeypatch.setattr(job, "InvestmentClassifier", _Classifier)
    monkeypatch.setattr(
        job, "compute_work_item_metrics_daily", lambda **_kwargs: ([], [], [])
    )
    monkeypatch.setattr(
        job, "compute_work_item_state_durations_daily", lambda **_kwargs: []
    )
    monkeypatch.setattr(
        job,
        "_discover_repos",
        lambda **_kwargs: [
            DiscoveredRepo(
                repo_id=repo_id,
                full_name="group/widget",
                source="gitlab",
                settings={},
            )
        ],
    )

    # Inject the fake GitLab client at the metrics-dependency seam that
    # fetch_gitlab_work_items resolves via get_metrics_dependencies().
    base = work_items_module.get_metrics_dependencies()
    overridden = dataclasses.replace(base, gitlab_client_factory=lambda: fake_client)
    monkeypatch.setattr(
        work_items_module,
        "get_metrics_dependencies",
        lambda: overridden,
    )


def test_gitlab_work_items_sync_writes_ai_attribution_with_org_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    org_id = uuid.uuid4()
    repo_id = uuid.uuid4()
    sink = _FakeClickHouseSink("clickhouse://test")
    fake_client = _FakeGitLabClient(
        issues=[_gitlab_issue(10)],
        mrs=[
            _mr(iid=11, labels=["ai-assisted"]),
            _mr(iid=12, author_username="claude-code[bot]", author_bot=True),
            _mr(
                iid=13,
                description="Implementation details\n\nAI-Assisted-By: Claude Code",
            ),
            _mr(iid=14, description="Reviewed and implemented by a human."),
        ],
    )
    _patch_common(monkeypatch, sink=sink, repo_id=repo_id, fake_client=fake_client)

    run_job: Any = job.run_work_items_sync_job
    run_job(
        db_url="clickhouse://test",
        day=date(2026, 5, 2),
        backfill_days=1,
        provider="gitlab",
        org_id=str(org_id),
    )

    # MR-derived attribution records reached the sink, org-scoped to the real org.
    assert sink.ai_attributions
    assert {row.org_id for row in sink.ai_attributions} == {org_id}
    assert {row.repo_id for row in sink.ai_attributions} == {repo_id}
    assert {str(row.source) for row in sink.ai_attributions} >= {
        "pr_label",
        "bot_author",
        "commit_trailer",
    }
    # Subject ids use the canonical MR formula.
    assert any(
        row.subject_id == "gitlab:group/widget!11" for row in sink.ai_attributions
    )
    # The non-AI MR (#14) produced no attribution row.
    assert not any(
        row.subject_id == "gitlab:group/widget!14" for row in sink.ai_attributions
    )


def test_gitlab_work_items_sync_skips_attribution_when_org_blank(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Without a tenant scope, attribution must NOT be written with a blank org.
    repo_id = uuid.uuid4()
    sink = _FakeClickHouseSink("clickhouse://test")
    fake_client = _FakeGitLabClient(
        issues=[_gitlab_issue(10)],
        mrs=[_mr(iid=11, labels=["ai-assisted"])],
    )
    _patch_common(monkeypatch, sink=sink, repo_id=repo_id, fake_client=fake_client)

    run_job: Any = job.run_work_items_sync_job
    run_job(
        db_url="clickhouse://test",
        day=date(2026, 5, 2),
        backfill_days=1,
        provider="gitlab",
        org_id="",
    )

    assert sink.ai_attributions == []

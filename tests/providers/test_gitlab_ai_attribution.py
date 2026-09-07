from __future__ import annotations

import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

# Import connectors FIRST to break the providers._base <-> connectors circular
# import that otherwise ERRORs at collection in isolated runs (see CHAOS-2370).
import dev_health_ops.connectors  # noqa: F401
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
    # Subject id is the bare MR iid string so it joins the governance/impact
    # read paths (`subject_id = toString(git_pull_requests.number)`), which is
    # exactly `int(mr.iid)` for GitLab. A prefixed `gitlab:group/widget!42`
    # would never match and would fabricate "missing review" policy failures.
    assert rec.subject_id == "42"
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


def test_gitlab_attribution_subject_id_joins_git_pull_requests_number() -> None:
    """End-to-end JOIN contract (CHAOS-2379 round-2).

    The governance loader (``audit.ai_governance.loaders``) and the impact
    loader (``metrics.loaders.ai_impact``) resolve ``human_reviewed`` /
    coverage by joining ``ai_attribution.subject_id = toString(pr.number)``.
    The GitLab processor stores ``git_pull_requests.number = int(mr["iid"])``.
    This test reproduces BOTH sides from the SAME merge request and asserts the
    join equality holds, proving the attribution rows actually match their MR
    metadata instead of leaving ``human_reviewed`` NULL and fabricating
    high-severity "missing review" policy violations.
    """
    org_id = uuid.uuid4()
    repo_id = uuid.uuid4()
    raw_mr: dict[str, Any] = {
        "iid": 42,
        "labels": ["ai-assisted"],
        "author": {"username": "human-author", "name": "human-author"},
        "description": "normal MR",
        "source_branch": "feature/human-work",
        "created_at": "2026-04-17T09:30:00+00:00",
        "updated_at": "2026-04-17T09:30:00+00:00",
    }

    # Producer side — exactly what processors.gitlab writes to
    # git_pull_requests.number for this MR.
    pr_number = int(raw_mr.get("iid") or 0)

    records = gitlab_mr_ai_attributions(
        mr=raw_mr,
        project_full_path="group/widget",
        org_id=org_id,
        repo_id=repo_id,
    )

    assert records
    # The loader join is `a.subject_id = toString(pr.number)` scoped by repo_id.
    for rec in records:
        assert rec.subject_id == str(pr_number)
        assert rec.repo_id == repo_id  # join is also scoped on a.repo_id = pr.repo_id
    # Negative guard: the prefixed work-item id shape would NOT join.
    assert all(rec.subject_id != "gitlab:group/widget!42" for rec in records)


# ---------------------------------------------------------------------------
# CHAOS-5351: the live-path integration tests that used to live here
# (test_gitlab_work_items_sync_writes_ai_attribution_with_org_id,
# test_gitlab_work_items_sync_skips_attribution_when_org_blank) drove
# run_work_items_sync_job -> fetch_gitlab_work_items end to end with a fake
# GitLab client, asserting MR-derived AI attribution records reached
# write_ai_attribution() with the real org_id. Deleted along with
# run_work_items_sync_job -- the native provider-sync route
# (internal/providersync/gitlab_work_item_derived.go) is the production
# writer of ai_attribution now; the pure-function contract this file's other
# tests pin (detect_mr_attributions_*, gitlab_mr_ai_attributions_*,
# gitlab_attribution_subject_id_joins_git_pull_requests_number above) is
# unaffected and stays covered.
# ---------------------------------------------------------------------------

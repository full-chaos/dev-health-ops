"""Tests for capturing PR/MR <-> issue links that drive team inheritance.

The authoritative link is Linear -> source control (the issue's attachment
points at the GitHub PR / GitLab MR). On the GitHub side the reference often
lives only in a bot comment. These tests cover both capture paths and confirm
the captured edge drives the existing linked-issue team inheritance.
"""

from __future__ import annotations

from datetime import datetime, timezone

from dev_health_ops.metrics.compute_work_items import build_linked_issue_team_resolver
from dev_health_ops.models.work_items import WorkItem
from dev_health_ops.providers.github.normalize import (
    extract_github_comment_dependencies,
)
from dev_health_ops.providers.linear.normalize import (
    _work_item_id_from_pr_url,
    extract_linear_dependencies,
)
from dev_health_ops.providers.teams import ProjectKeyTeamResolver

NOW = datetime(2026, 6, 1, tzinfo=timezone.utc)


def _wi(work_item_id: str, provider: str, **kw: object) -> WorkItem:
    defaults: dict[str, object] = dict(
        title="t",
        type="task",
        status="done",
        status_raw=None,
        created_at=NOW,
        updated_at=NOW,
    )
    defaults.update(kw)
    return WorkItem(work_item_id=work_item_id, provider=provider, **defaults)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# URL -> work_item_id parser (provider-agnostic)
# --------------------------------------------------------------------------- #


def test_pr_url_parser_maps_github_and_gitlab() -> None:
    assert (
        _work_item_id_from_pr_url("https://github.com/full-chaos/ops/pull/921")
        == "ghpr:full-chaos/ops#921"
    )
    # GitHub Enterprise host -> same owner/repo id (host-agnostic).
    assert (
        _work_item_id_from_pr_url("https://github.example.com/o/r/pull/7")
        == "ghpr:o/r#7"
    )
    # GitLab MR, nested groups, any host.
    assert (
        _work_item_id_from_pr_url(
            "https://gitlab.com/group/sub/project/-/merge_requests/45"
        )
        == "gitlab:group/sub/project!45"
    )


def test_pr_url_parser_ignores_non_pr_urls() -> None:
    for url in (
        "https://www.figma.com/file/abc",
        "https://linear.app/team/issue/CHAOS-1",
        "https://github.com/owner/repo/issues/5",  # issue, not a PR
        "",
        None,
    ):
        assert _work_item_id_from_pr_url(url) is None


# --------------------------------------------------------------------------- #
# Linear attachment capture (primary)
# --------------------------------------------------------------------------- #


def test_linear_attachment_emits_pr_to_issue_edge() -> None:
    issue = {
        "attachments": {
            "nodes": [
                {"url": "https://github.com/full-chaos/ops/pull/12"},
                {"url": "https://www.notion.so/some-doc"},  # ignored
            ]
        }
    }
    deps = extract_linear_dependencies(issue=issue, work_item_id="linear:CHAOS-2400")
    assert len(deps) == 1
    edge = deps[0]
    # PR is the SOURCE, issue (with the team) is the TARGET.
    assert edge.source_work_item_id == "ghpr:full-chaos/ops#12"
    assert edge.target_work_item_id == "linear:CHAOS-2400"
    assert edge.relationship_type == "relates_to"  # inheritance-safe
    assert edge.relationship_type_raw == "linear_attachment"


def test_linear_attachment_edge_drives_pr_inheritance_direct_target() -> None:
    # The target is the real linear: id (no extkey ambiguity), so the resolver
    # attributes the PR to the issue's team with the builder unchanged.
    linear = _wi("linear:CHAOS-2400", "linear", project_key="CHAOS")
    pr = _wi("ghpr:full-chaos/ops#12", "github", type="pr", project_id="full-chaos/ops")
    deps = extract_linear_dependencies(
        issue={
            "attachments": {
                "nodes": [{"url": "https://github.com/full-chaos/ops/pull/12"}]
            }
        },
        work_item_id="linear:CHAOS-2400",
    )
    resolver = build_linked_issue_team_resolver(
        work_items=[linear, pr],
        dependencies=deps,
        project_key_resolver=ProjectKeyTeamResolver(
            project_key_to_team={"CHAOS": ("CHAOS", "Chaos Team")}
        ),
    )
    assert resolver.resolve(pr.work_item_id) == ("CHAOS", "Chaos Team")


# --------------------------------------------------------------------------- #
# GitHub PR comment capture (secondary)
# --------------------------------------------------------------------------- #


def test_github_comment_capture_from_linear_url_and_keyword() -> None:
    deps = extract_github_comment_dependencies(
        work_item_id="ghpr:o/r#1",
        comment_bodies=[
            "Linked to https://linear.app/full-chaos/issue/CHAOS-2400/title",
            None,
            "Also fixes PROJ-7 per discussion",
            "see https://linear.app/x/issue/CHAOS-2400/dup",  # duplicate -> one edge
        ],
    )
    targets = {d.target_work_item_id for d in deps}
    assert targets == {"extkey:CHAOS-2400", "extkey:PROJ-7"}
    rel = {d.target_work_item_id: d.relationship_type for d in deps}
    assert rel["extkey:CHAOS-2400"] == "relates_to"  # URL link
    assert rel["extkey:PROJ-7"] == "relates_to"  # 'fixes'


def test_github_comment_capture_preserves_blocking_intent() -> None:
    # A blocking keyword must NOT yield an inheritable edge.
    deps = extract_github_comment_dependencies(
        work_item_id="ghpr:o/r#1",
        comment_bodies=["This is blocked by CHAOS-9 until infra lands"],
    )
    assert len(deps) == 1
    assert deps[0].target_work_item_id == "extkey:CHAOS-9"
    assert deps[0].relationship_type == "blocked_by"  # excluded by the resolver


def test_github_comment_capture_ignores_incidental_and_versionish_tokens() -> None:
    # Bare mentions with no explicit linkage signal must NOT be captured —
    # otherwise an unrelated ticket reference could mis-attribute the PR.
    deps = extract_github_comment_dependencies(
        work_item_id="ghpr:o/r#1",
        comment_bodies=[
            "Reminds me of OTHER-123 from last quarter",  # incidental mention
            "bumped to v1-2 and python-3",
            "deploy-9 done",
            "CVE-2024 patched",
        ],
    )
    assert deps == []

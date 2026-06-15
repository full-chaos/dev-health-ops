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


def test_github_comment_capture_extracts_and_dedupes_keys() -> None:
    deps = extract_github_comment_dependencies(
        work_item_id="ghpr:o/r#1",
        comment_bodies=[
            "Linear: CHAOS-2400 linked this PR",
            None,
            "follow-up on CHAOS-2400",  # duplicate key -> one edge
            "no key in this comment",
        ],
    )
    assert [d.target_work_item_id for d in deps] == ["extkey:CHAOS-2400"]
    assert deps[0].relationship_type == "relates_to"
    assert deps[0].relationship_type_raw == "github_comment"


def test_github_comment_capture_ignores_lowercase_versionish_tokens() -> None:
    deps = extract_github_comment_dependencies(
        work_item_id="ghpr:o/r#1",
        comment_bodies=["bumped to v1-2 and python-3", "deploy-9 done"],
    )
    assert deps == []

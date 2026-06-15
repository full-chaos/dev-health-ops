"""Tests for capturing PR/MR <-> issue links that drive team inheritance.

The authoritative link is Linear -> source control (the issue's attachment
points at the GitHub PR / GitLab MR). On the GitHub side the reference often
lives only in a bot comment. These tests cover both capture paths and confirm
the captured edge drives the existing linked-issue team inheritance.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from dev_health_ops.metrics.compute_work_items import build_linked_issue_team_resolver
from dev_health_ops.models.work_items import WorkItem
from dev_health_ops.providers.github.normalize import (
    extract_github_comment_dependencies,
)
from dev_health_ops.providers.linear.client import LinearAuth, LinearClient
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
    # GitHub Enterprise host requires the integration sourceType to be trusted.
    assert (
        _work_item_id_from_pr_url("https://github.example.com/o/r/pull/7", "github")
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


def test_pr_url_parser_rejects_non_scm_host_unless_sourcetype() -> None:
    # A non-public host with a PR-shaped path is not trusted by host alone...
    assert _work_item_id_from_pr_url("https://evil.example/owner/repo/pull/1") is None
    # ...and a forged github-substring host is likewise rejected (no exact match).
    assert _work_item_id_from_pr_url("https://github.evil.example/o/r/pull/1") is None
    # The integration sourceType authorises a self-hosted/Enterprise host.
    assert (
        _work_item_id_from_pr_url(
            "https://gitlab.acme.internal/g/p/-/merge_requests/3", "gitlab"
        )
        == "gitlab:g/p!3"
    )
    # Public hosts are trusted by exact host.
    assert _work_item_id_from_pr_url("https://github.com/o/r/pull/9") == "ghpr:o/r#9"


def test_linear_attachment_gated_by_sourcetype_or_host() -> None:
    # A non-SCM attachment whose URL happens to look like a PR is dropped.
    issue = {
        "attachments": {
            "nodes": [
                {"url": "https://notion.so/o/r/pull/9", "sourceType": "notion"},
            ]
        }
    }
    assert extract_linear_dependencies(issue=issue, work_item_id="linear:CHAOS-1") == []
    # Same path but flagged as a github source -> captured.
    issue["attachments"]["nodes"][0]["sourceType"] = "github"
    deps = extract_linear_dependencies(issue=issue, work_item_id="linear:CHAOS-1")
    assert [d.source_work_item_id for d in deps] == ["ghpr:o/r#9"]


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


def test_github_comment_capture_from_linear_url_only() -> None:
    # Only the linear.app integration URL is trusted; a bare keyword mention in
    # a (third-party) comment is intentionally NOT captured.
    deps = extract_github_comment_dependencies(
        work_item_id="ghpr:o/r#1",
        comment_bodies=[
            "Linked to https://linear.app/full-chaos/issue/CHAOS-2400/title",
            None,
            "Also fixes PROJ-7 per discussion",  # keyword, no URL -> ignored
            "see https://linear.app/x/issue/CHAOS-2400/dup",  # duplicate -> one edge
        ],
    )
    assert [d.target_work_item_id for d in deps] == ["extkey:CHAOS-2400"]
    assert deps[0].relationship_type == "relates_to"
    assert deps[0].relationship_type_raw == "github_comment_linear_url"


def test_github_comment_capture_ignores_bare_keyword_mentions() -> None:
    # Without a linear.app URL, nothing is captured even with linkage keywords —
    # comments are third-party and keyword text is forgeable.
    deps = extract_github_comment_dependencies(
        work_item_id="ghpr:o/r#1",
        comment_bodies=["This is blocked by CHAOS-9", "Fixes CHAOS-10 too"],
    )
    assert deps == []


def test_github_comment_blocking_wins_over_relates_for_same_key() -> None:
    # Two URL links for the SAME key, one with blocking context, must resolve to
    # the blocking (non-inheritable) relationship.
    deps = extract_github_comment_dependencies(
        work_item_id="ghpr:o/r#1",
        comment_bodies=[
            "Tracked in https://linear.app/x/issue/CHAOS-1/foo",
            "Update: blocked by https://linear.app/x/issue/CHAOS-1/bar",
        ],
    )
    assert len(deps) == 1
    assert deps[0].target_work_item_id == "extkey:CHAOS-1"
    assert deps[0].relationship_type == "blocked_by"


def test_github_comment_blocking_intent_preserved_for_url_links() -> None:
    # "blocked by <linear url>" must stay non-inheritable, not default to
    # relates_to just because the reference is a URL.
    deps = extract_github_comment_dependencies(
        work_item_id="ghpr:o/r#1",
        comment_bodies=[
            "This PR is blocked by https://linear.app/x/issue/ABC-123/needs-infra"
        ],
    )
    assert len(deps) == 1
    assert deps[0].target_work_item_id == "extkey:ABC-123"
    assert deps[0].relationship_type == "blocked_by"


def test_client_paginates_issue_attachments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # When an issue's attachment page is truncated, the full set is fetched so a
    # PR/MR link past the first page is not silently dropped.
    client = LinearClient(auth=LinearAuth(api_key="test-key"))
    pages = [
        {
            "issue": {
                "attachments": {
                    "nodes": [{"url": "u1"}],
                    "pageInfo": {"hasNextPage": True, "endCursor": "c1"},
                }
            }
        },
        {
            "issue": {
                "attachments": {
                    "nodes": [{"url": "u2"}],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }
        },
    ]
    calls: list[dict[str, Any] | None] = []

    def _fake_execute(query: str, variables: dict[str, Any] | None = None) -> Any:
        calls.append(variables)
        return pages[len(calls) - 1]

    monkeypatch.setattr(client, "_execute", _fake_execute)
    nodes = client.get_issue_attachments("issue-1")
    assert [n["url"] for n in nodes] == ["u1", "u2"]
    assert len(calls) == 2
    assert calls[1] is not None and calls[1]["after"] == "c1"


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

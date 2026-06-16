"""CHAOS-2461: GitLab work-items sync must honor config-resolved credentials.

These tests pin the contract that the work-items GitLab client is built from
the organization-scoped, database-resolved credential (PAT + optional URL)
and never via a ``GITLAB_TOKEN``/``GITLAB_URL`` environment side-channel.
"""

from __future__ import annotations

import os
from unittest.mock import patch

from dev_health_ops.credentials import CredentialSource, GitLabCredentials
from dev_health_ops.metrics.job_work_items import _build_gitlab_work_client

_ORG_ID = "22222222-2222-2222-2222-222222222222"


def test_build_gitlab_work_client_uses_db_pat_without_env(
    monkeypatch,
) -> None:
    """A database-stored PAT is threaded into the client with no env mutation."""
    monkeypatch.delenv("GITLAB_TOKEN", raising=False)
    monkeypatch.delenv("GITLAB_URL", raising=False)
    creds = GitLabCredentials(
        token="db-gitlab-pat",
        base_url="https://gitlab.com",
        source=CredentialSource.DATABASE,
    )

    with patch(
        "dev_health_ops.credentials.resolver.resolve_credentials_sync",
        return_value=creds,
    ) as resolve:
        token, gitlab_url = _build_gitlab_work_client(org_id=_ORG_ID)

    assert token == "db-gitlab-pat"
    resolve.assert_called_once_with("gitlab", org_id=_ORG_ID, allow_env_fallback=True)
    # No os.environ side-channel: GITLAB_TOKEN must not have been set.
    assert "GITLAB_TOKEN" not in os.environ
    assert "GITLAB_URL" not in os.environ


def test_build_gitlab_work_client_uses_db_pat_with_custom_url(
    monkeypatch,
) -> None:
    """A self-hosted GitLab URL from the DB credential is threaded explicitly."""
    monkeypatch.delenv("GITLAB_TOKEN", raising=False)
    monkeypatch.delenv("GITLAB_URL", raising=False)
    creds = GitLabCredentials(
        token="db-selfhosted-pat",
        base_url="https://gitlab.example.com",
        source=CredentialSource.DATABASE,
    )

    with patch(
        "dev_health_ops.credentials.resolver.resolve_credentials_sync",
        return_value=creds,
    ) as resolve:
        token, gitlab_url = _build_gitlab_work_client(org_id=_ORG_ID)

    assert token == "db-selfhosted-pat"
    assert gitlab_url == "https://gitlab.example.com"
    resolve.assert_called_once_with("gitlab", org_id=_ORG_ID, allow_env_fallback=True)
    assert "GITLAB_TOKEN" not in os.environ
    assert "GITLAB_URL" not in os.environ


def test_build_gitlab_work_client_without_org_falls_back_to_env(
    monkeypatch,
) -> None:
    """With no organization scope, construction falls back to env vars."""
    monkeypatch.setenv("GITLAB_TOKEN", "env-gitlab-token")
    monkeypatch.setenv("GITLAB_URL", "https://gitlab.mycompany.com")

    token, gitlab_url = _build_gitlab_work_client(org_id="")

    assert token == "env-gitlab-token"
    assert gitlab_url == "https://gitlab.mycompany.com"


def test_org_scoped_resolution_wins_over_ambient_env_token(monkeypatch) -> None:
    """Ambient env credentials must not preempt an org's database credential.

    Regression test for CHAOS-2461: the GitLab path must not route to env
    resolution whenever ANY GitLab env credential exists with an org scope
    present — a tenant-boundary violation.
    """
    monkeypatch.setenv("GITLAB_TOKEN", "ambient-env-token")
    monkeypatch.setenv("GITLAB_URL", "https://ambient.gitlab.com")
    creds = GitLabCredentials(
        token="org-db-pat",
        base_url="https://gitlab.com",
        source=CredentialSource.DATABASE,
    )

    with patch(
        "dev_health_ops.credentials.resolver.resolve_credentials_sync",
        return_value=creds,
    ) as resolve:
        token, gitlab_url = _build_gitlab_work_client(org_id=_ORG_ID)

    resolve.assert_called_once_with("gitlab", org_id=_ORG_ID, allow_env_fallback=True)
    assert token == "org-db-pat"
    # The ambient env token must NOT have been used.
    assert token != "ambient-env-token"


def test_fetch_gitlab_work_items_passes_credentials_to_factory(
    monkeypatch,
) -> None:
    """fetch_gitlab_work_items threads token/gitlab_url into the client factory.

    Asserts that:
    - deps.gitlab_client_factory is called with the explicit token and url.
    - os.environ is NOT mutated (GITLAB_TOKEN/GITLAB_URL not added/changed).
    """
    monkeypatch.delenv("GITLAB_TOKEN", raising=False)
    monkeypatch.delenv("GITLAB_URL", raising=False)

    env_snapshot_before = dict(os.environ)

    captured_kwargs: dict = {}

    def fake_factory(*, token: str, gitlab_url: str | None = None):
        captured_kwargs["token"] = token
        captured_kwargs["gitlab_url"] = gitlab_url
        # Return a minimal stub that satisfies the iteration protocol.
        return _StubGitLabClient()

    from dev_health_ops.metrics import dependencies as deps_module
    from dev_health_ops.metrics.work_items import fetch_gitlab_work_items
    from dev_health_ops.providers.identity import IdentityResolver
    from dev_health_ops.providers.status_mapping import StatusMapping

    original_registry = deps_module.get_metrics_dependencies()
    patched_registry = original_registry.__class__(
        **{
            **original_registry.__dict__,
            "gitlab_client_factory": fake_factory,
        }
    )

    with patch.object(deps_module, "_registry", patched_registry):
        items, transitions, attributions = fetch_gitlab_work_items(
            repos=[],
            since=__import__("datetime").datetime(
                2024, 1, 1, tzinfo=__import__("datetime").timezone.utc
            ),
            status_mapping=StatusMapping({}, {}, {}, {}),
            identity=IdentityResolver({}),
            token="threaded-token",
            gitlab_url="https://gitlab.example.com",
            org_id="",
        )

    assert captured_kwargs["token"] == "threaded-token"
    assert captured_kwargs["gitlab_url"] == "https://gitlab.example.com"

    # os.environ must not have been mutated.
    env_snapshot_after = dict(os.environ)
    assert "GITLAB_TOKEN" not in env_snapshot_after
    assert "GITLAB_URL" not in env_snapshot_after
    # No new keys were added.
    assert env_snapshot_after == env_snapshot_before


class _StubGitLabClient:
    """Minimal GitLab client stub that returns empty iterables."""

    def iter_project_issues(self, **_kwargs):
        return iter([])

    def iter_project_merge_requests(self, **_kwargs):
        return iter([])

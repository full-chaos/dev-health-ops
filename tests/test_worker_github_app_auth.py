"""Regression tests for GitHub App auth support in the sync worker path.

Background sync previously extracted only the ``token`` key from decrypted
credentials and raised when it was absent, so GitHub App-auth sync configs
failed in the Celery worker (CHAOS-2234). These tests lock the fix: the worker
and repo-discovery paths build a typed :class:`GitHubCredentials` (PAT or App)
via ``github_credentials_from_mapping`` and mint an installation token for App
auth.

CHAOS-2786 moved BOTH discovery-side installation-token mints -- the frozen
``connectors.github.GitHubConnector`` (single-repo/owner App auth) and
``connectors.utils.github_app.GitHubAppTokenProvider`` (the ``all_repos``
App-installation-listing path, flagged by Codex review as a second
divergent implementation the first pass missed) -- onto
``dev_health_ops.providers.github.app_auth.mint_installation_token`` (a
standalone httpx utility). discovery/repos.py no longer imports anything
from ``dev_health_ops.connectors`` at all. The tests below cover both the
new wiring and that boundary.
"""

from __future__ import annotations

import ast
import inspect
from types import SimpleNamespace
from unittest.mock import patch

from dev_health_ops.credentials import CredentialSource
from dev_health_ops.credentials.resolver import github_credentials_from_mapping
from dev_health_ops.discovery import repos as repos_mod

# ---------------------------------------------------------------------------
# github_credentials_from_mapping
# ---------------------------------------------------------------------------


def test_mapping_builds_pat_credentials() -> None:
    creds = github_credentials_from_mapping({"token": "ghp_secret"})

    assert creds is not None
    assert creds.is_app_auth is False
    assert creds.token == "ghp_secret"
    assert creds.source == CredentialSource.DATABASE


def test_mapping_builds_app_credentials() -> None:
    creds = github_credentials_from_mapping(
        {
            "app_id": "12345",
            "private_key": "synthetic-private-key",
            "installation_id": "67890",
        }
    )

    assert creds is not None
    assert creds.is_app_auth is True
    assert creds.app_id == "12345"
    assert creds.private_key == "synthetic-private-key"
    assert creds.installation_id == "67890"


def test_mapping_resolves_private_key_path(tmp_path) -> None:
    key_path = tmp_path / "app.pem"
    key_path.write_text("pem-contents", encoding="utf-8")

    creds = github_credentials_from_mapping(
        {
            "app_id": "12345",
            "private_key_path": str(key_path),
            "installation_id": "67890",
        }
    )

    assert creds is not None
    assert creds.is_app_auth is True
    assert creds.private_key == "pem-contents"


def test_mapping_returns_none_when_empty() -> None:
    assert github_credentials_from_mapping({}) is None


def test_mapping_returns_none_when_app_incomplete() -> None:
    # Missing installation_id => not a complete App triple and no token.
    assert (
        github_credentials_from_mapping({"app_id": "12345", "private_key": "k"}) is None
    )


def test_mapping_returns_none_when_token_and_app_conflict() -> None:
    # token + App fields is rejected by GitHubCredentials validation.
    assert (
        github_credentials_from_mapping(
            {
                "token": "ghp_secret",
                "app_id": "12345",
                "private_key": "k",
                "installation_id": "67890",
            }
        )
        is None
    )


def test_mapping_ignores_none_values() -> None:
    creds = github_credentials_from_mapping(
        {"token": "ghp_secret", "app_id": None, "installation_id": None}
    )

    assert creds is not None
    assert creds.is_app_auth is False
    assert creds.token == "ghp_secret"


# ---------------------------------------------------------------------------
# discover_repos_for_config (repo discovery)
# ---------------------------------------------------------------------------


def _make_config(provider: str, sync_options: dict | None = None):
    return SimpleNamespace(provider=provider, sync_options=sync_options or {})


def test_discover_github_app_auth_mints_token_via_providers_app_auth() -> None:
    app_credentials = {
        "app_id": "12345",
        "private_key": "synthetic-private-key",
        "installation_id": "67890",
    }
    config = _make_config("github", {"search": "my-org/*"})

    with (
        patch.object(repos_mod, "discover_github_repos") as discover,
        patch(
            "dev_health_ops.providers.github.app_auth.mint_installation_token"
        ) as mint_token,
    ):
        mint_token.return_value = "installation-token"
        discover.return_value = [("my-org", "api")]

        result = repos_mod.discover_repos_for_config(config, app_credentials)

    assert result == [("my-org", "api")]
    # App credentials were converted to a typed credential and the providers
    # auth utility minted an installation token from them (no GitHubConnector).
    assert mint_token.call_args.kwargs["app_id"] == "12345"
    assert mint_token.call_args.kwargs["private_key"] == "synthetic-private-key"
    assert mint_token.call_args.kwargs["installation_id"] == "67890"
    # discover_github_repos receives the minted token string, not raw app fields.
    assert discover.call_args.args[1] == "installation-token"


def _is_connectors_target(name: str | None) -> bool:
    return name is not None and (
        name == "dev_health_ops.connectors"
        or name.startswith("dev_health_ops.connectors.")
    )


def _find_connectors_imports(source: str) -> list[str]:
    """Return every ``dev_health_ops.connectors``-reaching import in ``source``.

    Walks the AST (covering import statements nested inside function bodies,
    i.e. the lazy-import style this codebase uses throughout) rather than
    substring-matching, so formatting/comments can't fool it. Handles every
    import *form* that can name a connectors symbol:

    - ``import dev_health_ops.connectors[.x][ as y]``
    - ``from dev_health_ops.connectors[.x] import y``
    - ``from dev_health_ops import connectors`` -- the form a first version of
      this guard missed (flagged by Codex adversarial review round 2 on
      CHAOS-2786's PR): ``ImportFrom.module`` is just ``"dev_health_ops"``
      here, so a module-prefix-only check passes it clean. Fixed by resolving
      each imported *alias* to its fully-qualified dotted name
      (``f"{module}.{alias.name}"``) and checking that instead of the bare
      module string.
    - ``importlib.import_module("dev_health_ops.connectors...")`` with a
      string-literal argument (best-effort; a dynamically-built string isn't
      statically resolvable and isn't this codebase's style anyway).
    """
    tree = ast.parse(source)
    found: list[str] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.extend(
                alias.name for alias in node.names if _is_connectors_target(alias.name)
            )
        elif isinstance(node, ast.ImportFrom) and node.level == 0:
            module = node.module or ""
            for alias in node.names:
                full = f"{module}.{alias.name}" if module else alias.name
                if _is_connectors_target(full):
                    found.append(full)
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "import_module"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "importlib"
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
            and _is_connectors_target(node.args[0].value)
        ):
            found.append(node.args[0].value)

    return found


def test_connectors_import_guard_catches_named_submodule_import_form() -> None:
    """Prove the guard itself trips on the exact form it previously missed.

    Codex adversarial review round 2 verified that
    ``from dev_health_ops import connectors`` -- placed inside a function
    body, matching this codebase's lazy-import convention -- parses as
    ``ImportFrom(module='dev_health_ops', names=[alias(name='connectors')])``
    and slipped past a guard that only checked ``ImportFrom.module`` for a
    connectors prefix. This is a synthetic-source regression test for the
    *guard*, not for ``discovery/repos.py`` (see
    ``test_discovery_repos_module_has_no_connectors_package_import`` below
    for that): it proves ``_find_connectors_imports`` now flags this form,
    an unrelated sibling import in the same statement is not mistakenly
    flagged, and an unrelated module ("connectors" as a local/attribute name
    that isn't ``dev_health_ops.connectors``) is left alone.
    """
    regressed_source = """
def _mint_token_the_wrong_way():
    from dev_health_ops import connectors, credentials

    return connectors.github.GitHubConnector(credentials=credentials).token
"""
    found = _find_connectors_imports(regressed_source)
    assert found == ["dev_health_ops.connectors"]

    clean_source = """
def _mint_token_the_right_way():
    from dev_health_ops.providers.github.app_auth import mint_installation_token

    return mint_installation_token(app_id="1", private_key="k", installation_id="2")
"""
    assert _find_connectors_imports(clean_source) == []

    unrelated_source = """
def _uses_an_unrelated_local_named_connectors():
    connectors = {"github": object()}
    return connectors["github"]
"""
    assert _find_connectors_imports(unrelated_source) == []


def test_discovery_repos_module_has_no_connectors_package_import() -> None:
    """discovery/repos.py must not import ANYTHING from ``dev_health_ops.connectors``.

    A first-pass version of this test only rejected the literal strings
    ``GitHubConnector``/``connectors.github``, which passed even though
    ``_discover_github_app_installation_repos`` (the ``all_repos=True``
    App-auth path) still minted tokens via
    ``dev_health_ops.connectors.utils.github_app.GitHubAppTokenProvider`` --
    a second, divergent JWT/retry implementation living in the same module
    (caught by Codex adversarial review round 1 on CHAOS-2786's PR). Both
    App-auth call sites now go through ``providers.github.app_auth`` instead,
    so the boundary is: no ``dev_health_ops.connectors`` import anywhere in
    this module, full stop -- no allowlist. See
    ``test_connectors_import_guard_catches_named_submodule_import_form``
    above for proof the detector itself (round 2 hardening) can't be fooled
    by the ``from dev_health_ops import connectors`` form.
    """
    source = inspect.getsource(repos_mod)
    connectors_imports = _find_connectors_imports(source)

    assert connectors_imports == [], (
        "discovery/repos.py must not import from dev_health_ops.connectors "
        f"(found: {connectors_imports})"
    )
    assert "dev_health_ops.providers.github.app_auth" in source


def test_discover_github_pat_passes_token_through() -> None:
    from dev_health_ops.discovery import repos as repos_mod

    config = _make_config("github", {"search": "my-org/*"})

    with patch.object(repos_mod, "discover_github_repos") as discover:
        discover.return_value = [("my-org", "api")]
        result = repos_mod.discover_repos_for_config(config, {"token": "ghp_x"})

    assert result == [("my-org", "api")]
    assert discover.call_args.args[1] == "ghp_x"


def test_discover_github_attempts_anonymous_without_credentials() -> None:
    """Without credentials we still attempt anonymous public-repo discovery.

    Regression guard for CHAOS-2246: a credential-less GitHub config must not
    short-circuit to ``[]`` before discovery; it should attempt anonymous
    public-repo discovery with an empty token.
    """
    from dev_health_ops.discovery import repos as repos_mod

    config = _make_config("github", {"search": "my-org/*"})

    with patch.object(repos_mod, "discover_github_repos") as discover:
        discover.return_value = [("my-org", "public-api")]
        result = repos_mod.discover_repos_for_config(config, {})

    assert result == [("my-org", "public-api")]
    discover.assert_called_once()
    # Empty token signals the anonymous discovery path.
    assert discover.call_args.args[1] == ""


def test_discover_github_repos_uses_anonymous_client_without_token() -> None:
    """``discover_github_repos`` uses an unauthenticated client when token is empty."""
    from dev_health_ops.discovery import repos as repos_mod

    with patch("github.Github") as github_cls:
        client = github_cls.return_value
        client.get_organization.return_value.get_repos.return_value = []
        repos_mod.discover_github_repos({"search": "my-org/*"}, "")

    # No token => anonymous client constructed with no arguments.
    github_cls.assert_called_once_with()


def test_discover_github_repos_uses_token_client_when_present() -> None:
    """``discover_github_repos`` passes the token to the client when present."""
    from dev_health_ops.discovery import repos as repos_mod

    with patch("github.Github") as github_cls:
        client = github_cls.return_value
        client.get_organization.return_value.get_repos.return_value = []
        repos_mod.discover_github_repos({"search": "my-org/*"}, "ghp_x")

    github_cls.assert_called_once_with("ghp_x")

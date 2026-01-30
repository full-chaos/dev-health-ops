"""
Provider registry for resolving provider instances by name.

Usage:
    from dev_health_ops.providers.registry import get_provider, register_provider

    provider = get_provider("jira")
    batch = provider.ingest(ctx)
"""

from __future__ import annotations

import logging
from typing import Callable, Dict

from dev_health_ops.providers.base import Provider

logger = logging.getLogger(__name__)

# Registry mapping provider name -> provider class or factory
_registry: Dict[str, Callable[[], Provider]] = {}


def register_provider(name: str, factory: Callable[[], Provider]) -> None:
    """
    Register a provider factory under the given name.

    Args:
        name: unique provider identifier (e.g. "jira", "github", "gitlab")
        factory: callable that returns a Provider instance
    """
    _registry[name.lower()] = factory


def get_provider(name: str) -> Provider:
    """
    Resolve and instantiate a provider by name.

    Args:
        name: provider identifier (case-insensitive)

    Returns:
        Provider instance

    Raises:
        ValueError: if provider is not registered
    """
    key = name.lower()
    if key not in _registry:
        available = sorted(_registry.keys())
        raise ValueError(
            f"Unknown provider '{name}'. Available: {available or '(none registered)'}"
        )
    return _registry[key]()


def list_providers() -> list[str]:
    """
    Return list of registered provider names.
    """
    return sorted(_registry.keys())


def is_registered(name: str) -> bool:
    """
    Check if a provider is registered.
    """
    return name.lower() in _registry


# Auto-register built-in providers on import
def _register_builtins() -> None:
    """
    Register built-in providers.

    This is called at module load time. Providers that require optional
    dependencies are registered with lazy factories that import on demand.
    """

    # Jira provider (lazy import to avoid requiring jira package at startup)
    def _jira_factory() -> Provider:
        from dev_health_ops.providers.jira.provider import JiraProvider

        return JiraProvider()

    register_provider("jira", _jira_factory)

    # GitLab provider (lazy import to avoid requiring python-gitlab at startup)
    def _gitlab_factory() -> Provider:
        from dev_health_ops.providers.gitlab.provider import GitLabProvider

        return GitLabProvider()

    register_provider("gitlab", _gitlab_factory)

    # GitHub provider (lazy import to avoid requiring PyGithub at startup)
    def _github_factory() -> Provider:
        from dev_health_ops.providers.github.provider import GitHubProvider

        return GitHubProvider()

    register_provider("github", _github_factory)

    def _linear_factory() -> Provider:
        from dev_health_ops.providers.linear.provider import LinearProvider

        return LinearProvider()

    register_provider("linear", _linear_factory)


_register_builtins()

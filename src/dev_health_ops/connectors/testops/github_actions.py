"""Compatibility shim for the re-homed GitHub Actions TestOps adapter.

Deprecated: import from ``dev_health_ops.providers.github.testops_pipeline``.
This legacy connector path remains for one release only.
"""

from dev_health_ops.providers.github.testops_pipeline import GitHubActionsAdapter

__all__ = ["GitHubActionsAdapter"]

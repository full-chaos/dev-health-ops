"""Compatibility shim for the re-homed GitLab CI TestOps adapter.

Deprecated: import from ``dev_health_ops.providers.gitlab.testops_pipeline``.
This legacy connector path remains for one release only.
"""

from dev_health_ops.providers.gitlab.testops_pipeline import GitLabCIAdapter

__all__ = ["GitLabCIAdapter"]

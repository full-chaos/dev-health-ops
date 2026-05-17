"""Compatibility aliases for legacy TestOps connector package imports.

Deprecated: import TestOps adapters from ``dev_health_ops.providers.<provider>``
and shared contracts from ``dev_health_ops.providers._base``. This legacy
connector package remains for one release only.
"""

from dev_health_ops.providers.github.testops_pipeline import GitHubActionsAdapter
from dev_health_ops.providers.gitlab.testops_pipeline import GitLabCIAdapter

from .base import BasePipelineAdapter, PipelineSyncBatch

__all__ = [
    "BasePipelineAdapter",
    "PipelineSyncBatch",
    "GitHubActionsAdapter",
    "GitLabCIAdapter",
]

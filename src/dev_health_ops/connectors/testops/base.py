"""Compatibility aliases for legacy TestOps connector imports.

New TestOps providers should import these contracts from
``dev_health_ops.providers._base``. This module remains so existing processor
and test imports continue to work while the adapter modules are re-homed.
"""

from dev_health_ops.providers._base import BasePipelineAdapter, PipelineSyncBatch

__all__ = ["BasePipelineAdapter", "PipelineSyncBatch"]

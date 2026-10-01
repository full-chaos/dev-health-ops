"""
WellbeingMixin — emptied (CHAOS-7476); see the class docstring.

Formerly: user metrics write methods (table user_metrics_daily).

CHAOS-5245 deleted write_quality_drag/write_pipeline_stability (testops_risk's
Python compute+write) -- the native Go executor (CHAOS-4294) has no fallback
left.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dev_health_ops.metrics.sinks.clickhouse._insert import _ClickHouseSinkBase
else:

    class _ClickHouseSinkBase:
        pass


class WellbeingMixin(_ClickHouseSinkBase):
    """Empty since CHAOS-7476: ``write_user_metrics`` was deleted.

    The class stays only because the live-Python oracle loader
    (``internal/providersync/testdata/python_oracle_loader.py``) loads this module by path, and the
    frozen providersync goldens are keyed by that loader's text; remove this module together with
    that entry when the loader is retired. ``user_metrics_daily`` is written by the Go
    ``repo_user_commit`` and ``ic_finalize`` families, which refuse an empty organization.
    """

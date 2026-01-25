"""Canonical investment taxonomy re-export.

The authoritative taxonomy lives in `investment_taxonomy.py` to ensure it is
available to both compute-time (`work_graph/`) and request-time (API) code even
when `work_graph` is not installed as a package.
"""

from dev_health_ops.investment_taxonomy import SUBCATEGORIES, THEMES, theme_of

__all__ = ["THEMES", "SUBCATEGORIES", "theme_of"]

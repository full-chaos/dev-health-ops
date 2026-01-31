from dev_health_ops.utils.datetime import naive_utc, to_utc

import os
import importlib.util

_legacy_utils_path = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "utils.py"
)
_spec = importlib.util.spec_from_file_location("_legacy_utils", _legacy_utils_path)
_legacy_utils = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_legacy_utils)

REPO_ROOT = _legacy_utils.REPO_ROOT
BATCH_SIZE = _legacy_utils.BATCH_SIZE
MAX_WORKERS = _legacy_utils.MAX_WORKERS
AGGREGATE_STATS_MARKER = _legacy_utils.AGGREGATE_STATS_MARKER
REPO_PATH = _legacy_utils.REPO_PATH
SKIP_EXTENSIONS = _legacy_utils.SKIP_EXTENSIONS
CONNECTORS_AVAILABLE = _legacy_utils.CONNECTORS_AVAILABLE
is_skippable = _legacy_utils.is_skippable
iter_commits_since = _legacy_utils.iter_commits_since
collect_changed_files = _legacy_utils.collect_changed_files
match_pattern = _legacy_utils.match_pattern
_normalize_datetime = _legacy_utils._normalize_datetime
_parse_date = _legacy_utils._parse_date
_parse_since = _legacy_utils._parse_since
_since_from_date_backfill = _legacy_utils._since_from_date_backfill
_resolve_since = _legacy_utils._resolve_since
_resolve_max_commits = _legacy_utils._resolve_max_commits
_split_full_name = _legacy_utils._split_full_name

__all__ = [
    "naive_utc",
    "to_utc",
    "REPO_ROOT",
    "BATCH_SIZE",
    "MAX_WORKERS",
    "AGGREGATE_STATS_MARKER",
    "REPO_PATH",
    "SKIP_EXTENSIONS",
    "CONNECTORS_AVAILABLE",
    "is_skippable",
    "iter_commits_since",
    "collect_changed_files",
    "match_pattern",
    "_normalize_datetime",
    "_parse_date",
    "_parse_since",
    "_since_from_date_backfill",
    "_resolve_since",
    "_resolve_max_commits",
    "_split_full_name",
]

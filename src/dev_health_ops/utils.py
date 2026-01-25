import argparse
import fnmatch
import os
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Optional, Iterable, List, Union, Tuple

# Constants
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
BATCH_SIZE = 1000


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return int(default)
    try:
        return max(1, int(str(raw).strip()))
    except ValueError:
        return int(default)


# Keep default concurrency conservative; override via env.
MAX_WORKERS = _int_env("MAX_WORKERS", 4)
AGGREGATE_STATS_MARKER = "__AGGREGATE__"
REPO_PATH = os.getenv("REPO_PATH", ".")
SKIP_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".ico",
    ".pdf",
    ".zip",
    ".tar",
    ".gz",
    ".7z",
    ".exe",
    ".dll",
    ".so",
    ".dylib",
    ".class",
    ".pyc",
    ".o",
    ".obj",
    ".bin",
    ".bak",
    ".tmp",
    ".svg",
    ".eot",
    ".ttf",
    ".woff",
    ".woff2",
    ".mp4",
    ".mp3",
    ".wav",
    ".mov",
    ".avi",
    ".mkv",
    ".webm",
    ".jar",
    ".war",
    ".ear",
}
CONNECTORS_AVAILABLE = True
try:
    import importlib.util

    CONNECTORS_AVAILABLE = importlib.util.find_spec("connectors") is not None
except ImportError:
    CONNECTORS_AVAILABLE = False


def _normalize_datetime(dt: datetime) -> datetime:
    """Ensure datetime is offset-aware (UTC)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_date(value: str) -> date:
    from datetime import date
    import argparse

    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}', expected YYYY-MM-DD"
        ) from exc


def is_skippable(file_path: str) -> bool:
    """Check if a file should be skipped based on extension or name."""
    skip_dirs = {
        "node_modules",
        "vendor",
        ".git",
        ".svn",
        ".hg",
        ".idea",
        ".vscode",
        "__pycache__",
        "dist",
        "build",
        "target",
        "bin",
        "obj",
    }

    path = Path(file_path)
    if path.suffix.lower() in SKIP_EXTENSIONS:
        return True

    for part in path.parts:
        if part in skip_dirs:
            return True

    return False


def _parse_since(value: Optional[str]) -> Optional[datetime]:
    """
    Parse a --since/--start-date argument into a timezone-aware datetime.
    """
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as e:
        raise ValueError(f"Invalid --since value '{value}': {e}") from e

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed


def _since_from_date_backfill(day: date, backfill_days: int) -> datetime:
    """
    Calculate start datetime from a date and backfill window.
    """
    from datetime import timedelta, timezone

    backfill = max(1, int(backfill_days))
    start_day = day - timedelta(days=backfill - 1)
    return datetime(
        start_day.year,
        start_day.month,
        start_day.day,
        tzinfo=timezone.utc,
    )


def _resolve_since(ns: argparse.Namespace) -> Optional[datetime]:
    """
    Resolve start datetime from argparse namespace.
    """
    if getattr(ns, "date", None) is not None:
        return _since_from_date_backfill(ns.date, ns.backfill)
    if int(getattr(ns, "backfill", 1)) != 1:
        raise SystemExit("--backfill requires --date")
    return _parse_since(getattr(ns, "since", None))


def _resolve_max_commits(ns: argparse.Namespace) -> Optional[int]:
    """
    Resolve max commits per repo from argparse namespace.
    """
    max_commits = getattr(ns, "max_commits_per_repo", None)
    if getattr(ns, "date", None) is not None or getattr(ns, "since", None):
        return max_commits if max_commits is not None else None
    return max_commits or 100


def iter_commits_since(repo, since: Optional[datetime]) -> Iterable:
    """
    Iterate over commits, stopping once we reach commits older than `since`.
    Expects a GitPython Repo object.
    """
    # Note: We don't type hint 'repo' strongly here to avoid importing gitpython if not needed elsewhere
    for commit in repo.iter_commits():
        commit_dt = _normalize_datetime(commit.committed_datetime)
        if since and commit_dt < since:
            break
        yield commit


def collect_changed_files(repo_root: Union[str, Path], commits: Iterable) -> List[Path]:
    """
    Collect unique file paths touched by the provided commits.
    Expects iterable of GitPython commit objects.
    """
    root_path = Path(repo_root).resolve()
    paths = set()

    for commit in commits:
        try:
            diffs = (
                commit.parents[0].diff(commit, create_patch=False)
                if commit.parents
                else commit.diff(None, create_patch=False)
            )
        except Exception:
            continue

        for diff in diffs:
            file_path = diff.b_path if diff.b_path else diff.a_path
            if not file_path or is_skippable(file_path):
                continue
            # We construct full path to check existence, but return relative paths?
            # Original code returned Paths.
            # Original code: candidate = root_path / file_path; if candidate.exists(): paths.add(candidate)
            candidate = (root_path / file_path).resolve()
            if candidate.exists():
                paths.add(candidate)

    return sorted(paths)


def _split_full_name(full_name: str) -> Tuple[str, str]:
    parts = (full_name or "").split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid repo/project full name: {full_name}")
    return parts[0], parts[1]


def match_pattern(value: str, pattern: str) -> bool:
    """
    Match a string value against an fnmatch-style pattern (case-insensitive).
    """
    if not pattern or pattern == "*":
        return True
    return fnmatch.fnmatch((value or "").lower(), pattern.lower())

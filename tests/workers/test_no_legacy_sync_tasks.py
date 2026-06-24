"""Static guard: legacy sync entrypoints must not reappear (CHAOS-2647).

Scans the source text of the three surviving task-surface files and asserts
that deleted module files are absent.  Any re-introduction of a forbidden
identifier causes an immediate CI failure.
"""

from __future__ import annotations

from pathlib import Path

# ---------------------------------------------------------------------------
# Repo-relative path resolution
# ---------------------------------------------------------------------------

_WORKTREE_ROOT = Path(__file__).resolve().parents[2]
_SRC = _WORKTREE_ROOT / "src" / "dev_health_ops"

_SCANNED_FILES = [
    _SRC / "workers" / "tasks.py",
    _SRC / "api" / "admin" / "routers" / "sync.py",
    _SRC / "workers" / "sync_scheduler.py",
]

_DELETED_MODULES = [
    _SRC / "workers" / "sync_tasks.py",
    _SRC / "workers" / "sync_misc.py",
    _SRC / "workers" / "sync_backfill.py",
    _SRC / "workers" / "sync_batch.py",
    _SRC / "workers" / "sync_runtime.py",
]

_FORBIDDEN_IDENTIFIERS = [
    "run_sync_config",
    "run_backfill",
    "run_work_items_sync",
    "dispatch_batch_sync",
    "_run_sync_for_repo",
]


def test_deleted_legacy_modules_are_absent() -> None:
    """Deleted worker modules must not exist in the source tree."""
    for path in _DELETED_MODULES:
        assert not path.exists(), (
            f"Deleted legacy module {path.relative_to(_WORKTREE_ROOT)} "
            "must not exist — it was removed as part of the sync fan-out "
            "consolidation (CHAOS-2647)."
        )


def test_no_forbidden_legacy_identifiers_in_task_surfaces() -> None:
    """Forbidden legacy entrypoints must not appear in surviving task files."""
    for source_file in _SCANNED_FILES:
        assert source_file.exists(), (
            f"Expected source file {source_file.relative_to(_WORKTREE_ROOT)} "
            "to exist for scanning."
        )
        text = source_file.read_text(encoding="utf-8")
        for identifier in _FORBIDDEN_IDENTIFIERS:
            assert identifier not in text, (
                f"Forbidden legacy identifier {identifier!r} found in "
                f"{source_file.relative_to(_WORKTREE_ROOT)}. "
                "This entrypoint was deleted in the sync fan-out consolidation "
                "(CHAOS-2647) and must not reappear."
            )

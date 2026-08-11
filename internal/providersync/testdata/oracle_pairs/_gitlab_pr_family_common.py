from __future__ import annotations

import importlib.util
import pathlib
from datetime import datetime
from typing import Any

from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
PROCESSOR_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/gitlab.py"
BASE_GIT_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/base_git.py"
PR_STATE_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/pr_state.py"
MODEL_SOURCE = REPO_ROOT / "src/dev_health_ops/models/git.py"


def parse_time(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _load_pr_state() -> Any:
    spec = importlib.util.spec_from_file_location(
        "dev_health_ops_gitlab_pr_state_oracle_target",
        PR_STATE_SOURCE.resolve(strict=True),
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load {PR_STATE_SOURCE}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _live_values(
    case: dict[str, Any],
) -> tuple[Any, Any, list[Any], datetime | None, datetime]:
    """Run the real GitLab mapper and row builder dependencies."""
    processor = load_live_module(PROCESSOR_SOURCE)
    base_git = load_live_module(BASE_GIT_SOURCE)
    state = _load_pr_state()
    mr = case["mr"]
    created_at = parse_time(mr.get("created_at"))
    normalized_at = parse_time(case["normalized_at"])
    if normalized_at is None:
        raise ValueError("normalized_at is required")
    approvals = case.get("approvals")
    notes = case.get("notes") or []
    reviews, first_review_at = processor.map_gitlab_mr_reviews(
        repo_id=case["repo_id"],
        number=int(mr.get("iid") or 0),
        approvals=approvals,
        notes=notes,
        fallback_at=created_at,
        author_username=(mr.get("author") or {}).get("username"),
    )
    merged_at = parse_time(mr.get("merged_at"))
    closed_at = parse_time(mr.get("closed_at"))
    row = base_git.build_git_pull_request(
        repo_id=case["repo_id"],
        number=int(mr.get("iid") or 0),
        title=mr.get("title") or None,
        body=mr.get("description"),
        state=state.normalize_pr_state(mr.get("state"), merged_at),
        author_name=(mr.get("author") or {}).get("username") or "Unknown",
        author_email=None,
        created_at=created_at,
        merged_at=merged_at,
        closed_at=closed_at,
        head_branch=mr.get("source_branch"),
        base_branch=mr.get("target_branch"),
        first_review_at=first_review_at,
        changes_requested_count=sum(
            1 for review in reviews if review.state == "CHANGES_REQUESTED"
        ),
        reviews_count=len(reviews),
        comments_count=int(mr.get("user_notes_count") or 0),
    )
    return row, reviews, first_review_at, created_at, normalized_at


def build_pr_row(case: dict[str, Any]) -> dict[str, Any]:
    row, _, _, _, _ = _live_values(case)
    return vars(row)


def build_review_row(case: dict[str, Any]) -> dict[str, Any]:
    _, reviews, _, _, _ = _live_values(case)
    index = int(case.get("review_index", 0))
    if index < 0 or index >= len(reviews):
        raise ValueError(
            f"review_index {index} outside {len(reviews)} live review rows"
        )
    return vars(reviews[index])

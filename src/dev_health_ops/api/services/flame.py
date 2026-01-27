from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException

from ..models.schemas import FlameFrame, FlameResponse, FlameTimeline
from ..queries.client import clickhouse_client
from ..queries.flame import (
    fetch_deployment,
    fetch_issue,
    fetch_pull_request,
    fetch_pull_request_reviews,
)
from ..queries.scopes import parse_uuid


def _now_like(reference: Optional[datetime]) -> datetime:
    now = datetime.now(timezone.utc)
    if reference and reference.tzinfo is not None:
        return now
    return now.replace(tzinfo=None)


def _frame(
    *,
    frame_id: str,
    parent_id: Optional[str],
    label: str,
    start: Optional[datetime],
    end: Optional[datetime],
    state: str,
    category: str,
) -> Optional[FlameFrame]:
    if start is None or end is None:
        return None
    if end <= start:
        return None
    return FlameFrame(
        id=frame_id,
        parent_id=parent_id,
        label=label,
        start=start,
        end=end,
        state=state,
        category=category,
    )


def _parse_repo_entity(entity_id: str) -> Tuple[str, str]:
    if ":" not in entity_id:
        raise HTTPException(
            status_code=400, detail="Entity id must include repo_id prefix"
        )
    repo_id, item_id = entity_id.split(":", 1)
    if not parse_uuid(repo_id):
        raise HTTPException(status_code=400, detail="Invalid repo id")
    if not item_id:
        raise HTTPException(status_code=400, detail="Entity id missing suffix")
    return repo_id, item_id


def validate_flame_frames(timeline: FlameTimeline, frames: List[FlameFrame]) -> bool:
    if not frames:
        return False
    top_level = [frame for frame in frames if frame.parent_id is None]
    if not top_level:
        return False

    ordered = sorted(top_level, key=lambda frame: frame.start)
    cursor = ordered[0].start
    if cursor > timeline.start:
        return False

    current_end = ordered[0].end
    for frame in ordered[1:]:
        if frame.start > current_end:
            return False
        if frame.end > current_end:
            current_end = frame.end
    return current_end >= timeline.end


def _review_state(value: Optional[str]) -> str:
    return (value or "").strip().lower()


def _rework_windows(
    reviews: List[Dict[str, Any]],
    *,
    review_start: datetime,
    review_end: datetime,
) -> List[Tuple[datetime, datetime]]:
    windows: List[Tuple[datetime, datetime]] = []
    if not reviews:
        return windows

    ordered = [r for r in reviews if r.get("submitted_at")]
    ordered.sort(key=lambda r: r.get("submitted_at"))

    for idx, review in enumerate(ordered):
        state = _review_state(review.get("state"))
        if state not in {"changes_requested", "requested_changes", "request_changes"}:
            continue
        start = review.get("submitted_at")
        if start is None:
            continue
        next_time = review_end
        for next_review in ordered[idx + 1 :]:
            candidate = next_review.get("submitted_at")
            if candidate and candidate > start:
                next_time = candidate
                break
        if start < review_start:
            start = review_start
        if next_time > review_end:
            next_time = review_end
        if next_time > start:
            windows.append((start, next_time))

    return windows


async def build_flame_response(
    *,
    db_url: str,
    entity_type: str,
    entity_id: str,
) -> FlameResponse:
    async with clickhouse_client(db_url) as sink:
        if entity_type == "pr":
            repo_id, suffix = _parse_repo_entity(entity_id)
            try:
                number = int(suffix)
            except Exception as exc:
                raise HTTPException(
                    status_code=400, detail="PR id must be numeric"
                ) from exc

            pr = await fetch_pull_request(sink, repo_id=repo_id, number=number)
            if not pr:
                raise HTTPException(status_code=404, detail="PR not found")

            reviews = await fetch_pull_request_reviews(
                sink, repo_id=repo_id, number=number
            )

            start = pr.get("created_at")
            end = pr.get("merged_at") or pr.get("closed_at") or _now_like(start)
            if start is None:
                raise HTTPException(status_code=404, detail="PR timeline unavailable")

            root_id = f"pr:{repo_id}:{number}"
            frames: List[FlameFrame] = []
            root = _frame(
                frame_id=root_id,
                parent_id=None,
                label="PR lifecycle",
                start=start,
                end=end,
                state="active",
                category="planned",
            )
            if root:
                frames.append(root)

            first_review_at = pr.get("first_review_at")
            if first_review_at and first_review_at > start:
                wait = _frame(
                    frame_id=f"{root_id}:wait",
                    parent_id=root_id,
                    label="Review waiting",
                    start=start,
                    end=first_review_at,
                    state="waiting",
                    category="planned",
                )
                if wait:
                    frames.append(wait)

            review_start = first_review_at or start
            review = _frame(
                frame_id=f"{root_id}:review",
                parent_id=root_id,
                label="Review and merge",
                start=review_start,
                end=end,
                state="active",
                category="planned",
            )
            if review:
                frames.append(review)
                for idx, (rework_start, rework_end) in enumerate(
                    _rework_windows(reviews, review_start=review_start, review_end=end),
                    start=1,
                ):
                    rework = _frame(
                        frame_id=f"{root_id}:rework:{idx}",
                        parent_id=review.id,
                        label="Rework loop",
                        start=rework_start,
                        end=rework_end,
                        state="active",
                        category="rework",
                    )
                    if rework:
                        frames.append(rework)

            timeline = FlameTimeline(start=start, end=end)
            if not validate_flame_frames(timeline, frames):
                raise HTTPException(status_code=422, detail="Flame frames have gaps")

            entity = {
                "repo_id": repo_id,
                "number": number,
                "title": pr.get("title"),
                "state": pr.get("state"),
            }
            return FlameResponse(entity=entity, timeline=timeline, frames=frames)

        if entity_type == "issue":
            issue = await fetch_issue(sink, work_item_id=entity_id)
            if not issue:
                raise HTTPException(status_code=404, detail="Issue not found")

            start = issue.get("created_at")
            if start is None:
                raise HTTPException(
                    status_code=404, detail="Issue timeline unavailable"
                )
            end = issue.get("completed_at") or _now_like(start)

            root_id = f"issue:{entity_id}"
            frames: List[FlameFrame] = []
            root = _frame(
                frame_id=root_id,
                parent_id=None,
                label="Issue lifecycle",
                start=start,
                end=end,
                state="active",
                category="planned",
            )
            if root:
                frames.append(root)

            started_at = issue.get("started_at")
            if started_at and started_at > start:
                waiting = _frame(
                    frame_id=f"{root_id}:wait",
                    parent_id=root_id,
                    label="Backlog waiting",
                    start=start,
                    end=started_at,
                    state="waiting",
                    category="planned",
                )
                if waiting:
                    frames.append(waiting)

            progress_start = started_at or start
            progress = _frame(
                frame_id=f"{root_id}:work",
                parent_id=root_id,
                label="Active work",
                start=progress_start,
                end=end,
                state="active",
                category="planned",
            )
            if progress:
                frames.append(progress)

            timeline = FlameTimeline(start=start, end=end)
            if not validate_flame_frames(timeline, frames):
                raise HTTPException(status_code=422, detail="Flame frames have gaps")

            entity = {
                "work_item_id": issue.get("work_item_id"),
                "provider": issue.get("provider"),
                "type": issue.get("type"),
                "status": issue.get("status"),
            }
            return FlameResponse(entity=entity, timeline=timeline, frames=frames)

        if entity_type == "deployment":
            repo_id, deployment_id = _parse_repo_entity(entity_id)
            deployment = await fetch_deployment(
                sink, repo_id=repo_id, deployment_id=deployment_id
            )
            if not deployment:
                raise HTTPException(status_code=404, detail="Deployment not found")

            start = (
                deployment.get("started_at")
                or deployment.get("merged_at")
                or deployment.get("deployed_at")
            )
            if start is None:
                raise HTTPException(
                    status_code=404, detail="Deployment timeline unavailable"
                )
            end = (
                deployment.get("finished_at")
                or deployment.get("deployed_at")
                or _now_like(start)
            )

            root_id = f"deploy:{repo_id}:{deployment_id}"
            frames: List[FlameFrame] = []
            root = _frame(
                frame_id=root_id,
                parent_id=None,
                label="Deployment lifecycle",
                start=start,
                end=end,
                state="ci",
                category="planned",
            )
            if root:
                frames.append(root)

            merged_at = deployment.get("merged_at")
            if merged_at and merged_at < start:
                queue = _frame(
                    frame_id=f"{root_id}:queue",
                    parent_id=root_id,
                    label="Queue waiting",
                    start=merged_at,
                    end=start,
                    state="waiting",
                    category="planned",
                )
                if queue:
                    frames.append(queue)

            pipeline = _frame(
                frame_id=f"{root_id}:pipeline",
                parent_id=root_id,
                label="Deploy pipeline",
                start=start,
                end=end,
                state="ci",
                category="planned",
            )
            if pipeline:
                frames.append(pipeline)

            deployed_at = deployment.get("deployed_at")
            if (
                pipeline
                and deployed_at
                and deployed_at > pipeline.start
                and end > deployed_at
            ):
                deploy = _frame(
                    frame_id=f"{root_id}:deploy",
                    parent_id=pipeline.id,
                    label="Deploy",
                    start=deployed_at,
                    end=end,
                    state="active",
                    category="planned",
                )
                if deploy:
                    frames.append(deploy)

            timeline = FlameTimeline(start=start, end=end)
            if not validate_flame_frames(timeline, frames):
                raise HTTPException(status_code=422, detail="Flame frames have gaps")

            entity = {
                "repo_id": repo_id,
                "deployment_id": deployment_id,
                "status": deployment.get("status"),
                "environment": deployment.get("environment"),
            }
            return FlameResponse(entity=entity, timeline=timeline, frames=frames)

    raise HTTPException(status_code=404, detail="Unknown entity type")

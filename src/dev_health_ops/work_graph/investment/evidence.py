"""Evidence and text bundle helpers for investment materialization."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

from dev_health_ops.work_graph.investment.types import TextBundle
from dev_health_ops.work_graph.investment.utils import clamp, evidence_quality_band

MAX_ISSUES = 6
MAX_PRS = 6
MAX_COMMITS = 12
MAX_FIELD_CHARS = 280
MAX_SOURCE_CHARS = 900


@dataclass(frozen=True)
class TimeBounds:
    start: datetime
    end: datetime


def _ensure_utc(value: Optional[datetime | str]) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            val_iso = value.replace(" ", "T")
            value = datetime.fromisoformat(val_iso.replace("Z", "+00:00"))
        except ValueError:
            return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _truncate_text(value: str, limit: int) -> str:
    compact = " ".join(str(value or "").split())
    if len(compact) <= limit:
        return compact
    return f"{compact[:limit].rstrip()}..."


def _commit_subject(message: Optional[str]) -> Optional[str]:
    if not message:
        return None
    for line in str(message).splitlines():
        line = line.strip()
        if line:
            return line
    return None


def _node_time_bounds(
    node_type: str,
    data: Dict[str, object],
) -> Tuple[Optional[datetime], Optional[datetime]]:
    if node_type == "issue":
        start = _ensure_utc(data.get("created_at"))  # type: ignore[arg-type]
        end = _ensure_utc(data.get("completed_at"))  # type: ignore[arg-type]
        end = end or _ensure_utc(data.get("updated_at"))  # type: ignore[arg-type]
        return start, end or start
    if node_type == "pr":
        start = _ensure_utc(data.get("created_at"))  # type: ignore[arg-type]
        end = _ensure_utc(data.get("merged_at"))  # type: ignore[arg-type]
        end = end or _ensure_utc(data.get("closed_at"))  # type: ignore[arg-type]
        return start, end or start
    if node_type == "commit":
        when = _ensure_utc(data.get("author_when"))  # type: ignore[arg-type]
        when = when or _ensure_utc(data.get("committer_when"))  # type: ignore[arg-type]
        return when, when
    return None, None


def compute_time_bounds(
    nodes: Iterable[Tuple[str, str]],
    work_item_map: Dict[str, Dict[str, object]],
    pr_map: Dict[str, Dict[str, object]],
    commit_map: Dict[str, Dict[str, object]],
) -> Optional[TimeBounds]:
    starts: List[datetime] = []
    ends: List[datetime] = []
    for node_type, node_id in nodes:
        data: Dict[str, object] = {}
        if node_type == "issue":
            data = work_item_map.get(node_id, {})
        elif node_type == "pr":
            data = pr_map.get(node_id, {})
        elif node_type == "commit":
            data = commit_map.get(node_id, {})
        start, end = _node_time_bounds(node_type, data)
        if start:
            starts.append(start)
        if end:
            ends.append(end)
    if not starts or not ends:
        return None
    return TimeBounds(start=min(starts), end=max(ends))


def _graph_density(node_count: int, edge_count: int) -> float:
    if node_count <= 1:
        return 1.0
    possible = node_count * (node_count - 1) / 2.0
    if possible <= 0:
        return 0.0
    return min(1.0, edge_count / possible)


def _edge_confidence(edges: Iterable[Dict[str, object]]) -> float:
    values = [float(edge.get("confidence") or 0.0) for edge in edges]
    if not values:
        return 0.0
    return sum(values) / float(len(values))


def compute_evidence_quality(
    *,
    text_bundle: TextBundle,
    nodes_count: int,
    edges: Iterable[Dict[str, object]],
) -> float:
    edges_list = list(edges)
    text_sources = text_bundle.text_source_count
    text_presence = clamp(text_sources / 3.0)
    text_richness = clamp(text_bundle.text_char_count / 1200.0)
    text_score = clamp((text_presence + text_richness) / 2.0)

    source_type_count = len(
        [key for key, texts in text_bundle.source_texts.items() if texts]
    )
    agreement_score = clamp(max(0, source_type_count - 1) / 2.0)

    density = _graph_density(nodes_count, len(edges_list))
    confidence = _edge_confidence(edges_list)
    structural_density = clamp((density + confidence) / 2.0)

    value = 0.4 * text_score + 0.3 * agreement_score + 0.3 * structural_density
    return clamp(value)


def build_text_bundle(
    *,
    issue_ids: List[str],
    pr_ids: List[str],
    commit_ids: List[str],
    work_item_map: Dict[str, Dict[str, object]],
    pr_map: Dict[str, Dict[str, object]],
    commit_map: Dict[str, Dict[str, object]],
    parent_titles: Dict[str, str],
    epic_titles: Dict[str, str],
    work_unit_id: str,
) -> TextBundle:
    source_texts: Dict[str, Dict[str, str]] = {"issue": {}, "pr": {}, "commit": {}}

    for issue_id in sorted(issue_ids)[:MAX_ISSUES]:
        item = work_item_map.get(issue_id) or {}
        parts: List[str] = []
        title = item.get("title")
        if title:
            parts.append(_truncate_text(str(title), MAX_FIELD_CHARS))
        description = item.get("description")
        if description:
            parts.append(_truncate_text(str(description), MAX_FIELD_CHARS))
        item_type = str(item.get("type") or "").strip()
        if item_type:
            parts.append(f"Type: {_truncate_text(item_type, MAX_FIELD_CHARS)}")
        labels = item.get("labels") or []
        if labels:
            label_text = ", ".join(str(label) for label in labels if label)
            if label_text:
                parts.append(f"Labels: {_truncate_text(label_text, MAX_FIELD_CHARS)}")
        parent_id = str(item.get("parent_id") or "").strip()
        if parent_id and parent_id in parent_titles:
            parts.append(
                f"Parent: {_truncate_text(parent_titles[parent_id], MAX_FIELD_CHARS)}"
            )
        epic_id = str(item.get("epic_id") or "").strip()
        if epic_id and epic_id in epic_titles:
            parts.append(
                f"Epic: {_truncate_text(epic_titles[epic_id], MAX_FIELD_CHARS)}"
            )
        if parts:
            text = "\n".join(parts)
            source_texts["issue"][issue_id] = _truncate_text(text, MAX_SOURCE_CHARS)

    for pr_id in sorted(pr_ids)[:MAX_PRS]:
        pr = pr_map.get(pr_id) or {}
        parts = []
        title = pr.get("title")
        if title:
            parts.append(_truncate_text(str(title), MAX_FIELD_CHARS))
        body = pr.get("body")
        if body:
            parts.append(_truncate_text(str(body), MAX_FIELD_CHARS))
        if parts:
            text = "\n".join(parts)
            source_texts["pr"][pr_id] = _truncate_text(text, MAX_SOURCE_CHARS)

    for commit_id in sorted(commit_ids)[:MAX_COMMITS]:
        commit = commit_map.get(commit_id) or {}
        subject = _commit_subject(commit.get("message"))
        if subject:
            source_texts["commit"][commit_id] = _truncate_text(
                str(subject), MAX_SOURCE_CHARS
            )

    source_block_lines: List[str] = []
    for source_type in ("issue", "pr", "commit"):
        for source_id, text in source_texts[source_type].items():
            if not text:
                continue
            source_block_lines.append(f"[{source_type}] {source_id}")
            source_block_lines.append(text)
            source_block_lines.append("")

    source_block = "\n".join(source_block_lines).strip()
    text_source_count = sum(
        1 for texts in source_texts.values() for text in texts.values() if text
    )
    text_char_count = sum(
        len(text) for texts in source_texts.values() for text in texts.values()
    )

    input_payload = {
        "work_unit_id": work_unit_id,
        "sources": source_texts,
    }
    serialized = json.dumps(input_payload, sort_keys=True, default=str)
    input_hash = hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    return TextBundle(
        source_block=source_block,
        source_texts=source_texts,
        input_hash=input_hash,
        text_source_count=text_source_count,
        text_char_count=text_char_count,
    )


def evidence_quality_band_for_bundle(
    *,
    value: float,
) -> str:
    return evidence_quality_band(value)

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Set

import yaml

from dev_health_ops.models.work_items import (
    WorkItemProvider,
    WorkItemStatusCategory,
    WorkItemType,
)

DEFAULT_STATUS_MAPPING_PATH = Path("src/dev_health_ops/config/status_mapping.yaml")

# If multiple label/status rules match, prefer "more terminal" states.
_STATUS_PRIORITY: List[WorkItemStatusCategory] = [
    "done",
    "canceled",
    "blocked",
    "in_review",
    "in_progress",
    "todo",
    "backlog",
    "unknown",
]


def _norm_key(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


def _index_values(values: Iterable[str], category: str) -> Dict[str, str]:
    indexed: Dict[str, str] = {}
    for raw in values:
        key = _norm_key(str(raw))
        if not key:
            continue
        indexed[key] = category
    return indexed


@dataclass(frozen=True)
class StatusMapping:
    status_by_provider: Mapping[str, Mapping[str, WorkItemStatusCategory]]
    label_status_by_provider: Mapping[str, Mapping[str, WorkItemStatusCategory]]
    type_by_provider: Mapping[str, Mapping[str, WorkItemType]]
    label_type_by_provider: Mapping[str, Mapping[str, WorkItemType]]

    def normalize_status(
        self,
        *,
        provider: WorkItemProvider,
        status_raw: Optional[str],
        labels: Sequence[str] = (),
        state: Optional[str] = None,
    ) -> WorkItemStatusCategory:
        """
        Normalize a raw status/category into a cross-provider bucket.

        Precedence:
        1) provider label mappings (GitHub/GitLab)
        2) provider status mappings
        3) provider state fallback (open/closed)
        4) unknown
        """
        provider_key = str(provider)

        # 1) Labels
        label_map = self.label_status_by_provider.get(provider_key) or {}
        matched: Set[WorkItemStatusCategory] = set()
        for label in labels or ():
            cat = label_map.get(_norm_key(str(label)))
            if cat:
                matched.add(cat)
        if matched:
            for candidate in _STATUS_PRIORITY:
                if candidate in matched:
                    return candidate

        # 2) Raw status
        status_map = self.status_by_provider.get(provider_key) or {}
        if status_raw:
            mapped = status_map.get(_norm_key(status_raw))
            if mapped:
                return mapped

        # 3) State fallback
        if state:
            state_norm = _norm_key(state)
            if state_norm in {"closed", "done", "merged"}:
                return "done"
            if state_norm in {"open", "opened"}:
                return "todo"

        return "unknown"

    def normalize_type(
        self,
        *,
        provider: WorkItemProvider,
        type_raw: Optional[str],
        labels: Sequence[str] = (),
    ) -> WorkItemType:
        provider_key = str(provider)

        label_map = self.label_type_by_provider.get(provider_key) or {}
        matched_types: Set[WorkItemType] = set()
        for label in labels or ():
            mapped = label_map.get(_norm_key(str(label)))
            if mapped:
                matched_types.add(mapped)
        if matched_types:
            # Bug/incident are most important for quality rollups.
            for candidate in [
                "incident",
                "bug",
                "epic",
                "story",
                "task",
                "chore",
                "issue",
                "unknown",
            ]:
                if candidate in matched_types:
                    return candidate  # type: ignore[return-value]

        type_map = self.type_by_provider.get(provider_key) or {}
        if type_raw:
            mapped = type_map.get(_norm_key(type_raw))
            if mapped:
                return mapped

        # Best-effort defaults.
        if provider_key in {"github", "gitlab"}:
            return "issue"
        return "unknown"


def load_status_mapping(path: Optional[Path] = None) -> StatusMapping:
    """
    Load `config/status_mapping.yaml` and build lookup indexes.

    The file path can be overridden via `STATUS_MAPPING_PATH`.
    """
    raw_path = os.getenv("STATUS_MAPPING_PATH")
    if raw_path:
        path = Path(raw_path)
    path = path or DEFAULT_STATUS_MAPPING_PATH

    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    # Base/default mapping shared across providers.
    base_status = payload.get("status_categories") or {}

    providers = payload.get("providers") or {}

    def _build_status_index(provider_name: str) -> Dict[str, WorkItemStatusCategory]:
        indexed: Dict[str, WorkItemStatusCategory] = {}

        # Start from base categories.
        for category, values in (base_status or {}).items():
            for key, mapped in _index_values(values or [], str(category)).items():
                indexed[key] = mapped  # type: ignore[assignment]

        # Apply provider overrides.
        prov_cfg = providers.get(provider_name) or {}
        for category, values in (prov_cfg.get("statuses") or {}).items():
            for key, mapped in _index_values(values or [], str(category)).items():
                indexed[key] = mapped  # type: ignore[assignment]

        # Ensure type is correct at runtime.
        return {k: v for k, v in indexed.items()}

    def _build_label_status_index(
        provider_name: str,
    ) -> Dict[str, WorkItemStatusCategory]:
        indexed: Dict[str, WorkItemStatusCategory] = {}
        prov_cfg = providers.get(provider_name) or {}
        for category, values in (prov_cfg.get("status_labels") or {}).items():
            for key, mapped in _index_values(values or [], str(category)).items():
                indexed[key] = mapped  # type: ignore[assignment]
        return {k: v for k, v in indexed.items()}

    def _build_type_index(provider_name: str) -> Dict[str, WorkItemType]:
        indexed: Dict[str, WorkItemType] = {}
        prov_cfg = providers.get(provider_name) or {}
        for category, values in (prov_cfg.get("types") or {}).items():
            for raw in values or []:
                key = _norm_key(str(raw))
                if not key:
                    continue
                indexed[key] = str(category)  # type: ignore[assignment]
        return indexed

    def _build_label_type_index(provider_name: str) -> Dict[str, WorkItemType]:
        indexed: Dict[str, WorkItemType] = {}
        prov_cfg = providers.get(provider_name) or {}
        for category, values in (prov_cfg.get("type_labels") or {}).items():
            for raw in values or []:
                key = _norm_key(str(raw))
                if not key:
                    continue
                indexed[key] = str(category)  # type: ignore[assignment]
        return indexed

    status_by_provider: Dict[str, Dict[str, WorkItemStatusCategory]] = {}
    label_status_by_provider: Dict[str, Dict[str, WorkItemStatusCategory]] = {}
    type_by_provider: Dict[str, Dict[str, WorkItemType]] = {}
    label_type_by_provider: Dict[str, Dict[str, WorkItemType]] = {}

    for provider in ("jira", "github", "gitlab"):
        status_by_provider[provider] = _build_status_index(provider)
        label_status_by_provider[provider] = _build_label_status_index(provider)
        type_by_provider[provider] = _build_type_index(provider)
        label_type_by_provider[provider] = _build_label_type_index(provider)

    return StatusMapping(
        status_by_provider=status_by_provider,
        label_status_by_provider=label_status_by_provider,
        type_by_provider=type_by_provider,
        label_type_by_provider=label_type_by_provider,
    )

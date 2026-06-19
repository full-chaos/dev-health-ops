from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from typing import Any

logger = logging.getLogger(__name__)

_WARNED_MOCK_FIXTURE_SURFACES: set[tuple[str, str]] = set()

_MOCK_FIXTURE_MARKERS = ("mock", "synthetic", "fixture")


def row_has_mock_fixture_provenance(row: Mapping[str, Any]) -> bool:
    values = (
        row.get("provider"),
        row.get("categorization_model_version"),
        row.get("llm_provider"),
        row.get("source"),
    )
    return any(
        marker in str(value or "").strip().lower()
        for value in values
        for marker in _MOCK_FIXTURE_MARKERS
    )


def warn_once_for_mock_fixture_rows(
    *, org_id: str, surface: str, rows: Iterable[Mapping[str, Any]]
) -> None:
    if not any(row_has_mock_fixture_provenance(row) for row in rows):
        return
    key = (org_id or "", surface)
    if key in _WARNED_MOCK_FIXTURE_SURFACES:
        return
    _WARNED_MOCK_FIXTURE_SURFACES.add(key)
    logger.warning(
        "Mock/fixture-sourced investment rows served to UI surface=%s org_id=%s",
        surface,
        org_id or "",
    )


def reset_mock_fixture_warning_state() -> None:
    _WARNED_MOCK_FIXTURE_SURFACES.clear()

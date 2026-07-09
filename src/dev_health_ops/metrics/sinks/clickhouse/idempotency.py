from __future__ import annotations

from collections.abc import Sequence

WORK_ITEMS_DEDUPED = "work_items FINAL"
SPRINTS_DEDUPED = "sprints FINAL"

WORK_ITEM_TRANSITION_SEMANTIC_COLUMNS = (
    "org_id",
    "repo_id",
    "work_item_id",
    "occurred_at",
    "provider",
    "from_status",
    "to_status",
    "from_status_raw",
    "to_status_raw",
    "actor",
)

WORK_ITEM_REOPEN_EVENT_SEMANTIC_COLUMNS = (
    "org_id",
    "work_item_id",
    "occurred_at",
    "from_status",
    "to_status",
    "from_status_raw",
    "to_status_raw",
    "actor",
)

WORK_ITEM_INTERACTION_SEMANTIC_COLUMNS = (
    "org_id",
    "work_item_id",
    "provider",
    "interaction_type",
    "occurred_at",
    "actor",
    "body_length",
)


def semantic_deduped_subquery(
    table: str,
    semantic_columns: Sequence[str],
    *,
    version_column: str = "last_synced",
) -> str:
    select_columns = ",\n        ".join(semantic_columns)
    group_columns = ", ".join(semantic_columns)
    return f"""(
    SELECT
        {select_columns},
        max({version_column}) AS {version_column}
    FROM {table}
    GROUP BY {group_columns}
)"""


WORK_ITEM_TRANSITIONS_DEDUPED = semantic_deduped_subquery(
    "work_item_transitions",
    WORK_ITEM_TRANSITION_SEMANTIC_COLUMNS,
)
WORK_ITEM_REOPEN_EVENTS_DEDUPED = semantic_deduped_subquery(
    "work_item_reopen_events",
    WORK_ITEM_REOPEN_EVENT_SEMANTIC_COLUMNS,
)
WORK_ITEM_INTERACTIONS_DEDUPED = semantic_deduped_subquery(
    "work_item_interactions",
    WORK_ITEM_INTERACTION_SEMANTIC_COLUMNS,
)

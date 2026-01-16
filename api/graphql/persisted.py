"""Persisted queries support for GraphQL analytics.

Provides a simple file-based registry for persisted queries with
schema versioning to invalidate stale queries.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

from .errors import PersistedQueryError


logger = logging.getLogger(__name__)

# Current schema version - increment when schema changes break queries
SCHEMA_VERSION = "1.0"

# Path to the persisted queries registry file
_REGISTRY_PATH = Path(__file__).parent / "persisted_queries.json"


@dataclass
class PersistedQuery:
    """A persisted query entry."""

    id: str
    query: str
    schema_version: str
    description: str = ""


# In-memory cache of persisted queries
_QUERY_CACHE: Dict[str, PersistedQuery] = {}
_CACHE_LOADED = False


def _load_registry() -> None:
    """Load the persisted query registry from disk."""
    global _CACHE_LOADED

    if _CACHE_LOADED:
        return

    if not _REGISTRY_PATH.exists():
        logger.debug("No persisted queries registry found at %s", _REGISTRY_PATH)
        _CACHE_LOADED = True
        return

    try:
        with open(_REGISTRY_PATH, "r") as f:
            data = json.load(f)

        for entry in data.get("queries", []):
            query_id = entry.get("id")
            if query_id:
                _QUERY_CACHE[query_id] = PersistedQuery(
                    id=query_id,
                    query=entry.get("query", ""),
                    schema_version=entry.get("schema_version", SCHEMA_VERSION),
                    description=entry.get("description", ""),
                )

        logger.info("Loaded %d persisted queries", len(_QUERY_CACHE))
        _CACHE_LOADED = True

    except Exception as e:
        logger.warning("Failed to load persisted queries: %s", e)
        _CACHE_LOADED = True


def load_persisted_query(query_id: str) -> str:
    """
    Load a persisted query by ID.

    Args:
        query_id: The unique identifier of the persisted query.

    Returns:
        The query text.

    Raises:
        PersistedQueryError: If the query is not found or version mismatch.
    """
    _load_registry()

    if query_id not in _QUERY_CACHE:
        raise PersistedQueryError(
            message=f"Persisted query not found: {query_id}",
            query_id=query_id,
        )

    entry = _QUERY_CACHE[query_id]

    # Check schema version
    if entry.schema_version != SCHEMA_VERSION:
        raise PersistedQueryError(
            message=(
                f"Persisted query '{query_id}' was created for schema version "
                f"{entry.schema_version} but current version is {SCHEMA_VERSION}"
            ),
            query_id=query_id,
            schema_version=entry.schema_version,
        )

    return entry.query


def get_schema_version() -> str:
    """Get the current schema version."""
    return SCHEMA_VERSION


def list_persisted_queries() -> list[Dict[str, str]]:
    """
    List all available persisted queries.

    Returns:
        List of dicts with id, description, and schema_version.
    """
    _load_registry()

    return [
        {
            "id": entry.id,
            "description": entry.description,
            "schema_version": entry.schema_version,
        }
        for entry in _QUERY_CACHE.values()
    ]


def register_query(
    query_id: str,
    query: str,
    description: str = "",
) -> None:
    """
    Register a persisted query (in-memory only, for testing).

    Args:
        query_id: Unique identifier for the query.
        query: The GraphQL query text.
        description: Optional description.
    """
    _QUERY_CACHE[query_id] = PersistedQuery(
        id=query_id,
        query=query,
        schema_version=SCHEMA_VERSION,
        description=description,
    )


def clear_cache() -> None:
    """Clear the persisted query cache (for testing)."""
    global _QUERY_CACHE, _CACHE_LOADED
    _QUERY_CACHE = {}
    _CACHE_LOADED = False

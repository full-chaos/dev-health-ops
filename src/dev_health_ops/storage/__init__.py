"""
Storage package for dev-health-ops.

This package provides storage backends for different databases:
- SQLAlchemyStore: PostgreSQL and SQLite via SQLAlchemy
- MongoStore: MongoDB via Motor
- ClickHouseStore: ClickHouse via clickhouse-connect

Usage:
    from dev_health_ops.storage import create_store, detect_db_type

    store = create_store("postgresql://localhost/mydb")
    async with store:
        await store.insert_repo(repo)
"""

# Re-export everything from the main storage module for backward compatibility
# This allows gradual migration: imports from dev_health_ops.storage work,
# and imports from dev_health_ops.storage.X will work once classes are extracted.

from dev_health_ops.storage_impl import (
    # Factory functions
    create_store,
    detect_db_type,
    resolve_db_type,
    run_with_store,
    # Store classes
    SQLAlchemyStore,
    MongoStore,
    ClickHouseStore,
    # Utilities
    model_to_dict,
)

__all__ = [
    "create_store",
    "detect_db_type",
    "resolve_db_type",
    "run_with_store",
    "SQLAlchemyStore",
    "MongoStore",
    "ClickHouseStore",
    "model_to_dict",
]

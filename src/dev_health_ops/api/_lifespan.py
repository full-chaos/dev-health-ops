"""Application lifespan management (startup / shutdown).

Extracted from ``api.main`` so that ``main.py`` remains composition-only.
The ``lifespan`` async context manager preserves the original startup logic
(license init + feature-bundle validation) and shutdown logic (closing the
shared ClickHouse client).
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from dev_health_ops.licensing import LicenseManager

from ._health import _postgres_url
from .queries.client import close_global_client

if TYPE_CHECKING:
    from fastapi import FastAPI

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Run startup and shutdown hooks for the API process.

    Startup:
      1. Initialize the license manager (best-effort; community tier on fail).
      2. Validate that every persisted ``FeatureBundle`` references only canonical
         feature keys when a Postgres URL is configured. Re-raise integrity
         errors so the process refuses to start with a corrupt registry; tolerate
         other failures (e.g. DB not yet ready in tests / containers).

    Shutdown:
      * Close the global ClickHouse client.
    """
    # Initialize licensing
    try:
        manager = LicenseManager.initialize()
        if manager.is_licensed:
            logger.info("License initialized: tier=%s", manager.tier.value)
        else:
            logger.info("No license configured, using community tier")
    except Exception as e:
        logger.warning(
            "License initialization failed: %s (using community tier)", e
        )

    # Validate FeatureBundle feature keys against the canonical
    # STANDARD_FEATURES registry.
    postgres_uri = _postgres_url()
    if postgres_uri:
        try:
            from dev_health_ops.api.billing.bundle_validation import (
                FeatureBundleIntegrityError,
                validate_bundle_keys,
            )
            from dev_health_ops.db import get_postgres_session

            async with get_postgres_session() as _session:
                await validate_bundle_keys(_session)
        except Exception as _exc:
            from dev_health_ops.api.billing.bundle_validation import (
                FeatureBundleIntegrityError,
            )

            if isinstance(_exc, FeatureBundleIntegrityError):
                # Integrity check failed; re-raise to abort startup.
                raise
            logger.warning(
                "FeatureBundle key validation skipped (DB not ready): %s", _exc
            )

    yield
    await close_global_client()


__all__ = ["lifespan"]

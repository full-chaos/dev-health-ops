"""Migration 028: add release_ref fields to deployments.

Adds canonical release join fields so deployments can be linked to telemetry and
release impact metrics. Idempotent on existing databases.
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def upgrade(client):
    log.info("Starting migration 028: add release_ref columns to deployments")
    client.command(
        """
        ALTER TABLE deployments
        ADD COLUMN IF NOT EXISTS release_ref String DEFAULT ''
        """
    )
    client.command(
        """
        ALTER TABLE deployments
        ADD COLUMN IF NOT EXISTS release_ref_confidence Float64 DEFAULT 0.0
        """
    )
    log.info("Completed migration 028: release_ref columns available on deployments")

"""Tests for `dev_health_ops.push.limits` (CHAOS-2700 brief decision 6,
amended by master-spec CC3): hardcoded fallback matches the server's own
defaults; server-reported `GET /schemas` `limits` is preferred when valid."""

from __future__ import annotations

from dev_health_ops.api.external_ingest.schemas import (
    MAX_BODY_BYTES_DEFAULT,
    MAX_RECORDS_DEFAULT,
)
from dev_health_ops.push.limits import (
    ABSOLUTE_MAX_BODY_BYTES,
    DEFAULT_LIMITS,
    limits_from_schema_response,
)


def test_default_limits_match_server_constants() -> None:
    assert DEFAULT_LIMITS.max_records_per_batch == MAX_RECORDS_DEFAULT
    assert DEFAULT_LIMITS.max_body_bytes == MAX_BODY_BYTES_DEFAULT


def test_limits_from_schema_response_prefers_server_values() -> None:
    doc = {"limits": {"maxRecordsPerBatch": 42, "maxBodyBytes": 4096}}

    limits = limits_from_schema_response(doc)

    assert limits.max_records_per_batch == 42
    assert limits.max_body_bytes == 4096


def test_limits_from_schema_response_falls_back_on_missing_document() -> None:
    assert limits_from_schema_response(None) == DEFAULT_LIMITS


def test_limits_from_schema_response_falls_back_on_malformed_limits() -> None:
    doc = {"limits": {"maxRecordsPerBatch": "not-a-number"}}

    limits = limits_from_schema_response(doc)

    assert limits == DEFAULT_LIMITS


def test_limits_from_schema_response_falls_back_on_missing_limits_key() -> None:
    assert limits_from_schema_response({"schemaVersions": []}) == DEFAULT_LIMITS


def test_limits_from_schema_response_clamps_excessive_server_body_limit() -> None:
    """Codex adversarial-review finding, round 2: an unauthenticated
    `GET /schemas` (reachable at whatever `--api-url` was supplied) must
    never be able to fully disable the bounded-read protection by
    advertising an enormous `maxBodyBytes`."""
    doc = {"limits": {"maxRecordsPerBatch": 1000, "maxBodyBytes": 999_999_999_999}}

    limits = limits_from_schema_response(doc)

    assert limits.max_body_bytes == ABSOLUTE_MAX_BODY_BYTES


def test_limits_from_schema_response_honors_reasonable_server_body_limit() -> None:
    """A legitimate, moderately-raised server limit (e.g. an admin setting
    `EXTERNAL_INGEST_MAX_BODY_BYTES` above the default) still passes
    through -- only values beyond the absolute ceiling are clamped."""
    doc = {"limits": {"maxRecordsPerBatch": 1000, "maxBodyBytes": 20_000_000}}

    limits = limits_from_schema_response(doc)

    assert limits.max_body_bytes == 20_000_000

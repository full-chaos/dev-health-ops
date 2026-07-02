"""API tests for GET /schemas + GET /schemas/{schema_version} (CHAOS-2692).

Uses the ASGITransport + AsyncClient pattern from
tests/api/test_external_ingest_router.py / tests/test_ingest_api.py. These
two routes take no auth dependency (D2) so, unlike that file's other tests,
no dependency_overrides are needed here.
"""

from __future__ import annotations

import importlib
import re
import sys

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from dev_health_ops.api.external_ingest.schemas import (
    RECORD_KIND_MODELS,
    SCHEMA_VERSION,
)
from dev_health_ops.api.main import app

# See tests/api/test_external_ingest_router.py for why the module must be
# force-loaded through sys.modules rather than imported via the package's
# __init__ (which shadows the module name with the exported APIRouter).
importlib.import_module("dev_health_ops.api.external_ingest.router")
router_mod = sys.modules["dev_health_ops.api.external_ingest.router"]

BASE = "/api/v1/external-ingest"
_ETAG_RE = re.compile(r'^"[0-9a-f]{64}"$')


@pytest_asyncio.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_list_schemas_no_auth_required(client):
    resp = await client.get(f"{BASE}/schemas")

    assert resp.status_code == 200
    body = resp.json()
    assert body["schemaVersions"] == [SCHEMA_VERSION]
    assert body["recordKinds"] == sorted(RECORD_KIND_MODELS)
    assert set(body["limits"]) == {"maxRecordsPerBatch", "maxBodyBytes"}


@pytest.mark.asyncio
async def test_get_schema_returns_bundled_json_schema_document(client):
    resp = await client.get(f"{BASE}/schemas/{SCHEMA_VERSION}")

    assert resp.status_code == 200
    assert _ETAG_RE.match(resp.headers["etag"])
    assert resp.headers["cache-control"] == "public, max-age=3600, must-revalidate"

    body = resp.json()
    assert body["schemaVersion"] == SCHEMA_VERSION
    assert set(body["recordKinds"]) == set(RECORD_KIND_MODELS)
    assert set(body["limits"]) == {"maxRecordsPerBatch", "maxBodyBytes"}

    # recordKinds entries are $ref + examples into $defs (D5), not inlined
    # per-kind schemas — resolve the $ref manually the way a customer's
    # validator would.
    commit_entry = body["recordKinds"]["commit.v1"]
    assert commit_entry["$ref"] == "#/$defs/CommitV1"
    assert len(commit_entry["examples"]) == 1
    assert "properties" in body["$defs"]["CommitV1"]

    assert body["envelope"] == {"$ref": "#/$defs/BatchEnvelope"}
    assert "properties" in body["$defs"]["BatchEnvelope"]


@pytest.mark.asyncio
async def test_get_schema_etag_round_trip_returns_304(client):
    first = await client.get(f"{BASE}/schemas/{SCHEMA_VERSION}")
    etag = first.headers["etag"]

    second = await client.get(
        f"{BASE}/schemas/{SCHEMA_VERSION}", headers={"If-None-Match": etag}
    )

    assert second.status_code == 304
    assert second.headers["etag"] == etag
    assert second.content == b""


@pytest.mark.asyncio
async def test_get_schema_stale_if_none_match_returns_200(client):
    resp = await client.get(
        f"{BASE}/schemas/{SCHEMA_VERSION}",
        headers={"If-None-Match": '"stale-etag-value"'},
    )

    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_get_schema_etag_changes_when_live_limits_change(client, monkeypatch):
    # Adversarial-review regression: the served ETag must cover the full
    # response body (schema + limits), not just the cached schema document —
    # otherwise a client holding a stale ETag could get a 304 for a response
    # whose limits actually changed.
    first = await client.get(f"{BASE}/schemas/{SCHEMA_VERSION}")
    etag = first.headers["etag"]
    assert first.json()["limits"]["maxRecordsPerBatch"] != 7

    monkeypatch.setenv("EXTERNAL_INGEST_MAX_RECORDS", "7")
    second = await client.get(
        f"{BASE}/schemas/{SCHEMA_VERSION}", headers={"If-None-Match": etag}
    )

    assert second.status_code == 200
    assert second.json()["limits"]["maxRecordsPerBatch"] == 7
    assert second.headers["etag"] != etag


def test_schema_discovery_rate_limit_key_ignores_arbitrary_bearer_tokens():
    # Adversarial-review regression: an unauthenticated caller must not be
    # able to mint a fresh rate-limit bucket per request by rotating a
    # bearer value the route never validates (D2, public discovery).
    class _FakeRequest:
        def __init__(self, headers):
            self.headers = headers
            self.client = None

    no_auth = _FakeRequest({})
    bearer_a = _FakeRequest({"authorization": "Bearer aaaaaaaaaaaaaaaa"})
    bearer_b = _FakeRequest({"authorization": "Bearer bbbbbbbbbbbbbbbb"})

    key_no_auth = router_mod._schema_discovery_rate_limit_key(no_auth)
    key_bearer_a = router_mod._schema_discovery_rate_limit_key(bearer_a)
    key_bearer_b = router_mod._schema_discovery_rate_limit_key(bearer_b)

    assert key_no_auth == key_bearer_a == key_bearer_b
    assert key_no_auth.startswith("ingest-ip:")


@pytest.mark.asyncio
async def test_get_schema_unknown_version_returns_404(client):
    resp = await client.get(f"{BASE}/schemas/external-ingest.v99")

    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "unsupported_schema_version"


def test_schemas_routes_appear_in_openapi_schema():
    paths = app.openapi()["paths"]

    assert f"{BASE}/schemas" in paths
    assert f"{BASE}/schemas/{{schema_version}}" in paths

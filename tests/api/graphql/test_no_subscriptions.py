"""The GraphQL schema serves no subscription.

Both the Strawberry schema and the exported SDL carry no Subscription root
type; the websocket transport on /graphql stays mounted but is inert: it
completes the connection handshake, then refuses any subscription operation
with a GraphQL error, over both protocols. Queries over HTTP are unchanged.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient
from strawberry.fastapi import BaseContext

from dev_health_ops.api.graphql.go_api_dispatcher import GoApiDispatchRouter
from dev_health_ops.api.graphql.schema import schema

SDL_PATH = (
    Path(__file__).resolve().parents[3]
    / "contracts"
    / "graphql"
    / "v1"
    / "schema.graphql"
)
REMOVED_NAMES = (
    "type Subscription",
    "MetricsUpdate",
    "TaskStatus",
    "SyncProgress",
    "metricsUpdated",
    "taskStatus",
    "syncProgress",
)


class _Context(BaseContext):
    org_id = "org-1"
    user = None


@pytest.fixture()
def client() -> TestClient:
    app = FastAPI()
    router = GoApiDispatchRouter(
        schema=schema,
        context_getter=_Context,
        path="",
    )
    app.include_router(router, prefix="/graphql")
    return TestClient(app)


def test_schema_has_no_subscription_root() -> None:
    assert schema._schema.subscription_type is None


def test_exported_sdl_names_none_of_the_removed_types() -> None:
    sdl = SDL_PATH.read_text()
    assert [name for name in REMOVED_NAMES if name in sdl] == []


@pytest.mark.parametrize(
    "module",
    ["subscriptions", "pubsub"],
)
def test_subscription_modules_are_gone(module: str) -> None:
    assert importlib.util.find_spec(f"dev_health_ops.api.graphql.{module}") is None


def test_queries_still_answer_over_http(client: TestClient) -> None:
    response = client.post("/graphql", json={"query": "{ __typename }"})
    assert response.status_code == 200
    assert response.json() == {"data": {"__typename": "Query"}}


@pytest.mark.parametrize(
    ("protocol", "subscribe"),
    [
        (
            "graphql-transport-ws",
            lambda query: {"id": "1", "type": "subscribe", "payload": {"query": query}},
        ),
        (
            "graphql-ws",
            lambda query: {"id": "1", "type": "start", "payload": {"query": query}},
        ),
    ],
)
def test_websocket_handshake_completes_and_subscriptions_are_refused(
    client: TestClient, protocol: str, subscribe
) -> None:
    query = 'subscription { metricsUpdated(orgId: "org-1") { day } }'
    with client.websocket_connect("/graphql", subprotocols=[protocol]) as ws:
        ws.send_json({"type": "connection_init"})
        assert ws.receive_json() == {"type": "connection_ack"}
        ws.send_json(subscribe(query))
        reply = ws.receive_json()
    assert reply["type"] == "error"
    assert "Schema is not configured to execute subscription operation" in str(
        reply["payload"]
    )

from __future__ import annotations

import importlib
import sys
from collections.abc import Callable, Iterator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


GRAPHQL_MODULES = [
    "dev_health_ops.api.graphql.security",
    "dev_health_ops.api.graphql.schema",
    "dev_health_ops.api.graphql.app",
]


@pytest.fixture
def graphql_client(monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[[str], TestClient]]:
    def _build(environment: str, *, security_enabled: bool | None = None) -> TestClient:
        monkeypatch.setenv("ENVIRONMENT", environment)
        monkeypatch.setenv("GRAPHQL_AUTH_REQUIRED", "false")
        monkeypatch.delenv("GRAPHQL_IDE_ENABLED", raising=False)
        monkeypatch.delenv("GRAPHQL_INTROSPECTION_ENABLED", raising=False)
        monkeypatch.delenv("GRAPHQL_SECURITY_ENABLED", raising=False)
        monkeypatch.delenv("GRAPHQL_MAX_QUERY_BYTES", raising=False)
        if security_enabled is not None:
            monkeypatch.setenv(
                "GRAPHQL_SECURITY_ENABLED", "true" if security_enabled else "false"
            )

        for module_name in GRAPHQL_MODULES:
            sys.modules.pop(module_name, None)

        security = importlib.import_module("dev_health_ops.api.graphql.security")
        graphql_app = importlib.import_module("dev_health_ops.api.graphql.app")

        app = FastAPI()
        app.add_middleware(security.GraphQLQuerySizeLimitMiddleware)
        app.include_router(graphql_app.create_graphql_app(), prefix="/graphql")
        return TestClient(app, raise_server_exceptions=False)

    yield _build


def test_graphiql_disabled_outside_development(graphql_client):
    client = graphql_client("production")

    response = client.get("/graphql?org_id=test-org", headers={"accept": "text/html"})

    assert response.status_code == 404


def test_graphiql_enabled_in_development(graphql_client):
    client = graphql_client("development")

    response = client.get("/graphql?org_id=test-org", headers={"accept": "text/html"})

    assert response.status_code == 200
    assert "GraphiQL" in response.text


@pytest.mark.parametrize(
    ("environment", "expect_error"),
    [("production", True), ("development", False)],
)
def test_introspection_rejected_outside_development(
    graphql_client, environment: str, expect_error: bool
):
    client = graphql_client(environment)

    response = client.post(
        "/graphql?org_id=test-org", json={"query": "{ __schema { queryType { name } } }"}
    )

    assert response.status_code == 200
    payload = response.json()
    if expect_error:
        assert "errors" in payload
        assert "introspection" in payload["errors"][0]["message"].lower()
    else:
        assert "errors" not in payload
        assert payload["data"]["__schema"]["queryType"]["name"] == "Query"


def test_query_exceeding_depth_limit_rejected(graphql_client):
    client = graphql_client("production")
    deep_query = "{ __type(name: \"Query\") { " + "ofType { " * 13 + "name" + " }" * 13 + " } }"

    response = client.post("/graphql?org_id=test-org", json={"query": deep_query})

    assert response.status_code == 200
    payload = response.json()
    assert "errors" in payload
    assert any(
        "depth exceeds limit of 12" in error["message"] for error in payload["errors"]
    )


def test_query_exceeding_alias_limit_rejected(graphql_client):
    client = graphql_client("production")
    aliases = " ".join(f"a{i}: __typename" for i in range(16))

    response = client.post("/graphql?org_id=test-org", json={"query": "{ " + aliases + " }"})

    assert response.status_code == 200
    payload = response.json()
    assert "errors" in payload
    assert "alias count exceeds limit of 15" in payload["errors"][0]["message"]


def test_query_size_limit_rejected_before_parsing(graphql_client):
    client = graphql_client("production")
    oversized_query = "{ __typename }" + (" " * (16 * 1024))

    response = client.post("/graphql?org_id=test-org", json={"query": oversized_query})

    assert response.status_code == 413
    assert response.json()["detail"]["message"] == "GraphQL request body exceeds size limit"


def test_development_environment_permits_alias_depth_and_size_cases(graphql_client):
    client = graphql_client("development")
    aliases = " ".join(f"a{i}: __typename" for i in range(16))
    oversized_query = "{ __typename }" + (" " * (16 * 1024))

    alias_response = client.post(
        "/graphql?org_id=test-org", json={"query": "{ " + aliases + " }"}
    )
    size_response = client.post("/graphql?org_id=test-org", json={"query": oversized_query})

    assert alias_response.status_code == 200
    assert "errors" not in alias_response.json()
    assert size_response.status_code == 200
    assert "errors" not in size_response.json()

from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from dev_health_ops.api.main import app
from dev_health_ops.api.models.schemas import (
    ConstraintCard,
    ConstraintEvidence,
    Coverage,
    EventItem,
    Freshness,
    HomeResponse,
    MetricDelta,
    SparkPoint,
    SummarySentence,
    ExplainResponse,
    Contributor,
    SankeyLink,
    SankeyNode,
    SankeyResponse,
)
from dev_health_ops.api.models.filters import MetricFilter


def _validate(model, payload):
    if hasattr(model, "model_validate"):
        return model.model_validate(payload)
    return model.parse_obj(payload)


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture(autouse=True)
def mock_db_url(monkeypatch):
    """Set a dummy DATABASE_URL so endpoints don't return 503."""
    monkeypatch.setattr(
        "dev_health_ops.api.main._db_url",
        lambda: "clickhouse://localhost:8123/default",
    )


def test_home_endpoint_schema(client, monkeypatch):
    sample = HomeResponse(
        freshness=Freshness(
            last_ingested_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            sources={"github": "ok", "gitlab": "ok", "jira": "ok", "ci": "ok"},
            coverage=Coverage(
                repos_covered_pct=90.0,
                prs_linked_to_issues_pct=80.0,
                issues_with_cycle_states_pct=70.0,
            ),
        ),
        deltas=[
            MetricDelta(
                metric="cycle_time",
                label="Cycle Time",
                value=4.2,
                unit="days",
                delta_pct=12.0,
                spark=[
                    SparkPoint(ts=datetime(2024, 1, 1, tzinfo=timezone.utc), value=4.2)
                ],
            )
        ],
        summary=[
            SummarySentence(
                id="s1",
                text="Cycle time rose 12%.",
                evidence_link="/api/v1/explain?metric=cycle_time",
            )
        ],
        tiles={"understand": {"title": "Understand"}},
        constraint=ConstraintCard(
            title="Constraint",
            claim="Review congestion.",
            evidence=[
                ConstraintEvidence(label="Evidence", link="/api/v1/drilldown/prs")
            ],
            experiments=["Experiment"],
        ),
        events=[
            EventItem(
                ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
                type="spike",
                text="Cycle time spike.",
                link="/api/v1/explain?metric=cycle_time",
            )
        ],
    )

    async def _fake_home(**_):
        return sample

    monkeypatch.setattr("dev_health_ops.api.main.build_home_response", _fake_home)

    response = client.get("/api/v1/home")
    assert response.status_code == 200
    _validate(HomeResponse, response.json())
    assert response.headers.get("X-DevHealth-Deprecated") == "use POST with filters"


def test_home_post_uses_filters(client, monkeypatch):
    captured = {}

    async def _fake_home(**kwargs):
        captured["filters"] = kwargs["filters"]
        return HomeResponse(
            freshness=Freshness(
                last_ingested_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
                sources={"github": "ok", "gitlab": "ok", "jira": "ok", "ci": "ok"},
                coverage=Coverage(
                    repos_covered_pct=90.0,
                    prs_linked_to_issues_pct=80.0,
                    issues_with_cycle_states_pct=70.0,
                ),
            ),
            deltas=[],
            summary=[],
            tiles={},
            constraint=ConstraintCard(
                title="Constraint",
                claim="Review congestion.",
                evidence=[],
                experiments=[],
            ),
            events=[],
        )

    monkeypatch.setattr("dev_health_ops.api.main.build_home_response", _fake_home)

    response = client.post(
        "/api/v1/home",
        json={
            "filters": {
                "time": {"range_days": 21, "compare_days": 7},
                "scope": {"level": "team", "ids": ["team-a"]},
                "who": {},
                "what": {},
                "why": {},
                "how": {},
            }
        },
    )
    assert response.status_code == 200
    filters: MetricFilter = captured["filters"]
    assert filters.time.range_days == 21
    assert filters.time.compare_days == 7
    assert filters.scope.level == "team"
    assert filters.scope.ids == ["team-a"]


def test_home_query_translation_matches_post(client, monkeypatch):
    captured = {"get": None, "post": None}

    async def _fake_home(**kwargs):
        if captured["get"] is None:
            captured["get"] = kwargs["filters"]
        else:
            captured["post"] = kwargs["filters"]
        return HomeResponse(
            freshness=Freshness(
                last_ingested_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
                sources={"github": "ok", "gitlab": "ok", "jira": "ok", "ci": "ok"},
                coverage=Coverage(
                    repos_covered_pct=90.0,
                    prs_linked_to_issues_pct=80.0,
                    issues_with_cycle_states_pct=70.0,
                ),
            ),
            deltas=[],
            summary=[],
            tiles={},
            constraint=ConstraintCard(
                title="Constraint",
                claim="Review congestion.",
                evidence=[],
                experiments=[],
            ),
            events=[],
        )

    monkeypatch.setattr("dev_health_ops.api.main.build_home_response", _fake_home)

    response = client.get(
        "/api/v1/home",
        params={
            "scope_type": "repo",
            "scope_id": "org/api",
            "range_days": 10,
            "compare_days": 5,
        },
    )
    assert response.status_code == 200

    response = client.post(
        "/api/v1/home",
        json={
            "filters": {
                "time": {"range_days": 10, "compare_days": 5},
                "scope": {"level": "repo", "ids": ["org/api"]},
                "who": {},
                "what": {},
                "why": {},
                "how": {},
            }
        },
    )
    assert response.status_code == 200
    assert captured["get"] == captured["post"]


def test_explain_endpoint_schema(client, monkeypatch):
    sample = ExplainResponse(
        metric="cycle_time",
        label="Cycle Time",
        unit="days",
        value=4.0,
        delta_pct=10.0,
        drivers=[
            Contributor(
                id="team-a",
                label="team-a",
                value=5.0,
                delta_pct=15.0,
                evidence_link="/api/v1/drilldown/issues",
            )
        ],
        contributors=[
            Contributor(
                id="team-b",
                label="team-b",
                value=3.5,
                delta_pct=0.0,
                evidence_link="/api/v1/drilldown/issues",
            )
        ],
        drilldown_links={"prs": "/api/v1/drilldown/prs"},
    )

    async def _fake_explain(**_):
        return sample

    monkeypatch.setattr("dev_health_ops.api.main.build_explain_response", _fake_explain)

    response = client.get("/api/v1/explain", params={"metric": "cycle_time"})
    assert response.status_code == 200
    _validate(ExplainResponse, response.json())
    assert response.headers.get("X-DevHealth-Deprecated") == "use POST with filters"


def test_sankey_endpoint_schema(client, monkeypatch):
    sample = SankeyResponse(
        mode="investment",
        nodes=[SankeyNode(name="Initiative A")],
        links=[SankeyLink(source="Initiative A", target="Project B", value=12.0)],
        unit="items",
        label="Investment flow",
        description=(
            "Where effort allocates across initiatives, areas, issue types, and work items."
        ),
    )

    async def _fake_sankey(**_):
        return sample

    monkeypatch.setattr("dev_health_ops.api.main.build_sankey_response", _fake_sankey)

    response = client.post(
        "/api/v1/sankey",
        json={
            "mode": "investment",
            "filters": {
                "time": {"range_days": 21, "compare_days": 7},
                "scope": {"level": "team", "ids": ["team-a"]},
                "who": {},
                "what": {},
                "why": {},
                "how": {},
            },
        },
    )
    assert response.status_code == 200
    _validate(SankeyResponse, response.json())


def test_sankey_endpoint_get_schema(client, monkeypatch):
    sample = SankeyResponse(
        mode="investment",
        nodes=[SankeyNode(name="Initiative A")],
        links=[SankeyLink(source="Initiative A", target="Project B", value=12.0)],
        unit="items",
        label="Investment flow",
        description=(
            "Where effort allocates across initiatives, areas, issue types, and work items."
        ),
    )

    async def _fake_sankey(**_):
        return sample

    monkeypatch.setattr("dev_health_ops.api.main.build_sankey_response", _fake_sankey)

    response = client.get(
        "/api/v1/sankey",
        params={"mode": "investment", "scope_type": "team", "scope_id": "team-a"},
    )
    assert response.status_code == 200
    _validate(SankeyResponse, response.json())
    assert response.headers.get("X-DevHealth-Deprecated") == "use POST with filters"

from datetime import date, datetime, timezone

import pytest
from fastapi.testclient import TestClient

from dev_health_ops.api.main import app
from dev_health_ops.api.models.schemas import (
    CollaborationItem,
    CollaborationSection,
    Coverage,
    DriverStatement,
    FlowStageItem,
    Freshness,
    MetricBreakdownItem,
    MetricDefinition,
    MetricTimeseriesPoint,
    PersonDelta,
    PersonIdentity,
    PersonMetricBreakdowns,
    PersonMetricResponse,
    PersonSearchResult,
    PersonSummaryPerson,
    PersonSummaryResponse,
    PersonSummarySections,
    SparkPoint,
    SummarySentence,
    WorkMixItem,
)


def _validate(model, payload):
    if hasattr(model, "model_validate"):
        return model.model_validate(payload)
    return model.parse_obj(payload)


@pytest.fixture
def client():
    return TestClient(app)


def test_people_search_schema(client, monkeypatch):
    sample = [
        PersonSearchResult(
            person_id="abc123",
            display_name="Chris Doe",
            identities=[PersonIdentity(provider="github", handle="chris")],
            active=True,
        )
    ]

    async def _fake_search(**_):
        return sample

    monkeypatch.setattr("dev_health_ops.api.main.search_people_response", _fake_search)

    response = client.get("/api/v1/people", params={"q": "ch"})
    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload, list)
    _validate(PersonSearchResult, payload[0])


def test_people_summary_schema(client, monkeypatch):
    sample = PersonSummaryResponse(
        person=PersonSummaryPerson(
            person_id="abc123",
            display_name="Chris Doe",
            identities=[PersonIdentity(provider="github", handle="chris")],
        ),
        freshness=Freshness(
            last_ingested_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            sources={"github": "ok", "gitlab": "ok", "jira": "ok", "ci": "ok"},
            coverage=Coverage(
                repos_covered_pct=90.0,
                prs_linked_to_issues_pct=80.0,
                issues_with_cycle_states_pct=70.0,
            ),
        ),
        identity_coverage_pct=75.0,
        deltas=[
            PersonDelta(
                metric="cycle_time",
                label="Cycle Time",
                value=3.2,
                unit="days",
                delta_pct=5.0,
                spark=[
                    SparkPoint(
                        ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
                        value=3.2,
                    )
                ],
            )
        ],
        narrative=[
            SummarySentence(
                id="n1",
                text="Cycle time increased over the last 14 days.",
                evidence_link="/api/v1/people/abc123/metric?metric=cycle_time",
            )
        ],
        sections=PersonSummarySections(
            work_mix=[WorkMixItem(key="feature", name="Feature", value=12.0)],
            flow_breakdown=[FlowStageItem(stage="Active", value=10.0, unit="hours")],
            collaboration=CollaborationSection(
                review_load=[CollaborationItem(label="Reviews given", value=4.0)],
                handoff_points=[CollaborationItem(label="Items completed", value=6.0)],
            ),
        ),
    )

    async def _fake_summary(**_):
        return sample

    monkeypatch.setattr("dev_health_ops.api.main.build_person_summary_response", _fake_summary)

    response = client.get("/api/v1/people/abc123/summary")
    assert response.status_code == 200
    _validate(PersonSummaryResponse, response.json())


def test_people_metric_schema(client, monkeypatch):
    sample = PersonMetricResponse(
        metric="cycle_time",
        label="Cycle Time",
        definition=MetricDefinition(
            description="Time from start to completion.",
            interpretation="Lower values indicate faster delivery.",
        ),
        timeseries=[MetricTimeseriesPoint(day=date(2024, 1, 1), value=3.2)],
        breakdowns=PersonMetricBreakdowns(
            by_repo=[],
            by_work_type=[MetricBreakdownItem(label="feature", value=3.2)],
            by_stage=[],
        ),
        drivers=[
            DriverStatement(
                text="feature accounts for 60% of this period.",
                link="/api/v1/people/abc123/drilldown/issues?metric=cycle_time",
            )
        ],
    )

    async def _fake_metric(**_):
        return sample

    monkeypatch.setattr("dev_health_ops.api.main.build_person_metric_response", _fake_metric)

    response = client.get(
        "/api/v1/people/abc123/metric", params={"metric": "cycle_time"}
    )
    assert response.status_code == 200
    _validate(PersonMetricResponse, response.json())


def test_people_guardrails_reject_compare_params(client):
    response = client.get("/api/v1/people", params={"q": "ch", "rank": "true"})
    assert response.status_code == 400

    response = client.get(
        "/api/v1/people/abc123/summary", params={"compare_to": "team"}
    )
    assert response.status_code == 400


def test_people_drilldown_limit_is_capped(client, monkeypatch):
    captured = {}

    async def _fake_drilldown(**kwargs):
        captured["limit"] = kwargs["limit"]
        return {"items": [], "next_cursor": None}

    monkeypatch.setattr("dev_health_ops.api.main.build_person_drilldown_prs_response", _fake_drilldown)

    response = client.get(
        "/api/v1/people/abc123/drilldown/prs",
        params={"limit": 999},
    )
    assert response.status_code == 200
    assert captured["limit"] == 200


def test_people_responses_do_not_include_forbidden_fields(client, monkeypatch):
    sample = PersonSummaryResponse(
        person=PersonSummaryPerson(
            person_id="abc123",
            display_name="Chris Doe",
            identities=[PersonIdentity(provider="github", handle="chris")],
        ),
        freshness=Freshness(
            last_ingested_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            sources={"github": "ok", "gitlab": "ok", "jira": "ok", "ci": "ok"},
            coverage=Coverage(
                repos_covered_pct=90.0,
                prs_linked_to_issues_pct=80.0,
                issues_with_cycle_states_pct=70.0,
            ),
        ),
        identity_coverage_pct=60.0,
        deltas=[],
        narrative=[],
        sections=PersonSummarySections(
            work_mix=[],
            flow_breakdown=[],
            collaboration=CollaborationSection(review_load=[], handoff_points=[]),
        ),
    )
    metric_sample = PersonMetricResponse(
        metric="cycle_time",
        label="Cycle Time",
        definition=MetricDefinition(
            description="Time from start to completion.",
            interpretation="Lower values indicate faster delivery.",
        ),
        timeseries=[],
        breakdowns=PersonMetricBreakdowns(by_repo=[], by_work_type=[], by_stage=[]),
        drivers=[],
    )

    async def _fake_summary(**_):
        return sample

    monkeypatch.setattr("dev_health_ops.api.main.build_person_summary_response", _fake_summary)

    async def _fake_metric(**_):
        return metric_sample

    monkeypatch.setattr("dev_health_ops.api.main.build_person_metric_response", _fake_metric)

    response = client.get("/api/v1/people/abc123/summary")
    assert response.status_code == 200
    payload = response.json()
    text = str(payload).lower()
    for forbidden in ["rank", "percentile", "score", "leaderboard", "top", "bottom"]:
        assert forbidden not in text

    response = client.get(
        "/api/v1/people/abc123/metric", params={"metric": "cycle_time"}
    )
    assert response.status_code == 200
    payload = response.json()
    text = str(payload).lower()
    for forbidden in ["rank", "percentile", "score", "leaderboard", "top", "bottom"]:
        assert forbidden not in text

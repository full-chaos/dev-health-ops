from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import uuid4

from dev_health_ops.work_graph.operational_edges import build_operational_incident_edges


def test_mapped_service_incident_emits_provenance_backed_deployment_edge() -> None:
    now = datetime(2026, 7, 17, tzinfo=timezone.utc)
    repo_id = uuid4()
    sink = MagicMock()

    def query(query: str, _params: dict[str, str]) -> list[dict[str, object]]:
        if "operational_service_repository_mappings" in query:
            return [
                {
                    "service_id": "svc-1",
                    "repo_id": repo_id,
                    "relationship_provenance": "admin_configuration",
                    "relationship_confidence": 1.0,
                    "mapping_kind": "admin_configuration_exact",
                    "rule_id": "admin.v1",
                    "source_url": None,
                }
            ]
        if "operational_incidents" in query:
            return [
                {
                    "id": "inc-1",
                    "service_id": "svc-1",
                    "escalation_policy_id": None,
                    "started_at": now,
                    "source_url": "https://pagerduty.example/inc-1",
                }
            ]
        if "operational_services" in query:
            return []
        if (
            "operational_alerts" in query
            or "operational_incident_timeline_events" in query
            or "operational_incident_notes" in query
            or "operational_incident_responders" in query
            or "work_items" in query
            or "repos" in query
        ):
            return []
        if "deployments" in query:
            return [
                {
                    "repo_id": repo_id,
                    "deployment_id": "deploy-1",
                    "environment": "production",
                    "deployed_at": now,
                }
            ]
        return []

    sink.query_dicts.side_effect = query

    edges = build_operational_incident_edges(sink, "org-a", now, 7, 0.3)

    deployment_edge = next(
        edge for edge in edges if edge.edge_type.value == "linked_incident"
    )
    assert deployment_edge.source_id == "deploy-1"
    assert deployment_edge.target_id == "inc-1"
    assert deployment_edge.provenance.value == "heuristic"
    assert deployment_edge.confidence == 0.3
    assert "operational_service_mapped_deployment_window.v1" in deployment_edge.evidence
    assert "environment:production" in deployment_edge.evidence


def test_unmapped_service_never_emits_deployment_edge() -> None:
    now = datetime(2026, 7, 17, tzinfo=timezone.utc)
    sink = MagicMock()
    sink.query_dicts.return_value = []

    edges = build_operational_incident_edges(sink, "org-a", now, 7, 0.3)

    assert not [edge for edge in edges if edge.edge_type.value == "linked_incident"]


def test_mapping_edge_preserves_heuristic_provenance_and_rule_id() -> None:
    now = datetime(2026, 7, 17, tzinfo=timezone.utc)
    repo_id = uuid4()
    sink = MagicMock()

    def query(query: str, _params: dict[str, object]) -> list[dict[str, object]]:
        if "operational_service_repository_mappings" in query:
            return [
                {
                    "service_id": "svc-1",
                    "repo_id": repo_id,
                    "provider": "catalog",
                    "relationship_provenance": "bounded_service_repository_heuristic",
                    "relationship_confidence": 0.4,
                    "mapping_kind": "bounded_service_repository_heuristic",
                    "rule_id": "pagerduty.service_repository.bounded_name_match.v1",
                    "source_url": "https://pagerduty.example/services/svc-1",
                }
            ]
        return []

    sink.query_dicts.side_effect = query

    edges = build_operational_incident_edges(sink, "org-a", now, 7, 0.3)

    mapping_edge = next(
        edge for edge in edges if edge.edge_type.value == "maps_to_repository"
    )
    assert mapping_edge.provenance.value == "heuristic"
    assert mapping_edge.confidence == 0.4
    assert mapping_edge.provider == "catalog"
    assert "pagerduty.service_repository.bounded_name_match.v1" in mapping_edge.evidence


def test_deployment_correlation_requires_explicit_environment_and_prior_deployment() -> (
    None
):
    now = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)
    repo_id = uuid4()
    sink = MagicMock()

    def query(query: str, _params: dict[str, object]) -> list[dict[str, object]]:
        if "operational_service_repository_mappings" in query:
            return [
                {
                    "service_id": "svc-1",
                    "repo_id": repo_id,
                    "provider": "pagerduty",
                    "relationship_provenance": "pagerduty_service_metadata",
                    "relationship_confidence": 0.95,
                    "mapping_kind": "pagerduty_service_metadata_exact",
                    "rule_id": "metadata.v1",
                    "source_url": None,
                }
            ]
        if "operational_incidents" in query:
            return [
                {
                    "id": "inc-1",
                    "service_id": "svc-1",
                    "escalation_policy_id": None,
                    "started_at": now,
                    "source_url": None,
                }
            ]
        if "deployments" in query:
            return [
                {
                    "repo_id": repo_id,
                    "deployment_id": "unspecified",
                    "environment": "unspecified",
                    "deployed_at": now - timedelta(hours=1),
                },
                {
                    "repo_id": repo_id,
                    "deployment_id": "after-incident",
                    "environment": "production",
                    "deployed_at": now + timedelta(minutes=1),
                },
                {
                    "repo_id": repo_id,
                    "deployment_id": "before-incident",
                    "environment": "production",
                    "deployed_at": now - timedelta(hours=1),
                },
            ]
        return []

    sink.query_dicts.side_effect = query

    edges = build_operational_incident_edges(sink, "org-a", now, 7, 0.3)

    deployment_edges = [
        edge for edge in edges if edge.edge_type.value == "linked_incident"
    ]
    assert [edge.source_id for edge in deployment_edges] == ["before-incident"]


def test_operational_edges_apply_repository_and_time_scope() -> None:
    now = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)
    from_date = now - timedelta(days=2)
    to_date = now + timedelta(days=1)
    repo_id = uuid4()
    sink = MagicMock()
    queries: list[tuple[str, dict[str, object]]] = []

    def query(query: str, params: dict[str, object]) -> list[dict[str, object]]:
        queries.append((query, params))
        if "operational_service_repository_mappings" in query:
            return [
                {
                    "service_id": "svc-1",
                    "repo_id": repo_id,
                    "provider": "pagerduty",
                    "relationship_provenance": "pagerduty_service_metadata",
                    "relationship_confidence": 0.95,
                    "mapping_kind": "pagerduty_service_metadata_exact",
                    "rule_id": "metadata.v1",
                    "source_url": "https://github.com/full-chaos/checkout",
                }
            ]
        return []

    sink.query_dicts.side_effect = query

    edges = build_operational_incident_edges(
        sink,
        "org-a",
        now,
        7,
        0.3,
        from_date,
        to_date,
        repo_id,
    )

    assert any(edge.edge_type.value == "maps_to_repository" for edge in edges)
    mapping_query, mapping_params = next(
        (query, params)
        for query, params in queries
        if "operational_service_repository_mappings" in query
    )
    incident_query, incident_params = next(
        (query, params) for query, params in queries if "operational_incidents" in query
    )
    assert "repo_id = {repo_id:UUID}" in mapping_query
    assert "valid_from <= {now:DateTime}" in mapping_query
    assert "valid_to IS NULL OR valid_to > {now:DateTime}" in mapping_query
    assert "started_at >= {from_date:DateTime}" in incident_query
    assert "started_at <= {to_date:DateTime}" in incident_query
    assert mapping_params["repo_id"] == repo_id
    assert incident_params["from_date"] == from_date
    assert incident_params["to_date"] == to_date


def test_deployment_correlation_excludes_same_repo_id_from_another_org() -> None:
    now = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)
    shared_repo_id = uuid4()
    sink = MagicMock()
    queries: list[str] = []

    datasets: dict[str, list[dict[str, object]]] = {
        "operational_service_repository_mappings": [
            {
                "org_id": "org-a",
                "service_id": "svc-a",
                "repo_id": shared_repo_id,
                "relationship_provenance": "admin_configuration",
                "relationship_confidence": 1.0,
                "mapping_kind": "admin_configuration_exact",
                "rule_id": "admin.v1",
                "source_url": None,
            },
            {
                "org_id": "org-b",
                "service_id": "svc-b",
                "repo_id": shared_repo_id,
                "relationship_provenance": "admin_configuration",
                "relationship_confidence": 1.0,
                "mapping_kind": "admin_configuration_exact",
                "rule_id": "admin.v1",
                "source_url": None,
            },
        ],
        "operational_incidents": [
            {
                "org_id": "org-a",
                "id": "inc-a",
                "service_id": "svc-a",
                "escalation_policy_id": None,
                "started_at": now,
                "source_url": None,
            },
            {
                "org_id": "org-b",
                "id": "inc-b",
                "service_id": "svc-b",
                "escalation_policy_id": None,
                "started_at": now,
                "source_url": None,
            },
        ],
        "deployments": [
            {
                "org_id": "org-a",
                "repo_id": shared_repo_id,
                "deployment_id": "deploy-a",
                "environment": "production",
                "deployed_at": now - timedelta(hours=1),
            },
            {
                "org_id": "org-b",
                "repo_id": shared_repo_id,
                "deployment_id": "deploy-b",
                "environment": "production",
                "deployed_at": now - timedelta(hours=1),
            },
        ],
    }

    def query(query: str, params: dict[str, object]) -> list[dict[str, object]]:
        queries.append(query)
        dataset = next((rows for table, rows in datasets.items() if table in query), [])
        if "WHERE org_id = {org_id:String}" not in query:
            return dataset
        return [row for row in dataset if row["org_id"] == params["org_id"]]

    sink.query_dicts.side_effect = query

    edges = build_operational_incident_edges(sink, "org-a", now, 7, 0.3)

    deployment_edges = [
        edge for edge in edges if edge.edge_type.value == "linked_incident"
    ]
    assert [(edge.source_id, edge.target_id) for edge in deployment_edges] == [
        ("deploy-a", "inc-a")
    ]
    assert all("WHERE org_id = {org_id:String}" in query for query in queries)

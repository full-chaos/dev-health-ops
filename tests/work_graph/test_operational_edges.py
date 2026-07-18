from datetime import datetime, timezone
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

import pytest

from dev_health_ops.api.graphql.models.outputs import (
    WorkGraphEdgeType,
    WorkGraphNodeType,
)
from dev_health_ops.api.graphql.resolvers.work_graph import (
    _map_edge_type,
    _map_node_type,
)


@pytest.mark.parametrize(
    ("persisted_kind", "graphql_kind"),
    [
        ("operational_service", WorkGraphNodeType.OPERATIONAL_SERVICE),
        ("operational_alert", WorkGraphNodeType.OPERATIONAL_ALERT),
        ("incident_timeline_event", WorkGraphNodeType.INCIDENT_TIMELINE_EVENT),
        ("incident_responder", WorkGraphNodeType.INCIDENT_RESPONDER),
        ("escalation_policy", WorkGraphNodeType.ESCALATION_POLICY),
        ("repository", WorkGraphNodeType.REPOSITORY),
        ("user", WorkGraphNodeType.USER),
        ("team", WorkGraphNodeType.TEAM),
    ],
)
def test_maps_operational_node_kinds_to_their_graphql_enum(
    persisted_kind: str, graphql_kind: WorkGraphNodeType
) -> None:
    assert _map_node_type(persisted_kind) is graphql_kind


@pytest.mark.parametrize(
    ("persisted_kind", "graphql_kind"),
    [
        ("maps_to_repository", WorkGraphEdgeType.MAPS_TO_REPOSITORY),
        ("has_incident", WorkGraphEdgeType.HAS_INCIDENT),
        ("has_alert", WorkGraphEdgeType.HAS_ALERT),
        ("has_timeline_event", WorkGraphEdgeType.HAS_TIMELINE_EVENT),
        ("has_responder", WorkGraphEdgeType.HAS_RESPONDER),
        ("assigned_to", WorkGraphEdgeType.ASSIGNED_TO),
        ("escalates_with", WorkGraphEdgeType.ESCALATES_WITH),
        ("remediated_by", WorkGraphEdgeType.REMEDIATED_BY),
    ],
)
def test_maps_operational_edge_kinds_to_their_graphql_enum(
    persisted_kind: str, graphql_kind: WorkGraphEdgeType
) -> None:
    assert _map_edge_type(persisted_kind) is graphql_kind


@pytest.mark.parametrize("persisted_kind", ["unknown_node", "unknown_edge"])
def test_raises_for_an_unknown_persisted_work_graph_kind(persisted_kind: str) -> None:
    mapper = _map_node_type if persisted_kind == "unknown_node" else _map_edge_type

    with pytest.raises(ValueError, match=persisted_kind):
        mapper(persisted_kind)

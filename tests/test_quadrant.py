from datetime import date

from dev_health_ops.api.models.schemas import (
    QuadrantAnnotation,
    QuadrantAxes,
    QuadrantAxis,
    QuadrantPoint,
    QuadrantPointTrajectory,
    QuadrantResponse,
)
from dev_health_ops.api.services.quadrant import QUADRANT_DEFINITIONS


def _model_fields(model) -> set[str]:
    if hasattr(model, "model_fields"):
        return set(model.model_fields.keys())
    return set(model.__fields__.keys())


def test_quadrant_schema_shape():
    response = QuadrantResponse(
        axes=QuadrantAxes(
            x=QuadrantAxis(metric="churn", label="Churn", unit="loc"),
            y=QuadrantAxis(metric="throughput", label="Throughput", unit="items"),
        ),
        points=[
            QuadrantPoint(
                entity_id="team-a",
                entity_label="Team A",
                x=120.0,
                y=42.0,
                window_start=date(2024, 1, 1),
                window_end=date(2024, 1, 8),
                evidence_link="/api/v1/explain?metric=throughput",
                trajectory=[
                    QuadrantPointTrajectory(x=100.0, y=40.0, window="2024-01-01")
                ],
            )
        ],
        annotations=[
            QuadrantAnnotation(
                type="boundary",
                description="Saturation zone",
                x_range=[20.0, 80.0],
                y_range=[5.0, 40.0],
            )
        ],
    )
    assert response.axes.x.metric == "churn"


def test_quadrant_no_rank_fields():
    forbidden = {"rank", "percentile", "score"}
    assert forbidden.isdisjoint(_model_fields(QuadrantPoint))


def test_quadrant_axis_label_snapshot():
    expected = {
        "churn_throughput": ("Churn", "Throughput"),
        "cycle_throughput": ("Cycle Time", "Throughput"),
        "wip_throughput": ("WIP", "Throughput"),
        "review_load_latency": ("Review Load", "Review Latency"),
    }
    assert {
        key: (definition.x.label, definition.y.label)
        for key, definition in QUADRANT_DEFINITIONS.items()
    } == expected

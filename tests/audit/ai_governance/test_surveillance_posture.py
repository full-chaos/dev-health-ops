from __future__ import annotations

from dataclasses import fields

from dev_health_ops.audit.ai_governance.models import (
    AIGovernanceArtifact,
    AIGovernanceCoverageDaily,
    AIGovernanceViolation,
)
from dev_health_ops.metrics.sinks.clickhouse.ai_governance import (
    COVERAGE_COLUMNS,
    POLICY_EVENT_COLUMNS,
)

FORBIDDEN_FIELD_NAMES = {"prompt", "session", "transcript", "ide", "user_keystroke"}


def test_persisted_governance_models_do_not_store_surveillance_fields() -> None:
    persisted_names = set(POLICY_EVENT_COLUMNS) | set(COVERAGE_COLUMNS)
    for model in (
        AIGovernanceArtifact,
        AIGovernanceCoverageDaily,
        AIGovernanceViolation,
    ):
        persisted_names.update(field.name for field in fields(model))

    assert persisted_names.isdisjoint(FORBIDDEN_FIELD_NAMES)

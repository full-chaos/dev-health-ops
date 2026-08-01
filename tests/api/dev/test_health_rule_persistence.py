"""Non-database unit tests for ``health_rule_persistence``'s

production/test-scope split (CHAOS-3302, round 4). ``record_rule_version_
fingerprint`` (production) resolves its ``rule_id`` argument against
``HEALTH_RULE_REGISTRY`` *before* ever touching the database session --
these tests exercise exactly that resolution boundary and need no real
database (the live-Postgres drift/idempotency proofs live in
``test_health_rule_persistence_postgres.py``, gated behind
``DEV_HEALTH_POSTGRES_TEST_URI``).
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.dev.health_rule_persistence import (
    record_rule_version_fingerprint,
)
from dev_health_ops.api.dev.health_rule_registry import (
    HEALTH_RULE_REGISTRY,
    UnknownRuleError,
)


@pytest.mark.asyncio
async def test_record_rule_version_fingerprint_rejects_unknown_rule_id() -> None:
    """The production seam resolves ``rule_id`` against ``HEALTH_RULE_REGISTRY``

    before ever touching the database session -- an unresolvable rule_id is
    rejected before any write is attempted, so ``session=None`` is never
    dereferenced.
    """

    with pytest.raises(UnknownRuleError):
        await record_rule_version_fingerprint(None, "health_rule.not_a_real_rule.v1")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_record_rule_version_fingerprint_rejects_a_rule_object_in_place_of_an_id() -> (
    None
):
    """Codex finding (medium, round 4): a caller cannot smuggle a

    caller-constructed ``HealthRuleDefinition`` through the production seam
    by passing it positionally where a ``rule_id`` string belongs.
    ``HEALTH_RULE_REGISTRY.rule()`` only resolves string keys, so a
    ``HealthRuleDefinition`` object -- even one carrying a canonical
    ``rule_id``/``rule_version`` pair but a mutated field, exactly the
    round-4 repro shape ("a normally constructed rule with the canonical
    ID/version but a changed threshold") -- fails resolution rather than
    being persisted as if it were the real canonical rule.
    """

    canonical_rule = HEALTH_RULE_REGISTRY.rule("health_rule.completion_stalled.v1")
    assert canonical_rule.threshold is not None
    drifted_rule = canonical_rule.model_copy(
        update={"threshold": canonical_rule.threshold + 100}
    )
    with pytest.raises(UnknownRuleError):
        await record_rule_version_fingerprint(None, drifted_rule)  # type: ignore[arg-type]

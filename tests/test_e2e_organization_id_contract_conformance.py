"""The live-e2e seed organization id must satisfy the job contract.

CHAOS-3903: the seed id was 11111111-2222-3333-4444-555555555555, which
``uuid.UUID`` accepts and PostgreSQL stores happily, but which is not RFC 4122
-- its variant nibble is 4, and the contract requires [89ab]. Every
fixed-schedule fan-out that enumerated organizations then failed its whole
window on that one row, holding the scheduler permanently unready.

The pattern is READ FROM the Go source that enforces it rather than restated
here. A second hand-maintained copy would drift from the real rule, which is
the precise failure mode this guard exists to catch.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_CONTRACT_SOURCE = _REPO_ROOT / "internal" / "jobcontract" / "types.go"
_E2E_SCRIPT = _REPO_ROOT / "ci" / "run_live_backend_e2e.sh"


def _contract_uuid_pattern() -> re.Pattern[str]:
    source = _CONTRACT_SOURCE.read_text(encoding="utf-8")
    match = re.search(r"uuidPattern\s*=\s*regexp\.MustCompile\(`([^`]+)`\)", source)
    assert match, (
        f"uuidPattern not found in {_CONTRACT_SOURCE}. It was renamed or moved; "
        "retarget this guard at whatever now validates organization_id, do not "
        "delete it."
    )
    return re.compile(match.group(1))


def _seed_organization_ids() -> list[str]:
    script = _E2E_SCRIPT.read_text(encoding="utf-8")
    ids = re.findall(r'E2E_ORG_ID[",)]?\s*[,=]\s*"([0-9a-fA-F-]{36})"', script)
    assert ids, (
        f"no E2E_ORG_ID literal found in {_E2E_SCRIPT}; the seed moved and this "
        "guard now checks nothing"
    )
    return ids


def test_contract_pattern_rejects_the_id_that_caused_chaos_3903() -> None:
    """Negative control: without this, a pattern that matched anything passes."""
    assert not _contract_uuid_pattern().match("11111111-2222-3333-4444-555555555555")


@pytest.mark.parametrize("organization_id", _seed_organization_ids())
def test_e2e_seed_organization_id_satisfies_the_job_contract(
    organization_id: str,
) -> None:
    assert _contract_uuid_pattern().match(organization_id), (
        f"{organization_id} is not accepted by jobcontract.uuidPattern, so every "
        "fixed-schedule fan-out would degrade this organization instead of "
        "producing its work"
    )

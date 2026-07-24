from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PUBLIC_GUIDE = ROOT / "docs" / "admin" / "data-sources" / "jira-atlassian.md"
PROVIDER_DOC = (
    ROOT / ".github" / "docs-legacy" / "providers" / "jira-service-management.md"
)
# The canonical operational model is preserved source evidence, not a public page.
MODEL_DOC = (
    ROOT / ".github" / "docs-legacy" / "architecture" / "canonical-operational-model.md"
)


def test_jsm_internal_contract_is_archived_and_public_guide_is_task_oriented() -> None:
    assert PROVIDER_DOC.is_file(), f"missing archived JSM contract: {PROVIDER_DOC}"
    public = PUBLIC_GUIDE.read_text(encoding="utf-8")
    assert "page_id: admin-jira" in public
    assert "Connect and verify Jira" in public
    assert "not yet a supported\nadministrator workflow" in public
    assert "Jira Service Management provider contract" not in public
    assert "api.atlassian.com/jsm/incidents" not in public


def test_jsm_matrix_uses_only_canonical_outcomes() -> None:
    provider = PROVIDER_DOC.read_text(encoding="utf-8")
    model = MODEL_DOC.read_text(encoding="utf-8")

    assert "| JSM incidents | **BLOCKED** | **GO** |" in provider
    assert "| JSM Ops Alerts | **BLOCKED** |" in provider
    assert "| Standalone Opsgenie | **NO_GO** |" in provider
    assert "| Jira Software Operations Information | **NO_GO** |" in provider
    assert "Conditional GO" not in provider + model
    assert "Future attention signal" not in provider + model


def test_jsm_no_go_rows_have_no_catalog_or_live_proof_claim() -> None:
    provider = PROVIDER_DOC.read_text(encoding="utf-8")

    assert "No standalone Opsgenie catalog entry" in provider
    assert "no authoritative read path" in provider
    assert "live tenant validation" in provider
    assert "configured JSM allowlist" in provider
    assert "OAuth installations" not in provider


def test_jsm_draft_contract_requires_bounded_jql_and_native_admission() -> None:
    provider = PROVIDER_DOC.read_text(encoding="utf-8")
    model = MODEL_DOC.read_text(encoding="utf-8")
    docs = provider + model

    assert '"Ticket category" = Incidents' in docs
    assert 'updated >= "<window_start>"' in docs
    assert 'updated < "<window_end>"' in docs
    assert "configured JSM allowlist intersected" in model
    assert "configured key is not an enumerated JSM service project" in provider
    assert "exact set returned by the JSM service desk enumeration" not in docs
    assert (
        "https://api.atlassian.com/jsm/incidents/cloudId/<cloud_id>/v1/incident/<issue_id>"
        in docs
    )
    assert "Only HTTP 200 admits" in docs
    assert "HTTP 404 is a negative admission" in docs
    assert "fails closed" in docs
    assert "WorkItem" in docs and "OperationalIncident" in docs
    assert "JSM Ops Alerts remain separate" in provider
    assert "not\nimplemented" in provider
    assert "T6A remains **BLOCKED**" in provider

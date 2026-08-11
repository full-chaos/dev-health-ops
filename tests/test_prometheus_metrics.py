"""Tests for custom Prometheus metrics (CHAOS-656).

Verifies metric registration, labeling, and convenience helpers
for GitHub API, Celery, ClickHouse, and LLM metrics.
"""

from __future__ import annotations

import pytest
from prometheus_client import REGISTRY

from dev_health_ops.metrics.prometheus import (
    CONTEXT_FABRIC_DOCUMENTS_INDEXED_TOTAL,
    CONTEXT_FABRIC_DOCUMENTS_REMOVED_TOTAL,
    CONTEXT_FABRIC_GRAPH_DOCUMENT_REMOVAL_OUTCOME_TOTAL,
    CONTEXT_FABRIC_GRAPH_ORG_DELETION_VISITS_TOTAL,
    CONTEXT_FABRIC_GRAPH_PROJECTION_WRITES_TOTAL,
    CONTEXT_FABRIC_GRAPH_PURGES_TOTAL,
    CONTEXT_FABRIC_GRAPH_QUERY_DURATION_SECONDS,
    CONTEXT_FABRIC_GRAPH_QUERY_OUTCOME_TOTAL,
    CONTEXT_FABRIC_GRAPH_WATERMARK_STATE_TOTAL,
    GITHUB_API_REQUESTS_TOTAL,
    GITHUB_RATE_LIMIT_REMAINING,
    record_context_fabric_document_removed,
    record_context_fabric_documents_indexed,
    record_context_fabric_graph_document_removal,
    record_context_fabric_graph_org_deletion_visit,
    record_context_fabric_graph_projection,
    record_context_fabric_graph_purge,
    record_context_fabric_graph_query,
    record_context_fabric_graph_watermark,
    record_github_api_request,
    record_github_rate_limit,
)


class TestGitHubApiMetrics:
    """GitHub API Prometheus metrics."""

    def test_request_counter_increments(self):
        """record_github_api_request increments the counter with correct labels."""
        before = GITHUB_API_REQUESTS_TOTAL.labels(
            endpoint="/repos", status_code="200"
        )._value.get()

        record_github_api_request("/repos", "200")

        after = GITHUB_API_REQUESTS_TOTAL.labels(
            endpoint="/repos", status_code="200"
        )._value.get()
        assert after == before + 1

    def test_request_counter_different_status_codes(self):
        """Different status codes are tracked independently."""
        record_github_api_request("/pulls", "200")
        record_github_api_request("/pulls", "429")

        count_200 = GITHUB_API_REQUESTS_TOTAL.labels(
            endpoint="/pulls", status_code="200"
        )._value.get()
        count_429 = GITHUB_API_REQUESTS_TOTAL.labels(
            endpoint="/pulls", status_code="429"
        )._value.get()

        assert count_200 >= 1
        assert count_429 >= 1

    def test_rate_limit_gauge_set(self):
        """record_github_rate_limit sets the gauge value."""
        record_github_rate_limit("core", 4500)

        value = GITHUB_RATE_LIMIT_REMAINING.labels(resource="core")._value.get()
        assert value == 4500

    def test_rate_limit_gauge_updates(self):
        """Gauge reflects the latest value, not cumulative."""
        record_github_rate_limit("search", 30)
        record_github_rate_limit("search", 25)

        value = GITHUB_RATE_LIMIT_REMAINING.labels(resource="search")._value.get()
        assert value == 25

    def test_counter_registered_in_default_registry(self):
        """GitHub metrics are registered in the default Prometheus registry."""
        metric_names = [m.name for m in REGISTRY.collect()]
        # prometheus_client strips _total suffix from Counter names in registry
        assert "devhealth_github_api_requests" in metric_names

    def test_gauge_registered_in_default_registry(self):
        """Rate limit gauge is registered in the default Prometheus registry."""
        metric_names = [m.name for m in REGISTRY.collect()]
        assert "devhealth_github_rate_limit_remaining" in metric_names


class TestContextFabricDocumentMetrics:
    """Context Fabric graph arm: embedded-surface document lifecycle
    (issue 3632)."""

    def test_indexed_counter_increments_by_the_given_count(self):
        before = CONTEXT_FABRIC_DOCUMENTS_INDEXED_TOTAL._value.get()

        record_context_fabric_documents_indexed(3)

        after = CONTEXT_FABRIC_DOCUMENTS_INDEXED_TOTAL._value.get()
        assert after == before + 3

    def test_indexed_counter_is_a_no_op_for_zero(self):
        """A batch with no documents must not even touch the counter --
        distinguishes "zero documents written" from "one call that happened
        to write zero", which matters for a rate-based alert reading this
        counter's call frequency, not just its value.
        """

        before = CONTEXT_FABRIC_DOCUMENTS_INDEXED_TOTAL._value.get()

        record_context_fabric_documents_indexed(0)

        after = CONTEXT_FABRIC_DOCUMENTS_INDEXED_TOTAL._value.get()
        assert after == before

    def test_removed_counter_increments_with_its_reason_label(self):
        before = CONTEXT_FABRIC_DOCUMENTS_REMOVED_TOTAL.labels(
            reason="approval_revoked"
        )._value.get()

        record_context_fabric_document_removed("approval_revoked")

        after = CONTEXT_FABRIC_DOCUMENTS_REMOVED_TOTAL.labels(
            reason="approval_revoked"
        )._value.get()
        assert after == before + 1

    def test_removed_counter_tracks_reasons_independently(self):
        record_context_fabric_document_removed("policy_forbidden")
        record_context_fabric_document_removed("cross_tenant")

        policy_forbidden = CONTEXT_FABRIC_DOCUMENTS_REMOVED_TOTAL.labels(
            reason="policy_forbidden"
        )._value.get()
        cross_tenant = CONTEXT_FABRIC_DOCUMENTS_REMOVED_TOTAL.labels(
            reason="cross_tenant"
        )._value.get()

        assert policy_forbidden >= 1
        assert cross_tenant >= 1

    def test_indexed_counter_registered_in_default_registry(self):
        metric_names = [m.name for m in REGISTRY.collect()]
        assert "devhealth_context_fabric_documents_indexed" in metric_names

    def test_removed_counter_registered_in_default_registry(self):
        metric_names = [m.name for m in REGISTRY.collect()]
        assert "devhealth_context_fabric_documents_removed" in metric_names


class TestContextFabricGraphOperationalMetrics:
    """Every graph operational metric is closed-label and content-safe."""

    def test_query_outcome_records_disabled_and_failure_independently(self):
        disabled_before = CONTEXT_FABRIC_GRAPH_QUERY_OUTCOME_TOTAL.labels(
            outcome="disabled"
        )._value.get()
        failure_before = CONTEXT_FABRIC_GRAPH_QUERY_OUTCOME_TOTAL.labels(
            outcome="provider_failure"
        )._value.get()
        duration_before = CONTEXT_FABRIC_GRAPH_QUERY_DURATION_SECONDS.labels(
            outcome="disabled"
        )._sum.get()

        record_context_fabric_graph_query(outcome="disabled", duration_seconds=0.01)
        record_context_fabric_graph_query(
            outcome="provider_failure", duration_seconds=0.02
        )

        assert (
            CONTEXT_FABRIC_GRAPH_QUERY_OUTCOME_TOTAL.labels(
                outcome="disabled"
            )._value.get()
            == disabled_before + 1
        )
        assert (
            CONTEXT_FABRIC_GRAPH_QUERY_OUTCOME_TOTAL.labels(
                outcome="provider_failure"
            )._value.get()
            == failure_before + 1
        )
        assert (
            CONTEXT_FABRIC_GRAPH_QUERY_DURATION_SECONDS.labels(
                outcome="disabled"
            )._sum.get()
            >= duration_before + 0.01
        )

    def test_unknown_query_outcome_is_rejected(self):
        with pytest.raises(ValueError, match="closed Context Fabric"):
            record_context_fabric_graph_query(
                outcome="question text must not become a label",
                duration_seconds=0.01,
            )

    def test_projection_failure_watermark_and_deletion_negative_states_record(self):
        projection_before = CONTEXT_FABRIC_GRAPH_PROJECTION_WRITES_TOTAL.labels(
            outcome="failed"
        )._value.get()
        unavailable_before = CONTEXT_FABRIC_GRAPH_WATERMARK_STATE_TOTAL.labels(
            state="unavailable"
        )._value.get()
        purge_before = CONTEXT_FABRIC_GRAPH_PURGES_TOTAL.labels(
            outcome="failed", dry_run="false"
        )._value.get()
        document_before = CONTEXT_FABRIC_GRAPH_DOCUMENT_REMOVAL_OUTCOME_TOTAL.labels(
            outcome="not_found", reason="approval_revoked"
        )._value.get()
        visit_before = CONTEXT_FABRIC_GRAPH_ORG_DELETION_VISITS_TOTAL.labels(
            outcome="unknown", dry_run="false"
        )._value.get()

        record_context_fabric_graph_projection("failed")
        record_context_fabric_graph_watermark(state="unavailable", lag_seconds=0.0)
        record_context_fabric_graph_purge(outcome="failed", dry_run=False)
        record_context_fabric_graph_document_removal(
            outcome="not_found", reason="approval_revoked"
        )
        record_context_fabric_graph_org_deletion_visit(outcome="unknown", dry_run=False)

        assert (
            CONTEXT_FABRIC_GRAPH_PROJECTION_WRITES_TOTAL.labels(
                outcome="failed"
            )._value.get()
            == projection_before + 1
        )
        assert (
            CONTEXT_FABRIC_GRAPH_WATERMARK_STATE_TOTAL.labels(
                state="unavailable"
            )._value.get()
            == unavailable_before + 1
        )
        assert (
            CONTEXT_FABRIC_GRAPH_PURGES_TOTAL.labels(
                outcome="failed", dry_run="false"
            )._value.get()
            == purge_before + 1
        )
        assert (
            CONTEXT_FABRIC_GRAPH_DOCUMENT_REMOVAL_OUTCOME_TOTAL.labels(
                outcome="not_found", reason="approval_revoked"
            )._value.get()
            == document_before + 1
        )
        assert (
            CONTEXT_FABRIC_GRAPH_ORG_DELETION_VISITS_TOTAL.labels(
                outcome="unknown", dry_run="false"
            )._value.get()
            == visit_before + 1
        )

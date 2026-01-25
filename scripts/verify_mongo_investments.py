#!/usr/bin/env python3
"""Quick verification script for MongoDB work unit investment methods."""

import uuid
from datetime import datetime, timezone
from dev_health_ops.metrics.sinks.mongo import MongoMetricsSink
from dev_health_ops.metrics.schemas import (
    WorkUnitInvestmentRecord,
    WorkUnitInvestmentEvidenceQuoteRecord,
)


def main():
    print("Connecting to MongoDB...")
    sink = MongoMetricsSink("mongodb://127.0.0.1:27017", db_name="dev_health_test")
    print("✓ Connected")

    print("Creating indexes...")
    sink.ensure_indexes()
    print("✓ Indexes created")

    print("Testing write_work_unit_investments...")
    investment = WorkUnitInvestmentRecord(
        work_unit_id="verify-test-001",
        work_unit_type="pr",
        work_unit_name="Verification Test",
        from_ts=datetime(2025, 1, 1, tzinfo=timezone.utc),
        to_ts=datetime(2025, 1, 2, tzinfo=timezone.utc),
        repo_id=uuid.uuid4(),
        provider="github",
        effort_metric="loc",
        effort_value=100.0,
        theme_distribution_json={"feature": 0.6, "maintenance": 0.4},
        subcategory_distribution_json={"new_feature": 0.6, "refactor": 0.4},
        structural_evidence_json='{"files": ["test.py"]}',
        evidence_quality=0.85,
        evidence_quality_band="high",
        categorization_status="success",
        categorization_errors_json="[]",
        categorization_model_version="v1.0",
        categorization_input_hash="abc123",
        categorization_run_id="verify-run-001",
        computed_at=datetime.now(timezone.utc),
    )
    sink.write_work_unit_investments([investment])
    print("✓ write_work_unit_investments")

    print("Testing write_work_unit_investment_quotes...")
    quote = WorkUnitInvestmentEvidenceQuoteRecord(
        work_unit_id="verify-test-001",
        quote="Added new authentication feature",
        source_type="pr_description",
        source_id="PR-123",
        computed_at=datetime.now(timezone.utc),
        categorization_run_id="verify-run-001",
    )
    sink.write_work_unit_investment_quotes([quote])
    print("✓ write_work_unit_investment_quotes")

    print("Verifying data...")
    inv_doc = sink.db["work_unit_investments"].find_one(
        {"work_unit_id": "verify-test-001"}
    )
    assert inv_doc is not None, "Investment document not found!"
    assert inv_doc["theme_distribution_json"] == {"feature": 0.6, "maintenance": 0.4}
    print(f"✓ Document _id: {inv_doc['_id']}")

    quote_doc = sink.db["work_unit_investment_quotes"].find_one(
        {"work_unit_id": "verify-test-001"}
    )
    assert quote_doc is not None, "Quote document not found!"
    print("✓ Quote found")

    print("Cleaning up...")
    sink.db["work_unit_investments"].delete_many({"work_unit_id": "verify-test-001"})
    sink.db["work_unit_investment_quotes"].delete_many(
        {"work_unit_id": "verify-test-001"}
    )
    print("✓ Test data cleaned")

    sink.close()
    print("\n🎉 All MongoDB verification tests PASSED!")


if __name__ == "__main__":
    main()

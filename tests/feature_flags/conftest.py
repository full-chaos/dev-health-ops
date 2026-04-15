from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

import pytest


@pytest.fixture
def join_provider_case_map() -> dict[str, dict[str, Any]]:
    base_completed_at = datetime(2026, 3, 8, 14, 0, tzinfo=timezone.utc)
    return {
        "github": {
            "provider": "github",
            "input": {
                "issue_id": "GH-101",
                "pull_request_number": 42,
                "deployment_id": "gh-deploy-42",
                "environment": "production",
                "completed_at": base_completed_at,
                "tag": "v1.2.3",
            },
            "expected": {
                "release_ref": "v1.2.3",
                "environment": "production",
                "provenance": "native",
                "confidence": 1.0,
            },
        },
        "gitlab": {
            "provider": "gitlab",
            "input": {
                "issue_id": "GL-202",
                "pull_request_number": 108,
                "deployment_id": "gl-deploy-108",
                "environment": "staging",
                "completed_at": base_completed_at + timedelta(hours=1),
                "tag": "release-2026.03.08",
            },
            "expected": {
                "release_ref": "release-2026.03.08",
                "environment": "staging",
                "provenance": "native",
                "confidence": 1.0,
            },
        },
        "generic": {
            "provider": "generic",
            "input": {
                "issue_id": "GEN-303",
                "pull_request_number": 7,
                "deployment_id": "deploy-opaque-7",
                "environment": "prod",
                "completed_at": base_completed_at + timedelta(hours=2),
                "tag": None,
            },
            "expected": {
                "release_ref": "deploy-opaque-7",
                "environment": "prod",
                "provenance": "heuristic",
                "confidence": 0.3,
            },
        },
    }


@pytest.fixture
def dedupe_event_case_map() -> dict[str, dict[str, Any]]:
    event_ts = datetime(2026, 3, 8, 16, 0, tzinfo=timezone.utc)
    return {
        "feature_flag.change": {
            "record": {
                "org_id": "acme",
                "event_type": "toggle",
                "flag_key": "checkout_redesign",
                "environment": "production",
                "event_ts": event_ts,
                "dedupe_key": "ff-change-001",
            }
        },
        "feature_flag.exposure": {
            "record": {
                "org_id": "acme",
                "signal_type": "feature_flag.exposure",
                "flag_key": "checkout_redesign",
                "environment": "production",
                "bucket_start": event_ts,
                "bucket_end": event_ts + timedelta(hours=1),
                "dedupe_key": "ff-exposure-001",
            }
        },
        "telemetry.signal": {
            "record": {
                "org_id": "acme",
                "signal_type": "friction.click_rage",
                "environment": "production",
                "release_ref": "v1.2.3",
                "bucket_start": event_ts,
                "bucket_end": event_ts + timedelta(hours=1),
                "dedupe_key": "telemetry-001",
            }
        },
        "release.deployment": {
            "record": {
                "org_id": "acme",
                "provider": "github",
                "deployment_id": "gh-deploy-42",
                "environment": "production",
                "event_ts": event_ts,
                "dedupe_key": "deployment-001",
            }
        },
    }


@pytest.fixture
def metric_formula_case_map() -> dict[str, dict[str, Any]]:
    deployment_completed_at = datetime(2026, 3, 8, 10, 0, tzinfo=timezone.utc)
    first_rollout_at = datetime(2026, 3, 9, 8, 0, tzinfo=timezone.utc)
    half_exposure_at = first_rollout_at + timedelta(hours=4)
    return {
        "release_user_friction_delta": {
            "inputs": {"baseline_rate": 0.10, "post_rate": 0.15},
            "expected": 0.50,
        },
        "release_error_rate_delta": {
            "inputs": {"baseline_rate": 0.02, "post_rate": 0.03},
            "expected": 0.50,
        },
        "time_to_first_user_issue_after_release": {
            "inputs": {
                "deployment_completed_at": deployment_completed_at,
                "linked_issue_created_at": deployment_completed_at + timedelta(hours=5),
            },
            "expected": 5.0,
        },
        "release_impact_confidence_score": {
            "inputs": {
                "weights": {
                    "linkage_quality": 0.5,
                    "coverage_ratio": 0.3,
                    "sample_sufficiency": 0.2,
                },
                "values": {
                    "linkage_quality": 0.9,
                    "coverage_ratio": 0.8,
                    "sample_sufficiency": 0.5,
                },
            },
            "expected": 0.79,
        },
        "release_impact_coverage_ratio": {
            "inputs": {"matched_events": 6, "eligible_events": 10},
            "expected": 0.60,
        },
        "flag_exposure_rate": {
            "inputs": {"exposed_sessions": 150, "eligible_sessions": 200},
            "expected": 0.75,
        },
        "flag_activation_rate": {
            "inputs": {"activated_sessions": 50, "exposed_sessions": 100},
            "expected": 0.50,
        },
        "flag_reliability_guardrail": {
            "inputs": {"error_free_sessions": 270, "total_sessions": 300},
            "expected": 0.90,
        },
        "flag_friction_delta": {
            "inputs": {"baseline_rate": 0.20, "post_rate": 0.25},
            "expected": 0.25,
        },
        "flag_rollout_half_life": {
            "inputs": {
                "first_rollout_event_ts": first_rollout_at,
                "half_exposure_event_ts": half_exposure_at,
            },
            "expected": 4.0,
        },
        "flag_churn_rate": {
            "inputs": {"change_events": 8, "weeks_in_window": 4},
            "expected": 2.0,
        },
        "issue_to_release_impact_link_rate": {
            "inputs": {"linked_completed_work_items": 30, "completed_work_items": 50},
            "expected": 0.60,
        },
        "rollback_or_disable_after_impact_spike": {
            "inputs": {
                "deploy_ts": deployment_completed_at,
                "window_hours": 72,
                "events_in_window": ["toggle_off", "rollback"],
            },
            "expected": 2,
        },
    }


@pytest.fixture
def confidence_case_map() -> dict[str, dict[str, Any]]:
    return {
        "native": {"expected_min": 1.0, "expected_max": 1.0},
        "explicit_text": {"expected_min": 0.8, "expected_max": 0.9},
        "heuristic": {"expected_min": 0.3, "expected_max": 0.3},
    }


@pytest.fixture
def drift_gate_case_map() -> dict[str, dict[str, Any]]:
    return {
        "schema_version_shift": {
            "baseline": {"schema_version": "1", "hourly_volume": 100},
            "candidate": {"schema_version": "2", "hourly_volume": 98},
            "expected_flag": True,
        },
        "volume_shift": {
            "baseline": {"schema_version": "1", "hourly_volume": 100},
            "candidate": {"schema_version": "1", "hourly_volume": 240},
            "expected_flag": True,
        },
    }


@pytest.fixture
def coverage_gate_case_map() -> dict[str, dict[str, Any]]:
    return {
        "show": {
            "coverage_ratio": 0.70,
            "min_sample_met": True,
            "expected_visibility": "show",
        },
        "warn": {
            "coverage_ratio": 0.50,
            "min_sample_met": True,
            "expected_visibility": "warn",
        },
        "suppress": {
            "coverage_ratio": 0.49,
            "min_sample_met": True,
            "expected_visibility": "suppress",
        },
    }


@pytest.fixture
def recomputation_case() -> dict[str, Any]:
    anchor_day = date(2026, 3, 15)
    return {
        "anchor_day": anchor_day,
        "window_days": 7,
        "recomputed_days": [anchor_day - timedelta(days=offset) for offset in range(7)],
        "stable_days": [anchor_day - timedelta(days=offset) for offset in range(7, 10)],
        "late_bucket": {
            "event_ts": datetime(2026, 3, 12, 9, 0, tzinfo=timezone.utc),
            "ingested_at": datetime(2026, 3, 15, 9, 30, tzinfo=timezone.utc),
        },
        "expected_data_completeness_floor": 0.0,
    }


@pytest.fixture
def sink_round_trip_case() -> dict[str, Any]:
    computed_at = datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc)
    return {
        "record": {
            "org_id": "acme",
            "release_ref": "v1.2.3",
            "environment": "production",
            "repo_id": "repo-1",
            "day": date(2026, 3, 15),
            "coverage_ratio": 0.82,
            "data_completeness": 0.91,
            "instrumentation_change_flag": False,
            "computed_at": computed_at,
        },
        "query": {
            "org_id": "acme",
            "release_ref": "v1.2.3",
            "environment": "production",
        },
    }


@pytest.fixture
def append_only_case() -> dict[str, Any]:
    base_record = {
        "org_id": "acme",
        "release_ref": "v1.2.3",
        "environment": "production",
        "day": date(2026, 3, 15),
        "coverage_ratio": 0.70,
    }
    return {
        "first": {
            **base_record,
            "computed_at": datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc),
        },
        "second": {
            **base_record,
            "coverage_ratio": 0.82,
            "computed_at": datetime(2026, 3, 15, 13, 0, tzinfo=timezone.utc),
        },
    }


@pytest.fixture
def org_isolation_case() -> dict[str, Any]:
    shared_key = {
        "release_ref": "v1.2.3",
        "environment": "production",
        "repo_id": "repo-1",
    }
    return {
        "acme_record": {"org_id": "acme", **shared_key, "coverage_ratio": 0.75},
        "globex_record": {"org_id": "globex", **shared_key, "coverage_ratio": 0.20},
        "query_org_id": "acme",
    }


@pytest.fixture
def environment_normalization_case_map() -> dict[str, dict[str, str]]:
    return {
        "trim_and_lower": {
            "deployment_environment": " Production ",
            "telemetry_environment": "production",
            "expected_environment": "production",
        },
        "staging_casefold": {
            "deployment_environment": "STAGING",
            "telemetry_environment": "staging",
            "expected_environment": "staging",
        },
    }

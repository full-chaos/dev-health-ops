from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from hashlib import sha256

from dev_health_ops.api.product_telemetry.persist import BLOCKED_PAYLOAD_KEYS
from dev_health_ops.fixtures.generators.product_telemetry import (
    ProductTelemetryGenerator,
    ProductTelemetrySeedSpec,
    product_telemetry_org_hash,
)

FIXED_END = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)


def _spec(**overrides) -> ProductTelemetrySeedSpec:
    base = {
        "org_id": "00000000-0000-0000-0000-000000000001",
        "days": 3,
        "sessions_per_day": 4,
        "seed": 42,
        "end_time": FIXED_END,
    }
    base.update(overrides)
    return ProductTelemetrySeedSpec(**base)


def test_generator_is_deterministic_for_same_seed_and_org() -> None:
    events_a = ProductTelemetryGenerator(_spec()).generate_events()
    events_b = ProductTelemetryGenerator(_spec()).generate_events()

    assert len(events_a) == len(events_b)
    for a, b in zip(events_a, events_b):
        assert a.event_id == b.event_id
        assert a.name == b.name
        assert a.session_id == b.session_id
        assert a.payload == b.payload
        assert a.ts == b.ts


def test_generator_diverges_between_orgs_with_same_seed() -> None:
    events_a = ProductTelemetryGenerator(_spec(org_id="org-a")).generate_events()
    events_b = ProductTelemetryGenerator(_spec(org_id="org-b")).generate_events()

    # Different org_ids must produce different streams so per-org dashboards
    # show distinct data, but each org must still be internally deterministic.
    assert events_a[0].event_id != events_b[0].event_id
    assert events_a[0].org_id_hash != events_b[0].org_id_hash


def test_generator_emits_canonical_org_id_hash() -> None:
    spec = _spec(org_id="abc-123")
    generator = ProductTelemetryGenerator(spec)
    expected_hash = sha256(b"abc-123").hexdigest()

    assert generator.org_id_hash == expected_hash
    assert product_telemetry_org_hash("abc-123") == expected_hash

    events = generator.generate_events()
    assert events, "expected non-empty event stream"
    assert all(event.org_id_hash == expected_hash for event in events)


def test_generator_payloads_omit_all_blocked_keys() -> None:
    events = ProductTelemetryGenerator(_spec()).generate_events()

    for event in events:
        offending = BLOCKED_PAYLOAD_KEYS.intersection(event.payload)
        assert not offending, (
            f"event {event.name} payload leaked blocked keys: {sorted(offending)}"
        )


def test_generator_covers_every_typed_event_name() -> None:
    # Use a larger spec so low-frequency event names (client_error, guide_opened)
    # have a realistic chance of firing across the run.
    spec = _spec(days=5, sessions_per_day=20)
    events = ProductTelemetryGenerator(spec).generate_events()
    names = Counter(e.name for e in events)

    # Always-emitted lifecycle events
    assert names["session_started"] > 0
    assert names["session_ended"] > 0
    assert names["page_viewed"] > 0
    assert names["feature_viewed"] > 0
    # Probability-gated events fire at least once across 100 sessions
    assert names["chart_interacted"] > 0
    assert names["navigation_interacted"] > 0
    assert names["filter_changed"] > 0
    assert names["guide_opened"] > 0


def test_generator_orders_session_started_before_session_ended() -> None:
    events = ProductTelemetryGenerator(_spec()).generate_events()

    sessions: dict[str, dict[str, datetime]] = {}
    for event in events:
        bucket = sessions.setdefault(event.session_id, {})
        if event.name == "session_started":
            bucket["start"] = event.ts
        elif event.name == "session_ended":
            bucket["end"] = event.ts

    assert sessions, "expected at least one session"
    for session_id, marks in sessions.items():
        assert "start" in marks, f"session {session_id} missing session_started"
        assert "end" in marks, f"session {session_id} missing session_ended"
        assert marks["start"] < marks["end"], (
            f"session {session_id} ended before it started"
        )


def test_generator_session_ended_payload_matches_dashboard_contract() -> None:
    """session_ended events must carry the keys the platform SQL extracts."""
    events = ProductTelemetryGenerator(_spec()).generate_events()
    enders = [e for e in events if e.name == "session_ended"]
    assert enders, "expected at least one session_ended event"

    for event in enders:
        payload = event.payload
        assert isinstance(payload["durationMs"], int)
        assert payload["durationMs"] > 0
        assert isinstance(payload["pagesViewed"], int)
        assert payload["pagesViewed"] >= 0
        assert isinstance(payload["interactions"], int)
        assert payload["interactions"] >= 0


def test_generator_respects_day_count_in_event_timestamps() -> None:
    spec = _spec(days=3, sessions_per_day=2)
    events = ProductTelemetryGenerator(spec).generate_events()

    timestamps = [e.ts for e in events]
    assert timestamps, "expected events"
    earliest = min(timestamps)
    latest = max(timestamps)
    span_days = (latest - earliest).days

    # Sessions fan out across the requested window; the spread should be < days
    # but the earliest event must fall within the window.
    assert span_days <= spec.days
    assert earliest >= FIXED_END.replace(
        hour=0, minute=0, second=0, microsecond=0
    ).replace(tzinfo=timezone.utc).replace(year=2026, month=5, day=22)

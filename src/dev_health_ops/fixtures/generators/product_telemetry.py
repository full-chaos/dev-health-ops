"""Synthetic product telemetry event generator for fixtures.

Produces realistic, privacy-safe event streams (typed names, route patterns,
session lifecycles) that match the schema enforced by
``dev_health_ops.api.product_telemetry.schemas.ProductTelemetryEvent``. The
emitted ``org_id_hash`` is the canonical ``sha256(str(org_id))`` digest used
by the GraphQL resolvers, so seeded rows light up the per-org dashboard and
the platform-admin top-orgs roll-up out of the box.

The generator is deterministic given a seed and the same ``(org_id, days,
sessions_per_day)`` inputs — important for reproducible local QA and for the
multi-org seeder to fan out work across orgs without cross-talk.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Any

from dev_health_ops.api.product_telemetry.persist import BLOCKED_PAYLOAD_KEYS
from dev_health_ops.api.product_telemetry.schemas import ProductTelemetryEvent

SCHEMA_VERSION = "2026-05-telemetry-v1"
SOURCE = "dev-health-web"

# Route patterns the frontend normalizer (web/src/lib/telemetry/routePatterns.ts)
# is known to emit. Kept short and realistic — the dashboard SQL groups on
# route_pattern so high cardinality here hurts the visual.
ROUTE_PATTERNS: tuple[str, ...] = (
    "/dashboard",
    "/metrics",
    "/metrics/dora",
    "/investment",
    "/code",
    "/work",
    "/prs",
    "/issues",
    "/deployments",
    "/security",
    "/people/[person_id]/metrics/[metric]",
    "/reports/[id]",
    "/superadmin/product-telemetry",
)

FEATURES: tuple[tuple[str, str], ...] = (
    # (feature, surface)
    ("dashboard", "dashboard"),
    ("investment", "metrics"),
    ("dora", "metrics"),
    ("code", "code"),
    ("work", "work"),
    ("testops", "testops"),
    ("feature-flags", "feature-flags"),
    ("security", "security"),
    ("ai-workflow", "ai"),
)

FILTER_KEYS: tuple[str, ...] = (
    "scope",
    "date",
    "repo",
    "developer",
    "work",
    "flow",
    "artifact",
    "blocked",
    "issueType",
)

FILTER_VIEWS: tuple[str, ...] = (
    "dashboard",
    "metrics",
    "investment",
    "code",
    "work",
    "security",
)

CHART_KINDS: tuple[str, ...] = (
    "quadrant",
    "timeseries",
    "treemap",
    "flame",
    "sankey",
)

CHART_ACTIONS: tuple[str, ...] = (
    "point_selected",
    "overlay_toggled",
    "overlay_ignored",
    "drilldown",
    "zoom",
)

NAV_GROUPS: tuple[str, ...] = (
    "operate",
    "improve",
    "investigate",
    "admin",
)

NAV_ITEMS: tuple[str, ...] = (
    "dashboard",
    "metrics",
    "code",
    "work",
    "security",
    "ai",
    "settings",
)

GUIDES: tuple[str, ...] = (
    "quadrant-guide",
    "investment-guide",
    "dora-guide",
    "compounding-risk-guide",
)

ERROR_BOUNDARIES: tuple[str, ...] = ("route", "global")
ERROR_CLASSES: tuple[str, ...] = (
    "RenderError",
    "FetchError",
    "TypeError",
    "ChunkLoadError",
)


def product_telemetry_org_hash(org_id: str) -> str:
    """Hash an org id with the same recipe used by the GraphQL resolvers."""
    return sha256(org_id.encode()).hexdigest()


@dataclass(frozen=True)
class ProductTelemetrySeedSpec:
    """Inputs for a single-org seeding run.

    Half-open semantics: ``days`` events are generated across
    ``[end_time - days, end_time)`` so back-to-back invocations don't
    double-cover the boundary day.
    """

    org_id: str
    days: int
    sessions_per_day: int
    seed: int | None = None
    end_time: datetime | None = None


class ProductTelemetryGenerator:
    """Deterministic generator for ``ProductTelemetryEvent`` streams.

    The output goes straight into ``persist_product_telemetry_events`` and
    therefore must already obey the payload sanitizer — no blocked keys
    (email, name, userId, orgId, url, query, search, stack, message, title,
    body) can appear in any event.
    """

    def __init__(self, spec: ProductTelemetrySeedSpec) -> None:
        self.spec = spec
        self.org_id_hash = product_telemetry_org_hash(spec.org_id)
        # Mix the (seed, org_id) pair into a stable 64-bit RNG seed so
        # parallel per-org generation stays deterministic but does not
        # collide across orgs.
        seed_material = f"{spec.seed if spec.seed is not None else 0}|{spec.org_id}"
        rng_seed = int.from_bytes(
            sha256(seed_material.encode()).digest()[:8], "big", signed=False
        )
        self._rng = random.Random(rng_seed)

    # ------------------------------------------------------------------ helpers

    def _new_event_id(self) -> str:
        return f"evt_{self._rng.getrandbits(64):016x}"

    def _new_session_id(self) -> str:
        return f"ses_{self._rng.getrandbits(64):016x}"

    def _new_anon_user_id(self) -> str:
        # Re-use a small pool of anon users so uniqExact() in dashboard
        # queries returns realistic counts. Picking from N bins keeps cardinality
        # bounded but still > 1 so aggregates are non-degenerate.
        bin_count = max(5, self.spec.sessions_per_day // 2)
        return f"anon_{self._rng.randrange(bin_count):04d}"

    def _pick_route(self) -> str:
        return self._rng.choice(ROUTE_PATTERNS)

    def _payload_is_safe(self, payload: dict[str, Any]) -> bool:
        return BLOCKED_PAYLOAD_KEYS.isdisjoint(payload.keys())

    def _event(
        self,
        *,
        name: str,
        ts: datetime,
        session_id: str,
        anon_user_id: str,
        route_pattern: str | None,
        payload: dict[str, Any],
    ) -> ProductTelemetryEvent:
        assert self._payload_is_safe(payload), (
            f"payload contains blocked telemetry keys: "
            f"{sorted(BLOCKED_PAYLOAD_KEYS.intersection(payload))}"
        )
        return ProductTelemetryEvent.model_validate(
            {
                "name": name,
                "schemaVersion": SCHEMA_VERSION,
                "eventId": self._new_event_id(),
                "ts": ts.isoformat(),
                "sessionId": session_id,
                "anonymousUserId": anon_user_id,
                "orgIdHash": self.org_id_hash,
                "routePattern": route_pattern,
                "payload": payload,
            }
        )

    # ------------------------------------------------------------------ session

    def _generate_session(self, session_start: datetime) -> list[ProductTelemetryEvent]:
        """Emit a realistic event stream for a single anonymous session."""
        session_id = self._new_session_id()
        anon_user_id = self._new_anon_user_id()
        entry_route = self._pick_route()

        events: list[ProductTelemetryEvent] = []
        ts = session_start

        # session_started
        events.append(
            self._event(
                name="session_started",
                ts=ts,
                session_id=session_id,
                anon_user_id=anon_user_id,
                route_pattern=entry_route,
                payload={"entryRoutePattern": entry_route},
            )
        )

        # 3–8 page_viewed events
        current_route = entry_route
        prev_route: str | None = None
        pages_viewed = 0
        for _ in range(self._rng.randint(3, 8)):
            ts += timedelta(seconds=self._rng.randint(5, 90))
            prev_route, current_route = current_route, self._pick_route()
            pages_viewed += 1
            events.append(
                self._event(
                    name="page_viewed",
                    ts=ts,
                    session_id=session_id,
                    anon_user_id=anon_user_id,
                    route_pattern=current_route,
                    payload={
                        "routePattern": current_route,
                        "page": current_route.strip("/").split("/")[0] or "root",
                        "referrerRoutePattern": prev_route,
                    },
                )
            )

        interactions = 0

        # 1–3 feature_viewed
        for _ in range(self._rng.randint(1, 3)):
            ts += timedelta(seconds=self._rng.randint(2, 30))
            feature, surface = self._rng.choice(FEATURES)
            interactions += 1
            events.append(
                self._event(
                    name="feature_viewed",
                    ts=ts,
                    session_id=session_id,
                    anon_user_id=anon_user_id,
                    route_pattern=current_route,
                    payload={
                        "feature": feature,
                        "surface": surface,
                        "routePattern": current_route,
                    },
                )
            )

        # 0–3 filter_changed
        for _ in range(self._rng.randint(0, 3)):
            ts += timedelta(seconds=self._rng.randint(2, 30))
            interactions += 1
            events.append(
                self._event(
                    name="filter_changed",
                    ts=ts,
                    session_id=session_id,
                    anon_user_id=anon_user_id,
                    route_pattern=current_route,
                    payload={
                        "view": self._rng.choice(FILTER_VIEWS),
                        "filterKey": self._rng.choice(FILTER_KEYS),
                        "valueCount": self._rng.randint(1, 5),
                        "isCustomDateRange": self._rng.random() < 0.2,
                    },
                )
            )

        # 0–3 chart_interacted
        for _ in range(self._rng.randint(0, 3)):
            ts += timedelta(seconds=self._rng.randint(2, 30))
            interactions += 1
            events.append(
                self._event(
                    name="chart_interacted",
                    ts=ts,
                    session_id=session_id,
                    anon_user_id=anon_user_id,
                    route_pattern=current_route,
                    payload={
                        "chart": self._rng.choice(CHART_KINDS),
                        "action": self._rng.choice(CHART_ACTIONS),
                        "surface": self._rng.choice(NAV_GROUPS),
                        "scope": self._rng.choice(("org", "team", "repo", None)),
                    },
                )
            )

        # 0–2 navigation_interacted
        for _ in range(self._rng.randint(0, 2)):
            ts += timedelta(seconds=self._rng.randint(2, 20))
            interactions += 1
            events.append(
                self._event(
                    name="navigation_interacted",
                    ts=ts,
                    session_id=session_id,
                    anon_user_id=anon_user_id,
                    route_pattern=current_route,
                    payload={
                        "group": self._rng.choice(NAV_GROUPS),
                        "item": self._rng.choice(NAV_ITEMS),
                        "action": self._rng.choice(
                            ("group_expanded", "group_collapsed", "item_selected")
                        ),
                    },
                )
            )

        # 0–1 guide_opened
        if self._rng.random() < 0.4:
            ts += timedelta(seconds=self._rng.randint(2, 20))
            interactions += 1
            events.append(
                self._event(
                    name="guide_opened",
                    ts=ts,
                    session_id=session_id,
                    anon_user_id=anon_user_id,
                    route_pattern=current_route,
                    payload={
                        "guide": self._rng.choice(GUIDES),
                        "surface": self._rng.choice(NAV_GROUPS),
                    },
                )
            )

        # rare client_error (~5%)
        if self._rng.random() < 0.05:
            ts += timedelta(seconds=self._rng.randint(1, 10))
            events.append(
                self._event(
                    name="client_error",
                    ts=ts,
                    session_id=session_id,
                    anon_user_id=anon_user_id,
                    route_pattern=current_route,
                    payload={
                        "boundary": self._rng.choice(ERROR_BOUNDARIES),
                        "digest": f"d_{self._rng.getrandbits(32):08x}",
                        "errorClass": self._rng.choice(ERROR_CLASSES),
                        "routePattern": current_route,
                    },
                )
            )

        # session_ended
        ts += timedelta(seconds=self._rng.randint(5, 60))
        duration_ms = int((ts - session_start).total_seconds() * 1000)
        events.append(
            self._event(
                name="session_ended",
                ts=ts,
                session_id=session_id,
                anon_user_id=anon_user_id,
                route_pattern=current_route,
                payload={
                    "durationMs": duration_ms,
                    "pagesViewed": pages_viewed,
                    "interactions": interactions,
                },
            )
        )

        return events

    # ------------------------------------------------------------------ public

    def generate_events(self) -> list[ProductTelemetryEvent]:
        """Generate the full event stream for the configured spec."""
        end_time = self.spec.end_time or datetime.now(timezone.utc)
        # Half-open: start_day is inclusive, end_day is exclusive.
        start_day = (end_time - timedelta(days=self.spec.days)).replace(
            hour=9, minute=0, second=0, microsecond=0
        )

        events: list[ProductTelemetryEvent] = []
        for day_offset in range(self.spec.days):
            day_start = start_day + timedelta(days=day_offset)
            for session_index in range(self.spec.sessions_per_day):
                # Spread sessions across a workday-ish window.
                offset_minutes = self._rng.randint(0, 9 * 60)
                jitter_seconds = self._rng.randint(0, 59)
                session_start = day_start + timedelta(
                    minutes=offset_minutes, seconds=jitter_seconds
                )
                events.extend(self._generate_session(session_start))
        return events

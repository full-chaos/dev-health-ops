"""The startup drift check must tell apart four states that all look alike.

From outside the process, every one of these produces the same observable
behaviour -- no request is served by Go:

* nothing is enabled yet          (correct, expected, the default posture)
* everything enabled is dead      (an outage)
* the registry cannot be read     (a different outage)
* something is enabled and live   (healthy)

Between 2026-09-01 and 2026-09-07 the second state was in force and was
reported identically to the first: silence. The whole value of this check
is the distinction, so the distinction is what is tested.

The table read is monkeypatched rather than backed by a real Postgres:
what is under test is the DECISION, and driving it directly is what lets
all four branches be reached in CI. The real-database half of this
contract is proven on the Go plane, where the equivalent check runs
against a Postgres testcontainer
(``cmd/query-api/registry_route_integration_test.go``).
"""

from __future__ import annotations

import logging
from typing import Any

import pytest

from dev_health_ops.api.graphql import go_api_routing_drift
from dev_health_ops.api.graphql.go_api_schema_digest import current_schema_digest

LIVE = current_schema_digest()
SUPERSEDED = "sha256:67b87d38e46f767511b5d8435ffbfdd7dbe8aeab9dbe4073c7d7706de572f706"


class _RecordingCounter:
    """Stands in for the OTel/Prometheus counter, recording label values."""

    def __init__(self) -> None:
        self.results: list[str] = []

    def labels(self, **kwargs: str) -> _RecordingCounter:
        self.results.append(kwargs["result"])
        return self

    def inc(self) -> None:
        pass


@pytest.fixture
def counter(monkeypatch: pytest.MonkeyPatch) -> _RecordingCounter:
    recorder = _RecordingCounter()
    monkeypatch.setattr(
        go_api_routing_drift, "GO_API_ROUTING_DIGEST_DRIFT_TOTAL", recorder
    )
    return recorder


def _with_counts(monkeypatch: pytest.MonkeyPatch, counts: dict[str, int] | Exception):
    async def fake_count(_session: Any) -> dict[str, int]:
        if isinstance(counts, Exception):
            raise counts
        return counts

    monkeypatch.setattr(go_api_routing_drift, "count_rows_by_schema_digest", fake_count)


async def _run() -> str:
    return await go_api_routing_drift.check_routing_digest_drift(session=None)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_empty_table_is_reported_as_empty_not_stale(
    monkeypatch: pytest.MonkeyPatch,
    counter: _RecordingCounter,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _with_counts(monkeypatch, {})
    with caplog.at_level(logging.INFO):
        assert await _run() == "empty"
    assert counter.results == ["empty"]
    assert "go_api_routing.no_rows" in caplog.text
    assert "rows_stale" not in caplog.text
    # An empty table is the default posture. Logging it at ERROR would
    # train operators to ignore the level that DOES mean an outage.
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


@pytest.mark.asyncio
async def test_rows_at_the_live_digest_are_reported_live(
    monkeypatch: pytest.MonkeyPatch,
    counter: _RecordingCounter,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Superseded rows alongside live ones is the NORMAL shape after any
    # schema change: their presence must not make a healthy deployment
    # look broken.
    _with_counts(monkeypatch, {LIVE: 15, SUPERSEDED: 12})
    with caplog.at_level(logging.INFO):
        assert await _run() == "live"
    assert counter.results == ["live"]
    assert "go_api_routing.rows_live" in caplog.text
    assert "15" in caplog.text and "12" in caplog.text
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


@pytest.mark.asyncio
async def test_rows_only_at_a_superseded_digest_are_reported_stale(
    monkeypatch: pytest.MonkeyPatch,
    counter: _RecordingCounter,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The exact 2026-09-01 state: twelve rows, all dead, all invisible."""
    _with_counts(monkeypatch, {SUPERSEDED: 12})
    with caplog.at_level(logging.INFO):
        assert await _run() == "stale"

    assert counter.results == ["stale"]
    assert "go_api_routing.rows_stale" in caplog.text
    assert "12 rows" in caplog.text
    assert SUPERSEDED in caplog.text
    assert LIVE in caplog.text
    # ERROR, not INFO: this one IS an outage.
    assert [r for r in caplog.records if r.levelno >= logging.ERROR]
    # And it must name the way out, not merely the symptom.
    assert "dev-hops go-api routing enable" in caplog.text


@pytest.mark.asyncio
async def test_an_unreadable_registry_is_an_error_not_an_empty_table(
    monkeypatch: pytest.MonkeyPatch,
    counter: _RecordingCounter,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """ "The registry is down" must never render as "nothing is enabled".

    Same distinction ``go_api_registry_telemetry``'s ``error`` vs ``miss``
    labels already draw on the per-request path.
    """
    _with_counts(monkeypatch, RuntimeError("connection refused"))
    with caplog.at_level(logging.INFO):
        assert await _run() == "error"
    assert counter.results == ["error"]
    assert "drift_check_failed" in caplog.text
    assert "no_rows" not in caplog.text
    assert "rows_stale" not in caplog.text


@pytest.mark.asyncio
async def test_the_check_never_raises_into_startup(
    monkeypatch: pytest.MonkeyPatch, counter: _RecordingCounter
) -> None:
    """A diagnostic that can take the API down is worse than no diagnostic.

    The API serves every request from Python without this table, so no
    failure here may propagate -- ``_lifespan`` calls this during startup.
    """
    _with_counts(monkeypatch, RuntimeError("boom"))
    assert await _run() == "error"


@pytest.mark.asyncio
async def test_lifespan_actually_calls_the_drift_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The wiring, not just the function.

    Every test above drives ``check_routing_digest_drift`` directly. That
    proves the decision logic and proves NOTHING about whether anything
    calls it -- and a drift check nobody runs is exactly as silent as the
    six days it exists to prevent. This asserts the API's real startup
    path reaches it.
    """
    import contextlib

    from dev_health_ops.api import _lifespan as lifespan_module

    calls: list[object] = []

    async def spy(session: Any) -> str:
        calls.append(session)
        return "empty"

    class _Session:
        async def commit(self) -> None:
            pass

    @contextlib.asynccontextmanager
    async def fake_session():
        yield _Session()

    monkeypatch.setattr(
        "dev_health_ops.api.graphql.go_api_routing_drift.check_routing_digest_drift",
        spy,
    )
    monkeypatch.setattr("dev_health_ops.db.get_postgres_session", fake_session)
    monkeypatch.setattr(lifespan_module, "verify_rate_limit_config", lambda: None)
    monkeypatch.setattr(
        lifespan_module.LicenseManager, "initialize", staticmethod(lambda: None)
    )
    # A configured Postgres URL is what gates the whole block the drift
    # check lives in.
    monkeypatch.setattr(
        lifespan_module, "_postgres_url", lambda: "postgresql+asyncpg://x/y"
    )
    monkeypatch.setattr(
        "dev_health_ops.api.billing.bundle_validation.validate_bundle_keys",
        lambda _session: None,
    )

    async def fake_schema_status(_uri: str) -> tuple[bool, list[str]]:
        return True, ["0126"]

    monkeypatch.setattr(
        "dev_health_ops.migrate.application_schema_status", fake_schema_status
    )
    monkeypatch.setattr(
        "dev_health_ops.migrate.required_application_schema_revision", lambda: "0126"
    )

    async with lifespan_module.lifespan(None):  # type: ignore[arg-type]
        pass

    assert calls, (
        "api startup never called check_routing_digest_drift -- the drift "
        "signal exists but nothing runs it"
    )

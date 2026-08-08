"""CHAOS-3523 (B): dev/local escape hatch for the platform allowance cap.

``_bounded_operator_limit`` has always had two knobs: the ordinary operator
env var (``ASK_DEV_PLATFORM_MONTHLY_REQUEST_MAX`` / ``..._COST_MAX_MICROUSD``,
the desired VALUE) and the compiled ``hard_maximum`` it gets clamped to (the
CEILING). CHAOS-3523(B) adds a third, dev-only knob that can raise the
ceiling itself past the compiled hard maximum -- it does not change what the
ordinary env var means. Every test that wants to observe a raised (or
refused) EFFECTIVE limit therefore sets BOTH the new dev-only ceiling env var
AND the ordinary value env var to the same number: with the ceiling closed,
the ordinary value gets clamped straight back down to the compiled hard max,
independent of whatever the ceiling var says.

Production must never be able to raise the compiled-in hard maximum. The
escape hatch is gated on ``is_development_environment()`` --
``dev_health_ops.api.graphql.security``'s existing production/dev signal,
which already stakes real security posture on the same ENVIRONMENT value
(GraphQL introspection, the GraphiQL IDE) and defaults CLOSED
("production") when ENVIRONMENT/APP_ENV/ENV are all unset.

``tests/conftest.py``'s autouse ``setup_test_env`` fixture sets
``ENVIRONMENT=test`` for every test in this suite. "test" is deliberately
NOT in ``is_development_environment()``'s allowed set
({"development", "dev", "local"}), so the default posture in this whole
suite is already the CLOSED one -- tests below that want the dev-open path
must opt in explicitly with ``monkeypatch.setenv("ENVIRONMENT", ...)``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.dev.org_policy import (
    ASK_DEV_PLATFORM_MONTHLY_REQUEST_LIMIT_KEY,
    PLATFORM_MONTHLY_COST_LIMIT_HARD_MAX_MICROUSD,
    PLATFORM_MONTHLY_REQUEST_LIMIT_HARD_MAX,
    load_ask_dev_org_policy,
    platform_operator_cost_limit_microusd,
    platform_operator_request_limit,
)
from dev_health_ops.api.services.configuration.generic import SettingsService
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import Setting, SettingCategory
from tests._helpers import tables_of

_TABLES = tables_of(Setting)
_ASK_DEV_CATEGORY = SettingCategory.ASK_DEV.value

_REQUEST_DEV_ENV = "ASK_DEV_PLATFORM_MONTHLY_REQUEST_DEV_MAX"
_COST_DEV_ENV = "ASK_DEV_PLATFORM_MONTHLY_COST_DEV_MAX_MICROUSD"
_REQUEST_ENV = "ASK_DEV_PLATFORM_MONTHLY_REQUEST_MAX"
_COST_ENV = "ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD"

# An order of magnitude past the compiled hard maxima (5,000 / $500).
_RAISED_REQUEST = PLATFORM_MONTHLY_REQUEST_LIMIT_HARD_MAX * 10
_RAISED_COST_MICROUSD = PLATFORM_MONTHLY_COST_LIMIT_HARD_MAX_MICROUSD * 10


# ---------------------------------------------------------------------------
# Dev path: the override raises the ceiling past the compiled hard maximum.
# ---------------------------------------------------------------------------


def test_dev_override_raises_the_request_ceiling_past_hard_max(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.setenv(_REQUEST_DEV_ENV, str(_RAISED_REQUEST))
    monkeypatch.setenv(_REQUEST_ENV, str(_RAISED_REQUEST))

    assert platform_operator_request_limit() == _RAISED_REQUEST


@pytest.mark.parametrize("dev_environment", ["development", "dev", "local"])
def test_dev_override_raises_the_cost_ceiling_past_hard_max(
    monkeypatch: pytest.MonkeyPatch, dev_environment: str
) -> None:
    monkeypatch.setenv("ENVIRONMENT", dev_environment)
    monkeypatch.setenv(_COST_DEV_ENV, str(_RAISED_COST_MICROUSD))
    monkeypatch.setenv(_COST_ENV, str(_RAISED_COST_MICROUSD))

    assert platform_operator_cost_limit_microusd() == _RAISED_COST_MICROUSD


def test_dev_override_ceiling_still_clamps_a_value_above_the_raised_ceiling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raising the ceiling doesn't remove it: a value above even the raised
    ceiling still clamps to that ceiling, not the compiled hard max."""

    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.setenv(_REQUEST_DEV_ENV, str(_RAISED_REQUEST))
    monkeypatch.setenv(_REQUEST_ENV, str(_RAISED_REQUEST * 100))

    assert platform_operator_request_limit() == _RAISED_REQUEST


def test_malformed_dev_override_falls_back_to_the_compiled_hard_max(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail closed on bad input, same posture as the pre-existing operator
    env parsing (``_bounded_operator_limit``'s ``ValueError`` fallback)."""

    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.setenv(_REQUEST_DEV_ENV, "not-a-number")
    monkeypatch.setenv(_REQUEST_ENV, str(_RAISED_REQUEST))

    assert platform_operator_request_limit() == PLATFORM_MONTHLY_REQUEST_LIMIT_HARD_MAX


def test_non_positive_dev_override_falls_back_to_the_compiled_hard_max(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.setenv(_COST_DEV_ENV, "0")
    monkeypatch.setenv(_COST_ENV, str(_RAISED_COST_MICROUSD))

    assert (
        platform_operator_cost_limit_microusd()
        == PLATFORM_MONTHLY_COST_LIMIT_HARD_MAX_MICROUSD
    )


# ---------------------------------------------------------------------------
# Production path -- THE MANDATORY OBSERVED-REFUSAL TEST.
#
# Same env values as the dev-raise tests above, differing ONLY in
# ENVIRONMENT. This must be watched FAILING when
# ``is_development_environment()`` is removed from
# ``_bounded_operator_limit``'s guard (a run that always consults the
# dev-override env var regardless of environment). A test that only proves
# the dev path works proves nothing about production being closed -- see the
# mutation note in the PR description.
# ---------------------------------------------------------------------------


def test_dev_override_never_applies_when_environment_is_explicitly_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv(_REQUEST_DEV_ENV, str(_RAISED_REQUEST))
    monkeypatch.setenv(_REQUEST_ENV, str(_RAISED_REQUEST))
    monkeypatch.setenv(_COST_DEV_ENV, str(_RAISED_COST_MICROUSD))
    monkeypatch.setenv(_COST_ENV, str(_RAISED_COST_MICROUSD))

    assert platform_operator_request_limit() == PLATFORM_MONTHLY_REQUEST_LIMIT_HARD_MAX
    assert (
        platform_operator_cost_limit_microusd()
        == PLATFORM_MONTHLY_COST_LIMIT_HARD_MAX_MICROUSD
    )


def test_dev_override_never_applies_when_environment_is_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``environment_name()`` defaults to "production" with ENVIRONMENT,
    APP_ENV, and ENV all absent -- the default posture, not just an
    explicit opt-in, must refuse the override."""

    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.delenv("APP_ENV", raising=False)
    monkeypatch.delenv("ENV", raising=False)
    monkeypatch.setenv(_REQUEST_DEV_ENV, str(_RAISED_REQUEST))
    monkeypatch.setenv(_REQUEST_ENV, str(_RAISED_REQUEST))
    monkeypatch.setenv(_COST_DEV_ENV, str(_RAISED_COST_MICROUSD))
    monkeypatch.setenv(_COST_ENV, str(_RAISED_COST_MICROUSD))

    assert platform_operator_request_limit() == PLATFORM_MONTHLY_REQUEST_LIMIT_HARD_MAX
    assert (
        platform_operator_cost_limit_microusd()
        == PLATFORM_MONTHLY_COST_LIMIT_HARD_MAX_MICROUSD
    )


def test_dev_override_never_applies_under_the_suite_default_test_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Belt-and-suspenders: the autouse fixture's ENVIRONMENT=test posture
    (not "production", but also not in the dev allow-set) must refuse the
    override too -- "not explicitly dev" closes, "explicitly prod" is not
    the only closed state."""

    monkeypatch.setenv(_REQUEST_DEV_ENV, str(_RAISED_REQUEST))
    monkeypatch.setenv(_REQUEST_ENV, str(_RAISED_REQUEST))

    assert platform_operator_request_limit() == PLATFORM_MONTHLY_REQUEST_LIMIT_HARD_MAX


# ---------------------------------------------------------------------------
# Composition through the real call chain: load_ask_dev_org_policy()'s
# per-org stored-setting clamp (``_stored_limit``) must see the SAME
# raised-or-refused ceiling as the operator functions above, not just the
# operator functions in isolation.
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def session_maker(
    tmp_path: Path,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'chaos_3523_org_policy.db'}"
    )
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: Base.metadata.create_all(
                sync_connection, tables=_TABLES
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


async def _store_and_load_policy(
    session_maker: async_sessionmaker[AsyncSession], *, org_id: str, stored_value: str
):
    async with session_maker() as session:
        settings = SettingsService(session, org_id)
        await settings.set(
            ASK_DEV_PLATFORM_MONTHLY_REQUEST_LIMIT_KEY,
            stored_value,
            category=_ASK_DEV_CATEGORY,
            description="test",
        )
        await session.commit()

        return await load_ask_dev_org_policy(settings)


@pytest.mark.asyncio
async def test_stored_org_limit_reaches_the_raised_dev_ceiling(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.setenv(_REQUEST_DEV_ENV, str(_RAISED_REQUEST))
    monkeypatch.setenv(_REQUEST_ENV, str(_RAISED_REQUEST))

    # 20,000 exceeds the compiled hard max (5,000) but sits under the raised
    # dev ceiling (50,000): the stored value must survive uncapped.
    policy = await _store_and_load_policy(
        session_maker, org_id="org-chaos-3523-dev", stored_value="20000"
    )

    assert policy.platform_monthly_request_limit == 20_000


@pytest.mark.asyncio
async def test_stored_org_limit_stays_clamped_to_hard_max_in_production(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same stored value, same LEAKED dev-override + raised operator env, but
    a production ENVIRONMENT -- the org's own policy load must clamp to the
    unchanged compiled hard max, exactly as if the override did not exist."""

    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv(_REQUEST_DEV_ENV, str(_RAISED_REQUEST))
    monkeypatch.setenv(_REQUEST_ENV, str(_RAISED_REQUEST))

    policy = await _store_and_load_policy(
        session_maker, org_id="org-chaos-3523-prod", stored_value="20000"
    )

    assert (
        policy.platform_monthly_request_limit == PLATFORM_MONTHLY_REQUEST_LIMIT_HARD_MAX
    )

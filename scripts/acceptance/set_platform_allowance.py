#!/usr/bin/env python3
"""CHAOS-3575: lift the platform cost allowance out of a measurement run's way.

HARNESS-INTERFERENCE FIX, not a product limit decision. Run by
``armed_corpus_boot.sh`` as PRECONDITION C, INSIDE the api container, so it
resolves the limit with exactly the code the API itself runs.

THE RUN THIS EXISTS BECAUSE OF (2026-08-07, 10:03 PT): 59 of 90 active corpus
cases returned ``HTTP 429 cost_limit_reached`` before recording anything,
degrading the run to UNMEASURED. Root cause was not a leak or a stuck counter
-- the Valkey counter matched the ``dev_runs`` ground truth exactly. It was that
``ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD`` is the operator MAXIMUM, the
ceiling an org may be CONFIGURED UP TO, while the EFFECTIVE limit is the stored
per-org row -- which falls back to ``PLATFORM_MONTHLY_COST_LIMIT_DEFAULT
_MICROUSD`` ($100) when no row exists. The runner budgeted against $200; the
server enforced $100.

So this writes the stored row, and the compose override beside it raises the
operator max that clamps the row. NEITHER ALONE IS SUFFICIENT.

Writes through production's own ``SettingsService.set`` -- the exact transport
``load_ask_dev_org_policy`` reads -- rather than an INSERT that could drift from
it, then VERIFIES through ``load_ask_dev_org_policy`` itself. Reading back the
row just written would prove only that the write landed; it says nothing about
what the API RESOLVES, which is the question that matters and the one the
earlier run got wrong.

$500 is not a preference, it is the maximum the product accepts:
``PLATFORM_MONTHLY_COST_LIMIT_HARD_MAX_MICROUSD == 500_000_000`` clamps both the
operator max and the stored row, so a larger request is silently truncated. At
the observed ~$3.38M/case it is ~1.64x the ~$304M a 90-case corpus needs.
"""

from __future__ import annotations

import asyncio
import sys

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from dev_health_ops.api.dev.org_policy import (
    ASK_DEV_PLATFORM_MONTHLY_COST_LIMIT_KEY,
    PLATFORM_MONTHLY_COST_LIMIT_HARD_MAX_MICROUSD,
    load_ask_dev_org_policy,
)
from dev_health_ops.api.services.configuration.generic import SettingsService
from dev_health_ops.db import get_postgres_engine
from dev_health_ops.models.settings import SettingCategory

TARGET = PLATFORM_MONTHLY_COST_LIMIT_HARD_MAX_MICROUSD  # 500_000_000


async def main() -> int:
    engine = get_postgres_engine()
    factory = async_sessionmaker(engine, expire_on_commit=False)
    failures: list[str] = []
    async with factory() as session:
        from dev_health_ops.models.users import Organization  # noqa: PLC0415

        org_ids = [
            str(o) for o in (await session.scalars(select(Organization.id))).all()
        ]
        print(f"orgs found: {len(org_ids)}")
        for org_id in org_ids:
            settings = SettingsService(session, org_id)
            await settings.set(
                ASK_DEV_PLATFORM_MONTHLY_COST_LIMIT_KEY,
                str(TARGET),
                SettingCategory.ASK_DEV.value,
                description=(
                    "CHAOS-3219 Phase 3 harness-interference fix: lift the "
                    "acceptance cost allowance out of the way of measurement"
                ),
            )
        await session.commit()

        # VERIFY through the real resolve path, per org.
        for org_id in org_ids:
            policy = await load_ask_dev_org_policy(SettingsService(session, org_id))
            got = policy.platform_monthly_cost_limit_microusd
            ok = got == TARGET
            print(
                f"  org {org_id}: effective_cost_limit_microusd={got:,} "
                f"({'OK' if ok else 'MISMATCH'})"
            )
            if not ok:
                failures.append(f"{org_id}: resolved {got:,}, wanted {TARGET:,}")

    if failures:
        print("\nALLOWANCE FIX FAILED -- effective limit did not resolve as intended:")
        for f in failures:
            print("  - " + f)
        print(
            "\nMost likely cause: the api container's "
            "ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD operator max is still "
            "below the target, which clamps the stored row."
        )
        return 1
    print(
        f"\nALLOWANCE OK: every org resolves {TARGET:,} microUSD (${TARGET / 1e6:.0f})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

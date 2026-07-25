from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

if TYPE_CHECKING:
    from dev_health_ops.models.licensing import (
        FeatureFlag,
        OrgFeatureOverride,
        OrgLicense,
    )


@dataclass(frozen=True, slots=True)
class FeatureRows:
    features: tuple[FeatureFlag, ...]
    overrides: tuple[OrgFeatureOverride, ...]
    org_license: OrgLicense | None
    org_tier: str | None


def load_feature_rows_sync(
    session: Session,
    org_id: uuid.UUID,
    feature_keys: Sequence[str],
) -> FeatureRows:
    from dev_health_ops.models.licensing import (
        FeatureFlag,
        OrgFeatureOverride,
        OrgLicense,
    )
    from dev_health_ops.models.users import Organization

    features = tuple(
        session.scalars(
            select(FeatureFlag).where(FeatureFlag.key.in_(feature_keys))
        ).all()
    )
    feature_ids = tuple(feature.id for feature in features)
    overrides = (
        tuple(
            session.scalars(
                select(OrgFeatureOverride).where(
                    OrgFeatureOverride.org_id == org_id,
                    OrgFeatureOverride.feature_id.in_(feature_ids),
                )
            ).all()
        )
        if feature_ids
        else ()
    )
    return FeatureRows(
        features=features,
        overrides=overrides,
        org_license=session.scalar(
            select(OrgLicense).where(OrgLicense.org_id == org_id)
        ),
        org_tier=session.scalar(
            select(Organization.tier).where(Organization.id == org_id)
        ),
    )


def lock_feature_rows_sync(
    session: Session,
    org_id: uuid.UUID,
    feature_keys: Sequence[str],
) -> None:
    from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride

    features = tuple(
        session.scalars(
            select(FeatureFlag)
            .where(FeatureFlag.key.in_(feature_keys))
            .with_for_update()
        ).all()
    )
    feature_ids = tuple(feature.id for feature in features)
    if feature_ids:
        session.scalars(
            select(OrgFeatureOverride)
            .where(
                OrgFeatureOverride.org_id == org_id,
                OrgFeatureOverride.feature_id.in_(feature_ids),
            )
            .with_for_update()
        ).all()


async def load_feature_rows_async(
    session: AsyncSession,
    org_id: uuid.UUID,
    feature_keys: Sequence[str],
) -> FeatureRows:
    from dev_health_ops.models.licensing import (
        FeatureFlag,
        OrgFeatureOverride,
        OrgLicense,
    )
    from dev_health_ops.models.users import Organization

    features = tuple(
        (
            await session.scalars(
                select(FeatureFlag).where(FeatureFlag.key.in_(feature_keys))
            )
        ).all()
    )
    feature_ids = tuple(feature.id for feature in features)
    overrides = (
        tuple(
            (
                await session.scalars(
                    select(OrgFeatureOverride).where(
                        OrgFeatureOverride.org_id == org_id,
                        OrgFeatureOverride.feature_id.in_(feature_ids),
                    )
                )
            ).all()
        )
        if feature_ids
        else ()
    )
    return FeatureRows(
        features=features,
        overrides=overrides,
        org_license=await session.scalar(
            select(OrgLicense).where(OrgLicense.org_id == org_id)
        ),
        org_tier=await session.scalar(
            select(Organization.tier).where(Organization.id == org_id)
        ),
    )

"""Differential oracle: ``resolve_watermark`` vs ``get_watermark`` (CHAOS-3430).

``sync.watermark_lag.resolve_watermark`` is a second implementation of the
three-tier watermark precedence already implemented by
``sync.watermarks.get_watermark``.  The status surface needs the in-memory
form (many pairs at once, over an async session); the planner keeps the
query form.  Two implementations of one rule can only be trusted if something
runs both over the same inputs and compares — no type checker or code index
can answer whether they agree.

This module is that comparator.  It seeds real ``SyncWatermark`` rows in a
real session, then asserts the two paths return the same timestamp for every
(source, dataset) probe.  ``ACCEPTANCE_CASES`` is a set of precedence
behaviours the oracle must keep rediscovering: if a change makes one of them
stop being exercised, the comparator has been broken, not fixed.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import SyncWatermark
from dev_health_ops.sync.watermark_lag import resolve_watermark
from dev_health_ops.sync.watermarks import get_watermark

ORG_ID = "org-parity"
SOURCE = "owner/repo"
OTHER_SOURCE = "other/repo"

BASE = datetime(2026, 8, 5, 12, 0, 0, tzinfo=timezone.utc)


def _ts(days_back: float) -> datetime:
    return BASE - timedelta(days=days_back)


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def _row(*, source_id, repo_id, target, dataset_key, last_synced_at) -> SyncWatermark:
    return SyncWatermark(
        org_id=ORG_ID,
        source_id=source_id,
        repo_id=repo_id,
        target=target,
        dataset_key=dataset_key,
        last_synced_at=last_synced_at,
    )


#: (label, rows-to-seed, dataset probed) — each names a distinct precedence
#: tier or miss the two implementations must agree on.
ACCEPTANCE_CASES: list[tuple[str, list[dict], str]] = [
    (
        "canonical row only",
        [
            dict(
                source_id=SOURCE,
                repo_id=SOURCE,
                target="commits",
                dataset_key="commits",
                last_synced_at=_ts(1),
            )
        ],
        "commits",
    ),
    (
        "canonical wins over raw legacy sibling row",
        [
            dict(
                source_id=SOURCE,
                repo_id=SOURCE,
                target="commits",
                dataset_key="commits",
                last_synced_at=_ts(1),
            ),
            dict(
                source_id=SOURCE,
                repo_id=SOURCE,
                target="git",
                dataset_key="git",
                last_synced_at=_ts(60),
            ),
        ],
        "commits",
    ),
    (
        "reverse-legacy fallback warms a sibling from the raw git row",
        [
            dict(
                source_id=SOURCE,
                repo_id=SOURCE,
                target="git",
                dataset_key="git",
                last_synced_at=_ts(60),
            )
        ],
        "commit-stats",
    ),
    (
        "legacy target column lookup (target == dataset_key)",
        [
            dict(
                source_id="",
                repo_id=SOURCE,
                target="prs",
                dataset_key="prs",
                last_synced_at=_ts(3),
            )
        ],
        "prs",
    ),
    (
        "no row for this source",
        [
            dict(
                source_id=OTHER_SOURCE,
                repo_id=OTHER_SOURCE,
                target="commits",
                dataset_key="commits",
                last_synced_at=_ts(1),
            )
        ],
        "commits",
    ),
    (
        "no rows at all (cold start)",
        [],
        "commits",
    ),
    (
        "repo-metadata never resolves via the reverse-legacy fallback",
        [
            dict(
                source_id=SOURCE,
                repo_id=SOURCE,
                target="git",
                dataset_key="git",
                last_synced_at=_ts(60),
            )
        ],
        "repo-metadata",
    ),
    (
        "null watermark on an existing row",
        [
            dict(
                source_id=SOURCE,
                repo_id=SOURCE,
                target="commits",
                dataset_key="commits",
                last_synced_at=None,
            )
        ],
        "commits",
    ),
]


@pytest.mark.parametrize(
    "label,rows,dataset_key",
    ACCEPTANCE_CASES,
    ids=[case[0] for case in ACCEPTANCE_CASES],
)
def test_resolve_watermark_agrees_with_get_watermark(
    db_session, label: str, rows: list[dict], dataset_key: str
):
    for spec in rows:
        db_session.add(_row(**spec))
    db_session.flush()

    # Oracle: the production query path the planner actually uses.
    expected = get_watermark(db_session, ORG_ID, SOURCE, dataset_key)

    # Under test: the in-memory path the status surface uses, fed the same
    # rows the batch loader would have selected.
    loaded = list(
        db_session.execute(
            select(SyncWatermark).where(SyncWatermark.org_id == ORG_ID)
        ).scalars()
    )
    actual = resolve_watermark(loaded, SOURCE, dataset_key)

    assert actual == expected, f"{label}: {actual!r} != oracle {expected!r}"


def test_acceptance_set_covers_every_precedence_tier():
    """A comparator that stopped exercising a tier would pass while blind.

    Assert the acceptance set still contains a case for each tier, so
    trimming it is a test failure rather than a silent loss of coverage.
    """
    labels = {label for label, _, _ in ACCEPTANCE_CASES}
    for required in (
        "canonical row only",
        "canonical wins over raw legacy sibling row",
        "reverse-legacy fallback warms a sibling from the raw git row",
        "legacy target column lookup (target == dataset_key)",
        "no rows at all (cold start)",
    ):
        assert required in labels


def test_oracle_detects_a_broken_reimplementation(db_session):
    """The comparator must actually fail when the two paths disagree.

    A differential test that cannot fail reads as coverage.  Feed the
    in-memory path a deliberately truncated row set (canonical tier only)
    and confirm it then disagrees with the oracle on the reverse-legacy case.
    """
    db_session.add(
        _row(
            source_id=SOURCE,
            repo_id=SOURCE,
            target="git",
            dataset_key="git",
            last_synced_at=_ts(60),
        )
    )
    db_session.flush()

    expected = get_watermark(db_session, ORG_ID, SOURCE, "commit-stats")
    # SQLite round-trips drop tzinfo; compare on the wall value.
    assert expected is not None
    assert expected.replace(tzinfo=timezone.utc) == _ts(60)

    # Rows filtered as a naive canonical-only implementation would: the
    # reverse-legacy bridge row is dropped, so resolution must now miss.
    canonical_only = [
        row
        for row in db_session.execute(
            select(SyncWatermark).where(SyncWatermark.org_id == ORG_ID)
        ).scalars()
        if row.dataset_key == "commit-stats"
    ]
    assert resolve_watermark(canonical_only, SOURCE, "commit-stats") != expected

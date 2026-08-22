"""Repair ``integration_datasets`` rows that drifted out of agreement with the
config's ``sync_targets`` (CHAOS-4106).

Revision ID: 0108
Revises: 0107

DATA ONLY. This adds, drops and alters nothing. It flips ``is_enabled`` from
true to false on rows the operator's own checkbox selection cannot account for.

WHAT WAS WRONG
--------------
``IntegrationDataset`` rows are the plane the planner reads: ``sync/planner.py
::_load_enabled_datasets`` filters ``is_enabled IS TRUE`` and never consults
``sync_targets`` at all. But the only user-facing control over those rows is
the sync config form's target checkboxes -- ``PATCH /integrations/{id}/
datasets`` has no web caller anywhere in the frontend. So the two planes have
to be kept in agreement by code, and ``_reconcile_dataset_rows_for_sync_targets``
(CHAOS-3398) did that with a DIFF: its disable set was
``previous_sync_targets - new sync_targets``. A row that drifted enabled before
that reconciliation existed -- or by any path that wrote dataset rows without
writing ``sync_targets`` -- is named by no later edit's delta, so it survives
every subsequent save, forever.

On production this was live and load-bearing, not theoretical. The github
integration carried all five work-item dataset rows enabled while
``sync_targets`` had no ``work-items`` entry, so:

  - the planner kept minting work-item units (9361 successful units, the most
    recent window ending 2026-08-22 15:00) for a dataset the operator had
    deselected, spending GitHub API budget on it;
  - sync coverage, which reads the same enabled rows on the planner-managed
    path and was therefore telling the truth, advertised the resulting gaps as
    actionable "Available backfills" in the UI.

The companion change makes the reconciliation self-healing (disable set =
controlled universe - desired) so this cannot recur. This migration repairs the
rows that already drifted, since no future edit would otherwise reach them.

WHAT THIS MIGRATION OWNS, EXACTLY
---------------------------------
For each integration, the union of ``planner_dataset_keys(provider,
sync_targets)`` over its WHOLE-INTEGRATION configs (``source_id IS NULL``) is
"desired". Anything in ``operator_controlled_dataset_keys(provider)`` and not
in "desired" is disabled, if it is currently enabled.

Three deliberate narrowings:

``source_id IS NULL`` only. ``IntegrationDataset`` rows are SHARED by every
config pointing at the same integration (CHAOS-2762: a planner parent plus its
per-repo children). A source-scoped child's ``sync_targets`` is an intentional
subset of the integration's, so reconciling from a child would let one repo's
narrower selection disable a dataset for every sibling. This mirrors the guard
the edit path already applies at ``routers/sync.py``.

Desired is a UNION across sibling whole-integration configs, so a dataset one
config still selects is never disabled on behalf of another. Siblings are not
filtered by ``is_active``: a paused config's selection still pins its shared
rows on, which fails toward NOT disabling. Shared rows cannot express divergent
intent across sibling configs at all -- see CHAOS-4115.

The universe comes from ``operator_controlled_dataset_keys``, which runs the
same ``planner_dataset_keys`` used at create time over every operator-
selectable target. Two consequences that are the point rather than an
accident: ``blame`` IS in the universe on github/gitlab, because
``planner_dataset_keys`` expands "git" to imply it -- so blame follows the
"git" checkbox symmetrically, which is what the reconciliation docstring has
always promised. ``security`` is in NO universe on any provider: it is
reachable from no checkbox, being platform-managed by
``_ensure_security_dataset_for_scheduled_code_host_sync``, so this migration
cannot touch it. Any dataset key with no enabled-capable checkbox behind it is
likewise out of scope and left exactly as found.

If ``planner_dataset_keys`` raises for any of an integration's configs (it
rejects a PagerDuty config whose targets are not exactly ``{"operational"}``),
that whole integration is skipped. A config we cannot map is a config whose
intent we cannot read, and guessing is worse than leaving the rows alone.

IDEMPOTENT
----------
The predicate is ``is_enabled IS TRUE``, so a second run matches nothing and
updates zero rows. Re-running is a no-op by construction, not by luck.

WHY downgrade() IS A DOCUMENTED NO-OP
-------------------------------------
A deliberate exception to the house rule that downgrades are reversible, in the
same shape as 0107's.

No predicate can distinguish a row this migration disabled from one an operator
disabled: both are ``is_enabled = false``, and the table records no provenance
for the transition. Re-enabling everything on downgrade would therefore
resurrect syncing for datasets operators deliberately switched off -- exactly
the failure this migration exists to end -- and would do it silently, on
integrations that were never drifted in the first place.

To reverse this deliberately, an operator re-checks the target in the sync
config form, which is the supported path: the reconciliation re-enables the
row, and the action is attributable to a person.
"""

from __future__ import annotations

import json
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from dev_health_ops.sync.datasets import (
    operator_controlled_dataset_keys,
    planner_dataset_keys,
)

revision: str = "0108"
down_revision: str | None = "0107"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]


class _Unreadable:
    """Sentinel: a ``sync_targets`` payload whose intent cannot be determined."""


UNREADABLE = _Unreadable()


def _sync_targets(raw: object) -> list[str] | _Unreadable:
    """Normalise a ``sync_targets`` JSON column to a list of strings.

    The column is ``JSON``, which arrives already decoded on some drivers and
    as text on others; both shapes are handled rather than assumed.

    Anything that is NOT a decodable list returns :data:`UNREADABLE` rather
    than an empty list (Codex adversarial review, round 2). The difference is
    the whole safety property of this migration: an empty list is a valid,
    readable statement that the operator selected nothing, which makes every
    operator-controlled dataset undesired and therefore disables all of them.
    Silently mapping corrupt JSON, a JSON object, or a non-list scalar onto
    that same value would turn one unreadable row into a mass-disable for the
    entire integration. A data repair must fail closed on intent it cannot
    read.

    ``None`` is NOT unreadable: the column is NOT NULL in the live schema, and
    a missing value is treated as the empty selection it represents.
    """
    if raw is None:
        return []
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError:
            return UNREADABLE
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError:
            return UNREADABLE
    if not isinstance(raw, (list, tuple)):
        return UNREADABLE
    targets: list[str] = []
    for item in raw:
        if item is None:
            continue
        if not isinstance(item, str):
            return UNREADABLE
        targets.append(item)
    return targets


# Lightweight table handles rather than raw SQL. Building these statements with
# SQLAlchemy core keeps the migration dialect-portable without hand-rolled
# per-dialect string branching, and keeps `sqlalchemy.text` out of the file
# entirely -- a `text()` carrying an interpolated fragment trips
# `avoid-sqlalchemy-text` (CWE-89) in CI even when the interpolated value is a
# hardcoded literal, and suppressing that would be the wrong trade.
#
# Columns are declared untyped on purpose. `integration_id` is UUID on
# PostgreSQL and TEXT under the SQLite test fixtures; binding back the exact
# value each row yielded lets the driver round-trip its own representation
# instead of forcing a cast that is only correct on one of them.
_SYNC_CONFIGURATIONS = sa.table(
    "sync_configurations",
    sa.column("org_id"),
    sa.column("integration_id"),
    sa.column("provider"),
    sa.column("sync_targets"),
    sa.column("source_id"),
)

_INTEGRATION_DATASETS = sa.table(
    "integration_datasets",
    sa.column("org_id"),
    sa.column("integration_id"),
    sa.column("dataset_key"),
    sa.column("is_enabled"),
)


def upgrade() -> None:
    bind = op.get_bind()
    configs = bind.execute(
        sa.select(
            _SYNC_CONFIGURATIONS.c.org_id,
            _SYNC_CONFIGURATIONS.c.integration_id,
            _SYNC_CONFIGURATIONS.c.provider,
            _SYNC_CONFIGURATIONS.c.sync_targets,
        ).where(
            _SYNC_CONFIGURATIONS.c.integration_id.is_not(None),
            _SYNC_CONFIGURATIONS.c.source_id.is_(None),
        )
    ).fetchall()

    # (org_id, integration_id) -> desired dataset keys, unioned across every
    # whole-integration config sharing those rows.
    desired_by_integration: dict[tuple[str, str], set[str]] = {}
    provider_by_integration: dict[tuple[str, str], str] = {}
    # The driver's own representation of integration_id, bound back verbatim.
    raw_integration_id: dict[tuple[str, str], object] = {}
    unmappable: set[tuple[str, str]] = set()

    for org_id, integration_id, provider, raw_targets in configs:
        key = (str(org_id), str(integration_id))
        provider_by_integration[key] = str(provider)
        raw_integration_id[key] = integration_id
        desired_by_integration.setdefault(key, set())
        targets = _sync_targets(raw_targets)
        if isinstance(targets, _Unreadable):
            unmappable.add(key)
            continue
        try:
            keys = set(planner_dataset_keys(str(provider), targets))
        except ValueError:
            unmappable.add(key)
            continue
        desired_by_integration[key].update(keys)

    for key, desired in sorted(desired_by_integration.items()):
        if key in unmappable:
            # One unreadable whole-integration config poisons the whole
            # integration: its rows are shared, so a repair computed from the
            # readable siblings alone would disable datasets the unreadable one
            # may well select. Say so loudly -- a silent skip here looks
            # identical to "nothing needed repairing".
            print(
                "0108: skipping integration "
                f"{key[1]} (org {key[0]}): a whole-integration config's "
                "sync_targets could not be decoded; shared dataset rows are "
                "left untouched rather than disabled on a guess"
            )
            continue
        org_id, integration_id = key
        controlled = operator_controlled_dataset_keys(provider_by_integration[key])
        disable = sorted(controlled - desired)
        if not disable:
            continue
        bind.execute(
            _INTEGRATION_DATASETS.update()
            .where(
                _INTEGRATION_DATASETS.c.org_id == org_id,
                _INTEGRATION_DATASETS.c.integration_id == raw_integration_id[key],
                _INTEGRATION_DATASETS.c.dataset_key.in_(disable),
                _INTEGRATION_DATASETS.c.is_enabled.is_(True),
            )
            .values(is_enabled=False)
        )


def downgrade() -> None:
    """Intentionally does nothing -- see the module docstring."""

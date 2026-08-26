"""Split the single ``auto_import_teams`` flag into three independent
categories: teams, projects, members (CHAOS-4323).

Revision ID: 0112
Revises: 0111

DATA ONLY. This adds, drops and alters no column. ``SyncConfiguration.
sync_options`` is (and stays) a ``JSON`` column (``models/settings.py``); the
three flags are keys inside it, exactly like ``auto_import_teams`` always was.

PROVIDER-AWARE (codex adversarial-review, final round, HIGH): a naive
"derive all three from auto_import_teams" migration would give every
enabled GitHub config ``auto_import_projects=True`` -- GitHub has no
"Projects" import (``providers/team_capabilities.py``: only teams and
members are supported) and the API rejects that combination on every
subsequent write. A migrated-in-place row carrying an unsupported flag it
never explicitly chose would then make an operator's next, unrelated PATCH
(schedule, name, ...) fail 422, because ``sync.py`` validates the MERGED
options. This migration hardcodes the one provider/category combination
known unsupported at 0112's authorship time (GitHub + projects) rather than
importing the live ``team_capabilities`` module, deliberately: a migration's
behavior must stay fixed to what it actually did, independent of how the
capability map evolves after this revision ships.

WHAT WAS WRONG
--------------
One checkbox -- "Auto-import teams, projects & members" -- drove three
different imports (team rows, project ownership, member identities/
memberships) as a single unit, so an operator who wanted member identities
for attribution but not project ownership had no way to say so. chris
(2026-08-26): "get rid of auto import, it's confusing as fuck. Each of these
items needs to just be selectable."

WHAT THIS MIGRATION OWNS, EXACTLY
----------------------------------
For every ``sync_configurations`` row, ``sync_options`` gains two new keys,
and ``auto_import_teams`` itself is normalized:

- ``auto_import_teams`` was the JSON boolean ``true`` (Python ``is True``,
  not a truthiness check) -> ``auto_import_teams``, ``auto_import_projects``,
  ``auto_import_members`` all ``True``.
- Anything else -- ``false``, absent, or a malformed value ``sync_options``
  was never schema-validated before this PR (a string, a number, ``null``)
  -- -> all three explicitly ``False`` (a missing or malformed key is
  normalized to present-and-false, not left absent or coerced by
  truthiness, so every migrated row's intent is unambiguous going forward).

No other ``sync_options`` key is read, added, or removed. A row whose
``sync_options`` cannot be decoded (not a JSON object) is left untouched and
reported -- this migration fails closed on intent it cannot read, mirroring
0108's ``UNREADABLE`` handling for exactly the same reason: guessing at a
config's intent is worse than leaving it alone.

IDEMPOTENT
----------
Re-running computes the same three keys from whatever ``auto_import_teams``
currently holds and writes only rows whose ``sync_options`` would actually
change, so a second run is a no-op by construction.

WHY downgrade() DELETES RATHER THAN RESTORES
---------------------------------------------
``auto_import_projects``/``auto_import_members`` are dropped from
``sync_options``, leaving ``auto_import_teams`` exactly as it stands at
downgrade time. This IS lossy in general: once live, an operator can flip the
three flags independently (e.g. members-only), and collapsing back to one
flag cannot represent that combination -- there is no single boolean that
means "members but not teams or projects". Downgrading after independent use
therefore always reads as whatever ``auto_import_teams`` alone says, which may
understate what was selected. This is the same trade-off 0108 documents for
its own irreversible repair: reversing a genuine three-way selection is an
operator action (re-checking boxes in the now-single-flag form), not
something this migration can reconstruct from a value it never kept.
"""

from __future__ import annotations

import json
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0112"
down_revision: str | None = "0111"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]


class _Unreadable:
    """Sentinel: a ``sync_options`` payload whose intent cannot be determined."""


UNREADABLE = _Unreadable()

_TEAMS = "auto_import_teams"
_PROJECTS = "auto_import_projects"
_MEMBERS = "auto_import_members"

# Providers with no "Projects" import as of 0112's authorship (mirrors
# providers/team_capabilities.py's github row) -- hardcoded, not imported,
# so this migration's behavior stays fixed regardless of how that live
# capability map evolves later. Matched case-insensitively against the
# `provider` column the same way team_capabilities.auto_import_capabilities
# does.
_PROVIDERS_WITHOUT_PROJECTS = frozenset({"github"})

# Lightweight table handle rather than raw SQL/the ORM model -- keeps this
# migration dialect-portable (runs unchanged against the Postgres-JSON
# production column and the SQLite-TEXT test fixtures) and independent of
# whatever the live model looks like by the time this migration runs again
# during a rebuild. Same pattern as 0108's ``_SYNC_CONFIGURATIONS``/
# ``_INTEGRATION_DATASETS`` handles.
_SYNC_CONFIGURATIONS = sa.table(
    "sync_configurations",
    sa.column("id"),
    sa.column("provider"),
    # Explicit JSON type: without it, an UPDATE built from this untyped
    # handle binds a bare Python dict with no serialization info, which the
    # sqlite3 test driver rejects outright and which is fragile even on
    # Postgres drivers that special-case dict adaptation.
    sa.column("sync_options", sa.JSON()),
)


def _decode_options(raw: object) -> dict[str, object] | _Unreadable:
    """Normalise a ``sync_options`` JSON column to a dict.

    The column is ``JSON``, which arrives already decoded as a dict on some
    drivers and as text on others (mirrors 0108's ``_sync_targets``). ``None``
    is NOT unreadable: the column is NOT NULL in the live schema, and a
    missing value is treated as the empty options dict it represents.
    """

    if raw is None:
        return {}
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
    if not isinstance(raw, dict):
        return UNREADABLE
    return raw


def upgrade() -> None:
    bind = op.get_bind()
    rows = bind.execute(
        sa.select(
            _SYNC_CONFIGURATIONS.c.id,
            _SYNC_CONFIGURATIONS.c.provider,
            _SYNC_CONFIGURATIONS.c.sync_options,
        )
    ).fetchall()

    updated = 0
    skipped_unreadable = 0
    for row_id, provider, raw_options in rows:
        options = _decode_options(raw_options)
        if isinstance(options, _Unreadable):
            skipped_unreadable += 1
            print(
                f"0112: skipping sync_configurations row {row_id}: "
                "sync_options could not be decoded as a JSON object"
            )
            continue

        # CHAOS-4323 round 2 (codex adversarial-review, MEDIUM): a strict
        # identity check, not bool(). Python's bool() truthiness would treat
        # a malformed legacy value like the STRING "false" as enabled --
        # bool("false") is True. sync_options was never schema-validated
        # before this PR, so a row genuinely could carry one. Only the real
        # JSON boolean `true` (Python `True`) counts as enabled; anything
        # else -- a string, a number, null, absent -- is quarantined to
        # false, matching the new API-level rejection of non-bool values for
        # these keys on every write going forward.
        enabled = options.get(_TEAMS) is True
        # CHAOS-4323 final round (codex adversarial-review, HIGH):
        # provider-aware clamp. A provider with no "Projects" import (GitHub)
        # never gets auto_import_projects=True out of this migration, even
        # when the legacy single flag was enabled -- an unsupported flag a
        # migrated row never chose would otherwise reject that config's next,
        # unrelated PATCH at the API's capability-validation boundary.
        projects_enabled = enabled and (
            (provider or "").strip().lower() not in _PROVIDERS_WITHOUT_PROJECTS
        )
        new_options = dict(options)
        new_options[_TEAMS] = enabled
        new_options[_PROJECTS] = projects_enabled
        new_options[_MEMBERS] = enabled

        if new_options == options:
            continue

        bind.execute(
            _SYNC_CONFIGURATIONS.update()
            .where(_SYNC_CONFIGURATIONS.c.id == row_id)
            .values(sync_options=new_options)
        )
        updated += 1

    print(
        f"0112: updated {updated} sync_configurations row(s); "
        f"skipped {skipped_unreadable} unreadable row(s)"
    )


def downgrade() -> None:
    bind = op.get_bind()
    rows = bind.execute(
        sa.select(
            _SYNC_CONFIGURATIONS.c.id,
            _SYNC_CONFIGURATIONS.c.sync_options,
        )
    ).fetchall()

    for row_id, raw_options in rows:
        options = _decode_options(raw_options)
        if isinstance(options, _Unreadable):
            continue
        if _PROJECTS not in options and _MEMBERS not in options:
            continue
        new_options = dict(options)
        new_options.pop(_PROJECTS, None)
        new_options.pop(_MEMBERS, None)
        bind.execute(
            _SYNC_CONFIGURATIONS.update()
            .where(_SYNC_CONFIGURATIONS.c.id == row_id)
            .values(sync_options=new_options)
        )

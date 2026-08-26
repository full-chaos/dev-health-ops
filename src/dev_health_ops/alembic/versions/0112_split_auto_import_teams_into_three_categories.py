"""Split the single ``auto_import_teams`` flag into three independent
categories: teams, projects, members (CHAOS-4323).

Revision ID: 0112
Revises: 0111

DATA ONLY. This adds, drops and alters no column. ``SyncConfiguration.
sync_options`` is (and stays) a ``JSON`` column (``models/settings.py``); the
three flags are keys inside it, exactly like ``auto_import_teams`` always was.

PROVIDER-AWARE (codex adversarial-review, final round, HIGH; narrow
follow-up round, HIGH): a naive "derive all three from auto_import_teams"
migration would give every enabled GitHub config
``auto_import_projects=True`` -- GitHub has no "Projects" import
(``providers/team_capabilities.py``: only teams and members are supported)
-- and every enabled ``launchdarkly``/``pagerduty`` config all three flags
``True``, even though those two are valid ``sync_configurations.provider``
values (``api/admin/routers/sync.py``'s ``PROVIDER_SYNC_TARGETS``) that have
NO auto-import capability at all (absent from
``team_capabilities._AUTO_IMPORT_CAPABILITIES``, so
``auto_import_capabilities()`` falls through to
``_UNSUPPORTED_PROVIDER_CAPABILITY`` for them -- the same fallback an
unrecognized/future provider gets). Either case gives a migrated row a flag
it never explicitly chose, which the API rejects on every subsequent write
(``sync.py`` validates the MERGED options) -- an operator's next, unrelated
PATCH (schedule, name, ...) would 422. This migration hardcodes the full
category matrix for every provider known to have auto-import support at
0112's authorship time (github/gitlab/jira/linear) rather than importing the
live ``team_capabilities`` module, deliberately: a migration's behavior must
stay fixed to what it actually did, independent of how that map evolves
after this revision ships. Every OTHER provider (``launchdarkly``,
``pagerduty``, and any future addition) gets all three flags clamped to
``False`` regardless of the legacy value, matching
``_UNSUPPORTED_PROVIDER_CAPABILITY``.

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

# Frozen snapshot of providers/team_capabilities.py's per-category
# auto-import support at 0112's authorship time -- hardcoded, not imported,
# so this migration's behavior stays fixed regardless of how that live
# capability map evolves later (same rationale as the module docstring).
# (teams, projects, members). Any provider NOT a key here (e.g.
# "launchdarkly", "pagerduty" -- both valid sync_configurations.provider
# values with no team-autoimport support at all, per
# PROVIDER_SYNC_TARGETS in api/admin/routers/sync.py -- or an
# unrecognized/future provider) gets all three clamped False, mirroring
# team_capabilities._UNSUPPORTED_PROVIDER_CAPABILITY's fallback. Matched
# case-insensitively/stripped against the `provider` column, the same way
# team_capabilities.auto_import_capabilities() does.
_PROVIDER_CATEGORY_SUPPORT: dict[str, tuple[bool, bool, bool]] = {
    "github": (True, False, True),
    "gitlab": (True, True, True),
    "jira": (True, True, True),
    "linear": (True, True, True),
}
_UNSUPPORTED_CATEGORY_SUPPORT = (False, False, False)

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
        # CHAOS-4323 final round + narrow follow-up (codex adversarial-
        # review, both HIGH): provider-aware clamp against the FULL
        # per-category matrix, not just GitHub/projects. A provider with no
        # auto-import support at all for a category (GitHub+projects, or
        # every category for launchdarkly/pagerduty/an unrecognized
        # provider) never gets that flag set True out of this migration,
        # even when the legacy single flag was enabled -- an unsupported
        # flag a migrated row never chose would otherwise reject that
        # config's next, unrelated PATCH at the API's capability-validation
        # boundary.
        teams_supported, projects_supported, members_supported = (
            _PROVIDER_CATEGORY_SUPPORT.get(
                (provider or "").strip().lower(), _UNSUPPORTED_CATEGORY_SUPPORT
            )
        )
        new_options = dict(options)
        new_options[_TEAMS] = enabled and teams_supported
        new_options[_PROJECTS] = enabled and projects_supported
        new_options[_MEMBERS] = enabled and members_supported

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

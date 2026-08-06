"""CHAOS-3219: ``SyncConfiguration`` rows + watermark aging for the ask-dev-world
per-source terminal-state matrix (``sources.json``).

Grounded directly in ``api/dev/data_health_service.py``'s
``NativeDataHealthReader`` -- the real reader ``DataHealthService.inspect``
delegates to for every Ask Dev "how healthy/fresh is this source" question.
Every state this module realizes is a direct, documented consequence of a
signal that reader actually inspects:

* ``configured`` comes from whether an *active* ``SyncConfiguration`` row
  exists for a provider that ``_provider_supports`` maps onto the requested
  source (``build_sync_configuration`` / ``PROVIDER_SUPPORTS_SOURCE``).
* ``active_failure`` comes from ``SyncConfiguration.last_sync_success is
  False`` (``build_sync_configuration(..., last_sync_success=False)``).
* ``watermark`` comes from ``max(last_synced)`` over the source's own raw
  table, scoped by repo -- ``age_source_rows`` deletes and re-inserts each
  row with that column overwritten (see its own docstring for why a plain
  ``ALTER TABLE ... UPDATE`` cannot be used) so staleness is driven by the
  SAME column the reader reads, not by shifting business dates (commit
  ``authored_date`` etc.), which the reader never looks at.

No wall-clock ``datetime.now()``/``utcnow()`` calls anywhere in this module
(CHAOS-3392 lesson) -- every timestamp is threaded in by the caller, always
derived from the world's pinned ``now``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal

#: Mirrors ``data_health_service._provider_supports`` verbatim (kept as an
#: explicit, importable table here rather than re-deriving it, since that
#: function is private to ``api/dev`` and Lane 1a's territory is
#: ``fixtures/**`` only -- duplicating the closed, documented mapping is
#: safer than reaching across the lane boundary to import a private helper).
PROVIDER_SUPPORTS_SOURCE: dict[str, tuple[str, ...]] = {
    "work_items": ("jira", "linear", "github", "gitlab"),
    "work_units": ("jira", "linear", "github", "gitlab"),
    "incidents": ("pagerduty", "opsgenie", "incident"),
    "commits": ("github", "gitlab", "local", "git"),
    "pull_requests": ("github", "gitlab", "local", "git"),
    "reviews": ("github", "gitlab", "local", "git"),
    "deployments": ("github", "gitlab", "local", "git"),
}

#: The two raw-table source keys ``data_health_service._SOURCE_TABLES`` maps
#: to a ``(table, repo_column, watermark_column)`` triple that this module's
#: ``age_source_rows``/``zero_out_source`` mutate directly.
_SOURCE_TABLES: dict[str, tuple[str, str | None, str]] = {
    "work_items": ("work_items", "repo_id", "last_synced"),
    "work_units": ("work_unit_investments", "repo_id", "computed_at"),
    "pull_requests": ("git_pull_requests", "repo_id", "last_synced"),
    "reviews": ("git_pull_request_reviews", "repo_id", "last_synced"),
    "commits": ("git_commits", "repo_id", "last_synced"),
    "ci_runs": ("ci_pipeline_runs", "repo_id", "last_synced"),
    "deployments": ("deployments", "repo_id", "last_synced"),
    "incidents": ("operational_incidents", None, "last_synced"),
}

#: ``sync_coverage.STALE_FALLBACK_GRACE`` verbatim (48h) -- the grace applied
#: when no ``ScheduledJob`` row exists for the org/provider, which is the
#: only path this fixture world ever exercises (no ``ScheduledJob`` rows are
#: seeded). Kept as a literal constant here rather than importing the
#: private module for the same file-territory reason as
#: ``PROVIDER_SUPPORTS_SOURCE`` above.
STALE_FALLBACK_GRACE = timedelta(hours=48)

SyncProviderState = Literal["active_success", "active_failure", "unconfigured"]


@dataclass(frozen=True, slots=True)
class SyncConfigurationSpec:
    """Declarative shape of one org+provider ``SyncConfiguration`` row.

    ``state='unconfigured'`` means "do not create a row at all" -- callers
    filter these out before building ORM instances; kept as an explicit enum
    member (not just an absent dict key) so ``world.json``'s
    ``sync_providers`` block can name every provider's intended state
    uniformly, including the ones that resolve to "no row".
    """

    org_id: str
    provider: str
    state: SyncProviderState
    last_sync_at: datetime | None = None


def build_sync_configuration(spec: SyncConfigurationSpec):
    """One ``models.settings.SyncConfiguration`` ORM row for ``spec``.

    Returns ``None`` for ``state='unconfigured'`` -- the absence of a row
    IS the fixture (mirrors ``subjects.json``'s no-match convention: absence
    is deliberate, not an oversight).
    """

    if spec.state == "unconfigured":
        return None
    from dev_health_ops.models.settings import SyncConfiguration

    config = SyncConfiguration(
        name=f"ask-dev-world-{spec.provider}",
        provider=spec.provider,
        org_id=spec.org_id,
        sync_targets=["*"],
        is_active=True,
        planner_managed=False,
    )
    config.last_sync_at = spec.last_sync_at
    config.last_sync_success = spec.state == "active_success"
    if spec.state == "active_failure":
        config.last_sync_error = "ask-dev-world fixture: simulated sync failure"
    return config


def build_sync_configurations_for_org(
    org_id: str,
    provider_states: dict[str, SyncProviderState],
    *,
    as_of: datetime,
) -> list[Any]:
    """All non-``unconfigured`` ``SyncConfiguration`` rows for one org.

    ``as_of`` is used as ``last_sync_at`` for every configured provider --
    the world's pinned "now", never wall-clock time.
    """

    configs = []
    for provider, state in provider_states.items():
        record = build_sync_configuration(
            SyncConfigurationSpec(
                org_id=org_id,
                provider=provider,
                state=state,
                last_sync_at=as_of if state != "unconfigured" else None,
            )
        )
        if record is not None:
            configs.append(record)
    return configs


def sources_configured_for_org(
    provider_states: dict[str, SyncProviderState],
) -> set[str]:
    """Source keys with >=1 *configured* (non-unconfigured) supporting provider."""

    configured_providers = {
        provider
        for provider, state in provider_states.items()
        if state != "unconfigured"
    }
    return {
        source
        for source, providers in PROVIDER_SUPPORTS_SOURCE.items()
        if configured_providers & set(providers)
    }


class SourceAgingWriteError(RuntimeError):
    """``age_source_rows`` could not confirm the aged replacement rows were
    durably visible after writing them -- see the function's own docstring
    (Codex HIGH-5, 2026-08-05) for what this guards against."""


async def age_source_rows(
    client: Any,
    *,
    org_id: str,
    repo_id: str,
    source: str,
    stale_watermark: datetime,
) -> None:
    """Force ``source``'s watermark for ``repo_id`` to ``stale_watermark``.

    Realized as **delete-then-insert**, not an in-place ``ALTER TABLE ...
    UPDATE`` (every affected table's version column is a protected "key
    column" for ``ALTER ... UPDATE`` purposes even when it is not part of
    ``ORDER BY`` -- empirically confirmed live: ``Code: 420
    CANNOT_UPDATE_COLUMN``), and NOT insert-then-delete either, despite that
    being the more obvious way to avoid a delete-first zero-row window.

    Codex HIGH-5 (2026-08-05) history, corrected same-day after a LIVE
    two-generation run caught a worse bug than the one being fixed: an
    earlier revision of this function inserted the aged replacement rows
    BEFORE deleting the originals, reasoning that the worst a mid-operation
    crash could do is leave both versions present, which a
    ReplacingMergeTree resolves harmlessly via ``FINAL``. That reasoning
    missed that this table family is a `ReplacingMergeTree(<watermark
    column>)` and "aging" a row means writing a LOWER version value than
    what is already there -- the exact case ReplacingMergeTree's own merge
    rule (highest version wins) resolves the WRONG way. Live forensics via
    ``system.part_log`` on a real scratch database caught it directly: the
    aged INSERT landed (``NewPart``, 39 rows) and a background merge started
    ~300 MICROSECONDS later, completing in ~2.4ms and silently collapsing
    the just-inserted lower-version rows back to the pre-existing
    higher-version ones -- with NO crash, NO exception, under completely
    normal execution, on a table that already had many parts from earlier
    inserts in the same generation run (ClickHouse's background merge
    scheduler merges eagerly once a table accumulates enough small parts).
    Insert-then-delete is therefore not just crash-unsafe here, it is
    *plain-execution*-unsafe -- strictly worse than the bug it was meant to
    fix, and the corruption is silent unless independently verified (which
    is exactly what caught it: see the postcondition check below).

    Delete-then-insert avoids that hazard structurally: once the higher-
    version originals are gone, there is no competing row left for a merge
    to prefer, so whatever the insert writes IS the row, with no version
    race window at all. What delete-then-insert cannot avoid -- and nothing
    can, short of ClickHouse gaining cross-statement transactions for
    MergeTree tables -- is a process crash landing in the gap between the
    two statements, which does leave zero rows for `org_id`/`repo_id` until
    the next full regeneration. Given ``fixtures world`` runs exclusively
    against disposable SCRATCH databases (enforced at the entrypoint) that
    are always regenerated fresh rather than resumed in place after a
    failure, that residual window's blast radius is bounded to "this
    generation run failed and must be re-run," not "a live database is
    silently wrong forever." The postcondition check below closes the
    remaining, actually-preventable gap: an insert that returns success but
    is not yet confirmed durable is now impossible to mistake for a
    completed aging operation, whatever the underlying cause.

    Both mutations are synchronous (``mutations_sync=1`` / awaited) so the
    caller can trust the result is visible before ``run_fixtures_world``'s
    digest is computed.
    """

    table, repo_column, watermark_column = _SOURCE_TABLES[source]
    if repo_column is None:
        # incidents carries no repo_id column -- aging is org-wide for it.
        where = "org_id = {org_id:String}"
    else:
        where = f"org_id = {{org_id:String}} AND {repo_column} = {{repo_id:String}}"
    params = {"org_id": org_id, "repo_id": repo_id}

    result = await asyncio.to_thread(
        client.query,
        f"SELECT * FROM {table} FINAL WHERE {where}",  # noqa: S608
        parameters=params,
    )
    column_names = list(result.column_names)
    rows = [list(row) for row in result.result_rows]
    if not rows:
        return
    watermark_idx = column_names.index(watermark_column)
    for row in rows:
        row[watermark_idx] = stale_watermark

    await asyncio.to_thread(
        client.command,
        f"ALTER TABLE {table} DELETE WHERE {where} SETTINGS mutations_sync = 1",  # noqa: S608
        parameters=params,
    )
    await asyncio.to_thread(client.insert, table, rows, column_names=column_names)

    observed = await _count_rows_at_watermark(
        client,
        table=table,
        where=where,
        watermark_column=watermark_column,
        params=params,
        stale_watermark=stale_watermark,
    )
    if observed < len(rows):
        raise SourceAgingWriteError(
            f"age_source_rows: deleted the original rows and inserted "
            f"{len(rows)} aged row(s) into {table!r} for org_id={org_id!r} "
            f"repo_id={repo_id!r}, but only {observed} row(s) with "
            f"{watermark_column}={stale_watermark!r} are visible "
            "afterward -- the replacement was not confirmed durable. The "
            "table may currently have zero rows for this org/repo; this "
            "generation run must be treated as failed and re-run against a "
            "fresh scratch database, never resumed in place."
        )


async def _count_rows_at_watermark(
    client: Any,
    *,
    table: str,
    where: str,
    watermark_column: str,
    params: dict[str, Any],
    stale_watermark: datetime,
) -> int:
    """How many rows in ``table`` currently match ``where`` AND carry
    ``watermark_column == stale_watermark`` -- the read-your-write check
    ``age_source_rows`` uses to confirm its insert landed before it deletes
    anything. Split out as its own function so a failure-injection test can
    monkeypatch/stub it independently of the insert/delete calls."""

    result = await asyncio.to_thread(
        client.query,
        f"SELECT count() FROM {table} WHERE {where} AND {watermark_column} = "  # noqa: S608
        "{stale_watermark:DateTime64(3, 'UTC')}",
        parameters={**params, "stale_watermark": stale_watermark},
    )
    return int(result.result_rows[0][0])


async def zero_out_source(
    client: Any,
    *,
    org_id: str,
    repo_id: str,
    source: str,
) -> None:
    """Delete every ``source`` row for ``repo_id`` -- realizes NO_DATA.

    Distinct from ``age_source_rows``: an aged watermark still reads
    ``STALE`` (the reader saw *something*, just too old); zero rows reads
    ``NO_DATA`` (the reader's ``watermark is None`` branch).
    """

    table, repo_column, _watermark_column = _SOURCE_TABLES[source]
    if repo_column is None:
        where = "org_id = {org_id:String}"
    else:
        where = f"org_id = {{org_id:String}} AND {repo_column} = {{repo_id:String}}"
    query = f"ALTER TABLE {table} DELETE WHERE {where} SETTINGS mutations_sync = 1"
    await asyncio.to_thread(
        client.command,
        query,
        parameters={"org_id": org_id, "repo_id": repo_id},
    )

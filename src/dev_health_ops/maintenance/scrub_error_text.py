"""``dev-hops maintenance scrub-error-text`` (CHAOS-2780).

CHAOS-2766 (``sync/error_sanitize.py``) sanitizes every WRITE path into the
legacy free-form error-text columns, but rows persisted *before* that change
shipped keep their raw text at rest -- the wave-2 live gate showed real
``Authorization`` headers sitting in ``sync_run_units.error``. This command
applies the exact same ``sanitize_error_text`` helper to already-persisted
rows, in place, per column.

Design summary (see the CHAOS-2780 plan for the full rationale):
  * Dry-run by default; ``--apply`` mutates. ``--org`` optionally scopes to
    one organization; omitted, ALL organizations are scanned.
  * A registry keyed by SQLAlchemy **model classes** (never table-name
    strings) drives the scan -- this is what makes the
    ``sync_run_reference_discovery`` singular/plural typo structurally
    impossible to reproduce here: each table's report label is derived from
    ``Model.__tablename__``, not hand-typed.
  * Per table: keyset pagination on the UUID primary key, per-batch commit,
    and a compare-and-swap (CAS) update (``WHERE id = :id AND col = :old``)
    so a row rewritten by live sync between scan and update is left alone
    (counted ``skipped_concurrent``, never retried -- the newer value already
    went through the post-CHAOS-2766 sanitized write path).
  * ``sync_configurations.last_sync_stats`` is a JSON column whose ``'error'``
    string value (when present) is the only thing scrubbed; sibling keys are
    preserved. Its CAS predicate casts the column to text
    (``last_sync_stats::text = :raw_text``) because PostgreSQL's ``json``
    type (unlike ``jsonb``) preserves the stored text verbatim, giving a
    stable equality witness that plain ``json = json`` cannot provide.
  * Every changed row is counted as either ``redact`` (a credential-shaped
    pattern was found and substituted) or ``truncate_only`` (the value was
    already clean, but longer than the column's write-path length cap) --
    conflating the two would make "N rows would change" read as "N secrets
    found" when some fraction is pure length-cap truncation.
"""

from __future__ import annotations

import argparse
import logging
import os
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from sqlalchemy import Text, cast, or_, select, update
from sqlalchemy.orm import Session

from dev_health_ops.models.backfill import BackfillJob
from dev_health_ops.models.integrations import (
    SyncDispatchOutbox,
    SyncRun,
    SyncRunReferenceDiscovery,
    SyncRunUnit,
)
from dev_health_ops.models.settings import (
    IntegrationCredential,
    JobRun,
    ScheduledJob,
    SyncConfiguration,
)
from dev_health_ops.sync.dispatch_outbox import (
    _MAX_ERROR_LENGTH as _OUTBOX_MAX_ERROR_LENGTH,
)
from dev_health_ops.sync.error_sanitize import (
    DEFAULT_MAX_ERROR_TEXT_LENGTH,
    sanitize_error_text,
)

logger = logging.getLogger(__name__)

KIND_TEXT = "text"
KIND_JSON_ERROR_KEY = "json_error_key"

# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

# A statement-in/statement-out callable that narrows a SELECT (or UPDATE
# existence check) to one org_id. Most tables filter their own ``org_id``
# column directly; ``job_runs`` has none and must join ``scheduled_jobs``.
OrgFilter = Callable[[Any, str], Any]


@dataclass(frozen=True)
class ColumnSpec:
    """One scrub target column on a registry table."""

    name: str
    max_length: int
    kind: str = KIND_TEXT
    # Only meaningful for kind == KIND_JSON_ERROR_KEY: the dict key inside
    # the JSON column whose string value gets sanitized in place.
    json_error_field: str = "error"


@dataclass(frozen=True)
class TableSpec:
    """One registry entry: a model class plus the columns to scrub on it."""

    # Typed loosely on purpose: entries span 8 unrelated declarative model
    # classes with no common attribute-bearing base beyond ``Base`` itself
    # (which declares no columns), so a precise type here buys nothing but
    # per-callsite ``attr-defined`` noise on ``model.id``.
    model: Any
    columns: tuple[ColumnSpec, ...]
    org_filter: OrgFilter

    @property
    def label(self) -> str:
        # Derived from the model, never hand-typed -- see module docstring.
        return str(getattr(self.model, "__tablename__"))


def _direct_org_filter(model: type) -> OrgFilter:
    def _filter(stmt: Any, org_id: str) -> Any:
        return stmt.where(getattr(model, "org_id") == org_id)

    return _filter


def _job_run_org_filter(stmt: Any, org_id: str) -> Any:
    # job_runs has no org_id column (models/settings.py) -- scope via its
    # scheduled_jobs FK instead.
    return stmt.join(ScheduledJob, ScheduledJob.id == JobRun.job_id).where(
        ScheduledJob.org_id == org_id
    )


REGISTRY: tuple[TableSpec, ...] = (
    TableSpec(
        model=SyncRunUnit,
        columns=(ColumnSpec("error", DEFAULT_MAX_ERROR_TEXT_LENGTH),),
        org_filter=_direct_org_filter(SyncRunUnit),
    ),
    TableSpec(
        model=SyncRun,
        columns=(ColumnSpec("error", DEFAULT_MAX_ERROR_TEXT_LENGTH),),
        org_filter=_direct_org_filter(SyncRun),
    ),
    TableSpec(
        model=SyncRunReferenceDiscovery,
        columns=(ColumnSpec("error", DEFAULT_MAX_ERROR_TEXT_LENGTH),),
        org_filter=_direct_org_filter(SyncRunReferenceDiscovery),
    ),
    TableSpec(
        model=SyncDispatchOutbox,
        columns=(ColumnSpec("last_error", _OUTBOX_MAX_ERROR_LENGTH),),
        org_filter=_direct_org_filter(SyncDispatchOutbox),
    ),
    TableSpec(
        model=JobRun,
        columns=(
            ColumnSpec("error", DEFAULT_MAX_ERROR_TEXT_LENGTH),
            # Defensive historical scrub only -- no current write site
            # populates error_traceback (see module docstring / plan §3).
            ColumnSpec("error_traceback", DEFAULT_MAX_ERROR_TEXT_LENGTH),
        ),
        org_filter=_job_run_org_filter,
    ),
    TableSpec(
        model=BackfillJob,
        columns=(ColumnSpec("error_message", DEFAULT_MAX_ERROR_TEXT_LENGTH),),
        org_filter=_direct_org_filter(BackfillJob),
    ),
    TableSpec(
        model=SyncConfiguration,
        columns=(
            ColumnSpec("last_sync_error", DEFAULT_MAX_ERROR_TEXT_LENGTH),
            ColumnSpec(
                "last_sync_stats",
                DEFAULT_MAX_ERROR_TEXT_LENGTH,
                kind=KIND_JSON_ERROR_KEY,
            ),
        ),
        org_filter=_direct_org_filter(SyncConfiguration),
    ),
    TableSpec(
        model=IntegrationCredential,
        columns=(ColumnSpec("last_test_error", DEFAULT_MAX_ERROR_TEXT_LENGTH),),
        org_filter=_direct_org_filter(IntegrationCredential),
    ),
)


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------


@dataclass
class ColumnCounters:
    scanned: int = 0
    redact: int = 0
    truncate_only: int = 0
    skipped_concurrent: int = 0

    def add(self, other: ColumnCounters) -> None:
        self.scanned += other.scanned
        self.redact += other.redact
        self.truncate_only += other.truncate_only
        self.skipped_concurrent += other.skipped_concurrent


Counters = dict[tuple[str, str], ColumnCounters]


def _rowcount(result: object) -> int:
    return int(getattr(result, "rowcount", 0) or 0)


def _classify_change(old: str) -> str:
    """Return ``"redact"`` if a credential-shaped pattern was substituted,
    else ``"truncate_only"`` (the length cap was the only thing that
    changed the value). Only called once ``sanitize_error_text(old, ...)``
    is already known to differ from ``old``."""
    redacted_only = sanitize_error_text(old, max_length=None)
    if redacted_only != old:
        return "redact"
    return "truncate_only"


def _bump(counters: ColumnCounters, change_kind: str) -> None:
    if change_kind == "redact":
        counters.redact += 1
    else:
        counters.truncate_only += 1


# ---------------------------------------------------------------------------
# Scan / apply loop
# ---------------------------------------------------------------------------


def _select_columns(table_spec: TableSpec) -> list[Any]:
    cols: list[Any] = [table_spec.model.id]
    for col_spec in table_spec.columns:
        attr = getattr(table_spec.model, col_spec.name)
        cols.append(attr)
        if col_spec.kind == KIND_JSON_ERROR_KEY:
            cols.append(cast(attr, Text).label(f"_{col_spec.name}_raw"))
    return cols


def _process_text_column(
    session: Session,
    table_spec: TableSpec,
    col_spec: ColumnSpec,
    row_id: uuid.UUID,
    old: str | None,
    *,
    apply: bool,
    counters: ColumnCounters,
) -> None:
    if old is None:
        return
    counters.scanned += 1
    new = sanitize_error_text(old, max_length=col_spec.max_length)
    if new == old:
        return
    change_kind = _classify_change(old)
    if not apply:
        _bump(counters, change_kind)
        return
    result = session.execute(
        update(table_spec.model)
        .where(
            table_spec.model.id == row_id,
            getattr(table_spec.model, col_spec.name) == old,
        )
        .values(**{col_spec.name: new})
        .execution_options(synchronize_session=False)
    )
    if _rowcount(result) == 1:
        _bump(counters, change_kind)
    else:
        counters.skipped_concurrent += 1


def _process_json_error_key_column(
    session: Session,
    table_spec: TableSpec,
    col_spec: ColumnSpec,
    row_id: uuid.UUID,
    old_dict: dict[str, Any] | None,
    raw_text: str | None,
    *,
    apply: bool,
    counters: ColumnCounters,
) -> None:
    if old_dict is None:
        return
    counters.scanned += 1
    if not isinstance(old_dict, dict):
        return
    error_val = old_dict.get(col_spec.json_error_field)
    if not isinstance(error_val, str):
        return
    new_error = sanitize_error_text(error_val, max_length=col_spec.max_length)
    if new_error == error_val:
        return
    change_kind = _classify_change(error_val)
    if not apply:
        _bump(counters, change_kind)
        return
    new_dict = dict(old_dict)
    new_dict[col_spec.json_error_field] = new_error
    result = session.execute(
        update(table_spec.model)
        .where(
            table_spec.model.id == row_id,
            cast(getattr(table_spec.model, col_spec.name), Text) == raw_text,
        )
        .values(**{col_spec.name: new_dict})
        .execution_options(synchronize_session=False)
    )
    if _rowcount(result) == 1:
        _bump(counters, change_kind)
    else:
        counters.skipped_concurrent += 1


def process_table(
    session: Session,
    table_spec: TableSpec,
    *,
    apply: bool,
    org_id: str | None,
    batch_size: int,
    counters: Counters,
) -> None:
    """Scan (and optionally scrub) one registry table, batch by batch."""
    model = table_spec.model
    id_col = model.id
    select_cols = _select_columns(table_spec)
    not_null_clause = or_(
        *[getattr(model, c.name).is_not(None) for c in table_spec.columns]
    )

    for col_spec in table_spec.columns:
        counters.setdefault((table_spec.label, col_spec.name), ColumnCounters())

    last_id: uuid.UUID | None = None
    while True:
        stmt = select(*select_cols).where(not_null_clause)
        if last_id is not None:
            stmt = stmt.where(id_col > last_id)
        if org_id:
            stmt = table_spec.org_filter(stmt, org_id)
        stmt = stmt.order_by(id_col).limit(batch_size)

        rows = session.execute(stmt).all()
        if not rows:
            break
        last_id = rows[-1][0]

        for row in rows:
            offset = 1
            for col_spec in table_spec.columns:
                col_counters = counters[(table_spec.label, col_spec.name)]
                if col_spec.kind == KIND_TEXT:
                    old_value = row[offset]
                    offset += 1
                    _process_text_column(
                        session,
                        table_spec,
                        col_spec,
                        row[0],
                        old_value,
                        apply=apply,
                        counters=col_counters,
                    )
                else:
                    old_dict = row[offset]
                    raw_text = row[offset + 1]
                    offset += 2
                    _process_json_error_key_column(
                        session,
                        table_spec,
                        col_spec,
                        row[0],
                        old_dict,
                        raw_text,
                        apply=apply,
                        counters=col_counters,
                    )

        if apply:
            session.commit()

        if len(rows) < batch_size:
            break


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def _print_report(counters: Counters, *, apply: bool) -> None:
    redact_label = "redacted" if apply else "would_redact"
    truncate_label = "truncated_only" if apply else "would_truncate_only"
    print(
        f"{'column':<48s} {'scanned':>10s} {redact_label:>14s} "
        f"{truncate_label:>18s} {'skipped_concurrent':>19s}"
    )
    totals = ColumnCounters()
    for table_spec in REGISTRY:
        for col_spec in table_spec.columns:
            c = counters.get((table_spec.label, col_spec.name), ColumnCounters())
            column_label = f"{table_spec.label}.{col_spec.name}"
            print(
                f"{column_label:<48s} {c.scanned:>10d} {c.redact:>14d} "
                f"{c.truncate_only:>18d} {c.skipped_concurrent:>19d}"
            )
            totals.add(c)
    print(
        f"{'TOTAL':<48s} {totals.scanned:>10d} {totals.redact:>14d} "
        f"{totals.truncate_only:>18d} {totals.skipped_concurrent:>19d}"
    )
    if not apply:
        print()
        print("Dry-run: pass --apply to write these changes.")


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------


def _resolve_org_id(ns: argparse.Namespace) -> str | None:
    """Return the org_id scope for this run, or ``None`` for ALL orgs.

    CHAOS-2780 codex HIGH: ``ns.org`` alone is not enough here. The root
    ``--org`` argument defaults to a sentinel (not the ``ORG_ID`` env var
    directly), so an operator running this command with ORG_ID set in their
    shell (ordinary env usage, not an explicit scope opt-in) would
    otherwise have ``ns.org`` silently narrow a run that's supposed to
    cover every organization -- turning "0 would-change" into a false
    whole-DB completion signal. ``ns.org_explicit`` (set by
    ``cli._resolve_org`` from a sentinel-identity check against the
    argparse-parsed value, before this function ever sees ``ns`` --
    correctly handling every form argparse recognizes, including
    ``allow_abbrev``-shortened flags like ``--or``, which a raw-argv token
    scan misses) distinguishes an actually-typed ``--org`` from the env
    fallback; only the former scopes the scrub. Namespaces built directly
    (bypassing ``cli.main``, e.g. in tests) that don't set ``org_explicit``
    default to the safe "all orgs" behavior.
    """
    if not getattr(ns, "org_explicit", False):
        return None
    return getattr(ns, "org", None) or None


def collect_counters(ns: argparse.Namespace) -> tuple[Counters | None, bool]:
    """Run the scrub (scan-only or apply, per ``ns``) and return its raw
    per-column counters plus a batch-failure flag.

    Split out from :func:`run_scrub_error_text` so tests can assert on exact
    counts instead of parsing the printed report. Returns ``(None, True)``
    when no database URI is configured.
    """
    from dev_health_ops.db import get_postgres_session_sync_for_uri

    db_uri = (
        getattr(ns, "db", None)
        or os.getenv("POSTGRES_URI")
        or os.getenv("DATABASE_URI")
    )
    if not db_uri:
        logger.error(
            "PostgreSQL URI not configured. Pass --db or set POSTGRES_URI/DATABASE_URI."
        )
        return None, True

    apply = bool(getattr(ns, "apply", False))
    org_id = _resolve_org_id(ns)
    batch_size = int(getattr(ns, "batch_size", 1000) or 1000)
    if batch_size <= 0:
        batch_size = 1000

    counters: Counters = {}
    had_failure = False

    with get_postgres_session_sync_for_uri(db_uri) as session:
        for table_spec in REGISTRY:
            try:
                process_table(
                    session,
                    table_spec,
                    apply=apply,
                    org_id=org_id,
                    batch_size=batch_size,
                    counters=counters,
                )
            except Exception:
                logger.exception(
                    "scrub-error-text failed while processing %s", table_spec.label
                )
                had_failure = True
                session.rollback()

    return counters, had_failure


def run_scrub_error_text(ns: argparse.Namespace) -> int:
    counters, had_failure = collect_counters(ns)
    if counters is not None:
        org_id = _resolve_org_id(ns)
        scope = f"org={org_id}" if org_id else "ALL organizations"
        print(f"Org scope: {scope}")
        _print_report(counters, apply=bool(getattr(ns, "apply", False)))
    return 1 if had_failure else 0


__all__ = [
    "REGISTRY",
    "ColumnCounters",
    "ColumnSpec",
    "TableSpec",
    "collect_counters",
    "process_table",
    "run_scrub_error_text",
]

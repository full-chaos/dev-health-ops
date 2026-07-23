"""Insert River compatibility jobs from an async SQLAlchemy transaction.

This is a Phase 0 compatibility probe, not a production enqueue adapter. The
released ``riverqueue`` client currently fails River v0.40 unique insertion;
the probe keeps that failure reproducible while exercising the supported
non-unique transaction path.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import riverqueue
from riverqueue.driver import riversqlalchemy
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

JOB_KIND = "chaos3034_compat_v1"
DOMAIN_TABLE = "river_compat_domain"


@dataclass
class CompatArgs:
    """Minimal cross-language payload consumed by the Go probe worker."""

    marker: str
    source: str = "python"
    contract_version: int = 1
    kind: str = JOB_KIND

    def to_json(self) -> str:
        payload = asdict(self)
        payload.pop("kind")
        return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _async_database_url(database_url: str) -> str:
    url = make_url(database_url)
    if url.drivername in {"postgres", "postgresql"}:
        url = url.set(drivername="postgresql+asyncpg")
    elif url.drivername != "postgresql+asyncpg":
        raise ValueError(
            "database URL must use postgresql or postgresql+asyncpg, "
            f"got {url.drivername!r}"
        )
    return url.render_as_string(hide_password=False)


def _engine(database_url: str, *, pgbouncer: bool) -> AsyncEngine:
    kwargs: dict[str, Any] = {}
    if pgbouncer:
        kwargs = {
            "poolclass": NullPool,
            "connect_args": {
                "statement_cache_size": 0,
                "prepared_statement_cache_size": 0,
                "prepared_statement_name_func": (lambda: f"__asyncpg_{uuid.uuid4()}__"),
            },
        }
    return create_async_engine(_async_database_url(database_url), **kwargs)


async def _prepare(engine: AsyncEngine) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {DOMAIN_TABLE} (
                    marker text PRIMARY KEY,
                    created_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
        )


def _insert_opts(args: argparse.Namespace) -> riverqueue.InsertOpts:
    scheduled_at = None
    if args.scheduled_delay_ms:
        scheduled_at = datetime.now(timezone.utc) + timedelta(
            milliseconds=args.scheduled_delay_ms
        )
    return riverqueue.InsertOpts(
        max_attempts=args.max_attempts,
        priority=args.priority,
        queue=args.queue,
        scheduled_at=scheduled_at,
        tags=["phase0", "python"],
    )


async def _insert(args: argparse.Namespace, *, rollback: bool) -> dict[str, Any]:
    engine = _engine(args.database_url, pgbouncer=args.pgbouncer)
    client = riverqueue.AsyncClient(riversqlalchemy.AsyncDriver(engine))
    await _prepare(engine)
    inserted_job_id: int | None = None
    try:
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    text(f"INSERT INTO {DOMAIN_TABLE} (marker) VALUES (:marker)"),
                    {"marker": args.marker},
                )
                result = await client.insert_tx(
                    connection,
                    CompatArgs(marker=args.marker),
                    _insert_opts(args),
                )
                inserted_job_id = result.job.id
                if rollback:
                    raise _RollbackRequested
        except _RollbackRequested:
            pass

        async with engine.connect() as connection:
            domain_count = await connection.scalar(
                text(f"SELECT count(*) FROM {DOMAIN_TABLE} WHERE marker = :marker"),
                {"marker": args.marker},
            )
            job_rows = (
                (
                    await connection.execute(
                        text(
                            """
                        SELECT
                            queue,
                            priority,
                            max_attempts,
                            state::text AS state,
                            scheduled_at > created_at AS scheduled_after_create,
                            tags,
                            args ->> 'contract_version' AS contract_version,
                            args ->> 'source' AS source
                        FROM river_job
                        WHERE kind = :kind AND args ->> 'marker' = :marker
                        """
                        ),
                        {"kind": JOB_KIND, "marker": args.marker},
                    )
                )
                .mappings()
                .all()
            )

        expected_count = 0 if rollback else 1
        if domain_count != expected_count or len(job_rows) != expected_count:
            raise RuntimeError(
                "transaction boundary mismatch: "
                f"expected {expected_count} domain row/job, got "
                f"{domain_count}/{len(job_rows)}"
            )

        job_contract: dict[str, Any] | None = None
        if job_rows:
            job = job_rows[0]
            expected_state = "scheduled" if args.scheduled_delay_ms else "available"
            job_contract = {
                "contract_version": int(job["contract_version"]),
                "max_attempts": job["max_attempts"],
                "priority": job["priority"],
                "queue": job["queue"],
                "scheduled_after_create": job["scheduled_after_create"],
                "source": job["source"],
                "state": job["state"],
                "tags": list(job["tags"]),
            }
            expected_contract = {
                "contract_version": 1,
                "max_attempts": args.max_attempts,
                "priority": args.priority,
                "queue": args.queue,
                "scheduled_after_create": bool(args.scheduled_delay_ms),
                "source": "python",
                "state": expected_state,
                "tags": ["phase0", "python"],
            }
            if job_contract != expected_contract:
                raise RuntimeError(
                    "inserted River job contract mismatch: "
                    f"expected {expected_contract!r}, got {job_contract!r}"
                )

        return {
            "domain_count": domain_count,
            "job_count": len(job_rows),
            "job_contract": job_contract,
            "job_id": inserted_job_id,
            "marker": args.marker,
            "mode": "rollback" if rollback else "commit",
        }
    finally:
        await engine.dispose()


async def _probe_unique(args: argparse.Namespace) -> dict[str, Any]:
    engine = _engine(args.database_url, pgbouncer=args.pgbouncer)
    client = riverqueue.AsyncClient(riversqlalchemy.AsyncDriver(engine))
    try:
        try:
            await client.insert(
                CompatArgs(marker=args.marker),
                riverqueue.InsertOpts(
                    queue=args.queue,
                    unique_opts=riverqueue.UniqueOpts(by_args=True),
                ),
            )
        except ProgrammingError as exc:
            message = str(exc)
            expected = "no unique or exclusion constraint matching"
            sqlstate = getattr(exc.orig, "sqlstate", None) or getattr(
                exc.orig, "pgcode", None
            )
            if expected not in message or sqlstate != "42P10":
                raise
            return {
                "marker": args.marker,
                "mode": "unique",
                "status": "unsupported",
                "reason_code": "river_0_40_unique_index_contract_missing",
                "sqlstate": sqlstate,
            }
        raise RuntimeError(
            "riverqueue 0.7.0 unique insert unexpectedly succeeded; "
            "re-evaluate the Phase 0 client decision"
        )
    finally:
        await engine.dispose()


class _RollbackRequested(Exception):
    """Sentinel used to exercise transaction rollback without hiding failures."""


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", required=True)
    parser.add_argument(
        "--mode", choices=("commit", "rollback", "unique"), required=True
    )
    parser.add_argument("--marker", required=True)
    parser.add_argument("--queue", required=True)
    parser.add_argument("--priority", type=int, default=2)
    parser.add_argument("--max-attempts", type=int, default=7)
    parser.add_argument("--scheduled-delay-ms", type=int, default=0)
    parser.add_argument("--pgbouncer", action="store_true")
    return parser


async def _main(args: argparse.Namespace) -> dict[str, Any]:
    if args.mode == "unique":
        return await _probe_unique(args)
    return await _insert(args, rollback=args.mode == "rollback")


if __name__ == "__main__":
    result = asyncio.run(_main(_parser().parse_args()))
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))

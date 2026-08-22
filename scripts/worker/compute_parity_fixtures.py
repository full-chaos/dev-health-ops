#!/usr/bin/env python3
"""Producer-derived fixtures and reference execution for compute-parity manifests.

``compare_compute_outputs.py`` compares two destinations. This script is what
*fills* them, and it is deliberately the only place a compute-parity fixture is
allowed to come from:

* ``provision`` creates an isolated scratch database and applies the checked-in
  ClickHouse migrations through ``dev-hops``. The schema is never hand-written.
* ``seed`` builds input rows with the production fixture generator
  (``SyntheticDataGenerator``) and persists them through the production writers
  (``ClickHouseStore.insert_*``, ``providers.operational_migration``). No row is
  hand-authored, so the fixture keeps production row shapes by construction.
* ``clone`` copies the declared input tables from one scratch database into the
  other. Both sides then consume byte-identical input, which is what makes an
  output comparison a parity claim rather than a coincidence. The comparator
  re-verifies this independently by digesting the inputs on both sides.
* ``produce`` runs the *Python* implementation of a kind against one
  destination. Manifests name this as the reference producer; a Go lane adds
  its own producer command alongside it.

Cloning rather than re-seeding is intentional. The fixture generators anchor
their windows on ``datetime.now()``, so two independent seed runs differ by the
wall-clock gap between them. Copying removes that class of false difference at
the source instead of asking every lane to reason about it.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

# The gate applies migrations under ordering contract 2; a fixture database
# written under a different contract would not match the schema the rest of the
# platform is validated against. Applied in main() and NOT at import: setting
# it at import time leaks into every test sharing the interpreter, and
# `guard_operational_writer_tables` then fails unrelated ClickHouseStore tests
# with `stale_state ... configured=2 stored=None`. Only the FULL unit suite
# shows that -- the polluted tests live in other files.
PROCESS_ENVIRONMENT = {
    "OPERATIONAL_ORDERING_CONTRACT": "2",
    "OTEL_ENABLED": "false",
}

FORBIDDEN_DATABASES = ("default",)
# A ClickHouse database name this script is willing to put into DDL. Anything
# else -- an unexpanded `${VAR}`, a quote, a semicolon, whitespace -- is refused
# rather than quoted-and-hoped: `provision` runs DROP DATABASE, and the safe
# handling of an unrecognisable identifier is not to execute it.
DATABASE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
PARITY_ORG_ID = "8f5c1f2e-6b4a-4a1e-9f0c-2f2a2d6d5a10"
PARITY_REPO_NAME = "compute-parity/dora-pilot"
PROVIDER = "synthetic"
PROVIDER_INSTANCE = "compute-parity"


class FixtureError(RuntimeError):
    """A safe reason a fixture step cannot run."""


def database_of(dsn: str) -> str:
    return dsn.rsplit("/", 1)[-1].split("?", 1)[0]


def guard(dsn: str) -> str:
    """Return the DSN's database after proving it is safe to name in DDL."""
    database = database_of(dsn)
    if not database or database in FORBIDDEN_DATABASES:
        raise FixtureError(f"refusing_scratch_database:{database or '<none>'}")
    if not DATABASE_NAME.match(database):
        raise FixtureError(f"refusing_unrecognised_database_name:{database!r}")
    return database


def quoted(database: str) -> str:
    """Backtick-quote an already-validated identifier.

    Validation is the real defence; quoting is the second layer, so a future
    caller that reaches this without going through :func:`guard` still cannot
    terminate the identifier.
    """
    if not DATABASE_NAME.match(database):
        raise FixtureError(f"refusing_unrecognised_database_name:{database!r}")
    return f"`{database}`"


def clickhouse_client(dsn: str) -> Any:
    import clickhouse_connect

    return clickhouse_connect.get_client(dsn=dsn)


def server_client(dsn: str) -> Any:
    """A client bound to the server but not to the (possibly absent) database."""
    import clickhouse_connect

    head, _, _ = dsn.rpartition("/")
    return clickhouse_connect.get_client(dsn=f"{head}/system")


# --------------------------------------------------------------------------
# Kind registry
# --------------------------------------------------------------------------

# Input tables cloned between destinations, in dependency order.
DORA_INPUT_TABLES = (
    "repos",
    "deployments",
    "operational_services",
    "operational_service_repository_mappings",
    "operational_incidents",
)


def seed_metrics_dora(dsn: str, *, seed: int, days: int) -> dict[str, Any]:
    """Seed the DORA pilot inputs through the production fixture producers."""
    from dev_health_ops.fixtures.generator import SyntheticDataGenerator
    from dev_health_ops.providers.operational_migration import (
        IssueIncidentSource,
        map_issue_incidents,
        write_operational_batch,
    )
    from dev_health_ops.storage import ClickHouseStore

    generator = SyntheticDataGenerator(repo_name=PARITY_REPO_NAME, seed=seed)
    repo = generator.generate_repo()
    deployments = generator.generate_deployments(days=days, deployments_per_day=2)
    incidents = generator.generate_incidents(days=days, incidents_per_day=1)
    if not deployments:
        raise FixtureError("fixture_generated_no_deployments")
    if not incidents:
        raise FixtureError("fixture_generated_no_incidents")

    async def write() -> None:
        store = ClickHouseStore(dsn)
        store.org_id = PARITY_ORG_ID
        async with store:
            store.org_id = PARITY_ORG_ID
            await store.insert_repo(repo)
            await store.insert_deployments(deployments)
            sources = [
                IssueIncidentSource(
                    org_id=PARITY_ORG_ID,
                    provider=PROVIDER,
                    provider_instance_id=PROVIDER_INSTANCE,
                    repo_id=repo.id,
                    repo_full_name=PARITY_REPO_NAME,
                    external_id=incident.incident_id,
                    issue_number=None,
                    source_url=None,
                    labels=("incident",),
                    raw_status=incident.status,
                    title=incident.incident_id,
                    description=None,
                    created_at=incident.started_at,
                    resolved_at=incident.resolved_at,
                    source_version_at=incident.resolved_at or incident.started_at,
                )
                for incident in incidents
            ]
            batch = map_issue_incidents(sources)
            # `map_issue_incidents` leaves `valid_from` unset. The canonical
            # incident projection (metrics/active_incidents.py) filters on
            # `valid_from <= {as_of}`, and a NULL there compares to NULL, so an
            # unbounded mapping matches nothing and DORA silently loses its
            # incident-derived metric. Production mappings carry a validity
            # start, so the fixture carries one too: the mapping's own
            # source_version_at, which is the earliest instant the mapping is
            # known to hold.
            batch = replace(
                batch,
                service_repository_mappings=tuple(
                    replace(mapping, valid_from=mapping.source_version_at)
                    for mapping in batch.service_repository_mappings
                ),
            )
            await write_operational_batch(store, batch)

    asyncio.run(write())
    return {
        "repo_id": str(repo.id),
        "deployments": len(deployments),
        "incidents": len(incidents),
        "org_id": PARITY_ORG_ID,
    }


def produce_metrics_dora(dsn: str, *, as_of: str, days: int) -> dict[str, Any]:
    """Run the Python DORA producer against one destination."""
    from dev_health_ops.metrics.job_dora import run_dora_metrics_job

    day = _as_of_date(as_of)
    run_dora_metrics_job(
        db_url=dsn,
        day=day,
        backfill_days=days,
        repo_id=None,
        repo_name=None,
        sink="clickhouse",
        metrics=None,
        org_id=PARITY_ORG_ID,
    )
    return {"day": day.isoformat(), "backfill_days": days}


KINDS: Mapping[str, Mapping[str, Any]] = {
    "metrics.dora": {
        "input_tables": DORA_INPUT_TABLES,
        "seed": seed_metrics_dora,
        "produce": produce_metrics_dora,
    }
}


def kind_entry(kind: str) -> Mapping[str, Any]:
    if kind not in KINDS:
        raise FixtureError(f"unknown_kind:{kind}")
    return KINDS[kind]


def _as_of_date(as_of: str) -> date:
    text = (as_of or "").strip()
    if not text:
        return datetime.now(timezone.utc).date()
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError as error:
        raise FixtureError("as_of_unparseable") from error


# --------------------------------------------------------------------------
# Steps
# --------------------------------------------------------------------------


def provision(dsn: str, *, reset: bool = False) -> dict[str, Any]:
    """Create and migrate a scratch database.

    Dropping is an explicit caller decision (``--reset``), never a default. A
    DSN typo that lands on a real database must not cost that database: the
    unconditional DROP this replaced would have obliged, and `default` was the
    only name it refused.
    """
    database = guard(dsn)
    identifier = quoted(database)
    client = server_client(dsn)
    try:
        exists = bool(
            client.query(
                "SELECT count() FROM system.databases WHERE name = {name:String}",
                parameters={"name": database},
            ).result_rows[0][0]
        )
        if exists and not reset:
            raise FixtureError(f"database_exists_pass_reset_to_drop:{database}")
        if exists:
            client.command(f"DROP DATABASE {identifier}")
        client.command(f"CREATE DATABASE {identifier}")
    finally:
        client.close()
    environment = dict(os.environ)
    environment.update(PROCESS_ENVIRONMENT)
    environment.update({"CLICKHOUSE_URI": dsn, "DATABASE_URI": dsn})
    for arguments in (["upgrade"], ["status", "--check"]):
        completed = subprocess.run(  # noqa: S603 -- fixed checked-in CLI
            [_dev_hops(), "migrate", "clickhouse", *arguments],
            env=environment,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            tail = (completed.stderr or completed.stdout or "").strip().splitlines()
            raise FixtureError(
                f"migrate_failed:{' '.join(arguments)}:{tail[-1][:200] if tail else ''}"
            )
    return {"database": database, "migrated": True, "reset": reset}


def _dev_hops() -> str:
    candidate = REPO_ROOT / ".venv" / "bin" / "dev-hops"
    return str(candidate) if candidate.exists() else "dev-hops"


def clone(kind: str, from_dsn: str, to_dsn: str) -> dict[str, Any]:
    source = guard(from_dsn)
    destination = guard(to_dsn)
    if source == destination:
        raise FixtureError("clone_source_and_destination_are_one_database")
    tables = kind_entry(kind)["input_tables"]
    source_identifier = quoted(source)
    destination_identifier = quoted(destination)
    client = clickhouse_client(to_dsn)
    copied: dict[str, int] = {}
    try:
        for table in tables:
            client.command(
                f"INSERT INTO {destination_identifier}.{table} "
                f"SELECT * FROM {source_identifier}.{table}"
            )
            result = client.query(
                f"SELECT count() FROM {destination_identifier}.{table}"
            )
            copied[table] = int(result.result_rows[0][0]) if result.result_rows else 0
    finally:
        client.close()
    return {"tables": copied}


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _dsn(argument: str | None) -> str:
    dsn = argument or os.environ.get("PARITY_DSN", "")
    if not dsn:
        raise FixtureError("dsn_required")
    guard(dsn)
    return dsn


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    steps = parser.add_subparsers(dest="step", required=True)

    provision_step = steps.add_parser(
        "provision", help="Create + migrate a scratch db."
    )
    provision_step.add_argument("--dsn")
    provision_step.add_argument(
        "--reset",
        action="store_true",
        help="Drop the database first if it already exists. Required to overwrite one.",
    )

    seed_step = steps.add_parser("seed", help="Seed producer-derived input rows.")
    seed_step.add_argument("--kind", required=True)
    seed_step.add_argument("--dsn")
    seed_step.add_argument("--seed", type=int, default=20260822)
    seed_step.add_argument("--days", type=int, default=14)

    clone_step = steps.add_parser(
        "clone", help="Copy declared input tables between dbs."
    )
    clone_step.add_argument("--kind", required=True)
    clone_step.add_argument("--from-dsn", required=True)
    clone_step.add_argument("--to-dsn", required=True)

    produce_step = steps.add_parser(
        "produce", help="Run the Python producer for a kind."
    )
    produce_step.add_argument("--kind", required=True)
    produce_step.add_argument("--dsn")
    produce_step.add_argument("--as-of", default="")
    produce_step.add_argument("--days", type=int, default=14)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    import json

    args = build_parser().parse_args(argv)
    for name, value in PROCESS_ENVIRONMENT.items():
        os.environ.setdefault(name, value)
    try:
        result: dict[str, Any]
        if args.step == "provision":
            result = provision(_dsn(args.dsn), reset=args.reset)
        elif args.step == "seed":
            handler: Callable[..., dict[str, Any]] = kind_entry(args.kind)["seed"]
            result = handler(_dsn(args.dsn), seed=args.seed, days=args.days)
        elif args.step == "clone":
            result = clone(args.kind, args.from_dsn, args.to_dsn)
        else:
            handler = kind_entry(args.kind)["produce"]
            as_of = args.as_of or os.environ.get("PARITY_AS_OF", "")
            result = handler(_dsn(args.dsn), as_of=as_of, days=args.days)
    except FixtureError as error:
        print(json.dumps({"status": "error", "failure": str(error)}), file=sys.stderr)
        return 2
    print(json.dumps({"status": "ok", "step": args.step, **result}, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

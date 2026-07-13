#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import contextlib
import inspect
import io
import logging
import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

_DOTENV_INTERPOLATION_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-(.*?))?\}")
_DOTENV_MALFORMED_INTERPOLATION_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)?")

# Sentinel default for the root --org argument (CHAOS-2780 codex HIGH, round
# 2). See ``_resolve_org`` for the full rationale: argparse itself must be
# the thing that decides whether --org was typed, not a re-implementation
# of its token/abbreviation matching.
_ORG_UNSET = object()


def resolve_org_id(ns: argparse.Namespace) -> str | None:
    """Return the org_id from the CLI namespace, or ``None``.

    NOTE: This is plumbing only — run functions accept org_id but don't yet
    filter queries by it. Query-level scoping (``WHERE org_id = …``) is a
    follow-up tracked per-function.
    """
    return getattr(ns, "org", None) or None


def _expand_dotenv_value(key: str, value: str) -> str:
    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        fallback = match.group(2)
        env_value = os.environ.get(name)
        if env_value is not None and env_value != "":
            return env_value
        if fallback is not None:
            return fallback
        if env_value == "":
            return ""
        raise ValueError(
            f".env value for {key} references {name}, but {name} is not set"
        )

    expanded = _DOTENV_INTERPOLATION_RE.sub(replace, value)
    malformed = _DOTENV_MALFORMED_INTERPOLATION_RE.search(expanded)
    if malformed:
        name = malformed.group(1)
        detail = f" for {name}" if name else ""
        raise ValueError(f".env value for {key} has malformed interpolation{detail}")
    return expanded


def _load_dotenv(path: Path) -> int:
    """
    Load a .env file into process environment (without overriding existing vars).
    Keeps dependencies minimal (avoids python-dotenv).
    """
    if not path.exists():
        return 0
    loaded = 0
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or key in os.environ:
            continue
        if (len(value) >= 2) and ((value[0] == value[-1]) and value[0] in {"'", '"'}):
            value = value[1:-1]
        value = _expand_dotenv_value(key, value)
        os.environ[key] = value
        loaded += 1
    return loaded


def _resolve_first_org_id(db_url: str | None) -> str | None:
    if not db_url:
        return None
    db_url_sync = db_url.replace("postgresql+asyncpg://", "postgresql://", 1)
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(db_url_sync)
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    text("SELECT id FROM organizations ORDER BY created_at ASC LIMIT 1")
                ).first()
                if row is None:
                    return None
                return str(row[0])
        finally:
            engine.dispose()
    except Exception:
        return None


def get_postgres_session():
    from dev_health_ops.db import get_postgres_session as real_get_postgres_session

    return real_get_postgres_session()


async def cleanup_expired(db) -> int:
    from dev_health_ops.api.services.refresh_tokens import (
        cleanup_expired as real_cleanup_expired,
    )

    return await real_cleanup_expired(db)


async def _run_refresh_token_cleanup() -> int:
    async with get_postgres_session() as db:
        deleted_count = await cleanup_expired(db)
        await db.commit()
    return deleted_count


async def _cmd_maintenance_cleanup_tokens(_ns: argparse.Namespace) -> int:
    deleted_count = await _run_refresh_token_cleanup()
    logging.getLogger(__name__).info("Deleted %s expired refresh tokens", deleted_count)
    return 0


async def _cmd_maintenance_cleanup_all(_ns: argparse.Namespace) -> int:
    deleted_count = await _run_refresh_token_cleanup()
    total_deleted = deleted_count
    logging.getLogger(__name__).info(
        "Maintenance cleanup complete: deleted=%s total=%s",
        deleted_count,
        total_deleted,
    )
    return 0


def _cmd_maintenance_scrub_error_text(ns: argparse.Namespace) -> int:
    from dev_health_ops.maintenance.scrub_error_text import run_scrub_error_text

    return run_scrub_error_text(ns)


# ---------------------------------------------------------------------------
# Recommendations commands
# ---------------------------------------------------------------------------


def _cmd_recommendations_compute(ns: argparse.Namespace) -> int:
    """Compute rule-based recommendations for a team and persist via ClickHouse sink."""
    import json
    from datetime import date, datetime, timezone

    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
    from dev_health_ops.recommendations import registry as recommendations_registry
    from dev_health_ops.recommendations.engine import RuleEngine
    from dev_health_ops.recommendations.loader import ClickHouseMetricsLoader

    analytics_db = getattr(ns, "analytics_db", None) or os.getenv("CLICKHOUSE_URI", "")
    if not analytics_db:
        logging.getLogger(__name__).error(
            "--analytics-db / CLICKHOUSE_URI is required for recommendations compute"
        )
        return 1

    team_id: str = ns.team
    window: str = ns.window
    org_id: str = getattr(ns, "org", None) or ""
    now = datetime.now(timezone.utc)

    # Override window bounds if --since / --until supplied
    if getattr(ns, "since", None) and getattr(ns, "until", None):
        since_date = date.fromisoformat(ns.since)
        until_date = date.fromisoformat(ns.until)
        window_days = (until_date - since_date).days
        now = datetime(
            until_date.year, until_date.month, until_date.day, tzinfo=timezone.utc
        )
        window = str(window_days)

    sink = ClickHouseMetricsSink(dsn=analytics_db)
    loader = ClickHouseMetricsLoader(client=sink.client, org_id=org_id)
    engine = RuleEngine(registry=recommendations_registry, loader=loader, now=now)

    try:
        # Full state: fired recommendations AND explicit fired=False tombstones
        # so a recovered signal is cleared, not left lingering (CHAOS-2373).
        records = engine.evaluate_state(team_id=team_id, window=window, org_id=org_id)
    except Exception as exc:
        logging.getLogger(__name__).error(
            "Recommendations evaluation failed for team=%r: %s", team_id, exc
        )
        return 1

    sink.write_recommendations(records)
    sink.close()

    fired = [r for r in records if r.fired]
    log = logging.getLogger(__name__)
    log.info(
        "recommendations compute: team=%r window=%s fired=%d rows=%d",
        team_id,
        window,
        len(fired),
        len(records),
    )
    if getattr(ns, "output_json", False):
        import sys
        from dataclasses import asdict

        print(
            json.dumps([asdict(r) for r in fired], default=str),
            file=sys.stdout,
        )
    return 0


def _register_recommendations_commands(subparsers: argparse._SubParsersAction) -> None:
    compute = subparsers.add_parser(
        "compute",
        help="Evaluate recommendation rules for a team and write results to ClickHouse.",
    )
    compute.add_argument("--team", required=True, help="Team ID to evaluate.")
    compute.add_argument(
        "--window",
        default="7d",
        help="Evaluation window, e.g. '7d' or '14d'. Default: 7d.",
    )
    compute.add_argument(
        "--since",
        default=None,
        metavar="YYYY-MM-DD",
        help="Override window start (exclusive end = --until).",
    )
    compute.add_argument(
        "--until",
        default=None,
        metavar="YYYY-MM-DD",
        help="Override window end (inclusive). Requires --since.",
    )
    compute.add_argument(
        "--output-json",
        action="store_true",
        help="Print fired recommendations as JSON to stdout.",
    )
    compute.set_defaults(func=_cmd_recommendations_compute)


_GLOBAL_FLAG_SPECS: tuple[tuple[tuple[str, ...], dict[str, object]], ...] = (
    (
        ("--log-level",),
        {"dest": "log_level", "help": "(global) Logging level. See root --help."},
    ),
    (("--db",), {"dest": "db", "help": "(global) PostgreSQL URI. See root --help."}),
    (
        ("--analytics-db",),
        {"dest": "analytics_db", "help": "(global) ClickHouse URI. See root --help."},
    ),
    (
        ("--org",),
        {
            "dest": "org",
            "help": "(global) Organization ID for multi-tenant scoping. See root --help.",
        },
    ),
    (
        ("-l", "--llm-provider"),
        {"dest": "llm_provider", "help": "(global) LLM provider. See root --help."},
    ),
    (
        ("-m", "--model"),
        {"dest": "model", "help": "(global) LLM model. See root --help."},
    ),
)


def _propagate_global_args_to_subparsers(parser: argparse.ArgumentParser) -> None:
    """Re-add root-parser globals on every leaf subparser.

    Argparse normally requires global flags (e.g. ``--org``) to appear BEFORE
    the subcommand. This walks the subparser tree and adds each global flag
    to every leaf parser with ``default=SUPPRESS`` so users can write either:

        dev-hops --org X fixtures generate ...
        dev-hops fixtures generate --org X ...

    Both forms populate ``ns.<dest>``. ``SUPPRESS`` ensures omitting the flag
    on the leaf does not clobber the value supplied on the root parser.
    """
    visited: set[int] = set()

    def walk(p: argparse.ArgumentParser) -> None:
        if id(p) in visited:
            return
        visited.add(id(p))
        sub_actions = [
            a for a in p._actions if isinstance(a, argparse._SubParsersAction)
        ]
        if sub_actions:
            for sa in sub_actions:
                for child in sa.choices.values():
                    walk(child)
            return
        # leaf parser: attach global flags if not already present
        existing = {opt for a in p._actions for opt in a.option_strings}
        for option_strings, kwargs in _GLOBAL_FLAG_SPECS:
            if any(opt in existing for opt in option_strings):
                continue
            try:
                p.add_argument(
                    *option_strings,
                    default=argparse.SUPPRESS,
                    # help is set per-spec to advertise the flag on every leaf
                    **kwargs,  # type: ignore[arg-type]
                )
            except argparse.ArgumentError as exc:
                # Best-effort propagation: some leaf parsers may already define
                # an equivalent/conflicting option. Keep building the CLI tree.
                logging.debug(
                    "Skipping global flag propagation for %s on parser %s: %s",
                    option_strings,
                    p.prog,
                    exc,
                )

    walk(parser)


def _is_help_invocation(argv: list[str] | None) -> bool:
    args = sys.argv[1:] if argv is None else argv
    return any(arg in {"-h", "--help"} for arg in args)


def _is_workers_inspect_json_invocation(argv: list[str] | None) -> bool:
    args = sys.argv[1:] if argv is None else argv
    if "workers" not in args or "inspect" not in args:
        return False
    if "--output=json" in args:
        return True
    return any(
        arg == "--output" and index + 1 < len(args) and args[index + 1] == "json"
        for index, arg in enumerate(args)
    )


def _is_push_invocation(argv: list[str] | None) -> bool:
    """`push` subcommands print their primary result to stdout (sample's raw
    envelope JSON, `--json` mode's single JSON object) and are frequently
    piped (``push sample --all | push validate -``, CI log capture) --
    Sentry/OTel init noise and their background-exporter retry chatter
    landing on stdout (same leak `_is_workers_inspect_json_invocation`
    exists for) would corrupt that output, so every `push` invocation gets
    the same quiet treatment regardless of `--json`."""
    args = sys.argv[1:] if argv is None else argv
    return bool(args) and args[0] == "push"


def _is_service_credential_invocation(argv: list[str] | None) -> bool:
    args = sys.argv[1:] if argv is None else argv
    return "service-credentials" in args


def _route_logs_to_stderr() -> None:
    for handler in logging.getLogger().handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.setStream(sys.stderr)


@contextlib.contextmanager
def _suppress_parser_construction_noise():
    previous_disable_level = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        with (
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            yield
    finally:
        logging.disable(previous_disable_level)


def _should_resolve_org(ns: argparse.Namespace) -> bool:
    if getattr(ns, "org", None) is not None:
        return False
    # The planner-config audit defaults to ALL orgs; never auto-resolve it to a
    # single (first) org, or the cross-org audit silently scans only one org.
    if (
        getattr(ns, "command", None) == "audit"
        and getattr(ns, "audit_command", None) == "planner-configs"
    ):
        return False
    # The error-text scrub defaults to ALL orgs too -- its own --org flag is
    # an explicit opt-in scope, so a missing --org must never silently
    # narrow it to a single (first) org.
    if (
        getattr(ns, "command", None) == "maintenance"
        and getattr(ns, "maintenance_command", None) == "scrub-error-text"
    ):
        return False
    # `push` runs against a customer's own FullChaos org over HTTP, usually
    # from a CI runner with no local DB at all -- auto-resolving --org to
    # "the first org in the local Postgres DB" is actively wrong here (and
    # would silently push to the wrong org if it ever did have DB access).
    # `push batch`/`push status` resolve their own --org/FULLCHAOS_ORG_ID
    # (CHAOS-2700 decision 12).
    if getattr(ns, "command", None) == "push":
        return False
    return not (
        getattr(ns, "command", None) == "migrate"
        and getattr(ns, "migrate_command", None) == "clickhouse"
        and getattr(ns, "ch_command", None) == "repair"
    )


# ---------------------------------------------------------------------------
# Preflight requirement checks
# ---------------------------------------------------------------------------
# Many subcommands need a database connection (ClickHouse and/or PostgreSQL)
# and/or an organization id that are supplied via *global* flags or env vars
# (``--analytics-db``/``CLICKHOUSE_URI``, ``--db``/``POSTGRES_URI``,
# ``--org``/``ORG_ID``). Because these are global+env, individual subparsers
# cannot mark them ``required=True``. Without a preflight, such commands fail
# deep inside their handler with a logged error or a raw traceback (exit 1)
# instead of a fast argparse-style usage error (exit 2).
#
# Each leaf command declares the inputs it needs in ``_COMMAND_REQUIREMENTS``;
# ``run_preflight_checks`` resolves them the same way the handlers do and calls
# ``parser.error(...)`` so the user gets a usage message naming exactly what is
# missing. The same requirements are appended to each command's ``--help``.
_REQ_CLICKHOUSE = "clickhouse"
_REQ_POSTGRES = "postgres"
_REQ_ORG = "org"
_REQ_SINK_DB = "sink_db"

# Stable display order for messages/epilogs.
_REQUIREMENT_ORDER: tuple[str, ...] = (
    _REQ_CLICKHOUSE,
    _REQ_POSTGRES,
    _REQ_ORG,
    _REQ_SINK_DB,
)

_COMMAND_REQUIREMENTS: dict[tuple[str, ...], frozenset[str]] = {
    # --- metrics (ClickHouse analytics store) ---
    ("metrics", "daily"): frozenset({_REQ_CLICKHOUSE}),
    ("metrics", "dora"): frozenset({_REQ_CLICKHOUSE}),
    ("metrics", "complexity"): frozenset({_REQ_CLICKHOUSE}),
    ("metrics", "release-impact"): frozenset({_REQ_CLICKHOUSE}),
    ("metrics", "validate-flags"): frozenset({_REQ_CLICKHOUSE}),
    ("metrics", "rebuild"): frozenset({_REQ_CLICKHOUSE}),
    ("metrics", "compounding-risk"): frozenset({_REQ_CLICKHOUSE, _REQ_ORG}),
    # capacity takes its ClickHouse DSN via its own required --db flag.
    ("metrics", "capacity"): frozenset({_REQ_SINK_DB}),
    # --- sync (persist to ClickHouse analytics store) ---
    ("sync", "git"): frozenset({_REQ_CLICKHOUSE}),
    ("sync", "prs"): frozenset({_REQ_CLICKHOUSE}),
    ("sync", "blame"): frozenset({_REQ_CLICKHOUSE}),
    ("sync", "cicd"): frozenset({_REQ_CLICKHOUSE}),
    ("sync", "deployments"): frozenset({_REQ_CLICKHOUSE}),
    ("sync", "incidents"): frozenset({_REQ_CLICKHOUSE}),
    ("sync", "security"): frozenset({_REQ_CLICKHOUSE}),
    ("sync", "tests"): frozenset({_REQ_CLICKHOUSE}),
    ("sync", "work-items"): frozenset({_REQ_CLICKHOUSE}),
    ("sync", "teams"): frozenset({_REQ_CLICKHOUSE}),
    ("teams", "reconcile"): frozenset({_REQ_CLICKHOUSE, _REQ_POSTGRES, _REQ_ORG}),
    # --- audit (read ClickHouse analytics store) ---
    ("audit", "perf"): frozenset({_REQ_CLICKHOUSE}),
    ("audit", "schema"): frozenset({_REQ_CLICKHOUSE}),
    ("audit", "planner-configs"): frozenset({_REQ_POSTGRES}),
    # --- other ClickHouse-backed commands ---
    ("recommendations", "compute"): frozenset({_REQ_CLICKHOUSE}),
    ("investment", "materialize"): frozenset({_REQ_CLICKHOUSE}),
    ("ai", "allowlist", "list"): frozenset({_REQ_CLICKHOUSE, _REQ_ORG}),
    ("ai", "allowlist", "set"): frozenset({_REQ_CLICKHOUSE, _REQ_ORG}),
    # --- PostgreSQL semantic store ---
    ("admin", "users", "create"): frozenset({_REQ_POSTGRES}),
    ("admin", "users", "list"): frozenset({_REQ_POSTGRES}),
    ("admin", "users", "update"): frozenset({_REQ_POSTGRES}),
    ("admin", "orgs", "create"): frozenset({_REQ_POSTGRES}),
    ("admin", "orgs", "delete"): frozenset({_REQ_POSTGRES}),
    ("admin", "orgs", "list"): frozenset({_REQ_POSTGRES}),
    ("admin", "llm-settings", "get"): frozenset({_REQ_POSTGRES, _REQ_ORG}),
    ("admin", "llm-settings", "set"): frozenset({_REQ_POSTGRES, _REQ_ORG}),
    ("admin", "llm-settings", "delete"): frozenset({_REQ_POSTGRES, _REQ_ORG}),
    ("admin", "features", "seed"): frozenset({_REQ_POSTGRES}),
    ("admin", "billing", "seed"): frozenset({_REQ_POSTGRES}),
    ("admin", "billing", "list"): frozenset({_REQ_POSTGRES}),
    ("admin", "billing", "pull-stripe"): frozenset({_REQ_POSTGRES}),
    ("admin", "billing", "sync-stripe"): frozenset({_REQ_POSTGRES}),
    ("admin", "bundles", "create"): frozenset({_REQ_POSTGRES}),
    ("admin", "bundles", "list"): frozenset({_REQ_POSTGRES}),
    ("admin", "bundles", "assign-plan"): frozenset({_REQ_POSTGRES}),
    ("admin", "bundles", "assign-org"): frozenset({_REQ_POSTGRES}),
    ("billing", "reconcile"): frozenset({_REQ_POSTGRES}),
    ("service-credentials", "create"): frozenset({_REQ_POSTGRES}),
    ("service-credentials", "list"): frozenset({_REQ_POSTGRES}),
    ("service-credentials", "rotate"): frozenset({_REQ_POSTGRES}),
    ("service-credentials", "revoke"): frozenset({_REQ_POSTGRES}),
    ("backfill", "run"): frozenset({_REQ_POSTGRES}),
    ("maintenance", "scrub-error-text"): frozenset({_REQ_POSTGRES}),
    # --- migrations that connect to a live database ---
    ("migrate", "clickhouse", "upgrade"): frozenset({_REQ_CLICKHOUSE}),
    ("migrate", "clickhouse", "status"): frozenset({_REQ_CLICKHOUSE}),
    ("migrate", "clickhouse", "repair"): frozenset({_REQ_CLICKHOUSE}),
    ("migrate", "postgres", "upgrade"): frozenset({_REQ_POSTGRES}),
    ("migrate", "postgres", "downgrade"): frozenset({_REQ_POSTGRES}),
    ("migrate", "postgres", "current"): frozenset({_REQ_POSTGRES}),
    ("migrate", "upgrade"): frozenset({_REQ_POSTGRES}),
    ("migrate", "downgrade"): frozenset({_REQ_POSTGRES}),
    ("migrate", "current"): frozenset({_REQ_POSTGRES}),
    # Bare ``migrate postgres`` / ``migrate clickhouse`` default to upgrade.
    ("migrate", "postgres"): frozenset({_REQ_POSTGRES}),
    ("migrate", "clickhouse"): frozenset({_REQ_CLICKHOUSE}),
}

_REQUIREMENT_MESSAGES: dict[str, str] = {
    _REQ_CLICKHOUSE: (
        "ClickHouse analytics database \u2014 pass --analytics-db or set CLICKHOUSE_URI "
        "(e.g. clickhouse://ch:ch@localhost:8123/default)"
    ),
    _REQ_POSTGRES: (
        "PostgreSQL semantic database \u2014 pass --db or set POSTGRES_URI/DATABASE_URI "
        "(e.g. postgresql+asyncpg://user:pass@localhost:5432/devhealth)"
    ),
    _REQ_ORG: (
        "organization id \u2014 pass --org or set ORG_ID "
        "(could not auto-resolve one from the database)"
    ),
    _REQ_SINK_DB: (
        "ClickHouse analytics database \u2014 pass --db or set CLICKHOUSE_URI "
        "(e.g. clickhouse://ch:ch@localhost:8123/default)"
    ),
}

_REQUIREMENT_HELP_LABELS: dict[str, str] = {
    _REQ_CLICKHOUSE: "ClickHouse (--analytics-db / CLICKHOUSE_URI)",
    _REQ_POSTGRES: "PostgreSQL (--db / POSTGRES_URI)",
    _REQ_ORG: "organization (--org / ORG_ID)",
    _REQ_SINK_DB: "ClickHouse (--db / CLICKHOUSE_URI)",
}


def _clickhouse_present(ns: argparse.Namespace) -> bool:
    return bool(getattr(ns, "analytics_db", None) or os.getenv("CLICKHOUSE_URI"))


def _clickhouse_value(ns: argparse.Namespace) -> str | None:
    return getattr(ns, "analytics_db", None) or os.getenv("CLICKHOUSE_URI")


def _postgres_present(ns: argparse.Namespace) -> bool:
    return bool(
        getattr(ns, "db", None)
        or os.getenv("POSTGRES_URI")
        or os.getenv("DATABASE_URI")
        or os.getenv("DATABASE_URL")
    )


def _org_present(ns: argparse.Namespace) -> bool:
    return bool(getattr(ns, "org", None) or os.getenv("ORG_ID"))


def _sink_db_present(ns: argparse.Namespace) -> bool:
    return bool(getattr(ns, "db", None) or os.getenv("CLICKHOUSE_URI"))


def _sink_db_value(ns: argparse.Namespace) -> str | None:
    return getattr(ns, "db", None) or os.getenv("CLICKHOUSE_URI")


_REQUIREMENT_PRESENCE = {
    _REQ_CLICKHOUSE: _clickhouse_present,
    _REQ_POSTGRES: _postgres_present,
    _REQ_ORG: _org_present,
    _REQ_SINK_DB: _sink_db_present,
}


def missing_requirements(ns: argparse.Namespace) -> list[str]:
    """Return human-readable messages for each unmet requirement of ``ns``."""
    requires = getattr(ns, "_requires", None) or frozenset()
    missing: list[str] = []
    for token in _REQUIREMENT_ORDER:
        if token in requires and not _REQUIREMENT_PRESENCE[token](ns):
            missing.append(_REQUIREMENT_MESSAGES[token])
    return missing


def run_preflight_checks(
    root_parser: argparse.ArgumentParser, ns: argparse.Namespace
) -> None:
    """Fast-fail with a usage error if the chosen command's inputs are missing."""
    missing = missing_requirements(ns)
    leaf = getattr(ns, "_leaf_parser", None) or root_parser
    if missing:
        leaf.error("missing required input(s):\n  - " + "\n  - ".join(missing))

    requires = getattr(ns, "_requires", None) or frozenset()
    from dev_health_ops.db import validate_sink_uri_scheme

    try:
        if _REQ_POSTGRES in requires and (uri := getattr(ns, "db", None)):
            from dev_health_ops.db import validate_postgres_uri_scheme

            validate_postgres_uri_scheme(uri)
        if _REQ_CLICKHOUSE in requires and (uri := _clickhouse_value(ns)):
            validate_sink_uri_scheme(uri)
        if _REQ_SINK_DB in requires and (uri := _sink_db_value(ns)):
            validate_sink_uri_scheme(uri)
    except ValueError as exc:
        leaf.error(str(exc))


def _attach_preflight_metadata(parser: argparse.ArgumentParser) -> None:
    """Attach ``_leaf_parser``/``_requires`` defaults and a help epilog per leaf."""

    def walk(p: argparse.ArgumentParser, path: tuple[str, ...]) -> None:
        # Attach to any *dispatchable* parser (one that has a handler). This
        # includes leaf commands and intermediate parsers that set a default
        # ``func`` (e.g. bare ``migrate postgres`` -> upgrade), so those forms
        # are guarded too. Always recurse so more-specific child commands can
        # override the metadata on the same namespace.
        if p.get_default("func") is not None:
            requires = _COMMAND_REQUIREMENTS.get(path, frozenset())
            p.set_defaults(_leaf_parser=p, _requires=requires)
            labels = [
                _REQUIREMENT_HELP_LABELS[t] for t in _REQUIREMENT_ORDER if t in requires
            ]
            if labels:
                note = "Requires: " + ", ".join(labels) + "."
                p.epilog = f"{p.epilog}\n\n{note}" if p.epilog else note
        for sa in (a for a in p._actions if isinstance(a, argparse._SubParsersAction)):
            for name, child in sa.choices.items():
                walk(child, path + (name,))

    walk(parser, ())


def build_parser() -> argparse.ArgumentParser:
    import dev_health_ops.api.billing.cli as billing_cli
    import dev_health_ops.backfill.cli as backfill_cli
    from dev_health_ops import migrate as migrate_mod
    from dev_health_ops import service_credentials
    from dev_health_ops.api import runner as api_runner
    from dev_health_ops.api.admin import cli as admin_cli
    from dev_health_ops.audit import (
        completeness,
        coverage,
        perf,
        schema,
    )
    from dev_health_ops.audit.ai_governance import cli as ai_governance_cli
    from dev_health_ops.fixtures import runner as fixtures_runner
    from dev_health_ops.metrics import (
        job_capacity,
        job_complexity_db,
        job_compounding_risk,
        job_daily,
        job_dora,
        job_ff_validation,
        job_release_impact,
        job_work_items,
    )
    from dev_health_ops.processors import sync as sync_processor
    from dev_health_ops.providers import teams as teams_provider
    from dev_health_ops.push import cli as push_cli
    from dev_health_ops.work_graph import runner as work_graph_runner
    from dev_health_ops.workers import runner as workers_runner

    parser = argparse.ArgumentParser(
        prog="dev-health-ops",
        description="Sync git data and compute developer health metrics.",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="Logging level (DEBUG, INFO, WARNING). Defaults to env LOG_LEVEL or INFO.",
    )
    parser.add_argument(
        "--db",
        default=os.getenv("POSTGRES_URI") or os.getenv("DATABASE_URI"),
        help="PostgreSQL URI for semantic data. Env: POSTGRES_URI or DATABASE_URI",
    )
    parser.add_argument(
        "--analytics-db",
        dest="analytics_db",
        default=os.getenv("CLICKHOUSE_URI"),
        help="ClickHouse URI for analytics data (metrics, work items). Env: CLICKHOUSE_URI",
    )
    parser.add_argument(
        "--org",
        # Sentinel, not os.getenv("ORG_ID") directly -- see _resolve_org.
        # main() resolves this to the ORG_ID env fallback immediately after
        # parse_args(), before any other code reads ns.org, so this is
        # transparent to every existing caller of main(); it only matters to
        # code that calls build_parser().parse_args() directly (tests must
        # call _resolve_org(ns) themselves in that case).
        default=_ORG_UNSET,
        help="Organization ID for multi-tenant scoping. Env: ORG_ID. If omitted, resolve from DB.",
    )
    from dev_health_ops.llm.cli import add_llm_arguments

    add_llm_arguments(parser)
    sub = parser.add_subparsers(dest="command", required=True)

    # ---- sync ----
    sync_parser = sub.add_parser("sync", help="Sync data from various sources.")
    sync_subparsers = sync_parser.add_subparsers(dest="sync_command", required=True)

    # Register sync commands (git, prs, blame, cicd, deployments, incidents)
    sync_processor.register_commands(sync_subparsers)
    # Register team sync
    teams_provider.register_commands(sync_subparsers)
    # Register work-items sync
    job_work_items.register_commands(sync_subparsers)

    # ---- metrics ----
    metrics_parser = sub.add_parser("metrics", help="Compute metrics.")
    metrics_subparsers = metrics_parser.add_subparsers(
        dest="metrics_command", required=True
    )

    job_daily.register_commands(metrics_subparsers)
    job_complexity_db.register_commands(metrics_subparsers)
    job_dora.register_commands(metrics_subparsers)
    job_capacity.register_commands(metrics_subparsers)
    job_release_impact.register_commands(metrics_subparsers)
    job_ff_validation.register_commands(metrics_subparsers)
    job_compounding_risk.register_commands(metrics_subparsers)

    # ---- audit ----
    audit_parser = sub.add_parser("audit", help="Run diagnostic audits.")
    audit_subparsers = audit_parser.add_subparsers(dest="audit_command", required=True)

    completeness.register_commands(audit_subparsers)
    schema.register_commands(audit_subparsers)
    perf.register_commands(audit_subparsers)
    coverage.register_commands(audit_subparsers)

    # ---- fixtures ----
    fixtures_runner.register_commands(sub)

    # ---- api ----
    api_runner.register_commands(sub)

    billing_cli.register_commands(sub)
    service_credentials.register_commands(sub)

    # ---- admin (user/org management) ----
    admin_cli.register_commands(sub)

    # ---- ai governance administration ----
    ai_governance_cli.register_commands(sub)

    # ---- work-graph & investment ----
    work_graph_runner.register_commands(sub)

    backfill_cli.register_backfill_commands(sub)

    # ---- push (customer-push external ingestion CLI, CHAOS-2700) ----
    push_cli.register_commands(sub)

    # ---- recommendations ----
    rec_parser = sub.add_parser(
        "recommendations", help="Compute and persist rule-based recommendations."
    )
    rec_subparsers = rec_parser.add_subparsers(
        dest="recommendations_command", required=True
    )
    _register_recommendations_commands(rec_subparsers)

    # ---- migrate ----
    migrate_mod.register_commands(sub)

    # ---- workers ----
    workers_parser = sub.add_parser(
        "workers", help="Manage background worker processes."
    )
    workers_subparsers = workers_parser.add_subparsers(
        dest="workers_command", required=True
    )
    workers_runner.register_commands(workers_subparsers)

    maintenance_parser = sub.add_parser(
        "maintenance", help="Run maintenance operations."
    )
    maintenance_subparsers = maintenance_parser.add_subparsers(
        dest="maintenance_command", required=True
    )

    cleanup_tokens_parser = maintenance_subparsers.add_parser(
        "cleanup-tokens", help="Delete expired refresh tokens."
    )
    cleanup_tokens_parser.set_defaults(func=_cmd_maintenance_cleanup_tokens)

    cleanup_all_parser = maintenance_subparsers.add_parser(
        "cleanup-all", help="Run all maintenance cleanup tasks."
    )
    cleanup_all_parser.set_defaults(func=_cmd_maintenance_cleanup_all)

    scrub_error_text_parser = maintenance_subparsers.add_parser(
        "scrub-error-text",
        help=(
            "Scrub pre-existing raw credential material from legacy "
            "error-text columns (CHAOS-2780). Dry-run unless --apply is passed."
        ),
    )
    scrub_error_text_parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Apply the scrub. Without this flag, reports per-column "
            "would-change counts only and mutates nothing."
        ),
    )
    scrub_error_text_parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Rows to scan per keyset-paginated batch (default: 1000).",
    )
    scrub_error_text_parser.set_defaults(func=_cmd_maintenance_scrub_error_text)

    _propagate_global_args_to_subparsers(parser)
    _attach_preflight_metadata(parser)
    return parser


def _resolve_org(ns: argparse.Namespace) -> None:
    """Resolve the root ``--org`` argument's sentinel default into its final
    value, and record whether ``--org`` was actually typed in
    ``ns.org_explicit``.

    The root ``--org`` argument defaults to the ``_ORG_UNSET`` sentinel
    (not ``os.getenv("ORG_ID")`` directly) so THIS function -- not a
    re-implementation of argparse's own token/abbreviation matching -- is
    what decides whether ``--org`` was typed. An earlier version of this
    fix scanned raw argv for literal ``--org``/``--org=`` tokens, but
    argparse's ``allow_abbrev=True`` default means ``--or X`` (or even
    ``--o X``, unambiguous here) also populates ``ns.org`` -- the argv scan
    missed that, misclassifying an operator's INTENTIONALLY-scoped
    ``--apply`` as "no flag" and silently widening it to ALL orgs (the
    inverse, more dangerous failure vs. the original ORG_ID-env-leak bug;
    CHAOS-2780 codex HIGH, round 2). Comparing ``ns.org`` against the
    sentinel by identity is correct for every form argparse recognizes --
    full, ``--org=X``, or any unambiguous abbreviation, before or after the
    subcommand -- because it relies on argparse's own action-dispatch
    (whichever form matched, the action fired and overwrote the sentinel)
    rather than re-parsing anything ourselves.

    That ambiguity-freedom is harmless for commands that WANT env-var
    convenience (the common case, preserved below), but commands whose
    "omit --org" semantics mean "ALL orgs" (``migrate clickhouse repair``,
    ``audit planner-configs``, ``maintenance scrub-error-text``) must not
    silently narrow to one tenant just because the operator's shell happens
    to export ORG_ID -- normal env usage, not an explicit scope opt-in.

    MUST run immediately after ``parser.parse_args()``, before ANY code
    (including ``_should_resolve_org``) reads ``ns.org`` -- until this
    runs, ``ns.org`` may still be the raw sentinel object, not a string or
    None. Code that calls ``build_parser().parse_args(...)`` directly
    (bypassing ``main()``) must call this itself before reading ``ns.org``
    or ``ns.org_explicit``.
    """
    ns.org_explicit = ns.org is not _ORG_UNSET
    if not ns.org_explicit:
        ns.org = os.getenv("ORG_ID")


def main(argv: list[str] | None = None) -> int:
    if os.getenv("DISABLE_DOTENV", "").strip().lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        try:
            _load_dotenv(REPO_ROOT / ".env")
        except ValueError as exc:
            print(f"dotenv error: {exc}", file=sys.stderr)
            return 2

    quiet_json_inspect = _is_workers_inspect_json_invocation(
        argv
    ) or _is_push_invocation(argv)
    service_credentials_output = _is_service_credential_invocation(argv)
    previous_otel_enabled = os.environ.get("OTEL_ENABLED")
    if quiet_json_inspect:
        os.environ["OTEL_ENABLED"] = "false"

    try:
        if (
            _is_help_invocation(argv)
            or quiet_json_inspect
            or service_credentials_output
        ):
            with _suppress_parser_construction_noise():
                parser = build_parser()
        else:
            parser = build_parser()
        ns = parser.parse_args(argv)
        _resolve_org(ns)

        if _should_resolve_org(ns):
            ns.org = _resolve_first_org_id(getattr(ns, "db", None))

        run_preflight_checks(parser, ns)
        if _REQ_POSTGRES in (getattr(ns, "_requires", None) or frozenset()) and ns.db:
            os.environ["POSTGRES_URI"] = ns.db

        if service_credentials_output:
            _route_logs_to_stderr()

        level_name = str(getattr(ns, "log_level", "") or "INFO").upper()
        level = getattr(logging, level_name, logging.INFO)
        logging.basicConfig(
            level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
        )

        from dev_health_ops.api.middleware.rate_limit import (
            log_rate_limit_configuration,
        )

        if not (
            (
                getattr(ns, "command", None) == "workers"
                and getattr(ns, "workers_command", None) == "inspect"
                and getattr(ns, "output", None) == "json"
            )
            # `push` never touches the in-process rate limiter (that's a
            # server-side concern for the API `push` talks to over HTTP) and
            # is stdout-sensitive (see _is_push_invocation) -- skip the log.
            or getattr(ns, "command", None) == "push"
        ):
            log_rate_limit_configuration()

        func = getattr(ns, "func", None)
        if func is None:
            parser.print_help()
            return 2
        if inspect.iscoroutinefunction(func):
            return asyncio.run(func(ns))
        return int(func(ns))
    finally:
        if quiet_json_inspect:
            if previous_otel_enabled is None:
                os.environ.pop("OTEL_ENABLED", None)
            else:
                os.environ["OTEL_ENABLED"] = previous_otel_enabled


if __name__ == "__main__":
    raise SystemExit(main())

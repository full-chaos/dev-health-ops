from __future__ import annotations

import argparse
import asyncio
import logging
import uuid
from datetime import timedelta

from dev_health_ops.db import get_postgres_session_sync, resolve_sink_uri
from dev_health_ops.utils.cli import (
    add_date_range_args,
    add_sink_arg,
    resolve_date_range,
    validate_sink,
)

from .operational_clickhouse import run_canonical_operational_backfill
from .runner import run_backfill_via_planner

logger = logging.getLogger(__name__)


def register_backfill_commands(subparsers: argparse._SubParsersAction) -> None:
    backfill_parser = subparsers.add_parser(
        "backfill", help="Historical backfill operations."
    )
    backfill_subparsers = backfill_parser.add_subparsers(
        dest="backfill_command", required=True
    )

    run_parser = backfill_subparsers.add_parser("run", help="Run historical backfill.")
    run_parser.add_argument(
        "--config-id",
        required=True,
        help="Sync configuration UUID (its organization is used; --org is optional)",
    )
    add_date_range_args(run_parser)
    run_parser.set_defaults(func=_cmd_backfill_run)

    operational_parser = backfill_subparsers.add_parser(
        "operational",
        help="Migrate legacy Atlassian Ops rows into canonical operational tables.",
    )
    operational_parser.add_argument("--org", required=True, help="Organization id")
    operational_parser.add_argument(
        "--atlassian-provider-instance-id",
        default="atlassian-ops",
        help="Atlassian Ops instance",
    )
    add_sink_arg(operational_parser)
    operational_parser.set_defaults(func=_cmd_backfill_operational)


def _cmd_backfill_run(ns: argparse.Namespace) -> int:
    try:
        end_day, backfill_days = resolve_date_range(ns)
        since = end_day - timedelta(days=backfill_days - 1)
        before = end_day
        # org is derived from the sync configuration (--config-id); --org is an
        # optional assertion checked below.
        requested_org_id = str(getattr(ns, "org", "") or "") or None

        # CHAOS-5351: this used to call run_backfill_for_config, which looped
        # run_work_items_sync_job (Python compute, deleted) over chunked
        # windows. The native provider-sync route
        # (cmd/dev-health-worker/provider_sync.go's work-items dataset case,
        # one per provider) is now the only production ingest path, so this
        # verb is repointed at the SAME planner/dispatch seam sync-time and
        # `sync run`/`backfill run`'s planner-based sibling already use --
        # run_backfill_via_planner's plan_sync_run(mode="backfill") + native
        # dispatch_sync_run (workers/sync_units.py). plan_sync_run
        # unconditionally arms CHAOS-4498 strict reference discovery for
        # every mode including backfill (sync/planner.py's
        # seed_reference_discovery_ledger call, "for every mode, backfill
        # included") and native dispatch blocks on it before running any
        # unit (proven by
        # test_reference_discovery_stage.py::test_backfill_runner_dispatch_path_blocks_until_discovery),
        # so no separate discovery-arming call is needed here the way
        # run_backfill_for_config's _run_strict_reference_discovery_for_backfill
        # used to make one explicitly.
        #
        # Two behaviour changes from the deleted path, both intentional:
        # 1. `--db`/sink selection is gone -- native dispatch resolves its
        #    own ClickHouse per session/credential, the way sync-time
        #    dispatch always has; there is no longer a "worker's ClickHouse
        #    vs custom --analytics-db" split to guard against.
        # 2. The old sync_options.get("repo")/("search") single-repo/glob
        #    narrowing is not carried over -- source_ids=None backfills
        #    every enabled source for the config's dataset_keys (sync
        #    planner's own "None means all enabled" contract), not just the
        #    one repo/project this config's sync_options happened to name.
        with get_postgres_session_sync() as session:
            from dev_health_ops.models.settings import SyncConfiguration
            from dev_health_ops.sync.trigger_routing import (
                canonical_sync_config_for_sync_run,
            )

            config = (
                session.query(SyncConfiguration)
                .filter(SyncConfiguration.id == uuid.UUID(ns.config_id))
                .one_or_none()
            )
            if config is None:
                raise ValueError(f"Sync configuration not found: {ns.config_id}")

            org_id = str(config.org_id)
            if requested_org_id and requested_org_id != org_id:
                raise ValueError(
                    f"Org mismatch: --org {requested_org_id} does not own sync "
                    f"config {ns.config_id} (owned by {org_id})"
                )

            provider = str(config.provider or "").strip().lower()
            sync_targets = tuple(str(t) for t in (config.sync_targets or []))
            # CHAOS-5351 (codex review, r1 P1): sync_targets is the LEGACY
            # target vocabulary a SyncConfiguration actually persists
            # (`git`/`prs`/`work-items`/`operational`/...), not the
            # canonical IntegrationDataset.dataset_key vocabulary
            # plan_sync_run's explicit dataset_keys filter expects
            # (`sync/planner.py:933-947` matches dataset_keys against exact
            # dataset_key rows). Passing sync_targets straight through
            # planned zero units for e.g. provider=github,
            # sync_targets=["git"] -- "git" is not a dataset_key, the
            # canonical keys it expands to are
            # repo-metadata/commits/commit-stats/files/blame.
            # planner_dataset_keys is the SAME single mapping used at
            # integration-create time, sync_targets-reconciliation-on-edit,
            # and the CHAOS-4106 data repair (sync/datasets.py's own
            # docstring) -- reuse it here rather than re-deriving the
            # translation a fourth time.
            from dev_health_ops.sync.datasets import (
                planner_dataset_keys,
                supported_legacy_targets,
            )

            if sync_targets:
                # team-lead ruling (r1 fix): refuse loudly on an unresolved
                # target instead of silently planning a narrower (possibly
                # empty) dataset_keys set. supported_legacy_targets(provider)
                # is the union of every dataset's legacy_targets for this
                # provider -- planner_dataset_keys can only ever produce a
                # dataset_key from a target inside that union (each spec's
                # dataset_key is only included when
                # `targets.intersection(spec.legacy_targets)` is non-empty),
                # so a target outside it can NEVER contribute a canonical
                # key, and this check is checked BEFORE calling
                # planner_dataset_keys, not after inspecting an empty result.
                known_targets = set(supported_legacy_targets(provider))
                unresolved = sorted({t for t in sync_targets if t not in known_targets})
                if unresolved:
                    raise ValueError(
                        f"backfill run: sync configuration {ns.config_id} "
                        f"({config.name!r}) has sync_targets {unresolved} "
                        f"that provider {provider!r} does not recognize as "
                        "a legacy target (checked against "
                        "sync.datasets.supported_legacy_targets) -- "
                        "refusing rather than silently planning a "
                        "narrower or empty dataset_keys set. Full "
                        f"sync_targets on this config: {sorted(sync_targets)}."
                    )
                dataset_keys = tuple(planner_dataset_keys(provider, list(sync_targets)))
            else:
                dataset_keys = None
            integration_id = (
                str(config.integration_id)
                if config.integration_id is not None
                else None
            )
            if integration_id is None:
                raise ValueError(
                    f"Sync configuration {ns.config_id} has no integration_id; "
                    "cannot plan a backfill"
                )

            # CHAOS-5351 (codex review, r1 P2): source_ids=None below means
            # "every enabled source for this integration" (sync/planner.py's
            # _load_enabled_sources, same filter reproduced read-only here)
            # -- count them now so the Info log states the actual scope
            # instead of the opaque literal "all_enabled".
            from dev_health_ops.models import IntegrationSource

            enabled_source_count = (
                session.query(IntegrationSource)
                .filter(
                    IntegrationSource.org_id == org_id,
                    IntegrationSource.integration_id == uuid.UUID(integration_id),
                    IntegrationSource.is_enabled.is_(True),
                )
                .count()
            )

            # CHAOS-4498 (codex review, P2 / CHAOS-4500), carried over
            # unchanged from run_backfill_for_config: the shared discovery
            # seam resolves CHAOS-4323 category selection via
            # canonical_sync_config_for_sync_run, which reads the run's
            # integration_id only -- it has no way to honour a specific
            # --config-id, and SyncRun has no config-id column to give it
            # one (tracked for a real fix by CHAOS-4500). Call the SAME
            # production resolver here, before planning anything, and fail
            # loud the moment it would not pick the operator's own config.
            resolved_config = canonical_sync_config_for_sync_run(session, config)
            if resolved_config is None or str(resolved_config.id) != str(config.id):
                resolved_desc = (
                    f"{resolved_config.id} ({resolved_config.name!r})"
                    if resolved_config is not None
                    else "no parent SyncConfiguration at all"
                )
                raise ValueError(
                    "backfill run: --config-id "
                    f"{ns.config_id} ({config.name!r}) is not the config the "
                    "shared reference-discovery resolver "
                    "(canonical_sync_config_for_sync_run) would use for "
                    f"integration {integration_id} -- it would resolve "
                    f"{resolved_desc} instead. This is a child config "
                    "(parent_id set) or one of several parent configs for "
                    "this integration; the discovery seam has no way to "
                    "honour --config-id specifically (CHAOS-4500). Point "
                    "--config-id at the integration's sole/canonical parent "
                    "SyncConfiguration, or resolve CHAOS-4500 first."
                )

        result = run_backfill_via_planner(
            integration_id,
            since,
            before,
            org_id=org_id,
            dataset_keys=dataset_keys,
            triggered_by="operator_backfill",
        )
        dispatch = result.get("dispatch") or {}
        # CHAOS-5351 (codex review, r1 P2): dispatch_status can be
        # "blocked_on_reference_discovery" (or any other non-"dispatched"
        # dispatch_sync_run outcome) even though run_backfill_via_planner's
        # own top-level result["status"] is unconditionally "success" once
        # dispatch_required is True -- that field means "we called dispatch
        # without raising," not "units are actually queued." Read dispatch's
        # own status for the log/print below instead of trusting the
        # top-level field.
        dispatch_status = dispatch.get("status") if dispatch else result.get("status")
        logger.info(
            "Backfill dispatched: since=%s before=%s provider=%s "
            "sync_targets=%s dataset_keys=%s source_ids=all_enabled(%s) "
            "unit_count=%s sync_run_id=%s dispatch_status=%s",
            since.isoformat(),
            before.isoformat(),
            provider,
            sorted(sync_targets) or "none",
            sorted(dataset_keys) if dataset_keys else "all_enabled",
            # CHAOS-5351 (codex review, r1 P2): source_ids=None is the
            # intentional "every enabled source for these dataset_keys"
            # broadening (see the comment above) -- log the ACTUAL enabled
            # source count (queried above, same filter
            # sync.planner._load_enabled_sources uses) rather than the
            # opaque literal "all_enabled", so an operator can read the
            # effective scope straight from this one log line instead of
            # inferring it from unit_count (which also varies with the
            # window/dataset_key count, so it cannot by itself answer "how
            # many sources did this touch").
            enabled_source_count,
            result.get("unit_count"),
            result.get("sync_run_id"),
            dispatch_status,
        )
        # CHAOS-5351 (codex review, r1 P2): print the REAL dispatch outcome
        # rather than a blanket "success" -- "success" previously printed
        # even when dispatch_status was "blocked_on_reference_discovery"
        # (or any other non-"dispatched" dispatch_sync_run outcome), which
        # reads as terminal/done to an operator when the run may still be
        # pending (CHAOS-4498 discovery, rate limiting, etc). This is a
        # status report, not a new pass/fail contract -- the CLI still
        # returns 0 for every outcome plan_sync_run/dispatch_sync_run
        # themselves treat as non-exceptional (a raised exception is the
        # only path to a nonzero return, unchanged from before).
        outcome = dispatch_status if dispatch_status else "success"
        print(
            f"Backfill {outcome}: {result.get('unit_count', 0)} unit(s) "
            f"planned for sync_run_id={result.get('sync_run_id')}"
        )
        return 0
    except Exception as exc:
        logger.error("Backfill failed: %s", exc)
        return 1


def _cmd_backfill_operational(ns: argparse.Namespace) -> int:
    try:
        validate_sink(ns)
        result = asyncio.run(
            run_canonical_operational_backfill(
                clickhouse_uri=resolve_sink_uri(ns),
                org_id=ns.org,
                atlassian_provider_instance_id=ns.atlassian_provider_instance_id,
            )
        )
        print(
            "Migrated canonical operational rows: "
            f"services={result.services}, incidents={result.incidents}, "
            f"alerts={result.alerts}, schedules={result.schedules}, "
            f"service_repository_mappings={result.service_repository_mappings}; "
            f"parity_verified={str(result.parity_verified).lower()}, "
            f"incidents={result.verified_incidents}/{result.expected_incidents}, "
            "service_repository_mappings="
            f"{result.verified_service_repository_mappings}/"
            f"{result.expected_service_repository_mappings}"
        )
        return 0
    except Exception as exc:
        logger.error("Canonical operational backfill failed: %s", exc)
        return 1

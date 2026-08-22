#!/usr/bin/env python3
"""Render the generated worker/queue mapping block in go-worker-runtime.md.

CHAOS-4044: the queue vocabulary mapping (which Celery queue's work landed on
which Go River queue/process) was hand-reconstructed from compose.yml and the
job registry twice by the same reader. CHAOS-4041 established that a
hand-authored artifact describing producer state goes stale silently; this
script generates the mechanical columns from the actual producers so that
can't happen here:

* ``deploy/go-workers/deployment.json`` -- the live Go process/queue manifest
  (source of truth per docs/contribute/architecture/go-worker-runtime.md).
* ``contracts/jobs/v1/registry.json`` -- kind -> queue, timeout, max_attempts.
* ``contracts/jobs/v1/migration-state.json`` -- per-kind rollout ``state``
  (``go_default`` / ``canary`` / ``celery_removed``), ``route``, and
  ``rollback_route``. This is what actually decides which plane is live for
  a given kind -- a queue is not uniformly "live"; a specific kind on it can
  still be `canary` (see ``sync.provider_unit`` below).
* ``compose.yml`` and ``deploy/docker-compose/compose.production.yml`` -- the
  dormant Celery ``-Q`` lists, parsed from the actual service definitions
  rather than retyped, and cross-checked against each other so a
  production-only queue change can't drift silently past the dev compose file.

The historical Celery-queue <-> Go-successor *correspondence* is not
mechanically derivable (the two vocabularies share only coincidental names --
see go-worker-runtime.md's Queue topology section) and is therefore curated
in ``CELERY_CORRESPONDENCE`` below, with citations. To keep that curation
honest, this script asserts every Go process and every Celery ``-Q`` queue
name it discovers is accounted for, and fails loudly (rather than silently
omitting a row) when a producer adds or removes one without a matching
update here.

CHAOS-4044 review note: an earlier version of this generator only looked at
``internal/jobs/metrics/remaining/families.json`` for per-kind route status,
which covers 7 of 21 kinds and rendered every other kind (including
``sync.provider_unit``) with a blanket "Go/River live" plane label. That
mislabeled a still-canary route as fully live. ``migration-state.json`` is
the actual per-kind authority and is now used for every kind.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEPLOYMENT_PATH = ROOT / "deploy" / "go-workers" / "deployment.json"
REGISTRY_PATH = ROOT / "contracts" / "jobs" / "v1" / "registry.json"
MIGRATION_STATE_PATH = ROOT / "contracts" / "jobs" / "v1" / "migration-state.json"
COMPOSE_PATH = ROOT / "compose.yml"
COMPOSE_PRODUCTION_PATH = ROOT / "deploy" / "docker-compose" / "compose.production.yml"
DOC_PATH = ROOT / "docs" / "contribute" / "architecture" / "go-worker-runtime.md"

BEGIN = "<!-- BEGIN GENERATED QUEUE MAP -->"
END = "<!-- END GENERATED QUEUE MAP -->"

# ---------------------------------------------------------------------------
# Curated correspondence (CHAOS-4044). Historical fact, not derivable from a
# single producer file. Cited per row. Kept deliberately small and reviewed:
# this is the part a human must update when the fleet's shape changes.
# ---------------------------------------------------------------------------
#
# Each entry: go_process -> {
#   "celery_queues": [Celery queue names historically carrying this work],
#   "celery_consumers": [Celery service names in compose.yml],
#   "plane": one-line plane/route-flag status,
#   "note": optional extra context,
# }
CELERY_CORRESPONDENCE: dict[str, dict[str, dict[str, Any]]] = {
    "heavy": {
        "investment": {
            "celery_queues": [],
            "celery_consumers": [],
            "plane": "Go-native -- no Celery predecessor",
        },
        "metrics": {
            "celery_queues": ["metrics", "backfill"],
            "celery_consumers": ["worker-heavy"],
            "plane": "Celery dormant since 2026-08-19 (CHAOS-4026); Go/River live",
            "note": (
                "backfill's family (metrics.remaining.membership_backfill) rides "
                "this queue, not a dedicated Go 'backfill' queue."
            ),
        },
        "reports": {
            "celery_queues": ["reports"],
            "celery_consumers": ["worker"],
            "plane": "Celery dormant since 2026-08-19 (CHAOS-4026); Go/River live",
        },
        "workgraph": {
            "celery_queues": ["default"],
            "celery_consumers": ["worker"],
            "plane": "Celery dormant since 2026-08-19 (CHAOS-4026); Go/River live",
            "note": "work_graph_tasks.py routed through the shared 'default' catch-all, not a dedicated queue.",
        },
    },
    "ops": {
        "coverage": {
            "celery_queues": [],
            "celery_consumers": [],
            "plane": "Go-native -- no Celery predecessor",
        },
        "heartbeat": {
            "celery_queues": ["default"],
            "celery_consumers": ["worker"],
            "plane": "Celery dormant since 2026-08-19 (CHAOS-4026); Go/River live",
            "note": "system_ops.phone_home_heartbeat routed through 'default', not a dedicated queue.",
        },
        "retention": {
            "celery_queues": [],
            "celery_consumers": ["beat"],
            "plane": "Go-native consolidated sweep; historical retention work was several discrete Beat-scheduled tasks (retired under CHAOS-4026, e.g. ask-dev-retention-sweep)",
        },
        "webhooks": {
            "celery_queues": ["webhooks"],
            "celery_consumers": ["worker"],
            "plane": "Celery dormant since 2026-08-19 (CHAOS-4026); Go/River live",
        },
    },
    "sync": {
        "sync": {
            "celery_queues": ["sync"],
            "celery_consumers": ["worker"],
            "plane": "Celery dormant since 2026-08-19 (CHAOS-4026); Go/River live. Historically also the shared fallback queue for all providers with PROVIDER_SYNC_QUEUES_ENABLED off.",
        },
    },
    "sync-provider": {
        "sync_provider": {
            "celery_queues": [
                "sync.github",
                "sync.gitlab",
                "sync.linear",
                "sync.jira",
                "sync.launchdarkly",
                "sync.github.light",
                "sync.github.medium",
                "sync.github.heavy",
                "sync.gitlab.light",
                "sync.gitlab.medium",
                "sync.gitlab.heavy",
                "sync.jira.medium",
                "sync.linear.medium",
            ],
            "celery_consumers": ["worker", "worker-heavy"],
            "plane": "Celery fleet-wide dormant since 2026-08-19 (CHAOS-4026). This queue's one kind is still `canary` per-kind (see below) -- do not read the fleet-wide Celery retirement as proof this specific route has cleared rollout. Enablement today is provider/dataset WORKER_*_ENABLED switches (dying -- CHAOS-4054 two-plane decision) plus -Q topology.",
            "note": (
                "The per-provider cost-class split (light/medium/heavy) has no Go "
                "equivalent yet -- CHAOS-4027, parked. All cost classes collapse "
                "onto the single sync_provider queue in Go."
            ),
        },
    },
    "scheduler": {
        "—": {
            "celery_queues": ["scheduler"],
            "celery_consumers": ["beat"],
            "plane": "Celery Beat retired 2026-08-21 (CHAOS-4026); Go scheduler is sole production owner",
            "note": "Control loop, not a River queue -- no -Q for this process.",
        },
    },
    "reconciler": {
        "—": {
            "celery_queues": [],
            "celery_consumers": [],
            "plane": "Go-native -- no Celery predecessor",
            "note": "Control loop, not a River queue -- no -Q for this process.",
        },
    },
    "stream-ingest": {
        "—": {
            "celery_queues": ["ingest"],
            "celery_consumers": ["worker-ingest"],
            "plane": "Celery dormant since 2026-08-19 (CHAOS-4026); Go stream runner live",
            "note": "Valkey stream consumer, not a River queue -- no -Q for this process.",
        },
    },
    "stream-external": {
        "—": {
            "celery_queues": ["external-ingest"],
            "celery_consumers": ["worker-external-ingest"],
            "plane": "Celery dormant since 2026-08-19 (CHAOS-4026); Go stream runner live",
            "note": "Valkey stream consumer, not a River queue -- no -Q for this process.",
        },
    },
    "stream-pagerduty": {
        "—": {
            "celery_queues": [],
            "celery_consumers": [],
            "plane": "Go-native -- no Celery predecessor",
            "note": "Valkey stream consumer, not a River queue -- no -Q for this process.",
        },
    },
}

# Celery queues that carry no work reachable through this table at all
# (telemetry, not a work queue). Excluded from the "every -Q name must be
# accounted for" check below with an explicit reason instead of silently.
CELERY_QUEUES_WITH_NO_GO_QUEUE = {
    "monitoring": "queue-depth telemetry; superseded by native worker_jobs_available / worker_job_oldest_age_seconds / worker_execution_saturation_ratio metrics, not a queue",
}


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def load_deployment() -> dict[str, dict]:
    data = _load_json(DEPLOYMENT_PATH)
    return {process["name"]: process for process in data["processes"]}


def load_registry() -> dict[str, dict]:
    data = _load_json(REGISTRY_PATH)
    return {job["kind"]: job for job in data["jobs"]}


def load_migration_state() -> dict[str, dict]:
    data = _load_json(MIGRATION_STATE_PATH)
    return {job["kind"]: job for job in data["jobs"]}


CELERY_Q_RE = re.compile(r"-Q\s+([A-Za-z0-9_.,-]+)")
SERVICE_NAME_RE = re.compile(r"^  ([a-z][a-z0-9-]*):\s*$")
LIST_ITEM_RE = re.compile(r"^\s*-\s*(.+?)\s*$")


def load_compose_celery_queues(compose_path: Path) -> dict[str, list[str]]:
    """Parse ``-Q`` lists per Celery service straight out of a compose file.

    Line-based on purpose: these compose files use YAML anchors/merge keys
    (``<<: *worker-base``) that a naive ``yaml.safe_load`` round-trip would
    have to re-resolve. Two ``command:`` shapes appear across the two files
    this is called on:

    * flow form (``compose.yml``): ``command: -A ... -Q <list> ...`` all on
      one physical line;
    * block-list form (``compose.production.yml``): one YAML list item per
      argument, so ``-Q`` and its value are two consecutive ``- ...`` lines.

    Both are handled so the dev and production queue lists can be
    cross-checked against each other below.
    """
    current_service: str | None = None
    queues_by_service: dict[str, list[str]] = {}
    pending_dash_q = False
    for raw_line in compose_path.read_text(encoding="utf-8").splitlines():
        service_match = SERVICE_NAME_RE.match(raw_line)
        if service_match:
            current_service = service_match.group(1)
            pending_dash_q = False
            continue
        if current_service is None:
            continue

        if pending_dash_q:
            list_item = LIST_ITEM_RE.match(raw_line)
            pending_dash_q = False
            if list_item:
                queues_by_service[current_service] = list_item.group(1).split(",")
            continue

        stripped = raw_line.strip()
        if stripped == "- -Q":
            pending_dash_q = True
            continue

        queue_match = CELERY_Q_RE.search(raw_line)
        if queue_match and "celery_app" in raw_line and "worker" in raw_line:
            queues_by_service[current_service] = queue_match.group(1).split(",")
    return queues_by_service


def render_block() -> str:
    deployment = load_deployment()
    registry = load_registry()
    migration_state = load_migration_state()
    compose_queues = load_compose_celery_queues(COMPOSE_PATH)
    compose_production_queues = load_compose_celery_queues(COMPOSE_PRODUCTION_PATH)

    # --- Consistency guards: fail loudly instead of silently omitting a row ---
    deployment_processes = set(deployment)
    curated_processes = set(CELERY_CORRESPONDENCE)
    if deployment_processes != curated_processes:
        raise SystemExit(
            "gen_queue_mapping_docs: deploy/go-workers/deployment.json process set "
            f"{sorted(deployment_processes)} does not match the curated "
            f"CELERY_CORRESPONDENCE keys {sorted(curated_processes)} in "
            "scripts/gen_queue_mapping_docs.py -- update the curated map."
        )

    registry_kinds = set(registry)
    migration_state_kinds = set(migration_state)
    if registry_kinds != migration_state_kinds:
        raise SystemExit(
            "gen_queue_mapping_docs: contracts/jobs/v1/registry.json kinds "
            f"{sorted(registry_kinds)} do not match "
            f"contracts/jobs/v1/migration-state.json kinds {sorted(migration_state_kinds)} "
            "-- every registered kind must have a migration-state row."
        )

    compose_only_dev = set(compose_queues) - set(compose_production_queues)
    compose_only_prod = set(compose_production_queues) - set(compose_queues)
    if compose_only_dev or compose_only_prod:
        raise SystemExit(
            "gen_queue_mapping_docs: compose.yml and "
            "deploy/docker-compose/compose.production.yml declare different Celery "
            f"service sets -- dev-only: {sorted(compose_only_dev)}, "
            f"production-only: {sorted(compose_only_prod)}."
        )
    for service, dev_queues in compose_queues.items():
        prod_queues = compose_production_queues[service]
        if set(dev_queues) != set(prod_queues):
            raise SystemExit(
                f"gen_queue_mapping_docs: Celery service '{service}' consumes "
                f"{sorted(dev_queues)} in compose.yml but {sorted(prod_queues)} in "
                "compose.production.yml -- the dormant Celery queue vocabulary has "
                "diverged between dev and production; update the curated map to "
                "match production before regenerating."
            )

    all_curated_celery_queues: set[str] = set()
    for queue_map in CELERY_CORRESPONDENCE.values():
        for curated_entry in queue_map.values():
            all_curated_celery_queues.update(curated_entry["celery_queues"])
    all_curated_celery_queues.update(CELERY_QUEUES_WITH_NO_GO_QUEUE)

    all_compose_queues: set[str] = set()
    for queues in compose_queues.values():
        all_compose_queues.update(queues)
    unaccounted = all_compose_queues - all_curated_celery_queues
    if unaccounted:
        raise SystemExit(
            f"gen_queue_mapping_docs: compose.yml declares Celery queue(s) {sorted(unaccounted)} "
            "not present in CELERY_CORRESPONDENCE or CELERY_QUEUES_WITH_NO_GO_QUEUE in "
            "scripts/gen_queue_mapping_docs.py -- document their Go successor (or lack of one)."
        )

    lines = [
        BEGIN,
        "| Go process (binary) | Go queue | Job kind(s) | Timeout(s) | Max attempts | Historical Celery queue(s) | Historical Celery consumer(s) | Plane / route status |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for process_name in sorted(deployment):
        process = deployment[process_name]
        entries = CELERY_CORRESPONDENCE[process_name]
        queues = process["queues"] or ["—"]
        for go_queue in queues:
            entry: dict[str, Any] | None = entries.get(go_queue)
            if entry is None:
                raise SystemExit(
                    f"gen_queue_mapping_docs: process '{process_name}' queue '{go_queue}' "
                    "from deployment.json has no CELERY_CORRESPONDENCE entry."
                )
            kinds = sorted(
                kind
                for kind in process["job_kinds"]
                if registry[kind]["queue"] == go_queue
            )
            if kinds:
                timeouts = sorted({registry[k]["timeout_seconds"] for k in kinds})
                attempts = sorted({registry[k]["max_attempts"] for k in kinds})
                kind_cell = "<br>".join(f"`{k}`" for k in kinds)
                timeout_cell = (
                    "-".join(str(t) for t in (timeouts[0], timeouts[-1]))
                    if len(timeouts) > 1
                    else str(timeouts[0])
                )
                attempts_cell = (
                    "-".join(str(a) for a in (attempts[0], attempts[-1]))
                    if len(attempts) > 1
                    else str(attempts[0])
                )
            else:
                kind_cell = "—"
                timeout_cell = "—"
                attempts_cell = "—"
            celery_queues_cell = (
                ", ".join(f"`{q}`" for q in entry["celery_queues"]) or "—"
            )
            celery_consumers_cell = (
                ", ".join(f"`{c}`" for c in entry["celery_consumers"]) or "—"
            )
            plane_cell = entry["plane"]
            if "note" in entry:
                plane_cell = f"{plane_cell}<br>{entry['note']}"
            for kind in kinds:
                state = migration_state[kind]
                flag = "" if state["state"] == "go_default" else " ⚠"
                plane_cell = (
                    f"{plane_cell}<br>`{kind}`: state=`{state['state']}`{flag}, "
                    f"route=`{state['route']}`, "
                    f"rollback_route=`{state['rollback_route']}` "
                    "(migration-state.json)"
                )
            lines.append(
                f"| `{process_name}` (`{process['binary']}`) | `{go_queue}` | {kind_cell} | "
                f"{timeout_cell} | {attempts_cell} | {celery_queues_cell} | "
                f"{celery_consumers_cell} | {plane_cell} |"
            )

    lines.append("")
    lines.append(
        "Celery queues carrying no work reachable through a Go queue at all "
        "(telemetry, not routed work):"
    )
    lines.append("")
    lines.append("| Celery queue | Why it has no Go queue |")
    lines.append("| --- | --- |")
    for queue, reason in sorted(CELERY_QUEUES_WITH_NO_GO_QUEUE.items()):
        lines.append(f"| `{queue}` | {reason} |")
    lines.append(END)
    return "\n".join(lines)


def update_doc() -> None:
    rendered = render_block()
    doc = DOC_PATH.read_text(encoding="utf-8")
    start = doc.find(BEGIN)
    stop = doc.find(END)
    if start == -1 or stop == -1 or stop < start:
        raise SystemExit(f"Generated queue-map markers not found in {DOC_PATH}")
    stop += len(END)
    updated = f"{doc[:start]}{rendered}{doc[stop:]}"
    DOC_PATH.write_text(updated, encoding="utf-8")


if __name__ == "__main__":
    update_doc()

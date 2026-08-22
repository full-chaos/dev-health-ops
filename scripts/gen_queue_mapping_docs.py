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
* ``internal/jobs/metrics/remaining/families.json`` -- per-kind
  ``route``/``rollback_route`` for the metrics.remaining.* family.
* ``compose.yml`` -- the dormant Celery ``-Q`` lists, parsed from the actual
  service definitions rather than retyped.

The historical Celery-queue <-> Go-successor *correspondence* is not
mechanically derivable (the two vocabularies share only coincidental names --
see go-worker-runtime.md's Queue topology section) and is therefore curated
in ``CELERY_CORRESPONDENCE`` below, with citations. To keep that curation
honest, this script asserts every Go process and every Celery ``-Q`` queue
name it discovers is accounted for, and fails loudly (rather than silently
omitting a row) when a producer adds or removes one without a matching
update here.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEPLOYMENT_PATH = ROOT / "deploy" / "go-workers" / "deployment.json"
REGISTRY_PATH = ROOT / "contracts" / "jobs" / "v1" / "registry.json"
REMAINING_FAMILIES_PATH = (
    ROOT / "internal" / "jobs" / "metrics" / "remaining" / "families.json"
)
COMPOSE_PATH = ROOT / "compose.yml"
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
            "plane": "Celery dormant since 2026-08-19 (CHAOS-4026); Go/River live. Enablement today is provider/dataset WORKER_*_ENABLED switches (dying -- CHAOS-4054 two-plane decision) plus -Q topology.",
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


def load_remaining_routes() -> dict[str, dict]:
    data = _load_json(REMAINING_FAMILIES_PATH)
    return {
        family["route_key"]: {
            "route": family["route"],
            "rollback_route": family["rollback_route"],
        }
        for family in data["families"]
    }


CELERY_Q_RE = re.compile(r"-Q\s+([A-Za-z0-9_.,-]+)")
SERVICE_NAME_RE = re.compile(r"^  ([a-z][a-z0-9-]*):\s*$")
CONTAINER_NAME_RE = re.compile(r"^\s*container_name:\s*(\S+)\s*$")


def load_compose_celery_queues() -> dict[str, list[str]]:
    """Parse ``-Q`` lists per Celery service straight out of compose.yml.

    Line-based on purpose: compose.yml uses YAML anchors/merge keys
    (``<<: *worker-base``) that a naive ``yaml.safe_load`` round-trip would
    have to re-resolve; the ``-Q`` argument is always on the same physical
    `command:` line as everything else, so a direct scan is both simpler and
    exactly as accurate as a full YAML parse for this one field.
    """
    current_service: str | None = None
    queues_by_service: dict[str, list[str]] = {}
    for raw_line in COMPOSE_PATH.read_text(encoding="utf-8").splitlines():
        service_match = SERVICE_NAME_RE.match(raw_line)
        if service_match:
            current_service = service_match.group(1)
            continue
        if current_service is None:
            continue
        queue_match = CELERY_Q_RE.search(raw_line)
        if queue_match and "celery_app" in raw_line and "worker" in raw_line:
            queues_by_service[current_service] = queue_match.group(1).split(",")
    return queues_by_service


def render_block() -> str:
    deployment = load_deployment()
    registry = load_registry()
    remaining_routes = load_remaining_routes()
    compose_queues = load_compose_celery_queues()

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
                if kind in remaining_routes:
                    route = remaining_routes[kind]
                    plane_cell = (
                        f"{plane_cell}<br>`{kind}`: route=`{route['route']}`, "
                        f"rollback_route=`{route['rollback_route']}` (families.json)"
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

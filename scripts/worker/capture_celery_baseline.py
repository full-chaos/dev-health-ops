#!/usr/bin/env python3
"""Capture a sanitized, read-only Celery runtime baseline from Docker.

The recorder intentionally stores only aggregate numbers and stable runtime
metadata. Docker logs, task arguments/results, stream keys, tenant IDs, DSNs,
and container environments are never written to disk.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

WORKER_PROFILES = {
    "worker": "general",
    "worker-ingest": "ingest",
    "worker-wi": "work_item_provider_medium",
    "worker-heavy": "heavy",
    "beat": "scheduler",
}
RESOURCE_SERVICES = (*WORKER_PROFILES, "postgres", "pgbouncer", "valkey")

QUEUE_PROFILES = {
    "general": (
        "default",
        "sync",
        "sync.github",
        "sync.gitlab",
        "sync.github.light",
        "sync.github.medium",
        "sync.gitlab.light",
        "sync.gitlab.medium",
        "webhooks",
        "reports",
    ),
    "work_item_provider_medium": (
        "scheduler",
        "sync.linear",
        "sync.jira",
        "sync.launchdarkly",
        "sync.jira.medium",
        "sync.linear.medium",
    ),
    "heavy": (
        "metrics",
        "backfill",
        "sync.github.heavy",
        "sync.gitlab.heavy",
    ),
    "ingest": ("ingest",),
    "external_ingest": ("external-ingest",),
    "monitoring": ("monitoring",),
}

STREAM_CLASSES = (
    ("ingest", "ingest:*:*"),
    ("product_telemetry", "product-telemetry:*"),
    ("external_ingest", "external-ingest:*:*"),
)

TASK_SUCCESS_RE = re.compile(
    r"Task (?P<task>[A-Za-z0-9_.-]+)\[[^\]]+\] succeeded in "
    r"(?P<seconds>[0-9]+(?:\.[0-9]+)?)s"
)
TASK_RETRY_RE = re.compile(r"Task (?P<task>[A-Za-z0-9_.-]+)\[[^\]]+\] retry:")
TASK_FAILURE_RE = re.compile(
    r"Task (?P<task>[A-Za-z0-9_.-]+)\[[^\]]+\] raised (?:unexpected|reject)"
)
TASK_REVOKED_RE = re.compile(
    r"Task (?P<task>[A-Za-z0-9_.-]+)\[[^\]]+\] (?:revoked|rejected|ignored)"
)
DOCKER_TIMESTAMP_RE = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z) "
)
UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-"
    r"[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}\b"
)
DSN_RE = re.compile(r"(?i)\b(?:postgres(?:ql)?|redis|valkey|clickhouse)(?:\+[^:]+)?://")
ABSOLUTE_PATH_RE = re.compile(r"(?:/Users/|/home/|/var/lib/|/private/)")
FORBIDDEN_JSON_KEYS = {
    "args",
    "kwargs",
    "payload",
    "result",
    "dsn",
    "credentials",
    "password",
    "secret",
    "token",
    "stream_key",
    "tenant_id",
    "org_id",
    "task_id",
}

BROKER_PROBE = r"""
import json
from datetime import datetime, timezone

from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.config import task_queues

out = []
now = datetime.now(timezone.utc)
with celery_app.connection_or_acquire() as connection:
    channel = connection.default_channel
    for queue in task_queues:
        depth = 0
        oldest = None
        age_observable = False
        priorities = getattr(channel, "priority_steps", [0])
        for priority in priorities:
            key = channel._q_for_pri(queue, priority)
            try:
                queue_depth = int(channel.client.llen(key))
            except Exception:
                queue_depth = 0
            depth += queue_depth
            if queue_depth <= 0:
                continue
            try:
                raw = channel.client.lindex(key, -1)
                payload = json.loads(raw)
                stamp = (payload.get("headers") or {}).get("enqueued_at")
                if stamp:
                    enqueued = datetime.fromisoformat(str(stamp))
                    if enqueued.tzinfo is None:
                        enqueued = enqueued.replace(tzinfo=timezone.utc)
                    age = max(0.0, (now - enqueued).total_seconds())
                    oldest = age if oldest is None else max(oldest, age)
                    age_observable = True
            except Exception:
                pass
        out.append({
            "queue": queue,
            "depth": depth,
            "oldest_age_seconds": oldest,
            "age_observable": age_observable,
        })
print(json.dumps({"queues": out}, sort_keys=True))
"""

STREAM_PROBE = r"""
import json
import os

import valkey

classes = (
    ("ingest", "ingest:*:*"),
    ("product_telemetry", "product-telemetry:*"),
    ("external_ingest", "external-ingest:*:*"),
)
client = valkey.from_url(os.environ["REDIS_URL"], decode_responses=True)
result = {}
for label, pattern in classes:
    stream_count = 0
    depth = 0
    pending = 0
    lag = 0
    lag_observed = False
    oldest_pending_seconds = None
    for key in client.scan_iter(match=pattern, _type="stream"):
        stream_count += 1
        depth += int(client.xlen(key))
        try:
            groups = client.xinfo_groups(key)
        except Exception:
            groups = []
        for group in groups:
            group_name = group.get("name")
            pending += int(group.get("pending", 0) or 0)
            group_lag = group.get("lag")
            if group_lag is not None:
                lag += int(group_lag)
                lag_observed = True
            try:
                entries = client.xpending_range(key, group_name, "-", "+", 1)
            except Exception:
                entries = []
            if entries:
                seconds = float(entries[0].get("time_since_delivered", 0) or 0) / 1000.0
                oldest_pending_seconds = (
                    seconds
                    if oldest_pending_seconds is None
                    else max(oldest_pending_seconds, seconds)
                )
    result[label] = {
        "stream_count": stream_count,
        "depth": depth,
        "pending": pending,
        "lag": lag if lag_observed else None,
        "oldest_pending_seconds": oldest_pending_seconds,
    }
print(json.dumps(result, sort_keys=True))
"""

CELERY_INSPECT_PROBE = r"""
import json

from dev_health_ops.workers.celery_app import celery_app

inspect = celery_app.control.inspect(timeout=5.0)
ping = inspect.ping() or {}
active = inspect.active() or {}
reserved = inspect.reserved() or {}
scheduled = inspect.scheduled() or {}
print(json.dumps({
    "responding_workers": len(ping),
    "active_jobs": sum(len(items) for items in active.values()),
    "reserved_jobs": sum(len(items) for items in reserved.values()),
    "scheduled_jobs": sum(len(items) for items in scheduled.values()),
}, sort_keys=True))
"""

POSTGRES_STATS_SQL = """
SELECT json_build_object(
  'numbackends', numbackends,
  'max_connections', current_setting('max_connections')::int,
  'xact_commit', xact_commit,
  'xact_rollback', xact_rollback,
  'blks_read', blks_read,
  'blks_hit', blks_hit,
  'blk_read_time_ms', blk_read_time,
  'blk_write_time_ms', blk_write_time,
  'temp_files', temp_files,
  'temp_bytes', temp_bytes,
  'deadlocks', deadlocks,
  'track_io_timing', current_setting('track_io_timing'),
  'total_locks', (
    SELECT count(*) FROM pg_locks
    WHERE database = (SELECT oid FROM pg_database WHERE datname = current_database())
  ),
  'waiting_locks', (
    SELECT count(*) FROM pg_locks
    WHERE database = (SELECT oid FROM pg_database WHERE datname = current_database())
      AND NOT granted
  )
)
FROM pg_stat_database
WHERE datname = current_database();
"""

SYNC_LEASE_STATS_SQL = """
SELECT json_build_object(
  'unit_count', count(*),
  'expired_lease_retry_count_total',
    coalesce(sum(expired_lease_retry_count), 0),
  'units_with_expired_lease_retry',
    count(*) FILTER (WHERE expired_lease_retry_count > 0),
  'currently_leased_units',
    count(*) FILTER (WHERE lease_owner IS NOT NULL),
  'currently_expired_leases',
    count(*) FILTER (
      WHERE lease_owner IS NOT NULL AND lease_expires_at < now()
    )
)
FROM sync_run_units;
"""


class CaptureError(RuntimeError):
    """A safe-to-display capture failure without raw command output."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def run_text(args: list[str], *, timeout: float = 30.0) -> str:
    """Run a probe without allowing command output into an exception."""
    try:
        result = subprocess.run(
            args,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise CaptureError(f"probe unavailable: {args[0]}") from exc
    if result.returncode != 0:
        raise CaptureError(f"probe failed: {args[0]}")
    return result.stdout


def run_json(args: list[str], *, timeout: float = 30.0) -> Any:
    text = run_text(args, timeout=timeout).strip()
    for line in reversed(text.splitlines()):
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    raise CaptureError(f"probe returned invalid JSON: {args[0]}")


def discover_containers(project: str) -> dict[str, dict[str, str]]:
    output = run_text(
        [
            "docker",
            "ps",
            "--filter",
            f"label=com.docker.compose.project={project}",
            "--format",
            '{{.Names}}\t{{.Label "com.docker.compose.service"}}\t{{.State}}',
        ]
    )
    discovered: dict[str, dict[str, str]] = {}
    for line in output.splitlines():
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        name, service, state = parts
        if service:
            discovered[service] = {"name": name, "state": state}
    required = set(RESOURCE_SERVICES)
    missing = sorted(required - discovered.keys())
    not_running = sorted(
        service
        for service in required & discovered.keys()
        if discovered[service]["state"] != "running"
    )
    if missing or not_running:
        details = []
        if missing:
            details.append("missing=" + ",".join(missing))
        if not_running:
            details.append("not_running=" + ",".join(not_running))
        raise CaptureError("required runtime unavailable (" + "; ".join(details) + ")")
    return discovered


def inspect_runtime(containers: dict[str, dict[str, str]]) -> dict[str, Any]:
    runtime: dict[str, Any] = {}
    for service in RESOURCE_SERVICES:
        name = containers[service]["name"]
        output = run_text(
            [
                "docker",
                "inspect",
                "--format",
                "{{.Image}}|{{.State.StartedAt}}|{{.RestartCount}}",
                name,
            ]
        ).strip()
        parts = output.split("|")
        if len(parts) != 3:
            raise CaptureError("docker inspect returned an unexpected shape")
        image_id, started_at, restart_count = parts
        runtime[service] = {
            "profile": WORKER_PROFILES.get(service, service),
            "image_id": image_id,
            "started_at": iso_utc(parse_utc(started_at)),
            "restart_count": int(restart_count),
        }
    return runtime


def source_revision(worker_container: str) -> dict[str, Any]:
    source = run_text(
        [
            "docker",
            "inspect",
            "--format",
            '{{range .Mounts}}{{if eq .Destination "/app"}}{{.Source}}{{end}}{{end}}',
            worker_container,
        ]
    ).strip()
    if not source:
        return {
            "revision": None,
            "basis": "unavailable_no_app_bind_mount",
            "worktree_dirty": None,
        }
    revision = run_text(["git", "-C", source, "rev-parse", "HEAD"]).strip()
    dirty = bool(run_text(["git", "-C", source, "status", "--porcelain"]).strip())
    return {
        "revision": revision,
        "basis": "bind_mount_git_head_at_capture",
        "worktree_dirty": dirty,
    }


def parse_size_bytes(value: str) -> int:
    number_and_unit = value.strip().split()[0]
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([KMGT]?i?B)", number_and_unit)
    if not match:
        raise ValueError(f"unsupported size: {value!r}")
    number = float(match.group(1))
    unit = match.group(2)
    multipliers = {
        "B": 1,
        "KB": 1000,
        "MB": 1000**2,
        "GB": 1000**3,
        "TB": 1000**4,
        "KiB": 1024,
        "MiB": 1024**2,
        "GiB": 1024**3,
        "TiB": 1024**4,
    }
    return int(number * multipliers[unit])


def docker_stats(containers: dict[str, dict[str, str]]) -> dict[str, Any]:
    names = [containers[service]["name"] for service in RESOURCE_SERVICES]
    output = run_text(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}", *names],
        timeout=45.0,
    )
    by_name: dict[str, Any] = {}
    for line in output.splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        by_name[row["Name"]] = row
    result: dict[str, Any] = {}
    for service in RESOURCE_SERVICES:
        name = containers[service]["name"]
        row = by_name.get(name)
        if row is None:
            raise CaptureError(f"docker stats omitted service {service}")
        result[service] = {
            "profile": WORKER_PROFILES.get(service, service),
            "cpu_cores": float(row["CPUPerc"].rstrip("%")) / 100.0,
            "memory_bytes": parse_size_bytes(row["MemUsage"].split("/", 1)[0]),
            "memory_limit_percent": float(row["MemPerc"].rstrip("%")),
            "pids": int(row["PIDs"]),
        }
    return result


def python_probe(container: str, code: str, *, timeout: float = 30.0) -> Any:
    return run_json(
        ["docker", "exec", container, "python", "-c", code], timeout=timeout
    )


def psql_probe(container: str, sql: str) -> Any:
    shell = (
        "exec psql --no-psqlrc --tuples-only --no-align "
        '--username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --command "$1"'
    )
    output = run_text(
        ["docker", "exec", container, "sh", "-c", shell, "baseline-probe", sql]
    ).strip()
    try:
        return json.loads(output)
    except json.JSONDecodeError as exc:
        raise CaptureError("PostgreSQL probe returned invalid JSON") from exc


def percentile(values: Iterable[float | int], fraction: float) -> float | None:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return None
    index = max(0, math.ceil(fraction * len(ordered)) - 1)
    return round(ordered[index], 6)


def summarize(values: Iterable[float | int]) -> dict[str, Any]:
    copied = list(values)
    return {
        "sample_count": len(copied),
        "min": percentile(copied, 0.0),
        "p50": percentile(copied, 0.50),
        "p95": percentile(copied, 0.95),
        "max": percentile(copied, 1.0),
    }


def task_family(task_name: str) -> str:
    short = task_name.rsplit(".", 1)[-1]
    exact = {
        "dispatch_scheduled_syncs": "scheduled_sync_dispatch",
        "reconcile_sync_dispatch": "sync_outbox_reconciler",
        "dispatch_sync_run": "sync_dispatch",
        "run_sync_unit": "sync_unit",
        "finalize_sync_run": "sync_finalizer",
        "run_ingest_consumer": "ingest",
        "run_product_telemetry_consumer": "product_telemetry",
        "run_external_ingest_consumer": "external_ingest",
        "flush_external_ingest_recompute": "external_recompute_flush",
        "external_ingest_stream_health": "external_stream_health",
        "monitor_queue_depths": "queue_monitor",
        "phone_home_heartbeat": "heartbeat",
    }
    if short in exact:
        return exact[short]
    if "webhook" in short:
        return "webhooks"
    if "report" in short:
        return "reports"
    if "billing" in short or "email" in short:
        return "billing_email"
    if short.startswith("prune_"):
        return "retention"
    if "investment" in short:
        return "investment"
    if "membership" in short:
        return "membership"
    if "complexity" in short:
        return "complexity"
    if "recommendation" in short or "capacity" in short:
        return "capacity_recommendations"
    if "release" in short or "dora" in short:
        return "dora_release"
    if "metric" in short:
        return "metrics"
    if "sync" in short:
        return "sync_other"
    return "other"


def parse_task_log_line(line: str) -> tuple[str, str, float | None] | None:
    success = TASK_SUCCESS_RE.search(line)
    if success:
        return success.group("task"), "success", float(success.group("seconds"))
    for pattern, outcome in (
        (TASK_RETRY_RE, "retry"),
        (TASK_FAILURE_RE, "failure"),
        (TASK_REVOKED_RE, "discard"),
    ):
        match = pattern.search(line)
        if match:
            return match.group("task"), outcome, None
    return None


def aggregate_task_logs(
    containers: dict[str, dict[str, str]], start: datetime, end: datetime
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    outcomes: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    durations: dict[str, list[float]] = defaultdict(list)
    task_names: dict[str, set[str]] = defaultdict(set)
    start_to_ready: dict[str, list[float]] = defaultdict(list)

    for service, profile in WORKER_PROFILES.items():
        if service == "beat":
            continue
        name = containers[service]["name"]
        args = [
            "docker",
            "logs",
            "--timestamps",
            "--since",
            iso_utc(start),
            "--until",
            iso_utc(end),
            name,
        ]
        try:
            process = subprocess.Popen(
                args,
                stdout=subprocess.PIPE,
                # Celery logs use the container's stderr stream. Merge it into
                # the in-memory parser, then retain only the aggregate fields.
                stderr=subprocess.STDOUT,
                text=True,
            )
        except OSError as exc:
            raise CaptureError("docker log stream unavailable") from exc
        assert process.stdout is not None
        for line in process.stdout:
            parsed = parse_task_log_line(line)
            if parsed is not None:
                task_name, outcome, duration = parsed
                family = task_family(task_name)
                outcomes[family][outcome] += 1
                task_names[family].add(task_name)
                if duration is not None:
                    durations[family].append(duration)
            if " ready." in line:
                timestamp_match = DOCKER_TIMESTAMP_RE.match(line)
                if timestamp_match:
                    ready_at = parse_utc(timestamp_match.group("timestamp"))
                    # Start-to-ready is filled by the caller from runtime metadata.
                    start_to_ready[profile].append(ready_at.timestamp())
        if process.wait() != 0:
            raise CaptureError("docker log stream failed")

    seconds = max((end - start).total_seconds(), 1.0)
    outcome_summary: dict[str, Any] = {}
    duration_summary: dict[str, Any] = {}
    for family in sorted(set(outcomes) | set(durations)):
        counts = {
            state: outcomes[family].get(state, 0)
            for state in ("success", "retry", "failure", "discard")
        }
        outcome_summary[family] = {
            "task_name_count": len(task_names[family]),
            "counts": counts,
            "rates_per_second": {
                state: round(count / seconds, 9) for state, count in counts.items()
            },
        }
        duration_summary[family] = summarize(durations[family])
    return outcome_summary, duration_summary, start_to_ready


def aggregate_queue_samples(
    samples: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    depth_values: dict[str, list[int]] = defaultdict(list)
    age_values: dict[str, list[float]] = defaultdict(list)
    nonempty_without_age: dict[str, int] = defaultdict(int)
    queue_to_profile = {
        queue: profile for profile, queues in QUEUE_PROFILES.items() for queue in queues
    }
    for sample in samples:
        profile_depths: dict[str, int] = defaultdict(int)
        profile_ages: dict[str, list[float]] = defaultdict(list)
        for row in sample.get("queues", []):
            profile = queue_to_profile.get(row["queue"], "unmapped")
            queue_depth = int(row["depth"])
            profile_depths[profile] += queue_depth
            oldest_age = row.get("oldest_age_seconds")
            if queue_depth > 0 and oldest_age is not None:
                profile_ages[profile].append(float(oldest_age))
            elif queue_depth > 0:
                nonempty_without_age[profile] += 1
        for profile in QUEUE_PROFILES:
            depth_values[profile].append(profile_depths.get(profile, 0))
            if profile_ages.get(profile):
                age_values[profile].append(max(profile_ages[profile]))

    depth_summary = {
        profile: summarize(values) for profile, values in sorted(depth_values.items())
    }
    age_summary: dict[str, Any] = {}
    for profile in sorted(QUEUE_PROFILES):
        summary = summarize(age_values.get(profile, []))
        summary["nonempty_samples_without_enqueued_at"] = nonempty_without_age.get(
            profile, 0
        )
        summary["status"] = (
            "recorded"
            if summary["sample_count"]
            else "not_observed_during_sampling_window"
        )
        age_summary[profile] = summary
    return depth_summary, age_summary


def aggregate_resource_samples(
    samples: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    cpu: dict[str, list[float]] = defaultdict(list)
    memory: dict[str, list[int]] = defaultdict(list)
    for sample in samples:
        for service, row in sample.items():
            if service not in WORKER_PROFILES:
                continue
            profile = row["profile"]
            cpu[profile].append(float(row["cpu_cores"]))
            memory[profile].append(int(row["memory_bytes"]))
    return (
        {profile: summarize(values) for profile, values in sorted(cpu.items())},
        {profile: summarize(values) for profile, values in sorted(memory.items())},
    )


def aggregate_stream_samples(
    samples: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    pending: dict[str, list[int]] = defaultdict(list)
    lag: dict[str, list[int]] = defaultdict(list)
    oldest: dict[str, list[float]] = defaultdict(list)
    streams: dict[str, list[int]] = defaultdict(list)
    for sample in samples:
        for family, row in sample.items():
            streams[family].append(int(row.get("stream_count", 0)))
            pending[family].append(int(row.get("pending", 0)))
            if row.get("lag") is not None:
                lag[family].append(int(row["lag"]))
            if row.get("oldest_pending_seconds") is not None:
                oldest[family].append(float(row["oldest_pending_seconds"]))
    pending_out = {}
    lag_out = {}
    oldest_out = {}
    for family, _pattern in STREAM_CLASSES:
        pending_out[family] = summarize(pending.get(family, []))
        pending_out[family]["stream_count"] = summarize(streams.get(family, []))
        lag_out[family] = summarize(lag.get(family, []))
        lag_out[family]["status"] = (
            "recorded"
            if lag_out[family]["sample_count"]
            else "consumer_group_lag_unavailable"
        )
        oldest_out[family] = summarize(oldest.get(family, []))
        oldest_out[family]["status"] = (
            "recorded"
            if oldest_out[family]["sample_count"]
            else "no_pending_entries_observed"
        )
    return pending_out, lag_out, oldest_out


def postgres_summary(samples: list[dict[str, Any]], elapsed: float) -> dict[str, Any]:
    if not samples:
        return {"status": "unavailable"}
    first = samples[0]
    last = samples[-1]
    xacts = max(
        0,
        int(last["xact_commit"])
        + int(last["xact_rollback"])
        - int(first["xact_commit"])
        - int(first["xact_rollback"]),
    )
    block_reads = max(0, int(last["blks_read"]) - int(first["blks_read"]))
    block_hits = max(0, int(last["blks_hit"]) - int(first["blks_hit"]))
    total_blocks = block_reads + block_hits
    return {
        "status": "recorded",
        "connections": summarize(int(row["numbackends"]) for row in samples),
        "max_connections": int(last["max_connections"]),
        "connection_saturation_ratio_max": round(
            max(int(row["numbackends"]) for row in samples)
            / max(int(last["max_connections"]), 1),
            6,
        ),
        "transactions_per_second": round(xacts / max(elapsed, 1.0), 6),
        "waiting_locks": summarize(int(row["waiting_locks"]) for row in samples),
        "total_locks": summarize(int(row["total_locks"]) for row in samples),
        "block_read_ratio": round(block_reads / total_blocks, 9)
        if total_blocks
        else None,
        "block_read_time_delta_ms": round(
            max(
                0.0, float(last["blk_read_time_ms"]) - float(first["blk_read_time_ms"])
            ),
            6,
        ),
        "block_write_time_delta_ms": round(
            max(
                0.0,
                float(last["blk_write_time_ms"]) - float(first["blk_write_time_ms"]),
            ),
            6,
        ),
        "track_io_timing": str(last["track_io_timing"]),
        "deadlock_count_delta": max(
            0, int(last["deadlocks"]) - int(first["deadlocks"])
        ),
        "temp_file_count_delta": max(
            0, int(last["temp_files"]) - int(first["temp_files"])
        ),
        "temp_bytes_delta": max(0, int(last["temp_bytes"]) - int(first["temp_bytes"])),
    }


def lease_summary(samples: list[dict[str, Any]], elapsed: float) -> dict[str, Any]:
    if not samples:
        return {"status": "unavailable"}
    first = samples[0]
    last = samples[-1]
    delta = int(last["expired_lease_retry_count_total"]) - int(
        first["expired_lease_retry_count_total"]
    )
    return {
        "status": "recorded_from_monotonic_row_counters",
        "expired_lease_retry_count_delta": max(0, delta),
        "counter_reset_detected": delta < 0,
        "rate_per_second": round(max(0, delta) / max(elapsed, 1.0), 9),
        "unit_count_end": int(last["unit_count"]),
        "units_with_expired_lease_retry_end": int(
            last["units_with_expired_lease_retry"]
        ),
        "currently_leased_units_end": int(last["currently_leased_units"]),
        "currently_expired_leases_end": int(last["currently_expired_leases"]),
    }


def start_to_ready_summary(
    ready_timestamps: dict[str, list[float]], runtime: dict[str, Any]
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for service, profile in WORKER_PROFILES.items():
        if service == "beat":
            continue
        started_at = parse_utc(runtime[service]["started_at"]).timestamp()
        candidates = [
            stamp - started_at
            for stamp in ready_timestamps.get(profile, [])
            if stamp >= started_at
        ]
        result[profile] = {
            "status": "recorded" if candidates else "ready_marker_unavailable",
            "seconds": round(min(candidates), 6) if candidates else None,
        }
    return result


def forbidden_key_paths(value: Any, prefix: str = "$") -> list[str]:
    paths: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            lowered = key.lower()
            if lowered in FORBIDDEN_JSON_KEYS:
                paths.append(f"{prefix}.{key}")
            paths.extend(forbidden_key_paths(child, f"{prefix}.{key}"))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            paths.extend(forbidden_key_paths(child, f"{prefix}[{index}]"))
    return paths


def assert_sanitized(capture: dict[str, Any]) -> None:
    forbidden = forbidden_key_paths(capture)
    if forbidden:
        raise CaptureError("capture contains forbidden raw-data keys")
    serialized = json.dumps(capture, sort_keys=True)
    if UUID_RE.search(serialized):
        raise CaptureError("capture contains a UUID-shaped identifier")
    if DSN_RE.search(serialized):
        raise CaptureError("capture contains a DSN")
    if ABSOLUTE_PATH_RE.search(serialized):
        raise CaptureError("capture contains an absolute host path")


def validate_capture_shape(capture: dict[str, Any]) -> None:
    required = {
        "schema_version",
        "evidence_version",
        "capture_status",
        "scope",
        "authoritative_for_baseline",
        "authoritative_for_canary",
        "captured_at",
        "source_revision",
        "window",
        "sources",
        "measurements",
        "observability_gaps",
        "review",
        "gates",
        "redaction",
    }
    missing = required - capture.keys()
    if missing:
        raise CaptureError("capture is missing required top-level fields")
    if capture["schema_version"] != 1:
        raise CaptureError("unsupported capture schema version")
    if capture["scope"] != "production_equivalent_local":
        raise CaptureError("capture scope must remain explicit")
    if not capture["observability_gaps"]:
        raise CaptureError("unavailable baseline signals must remain explicit")
    assert_sanitized(capture)


def atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def build_capture(args: argparse.Namespace) -> dict[str, Any]:
    containers = discover_containers(args.project)
    runtime = inspect_runtime(containers)
    revision = source_revision(containers["worker"]["name"])
    capture_started = utc_now()
    common_worker_start = max(
        parse_utc(runtime[service]["started_at"])
        for service in WORKER_PROFILES
        if service != "beat"
    )
    history_start = max(
        common_worker_start,
        capture_started - timedelta(seconds=args.history_seconds),
    )

    resource_samples: list[dict[str, Any]] = []
    queue_samples: list[dict[str, Any]] = []
    stream_samples: list[dict[str, Any]] = []
    postgres_samples: list[dict[str, Any]] = []
    lease_samples: list[dict[str, Any]] = []
    inspect_samples: list[dict[str, Any]] = []
    sample_times: list[datetime] = []

    deadline = time.monotonic() + args.duration_seconds
    sample_number = 0
    while True:
        sample_number += 1
        sample_times.append(utc_now())
        print(f"baseline sample {sample_number}", file=sys.stderr, flush=True)
        resource_samples.append(docker_stats(containers))
        queue_samples.append(
            python_probe(containers["worker"]["name"], BROKER_PROBE, timeout=45.0)
        )
        stream_samples.append(
            python_probe(containers["worker"]["name"], STREAM_PROBE, timeout=45.0)
        )
        postgres_samples.append(
            psql_probe(containers["postgres"]["name"], POSTGRES_STATS_SQL)
        )
        lease_samples.append(
            psql_probe(containers["postgres"]["name"], SYNC_LEASE_STATS_SQL)
        )
        if sample_number == 1 or time.monotonic() >= deadline:
            inspect_samples.append(
                python_probe(
                    containers["worker"]["name"],
                    CELERY_INSPECT_PROBE,
                    timeout=45.0,
                )
            )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(args.interval_seconds, remaining))

    capture_ended = utc_now()
    elapsed = max((sample_times[-1] - sample_times[0]).total_seconds(), 1.0)
    outcomes, durations, ready_timestamps = aggregate_task_logs(
        containers, history_start, capture_ended
    )
    queue_depth, queue_age = aggregate_queue_samples(queue_samples)
    worker_cpu, worker_memory = aggregate_resource_samples(resource_samples)
    stream_pending, stream_lag, stream_oldest = aggregate_stream_samples(stream_samples)
    start_ready = start_to_ready_summary(ready_timestamps, runtime)

    restart_counts = {
        service: runtime[service]["restart_count"] for service in WORKER_PROFILES
    }
    deployment_observed = any(
        parse_utc(runtime[service]["started_at"]) >= history_start
        for service in WORKER_PROFILES
        if service != "beat"
    )
    gap_codes = [
        "enqueue_to_start_not_emitted_by_celery_workers",
        "no_controlled_process_crash_in_read_only_window",
        "deployment_drain_start_not_emitted",
        "task_discard_not_distinguishable_from_all_terminal_paths",
        "celery_process_metrics_not_exported",
    ]

    capture: dict[str, Any] = {
        "schema_version": 1,
        "evidence_version": "v0-celery-baseline",
        "capture_status": "recorded_with_explicit_observability_gaps",
        "scope": "production_equivalent_local",
        "authoritative_for_baseline": True,
        "authoritative_for_canary": False,
        "captured_at": iso_utc(capture_ended),
        "source_revision": revision["revision"],
        "runtime": {
            "compose_project": args.project,
            "dataset_scope": "shared_production_equivalent_dataset",
            "runtime_kind": "celery",
            "source_revision_basis": revision["basis"],
            "source_worktree_dirty": revision["worktree_dirty"],
            "services": runtime,
        },
        "window": {
            "start": iso_utc(history_start),
            "end": iso_utc(capture_ended),
            "timezone": "UTC",
            "contains_worker_deployment": deployment_observed,
            "live_sampling": {
                "start": iso_utc(sample_times[0]),
                "end": iso_utc(sample_times[-1]),
                "requested_duration_seconds": args.duration_seconds,
                "interval_seconds": args.interval_seconds,
                "sample_count": len(sample_times),
            },
        },
        "sources": {
            "recorder": "scripts/worker/capture_celery_baseline.py",
            "task_outcomes": "docker_logs_streamed_and_reduced_in_memory",
            "queue_state": "read_only_kombu_valkey_probe",
            "stream_state": "read_only_valkey_xinfo_xpending_probe",
            "worker_resources": "docker_stats_no_stream",
            "postgres": "read_only_pg_stat_database_pg_locks",
            "sync_leases": "read_only_sync_run_units_aggregate",
            "restricted_raw_evidence_reference": None,
            "raw_evidence_retained": False,
            "label_substitutions": [],
            "missing_series": gap_codes,
        },
        "measurements": {
            "enqueue_to_start_seconds_by_profile": {
                "status": "unavailable",
                "reason_code": "enqueue_timestamp_not_present_in_worker_completion_logs",
            },
            "oldest_queue_age_seconds_by_profile": queue_age,
            "queue_depth_by_profile": queue_depth,
            "task_outcome_rates_by_family": outcomes,
            "successful_task_duration_seconds_by_family": durations,
            "process_crash_recovery_seconds": {
                "status": "not_observed",
                "reason_code": "read_only_capture_did_not_inject_a_crash",
                "container_restart_counts": restart_counts,
            },
            "deployment_drain_and_recovery_seconds": {
                "status": "unavailable",
                "reason_code": "drain_start_marker_not_emitted",
            },
            "worker_start_to_ready_seconds_by_profile": start_ready,
            "sync_unit_lease_expiry_rate": lease_summary(lease_samples, elapsed),
            "stream_pending_count_by_group": stream_pending,
            "stream_lag_by_group": stream_lag,
            "stream_oldest_pending_seconds_by_group": stream_oldest,
            "worker_cpu_cores_by_profile": worker_cpu,
            "worker_memory_bytes_by_profile": worker_memory,
            "postgres_connections": postgres_summary(postgres_samples, elapsed)[
                "connections"
            ],
            "postgres_transactions_per_second": postgres_summary(
                postgres_samples, elapsed
            )["transactions_per_second"],
            "postgres_lock_or_io_pressure": postgres_summary(postgres_samples, elapsed),
            "celery_inspect": {
                "status": "recorded",
                "samples": inspect_samples,
            },
        },
        "observability_gaps": [
            {
                "code": "enqueue_to_start_not_emitted_by_celery_workers",
                "effect": "cannot_set_enqueue_latency_parity_threshold",
            },
            {
                "code": "no_controlled_process_crash_in_read_only_window",
                "effect": "cannot_set_process_crash_recovery_threshold",
            },
            {
                "code": "deployment_drain_start_not_emitted",
                "effect": "start_to_ready_is_recorded_but_drain_duration_is_not",
            },
            {
                "code": "task_discard_not_distinguishable_from_all_terminal_paths",
                "effect": "discard_rate_is_a_lower_bound_from_explicit_log_markers",
            },
            {
                "code": "celery_process_metrics_not_exported",
                "effect": "task_aggregates_use_ephemeral_log_reduction_instead_of_prometheus",
            },
        ],
        "review": {
            "reviewed_by": [],
            "reviewed_at": None,
            "parity_thresholds_approved": False,
        },
        "gates": {
            "phase_1_foundation": "complete",
            "production_canary": "blocked_until_observability_gaps_and_threshold_review",
        },
        "redaction": {
            "contains_raw_logs": False,
            "contains_task_args_results_or_payloads": False,
            "contains_credentials_or_dsns": False,
            "contains_tenant_identifiers": False,
            "contains_stream_keys": False,
            "automated_validation": "pass",
        },
    }
    validate_capture_shape(capture)
    return capture


def default_output() -> Path:
    root = Path(__file__).resolve().parents[2]
    return root / (
        "docs/architecture/evidence/go-worker-migration/v0-celery-baseline/capture.json"
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Record a sanitized read-only baseline from a running Celery Docker "
            "Compose project."
        )
    )
    parser.add_argument("--project", default="dev-health")
    parser.add_argument("--duration-seconds", type=float, default=300.0)
    parser.add_argument("--interval-seconds", type=float, default=30.0)
    parser.add_argument("--history-seconds", type=float, default=86400.0)
    parser.add_argument("--output", type=Path, default=default_output())
    args = parser.parse_args(argv)
    if args.duration_seconds < 0:
        parser.error("--duration-seconds must be non-negative")
    if args.interval_seconds <= 0:
        parser.error("--interval-seconds must be positive")
    if args.history_seconds <= 0:
        parser.error("--history-seconds must be positive")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        capture = build_capture(args)
        atomic_write_json(args.output, capture)
    except CaptureError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(f"sanitized baseline written: {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

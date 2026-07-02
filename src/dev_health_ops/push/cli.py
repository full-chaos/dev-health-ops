"""`dev-hops push` command group (CHAOS-2700): validate, batch, sample,
status, export(stubbed).

Registered from ``dev_health_ops.cli.build_parser()`` via
``register_commands(sub)`` -- matches ``backfill_cli.register_backfill_
commands``'s convention exactly. ``batch``/``status`` handlers are ``async
def``: ``cli.py``'s dispatch loop already runs ``asyncio.run(func(ns))``
whenever ``inspect.iscoroutinefunction(func)`` is true (brief decision 1),
so no new dispatch machinery is needed.

``push`` subcommands are excluded from ``--org`` auto-resolution and the
ClickHouse/Postgres DB preflight system entirely (brief decision 12,
enforced in ``dev_health_ops/cli.py``'s ``_should_resolve_org``/
``_COMMAND_REQUIREMENTS``) -- ``validate``/``sample`` take no network args at
all (fully offline); ``batch``/``status`` resolve their own
``--api-url``/``--token``/``--org`` (flag > env, brief decision 11) and fail
with a usage error (exit 2) if unresolved, rather than argparse's own
``required=True`` (which would defeat the env-var fallback that the
acceptance criteria and master-spec CC29 both require).
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
from importlib import metadata as importlib_metadata
from typing import Any

import httpx

from dev_health_ops.api.external_ingest.schema_registry import (
    iter_record_kinds,
    load_example,
)
from dev_health_ops.api.external_ingest.schemas import SCHEMA_VERSION

from . import output as out
from .export import EXPORT_PROVIDERS
from .http_client import (
    IngestApiError,
    IngestClientConfig,
    IngestTransientError,
    get_batch_status,
    get_schema_document,
    post_batch,
)
from .limits import DEFAULT_LIMITS, limits_from_schema_response
from .poll import (
    DEFAULT_POLL_INTERVAL_SECONDS,
    DEFAULT_POLL_TIMEOUT_SECONDS,
    TERMINAL_STATUSES,
    PollTimeoutError,
    StreamUnavailableResult,
    poll_until_terminal,
)
from .validate import PayloadParseError, check_envelope_shape, validate_payload

logger = logging.getLogger(__name__)

_RECORD_KINDS: tuple[str, ...] = tuple(
    sorted(kind for kind, _model in iter_record_kinds())
)


def _cli_version() -> str:
    try:
        return importlib_metadata.version("dev-health-ops")
    except importlib_metadata.PackageNotFoundError:
        return "0.0.0-dev"


# ---------------------------------------------------------------------------
# Env var / flag resolution (brief decision 11: FULLCHAOS_INGEST_TOKEN is
# primary; FULLCHAOS_API_TOKEN is a deprecated alias kept only because the
# literal Linear acceptance criterion names it)
# ---------------------------------------------------------------------------


def _resolve_api_url(ns: argparse.Namespace) -> str | None:
    value = getattr(ns, "api_url", None) or os.environ.get("FULLCHAOS_API_URL")
    return value.rstrip("/") if value else value


def _resolve_token(ns: argparse.Namespace) -> str | None:
    flag = getattr(ns, "token", None)
    if flag:
        return flag
    primary = os.environ.get("FULLCHAOS_INGEST_TOKEN")
    if primary:
        return primary
    legacy = os.environ.get("FULLCHAOS_API_TOKEN")
    if legacy:
        logger.warning(
            "FULLCHAOS_API_TOKEN is a deprecated alias; set FULLCHAOS_INGEST_TOKEN instead"
        )
        return legacy
    return None


def _resolve_org(ns: argparse.Namespace) -> str | None:
    return getattr(ns, "org", None) or os.environ.get("FULLCHAOS_ORG_ID")


def _resolve_client_config(ns: argparse.Namespace) -> IngestClientConfig | None:
    """Resolves --api-url/--token/--org, printing a usage-style error and
    returning None if anything is missing. Callers turn a None into exit 2
    (brief decision 9/12) -- deliberately NOT argparse `required=True`,
    which would block the env-var fallback the acceptance criteria require."""
    api_url = _resolve_api_url(ns)
    token = _resolve_token(ns)
    org_id = _resolve_org(ns)
    missing = [
        label
        for label, value in (
            ("--api-url / FULLCHAOS_API_URL", api_url),
            ("--token / FULLCHAOS_INGEST_TOKEN", token),
            ("--org / FULLCHAOS_ORG_ID", org_id),
        )
        if not value
    ]
    if missing or api_url is None or token is None or org_id is None:
        print(f"error: missing required: {', '.join(missing)}", file=sys.stderr)
        return None
    return IngestClientConfig(api_url=api_url, token=token, org_id=org_id)


class PayloadTooLargeError(Exception):
    def __init__(self, max_bytes: int) -> None:
        super().__init__(f"payload exceeds {max_bytes} bytes")
        self.max_bytes = max_bytes


def _read_payload_arg(payload_arg: str, *, max_bytes: int) -> bytes:
    """Reads at most `max_bytes + 1` bytes from the file/stdin -- never
    buffers an unbounded source into memory before a size guard has a
    chance to reject it (Codex adversarial-review finding: a bad pipe or
    oversized CI artifact could otherwise be fully read into memory before
    the intended `payload_too_large` check ever runs). `io.BufferedReader.
    read(n)` (both `sys.stdin.buffer` and an open file handle are one)
    blocks until `n` bytes are read or EOF, so this reliably detects
    "larger than the cap" without reading past it."""
    if payload_arg == "-":
        data = sys.stdin.buffer.read(max_bytes + 1)
    else:
        with open(payload_arg, "rb") as f:
            data = f.read(max_bytes + 1)
    if len(data) > max_bytes:
        raise PayloadTooLargeError(max_bytes)
    return data


def _positive_finite_float(value: str) -> float:
    """Argparse `type=` validator for `--poll-timeout` (and the base check
    `_poll_interval_type` below builds on for `--poll-interval`) (Codex
    adversarial-review finding: unbounded `float` accepted
    `--poll-interval -1` -- a hot-loop of immediate GETs -- and
    `--poll-timeout inf`, which defeats `poll.py`'s bounded-termination
    guarantee entirely). Rejects non-numeric, non-finite (`inf`/`nan` --
    plain `float()` accepts both), zero, and negative values with a clean
    usage error (exit 2) instead of a runtime hang or API-hammering loop."""
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid float value: {value!r}") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError(
            f"must be a finite positive number, got {value!r}"
        )
    return parsed


#: Floor for `--poll-interval` specifically (Codex adversarial-review
#: finding, round 2): `_positive_finite_float` alone still accepts an
#: arbitrarily tiny positive value (e.g. `1e-300`), which hot-loops
#: `GET /batches/{id}` against the API for the entire `--poll-timeout`
#: window -- the same API-hammering risk the negative-interval fix was
#: meant to close, just approached from zero instead of below it.
MIN_POLL_INTERVAL_SECONDS = 0.5


def _poll_interval_type(value: str) -> float:
    parsed = _positive_finite_float(value)
    if parsed < MIN_POLL_INTERVAL_SECONDS:
        raise argparse.ArgumentTypeError(
            f"--poll-interval must be >= {MIN_POLL_INTERVAL_SECONDS}s, got {value!r}"
        )
    return parsed


def _kind_type(value: str) -> str:
    """Accepts both the versioned form (``pull_request.v1``, canonical
    everywhere per master-spec CC1) and the bare form (``pull_request``, as
    shown in the Linear issue's literal acceptance-criteria examples) for
    ergonomics -- normalizes to the versioned form."""
    if value in _RECORD_KINDS:
        return value
    candidate = f"{value}.v1"
    if candidate in _RECORD_KINDS:
        return candidate
    raise argparse.ArgumentTypeError(
        f"invalid choice: {value!r} (choose from {', '.join(_RECORD_KINDS)})"
    )


# ---------------------------------------------------------------------------
# register_commands
# ---------------------------------------------------------------------------


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    push_parser = subparsers.add_parser(
        "push", help="Customer-push external ingestion CLI (CHAOS-2690)."
    )
    push_sub = push_parser.add_subparsers(dest="push_command", required=True)

    _register_validate(push_sub)
    _register_batch(push_sub)
    _register_sample(push_sub)
    _register_status(push_sub)
    _register_export(push_sub)


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------


def _register_validate(push_sub: argparse._SubParsersAction) -> None:
    p = push_sub.add_parser(
        "validate", help="Validate a batch payload locally (no network call)."
    )
    p.add_argument(
        "payload", help="Path to a batch envelope JSON file, or '-' to read stdin."
    )
    p.add_argument(
        "--schema",
        default=SCHEMA_VERSION,
        choices=(SCHEMA_VERSION,),
        help=f"Schema version to validate against. Default/only supported: {SCHEMA_VERSION}.",
    )
    p.add_argument(
        "--json", action="store_true", help="Emit machine-readable JSON to stdout."
    )
    p.set_defaults(func=_cmd_validate)


def _cmd_validate(ns: argparse.Namespace) -> int:
    try:
        raw = _read_payload_arg(ns.payload, max_bytes=DEFAULT_LIMITS.max_body_bytes)
    except PayloadTooLargeError as exc:
        errors = [
            {
                "index": -1,
                "kind": None,
                "code": "payload_too_large",
                "message": str(exc),
                "path": None,
            }
        ]
        if ns.json:
            out.emit_json(
                {
                    "valid": False,
                    "itemsAccepted": 0,
                    "itemsRejected": 0,
                    "errors": errors,
                }
            )
        else:
            out.emit_rejection_table(errors)
        return out.EXIT_DATA_FAILURE
    except OSError as exc:
        print(f"error: cannot read payload: {exc}", file=sys.stderr)
        return out.EXIT_USAGE_ERROR

    try:
        outcome = validate_payload(raw)
    except PayloadParseError as exc:
        errors = exc.errors or [
            {
                "index": -1,
                "kind": None,
                "code": "invalid_envelope",
                "message": str(exc),
                "path": None,
            }
        ]
        if ns.json:
            out.emit_json(
                {
                    "valid": False,
                    "itemsAccepted": 0,
                    "itemsRejected": 0,
                    "errors": errors,
                }
            )
        else:
            out.emit_rejection_table(errors)
        return out.EXIT_DATA_FAILURE

    if ns.json:
        out.emit_json(
            {
                "valid": outcome.valid,
                "itemsAccepted": outcome.items_accepted,
                "itemsRejected": outcome.items_rejected,
                "errors": outcome.errors,
            }
        )
    elif outcome.valid:
        print(f"valid: {outcome.items_accepted} record(s) accepted")
    else:
        out.emit_rejection_table(outcome.errors)

    return out.EXIT_OK if outcome.valid else out.EXIT_DATA_FAILURE


# ---------------------------------------------------------------------------
# sample -- CC18/CC29: no push/samples/ dir; loads CHAOS-2692's packaged
# examples via schema_registry.load_example(kind) and wraps each bare
# payload into a full record envelope + single-record batch envelope.
# ---------------------------------------------------------------------------


#: Per-kind correlation-id ("wrapper externalId") derivation -- CC1: the
#: wrapper's externalId is the per-record correlation id used in rejection
#: diagnostics; kind-specific payload fields are authoritative for
#: normalization. Each rule below matches the natural key already present
#: in that kind's payload (mirrors master-spec section 2's own canonical
#: batch-envelope example, e.g. pull_request -> "acme/api#123").
def _derive_correlation_external_id(kind: str, payload: dict[str, Any]) -> str:
    base_kind = kind.rsplit(".", 1)[0]
    if base_kind == "repository":
        return str(payload["externalId"])
    if base_kind == "identity":
        return str(payload["canonicalId"])
    if base_kind == "team":
        return str(payload["id"])
    if base_kind == "work_item":
        return str(payload["externalKey"])
    if base_kind == "work_item_transition":
        return f"{payload['externalKey']}:{payload['occurredAt']}"
    if base_kind == "work_item_dependency":
        return f"{payload['sourceExternalKey']}->{payload['targetExternalKey']}"
    if base_kind == "pull_request":
        return f"{payload['repositoryExternalId']}#{payload['number']}"
    if base_kind == "review":
        return (
            f"{payload['repositoryExternalId']}#{payload['pullRequestNumber']}"
            f":review:{payload['reviewId']}"
        )
    if base_kind == "commit":
        return f"{payload['repositoryExternalId']}@{payload['hash']}"
    raise ValueError(f"no correlation-id rule for kind: {kind!r}")


def _sample_record(kind: str) -> dict[str, Any]:
    payload = load_example(kind)
    return {
        "kind": kind,
        "externalId": _derive_correlation_external_id(kind, payload),
        "payload": payload,
    }


def _wrap_batch_envelope(
    records: list[dict[str, Any]], *, idempotency_key: str
) -> dict[str, Any]:
    return {
        "schemaVersion": SCHEMA_VERSION,
        "idempotencyKey": idempotency_key,
        "source": {
            "type": "customer_push",
            # github supports every one of the 9 record kinds (master-spec
            # CC6), so a single combined `--all` sample stays valid even
            # though the packaged work-item examples (CHAOS-2692) happen to
            # carry Jira-flavored data -- CHAOS-2701 owns polished,
            # per-system customer-doc examples; this is a CLI smoke fixture.
            "system": "github",
            "instance": "acme/api",
            "producer": "dev-hops-cli",
            "producerVersion": _cli_version(),
        },
        "window": {
            "startedAt": "2026-06-20T00:00:00Z",
            "endedAt": "2026-06-26T00:00:00Z",
        },
        "records": records,
    }


def _register_sample(push_sub: argparse._SubParsersAction) -> None:
    p = push_sub.add_parser(
        "sample",
        help="Print a canonical sample batch envelope (CHAOS-2692's packaged examples).",
    )
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--kind",
        type=_kind_type,
        metavar="KIND",
        help=f"Record kind, bare or versioned (e.g. pull_request or pull_request.v1). Choices: {', '.join(_RECORD_KINDS)}.",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Print a combined batch envelope with one record of every kind.",
    )
    p.set_defaults(func=_cmd_sample)


def _cmd_sample(ns: argparse.Namespace) -> int:
    if ns.all:
        envelope = _wrap_batch_envelope(
            [_sample_record(kind) for kind in _RECORD_KINDS],
            idempotency_key="sample-full-batch",
        )
    else:
        envelope = _wrap_batch_envelope(
            [_sample_record(ns.kind)], idempotency_key=f"sample-{ns.kind}"
        )
    print(json.dumps(envelope, indent=2, sort_keys=True))
    return out.EXIT_OK


# ---------------------------------------------------------------------------
# batch
# ---------------------------------------------------------------------------


def _register_batch(push_sub: argparse._SubParsersAction) -> None:
    p = push_sub.add_parser(
        "batch", help="Submit a batch payload to the external-ingest API."
    )
    p.add_argument(
        "payload", help="Path to a batch envelope JSON file, or '-' to read stdin."
    )
    p.add_argument(
        "--api-url",
        default=None,
        help="FullChaos API base URL. Env: FULLCHAOS_API_URL.",
    )
    p.add_argument(
        "--token",
        default=None,
        help="Ingest token (fcpush_...). Env: FULLCHAOS_INGEST_TOKEN (deprecated alias: FULLCHAOS_API_TOKEN).",
    )
    p.add_argument(
        "--org", default=None, help="Organization ID. Env: FULLCHAOS_ORG_ID."
    )
    p.add_argument(
        "--poll",
        action="store_true",
        help="Poll GET /batches/{id} until a terminal status.",
    )
    p.add_argument(
        "--poll-interval",
        type=_poll_interval_type,
        default=DEFAULT_POLL_INTERVAL_SECONDS,
        help=(
            f"Seconds between polls (minimum {MIN_POLL_INTERVAL_SECONDS}). "
            f"Default: {DEFAULT_POLL_INTERVAL_SECONDS}."
        ),
    )
    p.add_argument(
        "--poll-timeout",
        type=_positive_finite_float,
        default=DEFAULT_POLL_TIMEOUT_SECONDS,
        help=f"Give up polling after this many seconds. Default: {DEFAULT_POLL_TIMEOUT_SECONDS}.",
    )
    p.add_argument(
        "--skip-limits-check",
        action="store_true",
        help="Skip the GET /schemas limits pre-flight; use hardcoded client defaults only.",
    )
    p.add_argument(
        "--json", action="store_true", help="Emit machine-readable JSON to stdout."
    )
    p.set_defaults(func=_cmd_batch)


def _emit_result(ns: argparse.Namespace, body: dict[str, Any]) -> None:
    if ns.json:
        out.emit_json(body)
        return
    ingestion_id = body.get("ingestionId") or body.get("ingestion_id")
    status = body.get("status")
    print(f"ingestion_id: {ingestion_id}")
    print(f"status: {status}")
    if "itemsReceived" in body:
        print(f"items_received: {body.get('itemsReceived')}")
    if "itemsAccepted" in body:
        print(f"items_accepted: {body.get('itemsAccepted')}")
    if "itemsRejected" in body:
        print(f"items_rejected: {body.get('itemsRejected')}")
    errors = body.get("errors")
    if errors:
        out.emit_rejection_table(errors)


def _emit_api_error(ns: argparse.Namespace, exc: IngestApiError) -> int:
    if ns.json:
        out.emit_json(
            {"error": {"code": exc.code, "message": exc.message, "errors": exc.errors}}
        )
    else:
        out.emit_api_error_human(exc.status_code, exc.code, exc.message, exc.errors)
    return out.EXIT_TRANSPORT_ERROR


def _emit_transport_error(ns: argparse.Namespace, message: str) -> int:
    if ns.json:
        out.emit_json({"error": {"code": "transport_error", "message": message}})
    else:
        print(f"error: transport_error: {message}", file=sys.stderr)
    return out.EXIT_TRANSPORT_ERROR


def _emit_stream_unavailable(
    ns: argparse.Namespace, status_body: dict[str, Any]
) -> int:
    hint = "re-run `push batch` (same idempotency key re-enqueues)"
    if ns.json:
        out.emit_json({**status_body, "hint": hint})
    else:
        _emit_result(ns, status_body)
        print(hint, file=sys.stderr)
    return out.EXIT_TRANSPORT_ERROR


def _emit_poll_timeout(ns: argparse.Namespace, last_status: dict[str, Any]) -> int:
    hint = "poll timed out; re-run `push status <ingestion_id> --poll` rather than resubmitting"
    if ns.json:
        out.emit_json({**last_status, "hint": hint})
    else:
        _emit_result(ns, last_status)
        print(hint, file=sys.stderr)
    return out.EXIT_POLL_TIMEOUT


def _exit_for_terminal_status(final: dict[str, Any]) -> int:
    status = final.get("status")
    items_rejected = final.get("itemsRejected", final.get("items_rejected", 0)) or 0
    if status == "completed" and items_rejected == 0:
        return out.EXIT_OK
    return out.EXIT_DATA_FAILURE


async def _cmd_batch(ns: argparse.Namespace) -> int:
    config = _resolve_client_config(ns)
    if config is None:
        return out.EXIT_USAGE_ERROR

    async with httpx.AsyncClient(timeout=30.0) as client:
        limits = DEFAULT_LIMITS
        if not ns.skip_limits_check:
            schema_doc = await get_schema_document(client, config.api_url)
            limits = limits_from_schema_response(schema_doc)

        # Limits resolved before reading the payload (Codex adversarial-
        # review finding): bounds the read against the live server limit
        # rather than fully buffering an oversized file/stdin stream first.
        try:
            raw = _read_payload_arg(ns.payload, max_bytes=limits.max_body_bytes)
        except PayloadTooLargeError as exc:
            errors = [
                {
                    "index": -1,
                    "kind": None,
                    "code": "payload_too_large",
                    "message": str(exc),
                    "path": None,
                }
            ]
            if ns.json:
                out.emit_json(
                    {
                        "valid": False,
                        "itemsAccepted": 0,
                        "itemsRejected": len(errors),
                        "errors": errors,
                    }
                )
            else:
                out.emit_rejection_table(errors)
            return out.EXIT_DATA_FAILURE
        except OSError as exc:
            print(f"error: cannot read payload: {exc}", file=sys.stderr)
            return out.EXIT_USAGE_ERROR

        envelope, shape_errors = check_envelope_shape(raw, limits=limits)
        if envelope is None:
            assert shape_errors is not None
            if ns.json:
                out.emit_json(
                    {
                        "valid": False,
                        "itemsAccepted": 0,
                        "itemsRejected": len(shape_errors),
                        "errors": shape_errors,
                    }
                )
            else:
                out.emit_rejection_table(shape_errors)
            return out.EXIT_DATA_FAILURE

        try:
            status_code, body = await post_batch(
                client, config, raw, idempotency_key=envelope.idempotency_key
            )
        except IngestApiError as exc:
            return _emit_api_error(ns, exc)
        except IngestTransientError as exc:
            return _emit_transport_error(ns, str(exc))

        if not ns.poll:
            _emit_result(ns, body)
            return out.EXIT_OK

        ingestion_id = body.get("ingestionId") or body.get("ingestion_id")
        if not isinstance(ingestion_id, str):
            return _emit_transport_error(
                ns, f"server response is missing ingestionId: {body!r}"
            )

        # CC13/CC22: a 200 (REPLAY) response already carries the full
        # status envelope -- short-circuit polling if it's already
        # terminal rather than paying for one more GET /batches/{id}.
        if status_code == 200 and body.get("status") in TERMINAL_STATUSES:
            final = body
        else:
            try:
                final = await poll_until_terminal(
                    client,
                    config,
                    ingestion_id,
                    interval_seconds=ns.poll_interval,
                    timeout_seconds=ns.poll_timeout,
                )
            except StreamUnavailableResult as exc:
                return _emit_stream_unavailable(ns, exc.status_body)
            except PollTimeoutError as exc:
                return _emit_poll_timeout(ns, exc.last_status)
            except IngestApiError as exc:
                return _emit_api_error(ns, exc)
            except IngestTransientError as exc:
                return _emit_transport_error(ns, str(exc))

        _emit_result(ns, final)
        return _exit_for_terminal_status(final)


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


def _register_status(push_sub: argparse._SubParsersAction) -> None:
    p = push_sub.add_parser(
        "status", help="Fetch (and optionally poll) a batch's ingestion status."
    )
    p.add_argument("ingestion_id", help="The ingestion_id returned by `push batch`.")
    p.add_argument(
        "--api-url",
        default=None,
        help="FullChaos API base URL. Env: FULLCHAOS_API_URL.",
    )
    p.add_argument(
        "--token",
        default=None,
        help="Ingest token (fcpush_...). Env: FULLCHAOS_INGEST_TOKEN (deprecated alias: FULLCHAOS_API_TOKEN).",
    )
    p.add_argument(
        "--org", default=None, help="Organization ID. Env: FULLCHAOS_ORG_ID."
    )
    p.add_argument(
        "--poll",
        action="store_true",
        help="Poll GET /batches/{id} until a terminal status.",
    )
    p.add_argument(
        "--poll-interval",
        type=_poll_interval_type,
        default=DEFAULT_POLL_INTERVAL_SECONDS,
        help=(
            f"Seconds between polls (minimum {MIN_POLL_INTERVAL_SECONDS}). "
            f"Default: {DEFAULT_POLL_INTERVAL_SECONDS}."
        ),
    )
    p.add_argument(
        "--poll-timeout",
        type=_positive_finite_float,
        default=DEFAULT_POLL_TIMEOUT_SECONDS,
        help=f"Give up polling after this many seconds. Default: {DEFAULT_POLL_TIMEOUT_SECONDS}.",
    )
    p.add_argument(
        "--json", action="store_true", help="Emit machine-readable JSON to stdout."
    )
    p.set_defaults(func=_cmd_status)


async def _cmd_status(ns: argparse.Namespace) -> int:
    config = _resolve_client_config(ns)
    if config is None:
        return out.EXIT_USAGE_ERROR

    async with httpx.AsyncClient(timeout=30.0) as client:
        if not ns.poll:
            try:
                body = await get_batch_status(client, config, ns.ingestion_id)
            except IngestApiError as exc:
                return _emit_api_error(ns, exc)
            except IngestTransientError as exc:
                return _emit_transport_error(ns, str(exc))
            _emit_result(ns, body)
            return out.EXIT_OK

        try:
            final = await poll_until_terminal(
                client,
                config,
                ns.ingestion_id,
                interval_seconds=ns.poll_interval,
                timeout_seconds=ns.poll_timeout,
            )
        except StreamUnavailableResult as exc:
            return _emit_stream_unavailable(ns, exc.status_body)
        except PollTimeoutError as exc:
            return _emit_poll_timeout(ns, exc.last_status)
        except IngestApiError as exc:
            return _emit_api_error(ns, exc)
        except IngestTransientError as exc:
            return _emit_transport_error(ns, str(exc))

        _emit_result(ns, final)
        return _exit_for_terminal_status(final)


# ---------------------------------------------------------------------------
# export (stubbed, brief decision 14)
# ---------------------------------------------------------------------------


def _register_export(push_sub: argparse._SubParsersAction) -> None:
    p = push_sub.add_parser(
        "export",
        help="Provider export helpers (not implemented in v1; extension point).",
    )
    p.add_argument(
        "provider", metavar="PROVIDER", help="Provider name, e.g. github or gitlab."
    )
    p.add_argument(
        "--repo",
        default=None,
        help="Repository full name (provider-specific; unused by the stub).",
    )
    p.set_defaults(func=_cmd_export)


def _cmd_export(ns: argparse.Namespace) -> int:
    handler = EXPORT_PROVIDERS.get(ns.provider)
    if handler is not None:
        return int(handler(ns))
    print(
        f"error: `push export {ns.provider}` is not implemented in v1 -- see CHAOS-2690 plan; "
        "use `dev-hops push sample` + hand-written export, or the provider's native FullChaos sync instead.",
        file=sys.stderr,
    )
    return out.EXIT_DATA_FAILURE


__all__ = ["register_commands"]

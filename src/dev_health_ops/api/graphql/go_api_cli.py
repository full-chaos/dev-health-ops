"""``dev-hops go-api routing`` -- the operator surface for Go-API rollout.

Until this existed, the ONLY code in the repository that could write a
``go_api_routing_state`` row was a pytest helper
(``tests/api/graphql/test_go_api_livelocal.py``). Enabling an operation on
a real database meant hand-written SQL, and hand-written SQL meant
hand-typing a ``schema_digest`` -- which is precisely how twelve rows came
to be seeded on 2026-09-01 at a digest that a same-day SDL change made
unreachable within hours. Nothing rejected them, nothing reported them,
and every request silently fell back to Python for six days.

Two commands, and the split between them is deliberate:

``enable``
    A mutation. It refuses -- exit 2, nothing written -- on any doubt at
    all. Four preflights run in order, and each answers a question the
    September failure could not:

    1. **Is query-api reachable?** No answer is not a pass. A measurement
       that did not happen must fail loudly (root ``AGENTS.md``).
    2. **Do both planes hash the same SDL?** The Python edge reads
       ``contracts/graphql/v1/schema.graphql`` from its checkout; the Go
       binary hashes its own ``go:embed``ed copy. Rows follow the
       *image*. Writing rows from a checkout that has moved ahead of the
       deployed image produces exactly the dead rows this command exists
       to recover from -- so a mismatch is refused, naming both digests.
    3. **Does the running binary register the operation, under the same
       document digest?** The local catalog is a checked-in mirror of
       ``registrydump``; the deployed image is the authority. An
       operation the binary does not serve cannot be enabled into it.
    4. **Has this exact candidate build been proven?** See
       ``go_api_routing_admin.ENABLEMENT_PROOF_STAGE``.

``status``
    A diagnostic. It NEVER refuses and never exits non-zero for an
    unhealthy state, because it is what an operator runs when things are
    already broken -- including when query-api is down, which it reports
    as ``UNREACHABLE`` rather than failing. It prints both planes'
    digests and, per registered operation, whether that operation's row
    is ``MATCH`` / ``STALE`` / ``MISSING`` and whether it is ``UNPROVEN``.

HTTP here is ``urllib.request`` from the standard library, not ``httpx``:
this is one small JSON GET, and keeping the module free of the web stack
means the CLI does not pay for (or fail on) an import it has no other use
for.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass

from .go_api_operation_catalog import catalog_entries
from .go_api_schema_digest import current_schema_digest

__all__ = ["register_commands"]

#: Bounds the /registry GET. This is a local, in-cluster call returning a
#: few hundred bytes; anything slower than this is a sick process, and an
#: operator waiting on an enablement should be told that quickly rather
#: than left hanging.
_REGISTRY_TIMEOUT_SECONDS = 10.0

#: Where an operator reads the recovery procedure. Referenced from every
#: refusal that a digest move can cause, so the message that stops the
#: command also says what to do about it.
_RUNBOOK = (
    "docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md "
    "(section: When the schema digest moves)"
)


class GoPlaneUnavailable(RuntimeError):
    """query-api could not be asked what it registers.

    Deliberately distinct from "query-api answered, and disagrees":
    unreachable is an environment problem an operator fixes by looking at
    the deployment, while a disagreement is a genuine drift finding. The
    two must not print the same message.
    """


@dataclass(frozen=True)
class GoPlaneRegistry:
    """What the RUNNING query-api says it serves."""

    schema_digest: str
    operations: dict[str, str]


def _fetch_go_plane_registry(base_url: str) -> GoPlaneRegistry:
    """GET ``{base_url}/registry``.

    Every failure -- transport, status, malformed body, missing field --
    raises :class:`GoPlaneUnavailable` with a message naming the URL. A
    partially-understood response is treated as no response: silently
    accepting a body missing ``schema_digest`` would defeat preflight 2
    exactly when it matters.
    """
    # Reject anything but http(s) BEFORE urlopen: urllib happily opens
    # file:// (and ftp://), so a malformed GO_API_QUERY_API_URL could have
    # this "read the running binary's registry" step read a local file
    # instead and report a confusing parse error rather than the real
    # problem. An operator-supplied URL is exactly where that typo lives.
    if not base_url.lower().startswith(("http://", "https://")):
        raise GoPlaneUnavailable(
            f"query-api URL must be http:// or https://, got {base_url!r}"
        )
    url = base_url.rstrip("/") + "/registry"
    try:
        with urllib.request.urlopen(url, timeout=_REGISTRY_TIMEOUT_SECONDS) as resp:
            if resp.status != 200:
                raise GoPlaneUnavailable(f"{url} returned HTTP {resp.status}")
            payload = json.loads(resp.read().decode("utf-8"))
    except GoPlaneUnavailable:
        raise
    except urllib.error.HTTPError as exc:
        raise GoPlaneUnavailable(
            f"{url} returned HTTP {exc.code} -- a 404 usually means the "
            "running query-api predates GET /registry, or its /query route "
            "is unmounted (CLICKHOUSE_URI / GO_API_REGISTRY_POSTGRES_URI / "
            "GO_API_ENVELOPE_* unset)"
        ) from exc
    except Exception as exc:
        raise GoPlaneUnavailable(f"{url} is unreachable: {exc}") from exc

    if not isinstance(payload, dict):
        raise GoPlaneUnavailable(f"{url} returned a non-object JSON body")
    schema_digest = payload.get("schema_digest")
    if not isinstance(schema_digest, str) or not schema_digest:
        raise GoPlaneUnavailable(f"{url} returned no usable schema_digest")
    entries = payload.get("operations")
    if not isinstance(entries, list):
        raise GoPlaneUnavailable(f"{url} returned no usable operations list")
    operations: dict[str, str] = {}
    for entry in entries:
        if (
            not isinstance(entry, dict)
            or not isinstance(entry.get("operation"), str)
            or not isinstance(entry.get("document_digest"), str)
        ):
            raise GoPlaneUnavailable(f"{url} returned a malformed operations entry")
        operations[entry["operation"]] = entry["document_digest"]
    return GoPlaneRegistry(schema_digest=schema_digest, operations=operations)


def _query_api_url(ns: argparse.Namespace) -> str | None:
    return getattr(ns, "query_api_url", None) or os.getenv("GO_API_QUERY_API_URL")


def _refuse(message: str) -> int:
    """Print a refusal to stderr and return the exit code.

    Exit 2, matching this CLI's existing usage-error convention
    (``run_preflight_checks`` calls ``parser.error``, which exits 2), so a
    refusal is distinguishable from a crash (1) by a calling script.
    """
    print(f"REFUSED: {message}", file=sys.stderr)
    return 2


def _resolve_requested_operations(
    requested: str, catalog: dict[str, str]
) -> tuple[list[str], str | None]:
    """Resolve ``--operations`` to a concrete, catalog-validated list.

    Returns ``(operations, error)``. An unknown operation name is an
    error rather than a silent skip: an operator who typos an operation
    should be told, not handed a successful-looking run that enabled
    everything except the one they cared about.
    """
    if requested.strip() == "all-registered":
        return sorted(catalog), None
    names = [name.strip() for name in requested.split(",") if name.strip()]
    if not names:
        return [], "--operations was empty"
    unknown = [name for name in names if name not in catalog]
    if unknown:
        return [], (
            f"unknown operation(s) {', '.join(sorted(unknown))} -- not in the "
            "registered-operation catalog (api/graphql/go_api_operations.json, "
            "regenerated by scripts/go_api/generate_operation_catalog.py from "
            f"registrydump). Known: {', '.join(sorted(catalog))}"
        )
    return sorted(set(names)), None


async def _cmd_routing_enable(ns: argparse.Namespace) -> int:
    from dev_health_ops.db import get_postgres_session

    from .go_api_routing_admin import (
        ENABLEMENT_PROOF_STAGE,
        ENABLEMENT_PROOF_TERMINAL_STATE,
        enable_operation,
        operations_with_enablement_proof,
    )

    catalog = dict(catalog_entries())
    if not catalog:
        return _refuse(
            "the registered-operation catalog is empty or failed to load "
            "(api/graphql/go_api_operations.json) -- refusing to enable "
            "anything on a catalog this process cannot read"
        )

    operations, error = _resolve_requested_operations(ns.operations, catalog)
    if error:
        return _refuse(error)

    local_digest = current_schema_digest()

    # --- Preflight 1: is query-api reachable at all? -------------------
    base_url = _query_api_url(ns)
    if not base_url:
        return _refuse(
            "no query-api URL: pass --query-api-url or set "
            "GO_API_QUERY_API_URL. Enabling rows without asking the running "
            "binary what it serves is how the 2026-09-01 rows were written; "
            "this command will not do it."
        )
    try:
        go_plane = _fetch_go_plane_registry(base_url)
    except GoPlaneUnavailable as exc:
        return _refuse(
            f"cannot read the running query-api's registry: {exc}. A "
            "measurement that did not happen is not a pass -- fix the "
            "deployment or point --query-api-url at the right process."
        )

    # --- Preflight 2: do both planes hash the same SDL? ----------------
    if go_plane.schema_digest != local_digest:
        return _refuse(
            f"schema digest MISMATCH between planes.\n"
            f"  this checkout (python edge): {local_digest}\n"
            f"  running query-api (go)     : {go_plane.schema_digest}\n"
            "Routing rows are keyed by schema_digest, so rows written now "
            "would be unreachable to the running binary -- the exact defect "
            "of 2026-09-01. Rebuild and redeploy the query-api image from "
            f"this SDL, THEN re-run this command. See {_RUNBOOK}"
        )

    # --- Preflight 3: does the binary register each operation? ---------
    not_registered = [op for op in operations if op not in go_plane.operations]
    if not_registered:
        return _refuse(
            f"the running query-api does not register: "
            f"{', '.join(sorted(not_registered))}. It serves "
            f"{len(go_plane.operations)} operation(s); the local catalog "
            f"lists {len(catalog)}. The deployed image is the authority -- "
            "an operation it does not serve cannot be enabled into it."
        )
    digest_divergent = [
        op for op in operations if go_plane.operations[op] != catalog[op]
    ]
    if digest_divergent:
        detail = "; ".join(
            f"{op}: local={catalog[op]} go={go_plane.operations[op]}"
            for op in sorted(digest_divergent)
        )
        return _refuse(
            f"document digest MISMATCH for {len(digest_divergent)} "
            f"operation(s): {detail}. The registered document text differs "
            "between this checkout and the deployed binary; a row written "
            "with the local digest would never be looked up. Regenerate the "
            "catalog (scripts/go_api/generate_operation_catalog.py) against "
            "the deployed revision, or redeploy."
        )

    # --- Preflight 4: is this candidate build proven? ------------------
    async with get_postgres_session() as session:
        proven = await operations_with_enablement_proof(
            session,
            schema_digest=local_digest,
            candidate_build=ns.candidate_build,
            operations=operations,
        )
        unproven = [op for op in operations if op not in proven]
        if unproven and not ns.acknowledge_unproven:
            return _refuse(
                f"no {ENABLEMENT_PROOF_STAGE}/"
                f"{ENABLEMENT_PROOF_TERMINAL_STATE} proof run recorded for "
                f"candidate build {ns.candidate_build} for: "
                f"{', '.join(sorted(unproven))}.\n"
                "Plan section 5 stage 3 requires the exact candidate build to "
                "have served the operation through real ingress, auth, "
                "parse/validate, dispatch and a real database -- a "
                "constructor, health check or bare 200 does not qualify. "
                "Record it with go_api_registry.record_proof_run, or pass "
                "--acknowledge-unproven to enable anyway (the row is then "
                "reported as UNPROVEN by `dev-hops go-api routing status` "
                "for as long as it is in force)."
            )

        for operation in operations:
            if operation in unproven:
                # One structured line PER ROW, not one per invocation: an
                # operator (or a log search six weeks later) must be able
                # to find which specific operations were turned on without
                # proof, not merely that some were.
                print(
                    "WARNING: go_api_routing.enabled_unproven "
                    f"operation={operation} stage_evidence=none "
                    f"candidate_build={ns.candidate_build} "
                    f"schema_digest={local_digest} mode={ns.mode}",
                    file=sys.stderr,
                )
            await enable_operation(
                session,
                schema_digest=local_digest,
                document_digest=catalog[operation],
                selected_operation=operation,
                candidate_build=ns.candidate_build,
                mode=ns.mode,
                rollout_percentage=ns.rollout,
            )
        await session.commit()

    print(
        f"enabled {len(operations)} operation(s) at schema_digest="
        f"{local_digest} candidate_build={ns.candidate_build} mode={ns.mode} "
        f"rollout={ns.rollout}"
    )
    for operation in operations:
        flag = " (UNPROVEN)" if operation in unproven else ""
        print(f"  {operation}{flag}")
    return 0


async def _cmd_routing_status(ns: argparse.Namespace) -> int:
    from dev_health_ops.db import get_postgres_session

    from .go_api_routing_admin import count_rows_by_schema_digest, routing_status_rows

    catalog = catalog_entries()
    local_digest = current_schema_digest()

    base_url = _query_api_url(ns)
    go_digest: str | None = None
    go_error: str | None = None
    if base_url:
        try:
            go_digest = _fetch_go_plane_registry(base_url).schema_digest
        except GoPlaneUnavailable as exc:
            go_error = str(exc)
    else:
        go_error = "no --query-api-url and GO_API_QUERY_API_URL is unset"

    async with get_postgres_session() as session:
        statuses = await routing_status_rows(
            session, live_schema_digest=local_digest, catalog=catalog
        )
        digest_counts = await count_rows_by_schema_digest(session)

    if ns.json:
        print(
            json.dumps(
                {
                    "python_plane_schema_digest": local_digest,
                    "go_plane_schema_digest": go_digest,
                    "go_plane_error": go_error,
                    "planes_agree": (
                        go_digest == local_digest if go_digest is not None else None
                    ),
                    "rows_by_schema_digest": digest_counts,
                    "operations": [
                        {
                            "operation": s.operation,
                            "document_digest": s.document_digest,
                            "digest_state": s.digest_state,
                            "mode": s.mode,
                            "current_candidate_build": s.current_candidate_build,
                            "rollout_percentage": s.rollout_percentage,
                            "owner": s.owner,
                            "updated_at": (
                                s.updated_at.isoformat() if s.updated_at else None
                            ),
                            "stale_digests": list(s.stale_digests),
                            "proven": s.proven,
                            "reachable": s.reachable,
                        }
                        for s in statuses
                    ],
                },
                indent=2,
            )
        )
        return 0

    print(f"python plane schema_digest : {local_digest}")
    if go_digest is not None:
        agree = "AGREE" if go_digest == local_digest else "MISMATCH"
        print(f"go plane schema_digest     : {go_digest}  [{agree}]")
        if go_digest != local_digest:
            print(
                "  !! Rows follow the deployed image. Every row written at "
                f"{local_digest} is unreachable to this binary. See {_RUNBOOK}"
            )
    else:
        print(f"go plane schema_digest     : UNREACHABLE ({go_error})")
    print("rows by schema_digest:")
    if digest_counts:
        for digest, count in sorted(digest_counts.items()):
            marker = "  <- live" if digest == local_digest else "  <- STALE"
            print(f"  {digest}  {count}{marker}")
    else:
        print("  (table is empty -- nothing is enabled for Go)")
    print()
    print(f"{'OPERATION':<24} {'DIGEST':<8} {'MODE':<10} {'ROLLOUT':<8} PROOF")
    for status in statuses:
        mode = status.mode or "-"
        rollout = (
            "-" if status.rollout_percentage is None else str(status.rollout_percentage)
        )
        if status.digest_state == "MATCH":
            proof = "ok" if status.proven else "UNPROVEN"
        else:
            proof = "-"
        print(
            f"{status.operation:<24} {status.digest_state:<8} {mode:<10} "
            f"{rollout:<8} {proof}"
        )
        if status.digest_state == "STALE":
            print(f"    stale rows at: {', '.join(status.stale_digests)}")
    return 0


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    """Register ``go-api routing enable|status`` under the root parser."""
    go_api = subparsers.add_parser(
        "go-api",
        help="Inspect and control Go-API (query-api) operation routing.",
    )
    go_api_sub = go_api.add_subparsers(dest="go_api_command", required=True)

    routing = go_api_sub.add_parser(
        "routing",
        help="Manage go_api_routing_state -- which operations query-api serves.",
    )
    routing_sub = routing.add_subparsers(dest="go_api_routing_command", required=True)

    enable = routing_sub.add_parser(
        "enable",
        help=(
            "Register a candidate build and point routing rows at it. "
            "Refuses unless the running query-api is reachable, agrees on "
            "the schema digest, registers every named operation, and has a "
            "recorded deployed-executed proof run."
        ),
    )
    enable.add_argument(
        "--operations",
        default="all-registered",
        help=(
            "Comma-separated operation names, or 'all-registered' (default) "
            "for every operation in the registrydump-generated catalog."
        ),
    )
    enable.add_argument(
        "--candidate-build",
        required=True,
        help=(
            "The candidate build these rows point at. Convention: the ops "
            "commit sha the running query-api image was built from."
        ),
    )
    enable.add_argument(
        "--mode",
        required=True,
        choices=["canary", "primary"],
        help=(
            "Routing mode. Only canary and primary make an operation "
            "reachable; python/disabled/shadow do not, so they are not "
            "offered by an 'enable' verb."
        ),
    )
    enable.add_argument(
        "--rollout",
        type=int,
        default=100,
        help=(
            "rollout_percentage written to the row (default 100). NOTE: "
            "neither plane enforces this yet -- PostgresSwitch.Enabled and "
            "the Python dispatcher both take no org argument, so canary "
            "means 'on for everyone, revocable'. Recorded, not obeyed."
        ),
    )
    enable.add_argument(
        "--query-api-url",
        dest="query_api_url",
        default=None,
        help="Base URL of the running query-api. Env: GO_API_QUERY_API_URL",
    )
    enable.add_argument(
        "--acknowledge-unproven",
        action="store_true",
        help=(
            "Enable operations that have no deployed-executed proof run for "
            "this candidate build. Logs one WARNING per row and marks them "
            "UNPROVEN in `status` for as long as they are in force."
        ),
    )
    enable.set_defaults(func=_cmd_routing_enable)

    status = routing_sub.add_parser(
        "status",
        help=(
            "Report both planes' schema digests and, per registered "
            "operation, whether its routing row is live (MATCH), dead "
            "(STALE), absent (MISSING), or enabled without proof (UNPROVEN). "
            "Never fails on an unhealthy state."
        ),
    )
    status.add_argument(
        "--query-api-url",
        dest="query_api_url",
        default=None,
        help="Base URL of the running query-api. Env: GO_API_QUERY_API_URL",
    )
    status.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON instead of the text table.",
    )
    status.set_defaults(func=_cmd_routing_status)

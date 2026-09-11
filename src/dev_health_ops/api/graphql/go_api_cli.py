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
    is ``MATCH`` / ``STALE`` / ``MISSING`` and whether it is ``UNPROVEN`` --
    plus, as a row of its own, any live row serving a document the catalog
    does not name (``DOCUMENT_DRIFT``) or an operation it does not register
    (``UNREGISTERED``).

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
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .go_api_operation_catalog import (
    catalog_entries,
    catalog_loaded_successfully,
)
from .go_api_schema_digest import current_schema_digest

if TYPE_CHECKING:
    from .go_api_routing_admin import AuthorizingReceipt

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


def _endpoint_label(url: str) -> str:
    """A safe name for the endpoint, built from parsed parts only.

    **Nothing in this module ever prints a query-api URL.** Five review
    rounds tried to make printing one safe -- first by matching the
    credential's shape (four leaks, each a character class the previous
    fix had not anticipated), then by deleting a known literal (which
    still leaked on a URL with no ``//``, because the credential could not
    be located to delete it, and still hid the host on a credential-free
    URL whose path happened to contain a ``:``).

    The lesson is that redaction is the wrong operation. A string that
    cannot be parsed cannot be reasoned about safely, and one that CAN be
    parsed does not need redacting -- its safe parts can simply be rebuilt.
    So this constructs a label from ``scheme`` and ``hostname`` and never
    touches the raw string.

    ``scheme`` is emitted only when it is ``http``/``https``: for
    ``alice:secret@host/path`` (no ``//``) ``urlsplit`` reads the scheme as
    ``alice`` -- which IS the username -- so an allowlist is what keeps the
    scheme itself from being the leak. Anything else is ``unparseable``.

    The port is deliberately omitted: ``urlsplit(...).port`` raises on a
    malformed one, which would be another failure path in the code whose
    entire job is not to fail interestingly.
    """
    try:
        parts = urllib.parse.urlsplit(url)
    except ValueError:
        return "unparseable"
    if parts.scheme not in ("http", "https") or not parts.hostname:
        return "unparseable"
    return f"{parts.scheme}://{parts.hostname}"


def _transport_failure(exc: Exception) -> str:
    """Describe a transport failure WITHOUT its message.

    An HTTP client's exception text routinely embeds the URL it was given,
    credentials and all, so ``str(exc)`` is never safe to print here. The
    class name plus an HTTP status (when the exception carries one) is the
    diagnostic content that matters, and neither can contain the URL.
    """
    code = getattr(exc, "code", None)
    if code is not None:
        return f"{exc.__class__.__name__} (HTTP {code})"
    return exc.__class__.__name__


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
            "query-api URL must be http:// or https://; the configured "
            "value is not (its raw text is deliberately not echoed -- it "
            "may carry credentials)"
        )
    url = base_url.rstrip("/") + "/registry"
    # Every message below names `endpoint`, never `url`: a URL can carry
    # credentials in its userinfo, and this module never prints one.
    endpoint = _endpoint_label(base_url)
    try:
        with urllib.request.urlopen(url, timeout=_REGISTRY_TIMEOUT_SECONDS) as resp:
            if resp.status != 200:
                raise GoPlaneUnavailable(
                    f"{endpoint}/registry returned HTTP {resp.status}"
                )
            payload = json.loads(resp.read().decode("utf-8"))
    except GoPlaneUnavailable:
        raise
    except urllib.error.HTTPError as exc:
        raise GoPlaneUnavailable(
            f"{endpoint}/registry returned HTTP {exc.code} -- a 404 usually means the "
            "running query-api predates GET /registry, or its /query route "
            "is unmounted (CLICKHOUSE_URI / GO_API_REGISTRY_POSTGRES_URI / "
            "GO_API_ENVELOPE_* unset)"
        ) from exc
    except Exception as exc:
        raise GoPlaneUnavailable(
            f"{endpoint}/registry is unreachable: {_transport_failure(exc)}"
        ) from exc

    if not isinstance(payload, dict):
        raise GoPlaneUnavailable(f"{endpoint}/registry returned a non-object JSON body")
    schema_digest = payload.get("schema_digest")
    if not isinstance(schema_digest, str) or not schema_digest:
        raise GoPlaneUnavailable(
            f"{endpoint}/registry returned no usable schema_digest"
        )
    entries = payload.get("operations")
    if not isinstance(entries, list):
        raise GoPlaneUnavailable(
            f"{endpoint}/registry returned no usable operations list"
        )
    operations: dict[str, str] = {}
    for entry in entries:
        if (
            not isinstance(entry, dict)
            or not isinstance(entry.get("operation"), str)
            or not isinstance(entry.get("document_digest"), str)
        ):
            raise GoPlaneUnavailable(
                f"{endpoint}/registry returned a malformed operations entry"
            )
        operation = entry["operation"]
        if operation in operations:
            # Silently keeping the last one would let a malformed or
            # tampered registry hide a second, different document digest
            # for the same operation -- and preflight 3 would then compare
            # against whichever copy happened to win.
            raise GoPlaneUnavailable(
                f"{endpoint}/registry lists operation {operation!r} more than once"
            )
        operations[operation] = entry["document_digest"]
    return GoPlaneRegistry(schema_digest=schema_digest, operations=operations)


def _recorded_by() -> str:
    """Who is running this command.

    Resolved by the tool, never typed by the operator and never inferred
    from their prose -- `recorded_by` answers "who" and `review_evidence`
    answers "why", and conflating them makes both unreliable. Falls back
    through the usual identity sources and finally to a literal, because a
    missing identity must read as unknown rather than as somebody.
    """
    for var in ("DEV_HOPS_OPERATOR", "SUDO_USER", "USER", "LOGNAME"):
        value = (os.getenv(var) or "").strip()
        if value:
            return value
    return "unknown"


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


def _enable_review_evidence(
    ns: argparse.Namespace, unproven: bool, receipt: AuthorizingReceipt | None = None
) -> str | None:
    """What goes in the row's `review_evidence`.

    An acknowledged-unproven enablement carries its reason DURABLY, on the
    row. Previously the only record was a WARNING line at the moment it
    happened: on 2026-09-07, 15 operations were enabled on an explicit
    ruling and that ruling lived in a chat message, which is precisely the
    "unreadable six weeks later" problem `status`'s UNPROVEN marker exists
    to flag.
    """
    supplied = (getattr(ns, "review_evidence", None) or "").strip()
    if unproven:
        prefix = "ACKNOWLEDGED-UNPROVEN: "
        return prefix + (supplied or "no reason given")
    # A proven row names the receipt that authorized it: without it the
    # row records nothing about WHICH
    # evidence carried the decision, and a predicate that admitted the
    # wrong receipt is indistinguishable from one that admitted the right one.
    if receipt is not None:
        return receipt.evidence() + (f"; {supplied}" if supplied else "")
    return supplied or None


async def _cmd_routing_enable(ns: argparse.Namespace) -> int:
    from dev_health_ops.db import get_postgres_session

    from .go_api_routing_admin import (
        ENABLEMENT_PROOF_STAGE,
        ENABLEMENT_TARGET_MODE_EDGE_ONLY,
        MEASUREMENT_ROUTE_EDGE,
        enable_operation,
        enablement_receipts,
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
        proven = await enablement_receipts(
            session,
            schema_digest=local_digest,
            candidate_build=ns.candidate_build,
            operations={op: catalog[op] for op in operations},
            target_mode=ns.mode,
        )
        unproven = [op for op in operations if op not in proven]
        if unproven and not ns.acknowledge_unproven:
            route_rule = (
                f"measured on the {MEASUREMENT_ROUTE_EDGE} route"
                if ns.mode == ENABLEMENT_TARGET_MODE_EDGE_ONLY
                else "measured on a recorded route"
            )
            return _refuse(
                f"no {ENABLEMENT_PROOF_STAGE} proof run admissible for "
                f"--mode {ns.mode} recorded for candidate build "
                f"{ns.candidate_build} for: {', '.join(sorted(unproven))}.\n"
                "An admissible receipt is a deployed_executed run at this "
                "exact (schema digest, document digest, operation, candidate "
                f"build) that is {route_rule}; bound to the serving build per "
                "request (build_binding = 'per_request' -- a NULL "
                "build_binding is a row written before alembic 0129, under "
                "older counting rules, and is never proof); recorded against "
                "a candidate_build that is not blank; and either terminated "
                "in 'match', or terminated in 'mismatch' with every "
                "difference cited against a named Python baseline defect "
                "(baseline_defect non-empty, no citation NULL or blank, AND "
                "differences_outside_baseline_defect = 0). Blank means made "
                "only of space, tab, newline, vertical tab, form feed, "
                "carriage return or NBSP. A mismatch is never rewritten -- it "
                "stays a mismatch in the row and is only READ as sufficient "
                "when fully cited.\n"
                + (
                    "Note: --mode primary requires EDGE evidence "
                    "specifically. A /query/proof receipt shows the build "
                    "CAN serve the operation, not that the product edge "
                    "DOES, and primary is served traffic.\n"
                    if ns.mode == ENABLEMENT_TARGET_MODE_EDGE_ONLY
                    else ""
                )
                + "Plan section 5 stage 3 requires the exact candidate build "
                "to have served the operation through real ingress, auth, "
                "parse/validate, dispatch and a real database -- a "
                "constructor, health check or bare 200 does not qualify. "
                "Record it by running cmd/go-api-prove against the deployed "
                "stack, or pass --acknowledge-unproven to enable anyway (the "
                "row is then reported as UNPROVEN by `dev-hops go-api routing "
                "status` for as long as it is in force)."
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
            else:
                # The proven twin of the line above: which receipt turned
                # this row on, so a log search finds the evidence behind
                # every enablement, not only behind the unproven ones.
                receipt = proven[operation]
                print(
                    f"INFO: go_api_routing.enabled operation={operation} "
                    f"receipt_id={receipt.receipt_id} "
                    f"terminal_state={receipt.terminal_state}"
                    + (
                        f" baseline_defect={','.join(receipt.baseline_defect)}"
                        if receipt.baseline_defect
                        else ""
                    )
                    + f" measurement_route={receipt.measurement_route} "
                    f"build_binding={receipt.build_binding} "
                    f"{receipt.shape_counts()} "
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
                review_evidence=_enable_review_evidence(
                    ns, operation in unproven, proven.get(operation)
                ),
                recorded_by=_recorded_by(),
            )
        await session.commit()

    if getattr(ns, "json", False):
        print(
            json.dumps(
                {
                    "schema_digest": local_digest,
                    "candidate_build": ns.candidate_build,
                    "mode": ns.mode,
                    "rollout": ns.rollout,
                    "operations": [
                        {
                            "operation": operation,
                            "document_digest": catalog[operation],
                            "proven": operation in proven,
                            "receipt": _receipt_json(proven.get(operation)),
                        }
                        for operation in operations
                    ],
                },
                indent=2,
            )
        )
        return 0
    print(
        f"enabled {len(operations)} operation(s) at schema_digest="
        f"{local_digest} candidate_build={ns.candidate_build} mode={ns.mode} "
        f"rollout={ns.rollout}"
    )
    for operation in operations:
        authorizing = proven.get(operation)
        if authorizing is None:
            print(f"  {operation} (UNPROVEN)")
            continue
        # A cited mismatch and a match must not print the same line: the
        # operator is told which kind of evidence turned the row on.
        cited = (
            f" baseline_defect={','.join(authorizing.baseline_defect)} "
            "(every difference cited)"
            if authorizing.baseline_defect
            else ""
        )
        print(
            f"  {operation}  proof: receipt {authorizing.receipt_id} "
            f"terminal_state={authorizing.terminal_state}{cited} "
            f"measurement_route={authorizing.measurement_route} "
            f"build_binding={authorizing.build_binding} "
            f"{authorizing.shape_counts()}"
        )
    return 0


def _receipt_json(receipt: AuthorizingReceipt | None) -> dict[str, object] | None:
    if receipt is None:
        return None
    return {
        "receipt_id": receipt.receipt_id,
        "terminal_state": receipt.terminal_state,
        "baseline_defect": list(receipt.baseline_defect),
        "measurement_route": receipt.measurement_route,
        "build_binding": receipt.build_binding,
        "observed_at": (
            receipt.observed_at.isoformat() if receipt.observed_at else None
        ),
        "covered_by_shape": (
            dict(receipt.covered_by_shape)
            if receipt.covered_by_shape is not None
            else None
        ),
        "outside_by_shape": (
            dict(receipt.outside_by_shape)
            if receipt.outside_by_shape is not None
            else None
        ),
    }


async def _cmd_routing_disable(ns: argparse.Namespace) -> int:
    """The off-ramp.

    `enable` shipped without one. For several hours on 2026-09-07 fifteen
    operations were live on the Go plane, four of them measurably
    divergent, and there was NO way to turn them off short of hand-written
    SQL -- the practice this whole surface exists to abolish. Plan section
    5 says "rollback is a registry change, not an image rollback"; this is
    the verb that makes that change possible.
    """
    from dev_health_ops.db import get_postgres_session

    from .go_api_routing_admin import DISABLE_MODES, apply_disable, plan_disable

    catalog = dict(catalog_entries())
    if not catalog:
        return _refuse(
            "the registered-operation catalog is empty or failed to load "
            "(api/graphql/go_api_operations.json)"
        )
    operations, error = _resolve_requested_operations(ns.operations, catalog)
    if error:
        return _refuse(error)
    if ns.mode not in DISABLE_MODES:
        return _refuse(f"--mode must be one of {', '.join(DISABLE_MODES)}")
    if ns.apply and not (getattr(ns, "review_evidence", None) or "").strip():
        return _refuse(
            "--apply requires --review-evidence: a mode change is a decision, "
            "and a decision with no durable reason is unreadable weeks later"
        )

    local_digest = current_schema_digest()
    selected = {op: catalog[op] for op in operations}

    async with get_postgres_session() as session:
        changes, problems = await plan_disable(
            session,
            schema_digest=local_digest,
            operations=selected,
            new_mode=ns.mode,
            expected_candidate_build=getattr(ns, "candidate_build", None),
        )
        if problems:
            return _refuse("; ".join(problems))

        actionable = [c for c in changes if not c.is_noop]
        print(f"schema_digest {local_digest}")
        print(f"{'OPERATION':<24} {'FROM':<10} -> {'TO':<10} CANDIDATE BUILD")
        for change in changes:
            current = change.current_mode or "(no row)"
            build = change.candidate_build or "-"
            suffix = "" if not change.is_noop else "   [no change]"
            print(
                f"{change.operation:<24} {current:<10} -> {change.new_mode:<10} "
                f"{build}{suffix}"
            )
            # A `primary` operation is the one Go is fully serving; saying so
            # out loud is cheap and the operator may not have realised.
            if change.current_mode == "primary" and change.new_mode == "disabled":
                print(
                    "    NOTE: this removes Go entirely for this operation -- "
                    "Python serves it from the next request."
                )

        if not ns.apply:
            print(
                f"\nDRY RUN: {len(actionable)} row(s) would change, "
                f"{len(changes) - len(actionable)} unchanged. "
                "Re-run with --apply --review-evidence '<why>' to write."
            )
            return 0

        applied = await apply_disable(
            session,
            schema_digest=local_digest,
            changes=changes,
            review_evidence=ns.review_evidence.strip(),
            recorded_by=_recorded_by(),
            expected_candidate_build=getattr(ns, "candidate_build", None),
        )
        await session.commit()

    for change in applied:
        # One structured line per row, so a log search finds the specific
        # operation and not merely that "something was disabled".
        print(
            f"go_api_routing.disabled operation={change.operation} "
            f"from={change.current_mode} to={change.new_mode} "
            f"schema_digest={local_digest} recorded_by={_recorded_by()}",
            file=sys.stderr,
        )
    print(f"\napplied: {len(applied)} row(s) now mode={ns.mode}")
    expected_rows = len([c for c in changes if c.current_mode is not None])
    if len(applied) != expected_rows:
        # Only possible with --candidate-build: a row was repointed between
        # the plan and the write, so the guarded UPDATE did not match it.
        # Silence here would read as success.
        print(
            f"WARNING: {expected_rows - len(applied)} row(s) did NOT change -- "
            "their candidate build moved between the plan and the write. "
            "Re-run `status` and decide again.",
            file=sys.stderr,
        )
        return 2
    return 0


async def _cmd_routing_status(ns: argparse.Namespace) -> int:
    from dev_health_ops.db import get_postgres_session

    from .go_api_routing_admin import (
        OperationStatus,
        count_rows_by_schema_digest,
        routing_status_rows,
    )

    catalog = catalog_entries()
    catalog_ok = catalog_loaded_successfully()
    # Unguarded, this raised out of `status` and emitted NOTHING -- not
    # even invalid JSON -- the same contract breach as the unguarded
    # database read one line higher up: this command reports what it can
    # and never dies on what it cannot.
    local_digest: str | None = None
    digest_error: str | None = None
    try:
        local_digest = current_schema_digest()
    except Exception as exc:
        digest_error = f"{type(exc).__name__}: {exc}"

    base_url = _query_api_url(ns)
    go_digest: str | None = None
    go_error: str | None = None
    if base_url:
        try:
            go_digest = _fetch_go_plane_registry(base_url).schema_digest
        except GoPlaneUnavailable as exc:
            # Safe by construction, and narrowly so: this catches only
            # GoPlaneUnavailable, whose every message is built in this
            # module from `_endpoint_label` (scheme + hostname) and
            # `_transport_failure` (class name + status). A raw transport
            # exception is never allowed to reach a caller, precisely so
            # that this line cannot become a leak. The 15-vector leak table
            # drives this path end to end.
            go_error = str(exc)
    else:
        go_error = "no --query-api-url and GO_API_QUERY_API_URL is unset"

    # Unguarded, this block raised a traceback and exited 1 whenever
    # Postgres was unreachable -- directly contradicting this command's
    # whole contract. `status` is what an
    # operator runs WHEN THINGS ARE BROKEN; a diagnostic that dies because
    # the thing it diagnoses is down is useless exactly when it is needed,
    # and it is the same "two states, one silence" mistake in a new place:
    # an operator would see a stack trace and learn nothing about the
    # planes' digests, which this command CAN still report without a
    # database.
    statuses: list[OperationStatus] = []
    digest_counts: dict[str, int] = {}
    db_error: str | None = None
    try:
        async with get_postgres_session() as session:
            # Without a live digest there is no key to classify rows
            # AGAINST, so MATCH/STALE/MISSING would be meaningless. The
            # per-digest census still is meaningful, so it is still read.
            if local_digest is not None:
                statuses = await routing_status_rows(
                    session, live_schema_digest=local_digest, catalog=catalog
                )
            digest_counts = await count_rows_by_schema_digest(session)
    except Exception as exc:
        db_error = f"{type(exc).__name__}: {exc}"

    if ns.json:
        print(
            json.dumps(
                {
                    "python_plane_schema_digest": local_digest,
                    "python_plane_digest_error": digest_error,
                    "go_plane_schema_digest": go_digest,
                    "go_plane_error": go_error,
                    # None means UNKNOWN, not "different". A comparison
                    # against a digest this process could not
                    # compute has no truth value, and asserting one is the
                    # exact failure mode this whole change exists to end:
                    # stating a confident wrong answer instead of "unknown".
                    "planes_agree": (
                        None
                        if (go_digest is None or local_digest is None)
                        else go_digest == local_digest
                    ),
                    "registry_db_error": db_error,
                    "catalog_loaded": catalog_ok,
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

    if local_digest is None:
        print(f"python plane schema_digest : UNAVAILABLE ({digest_error})")
        print(
            "  The canonical SDL could not be read, so this process cannot "
            "compute the routing key at all -- every dispatch will fall back "
            "to Python. Rows cannot be classified against a digest that does "
            "not exist."
        )
    else:
        print(f"python plane schema_digest : {local_digest}")
    if go_digest is None:
        print(f"go plane schema_digest     : UNREACHABLE ({go_error})")
    elif local_digest is None:
        # Both halves of the comparison are required for it to mean
        # anything. Print the fact, withhold the verdict.
        print(f"go plane schema_digest     : {go_digest}  [UNKNOWN]")
        print(
            "  The planes cannot be compared: this process could not compute "
            "its own digest (above). This is NOT a mismatch -- it is an "
            "unanswered question."
        )
    else:
        agree = "AGREE" if go_digest == local_digest else "MISMATCH"
        print(f"go plane schema_digest     : {go_digest}  [{agree}]")
        if go_digest != local_digest:
            print(
                "  !! Rows follow the deployed image. Every row written at "
                f"{local_digest} is unreachable to this binary. See {_RUNBOOK}"
            )
    if not catalog_ok:
        print(
            "!! CATALOG UNAVAILABLE: api/graphql/go_api_operations.json failed "
            "to load. Nothing can be reported per operation, and NOTHING is "
            "Go-eligible in this process. This is NOT the same as an empty "
            "catalog -- regenerate with scripts/go_api/generate_operation_catalog.py"
        )
    if db_error is not None:
        print(f"registry database        : UNREACHABLE ({db_error})")
        print(
            "  Routing rows cannot be read, so MATCH/STALE/MISSING is unknown "
            "-- this is NOT evidence that nothing is enabled."
        )
        return 0
    print("rows by schema_digest:")
    if digest_counts:
        for digest, count in sorted(digest_counts.items()):
            if local_digest is None:
                # Cannot say which of these is live without the live digest;
                # calling them all STALE would be a fabricated verdict.
                marker = "  <- live/stale UNKNOWN"
            else:
                marker = "  <- live" if digest == local_digest else "  <- STALE"
            print(f"  {digest}  {count}{marker}")
    else:
        print("  (table is empty -- nothing is enabled for Go)")
    print()
    print(f"{'OPERATION':<24} {'DIGEST':<14} {'MODE':<10} {'ROLLOUT':<8} PROOF")
    for status in statuses:
        mode = status.mode or "-"
        rollout = (
            "-" if status.rollout_percentage is None else str(status.rollout_percentage)
        )
        # A DOCUMENT_DRIFT row is LIVE, is the row actually in the table,
        # and this command computes `proven` for it -- the JSON says so.
        # Printing `-` here told the operator nothing about the one row
        # they are being asked to notice, and the terminal disagreed with
        # `--json` on the same run.
        if status.digest_state in ("MATCH", "DOCUMENT_DRIFT", "UNREGISTERED"):
            proof = "ok" if status.proven else "UNPROVEN"
        else:
            proof = "-"
        # Width 14, not 8: DOCUMENT_DRIFT is 14 characters, so an 8-wide
        # column shifted MODE / ROLLOUT / PROOF right on exactly that row.
        print(
            f"{status.operation:<24} {status.digest_state:<14} {mode:<10} "
            f"{rollout:<8} {proof}"
        )
        if status.digest_state == "STALE":
            print(f"    stale rows at: {', '.join(status.stale_digests)}")
        # A branch per state, so a new one cannot be silently unrendered:
        # DOCUMENT_DRIFT reached this renderer with nothing printed about
        # WHICH document is in the table, which is the only question it
        # raises.
        if status.digest_state == "DOCUMENT_DRIFT":
            print(
                f"    serving document {status.document_digest} -- the catalog "
                "does not name it, so the edge cannot dispatch this row"
            )
        if status.digest_state == "UNREGISTERED":
            print(
                f"    serving document {status.document_digest} -- the catalog "
                "does not register this operation at all, so the edge cannot "
                "dispatch this row"
            )
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
        "--json",
        action="store_true",
        help=(
            "Print the result as one JSON document on stdout: each operation, "
            "whether it is proven, and the receipt that authorized it."
        ),
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
    enable.add_argument(
        "--review-evidence",
        dest="review_evidence",
        default=None,
        help=(
            "Why this enablement is being made, recorded durably on each "
            "row. Prefixed ACKNOWLEDGED-UNPROVEN for any row enabled "
            "without a proof run."
        ),
    )
    enable.set_defaults(func=_cmd_routing_enable)

    disable = routing_sub.add_parser(
        "disable",
        help=(
            "Turn operations OFF -- the rollback half of the rollout. Sets "
            "mode to python/disabled/shadow, none of which are reachable to "
            "a real client. Deliberately has FEWER preflights than enable: "
            "it must work when the planes disagree or query-api is down, "
            "which is exactly when it is needed."
        ),
    )
    disable.add_argument(
        "--operations",
        default="all-registered",
        help="Comma-separated operation names, or 'all-registered' (default).",
    )
    disable.add_argument(
        "--mode",
        required=True,
        choices=["python", "disabled", "shadow"],
        help=(
            "python = the documented safe default (same as no row); "
            "disabled = same reachability but records a deliberate "
            "decision; shadow = the client still gets Python's response "
            "(the shadow executor does not exist and logs loudly)."
        ),
    )
    disable.add_argument(
        "--candidate-build",
        dest="candidate_build",
        default=None,
        help=(
            "Optional guard: refuse if a row points at a different build "
            "than this, i.e. someone repointed it since you looked. Never "
            "written -- disable changes mode only."
        ),
    )
    disable.add_argument(
        "--review-evidence",
        dest="review_evidence",
        default=None,
        help="Why. Required with --apply; recorded durably on each row.",
    )
    disable.add_argument(
        "--apply",
        action="store_true",
        help="Write the changes. Without it, prints what would change and exits 0.",
    )
    disable.set_defaults(func=_cmd_routing_disable)

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

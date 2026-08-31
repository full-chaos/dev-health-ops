"""Live-local proof (lane-go-api-livelocal): every registered Go query-api
document executed against the REAL local stack -- real ClickHouse, real
synced data in org `70d529e0-3c06-4597-8480-794fd02328b6` (admin@test.com)
-- asserting each one EXECUTES (no transport error, no GraphQL errors, no
unexecutable-SQL failure) and returns a SHAPE-SANE response (every field
the document selects is present in the response tree, not merely HTTP 200).

THIS IS NOT THE DUAL-RUN PARITY PROOF. That proof (9 files beside this one,
`test_go_api_dual_run_*.py`) compares Go against Python on producer-seeded
SCRATCH data, because parity needs both planes reading identical inputs and
real org data cannot be relied on to contain the adversarial shapes (LIMIT
ties, two-generation ReplacingMergeTree conflicts, NULL argMax rows) that
make a comparison meaningful. This file asks a different, narrower
question with no Python involved at all: does each document run on the
real engine against real data. See root AGENTS.md's differential-oracle
section for why the two claims are neither redundant nor substitutable.

Root AGENTS.md data vocabulary (fixed, load-bearing):
* "local"    = REAL synced data in org `70d529e0` on the compose stack --
               what this file uses.
* "fixtures" = `dev-hops fixtures generate` output, contrived -- NOT this.
* "live"     = prod -- NOT reachable from here, never attempted.

READ-ONLY BY CONSTRUCTION: this file issues zero ClickHouse writes. It
does not seed, does not clean up ClickHouse state, and must not -- the
local ClickHouse holds real dev data (root AGENTS.md's pre-push gate
"Safety rule (NON-NEGOTIABLE)" states this exact prohibition for a
sibling gate; the same discipline applies here for a different reason:
seeding would make "real synced data" no longer true of what was tested).
The only writes this file makes are to a disposable SCRATCH Postgres
database created and dropped per test (the `go_api_*` routing-registry
tables, plumbing that gates reachability -- never the org's actual data),
following the exact same pattern every `test_go_api_dual_run_*.py` file
already uses for that same registry.

ENUMERATION IS BY REFLECTION, NEVER A HAND-MAINTAINED LIST (the lane
brief's hardest constraint, twice-learned the hard way: CHAOS-4466 lost
71 tables and CHAOS-4495 lost two sites to exactly this class of drift).
`_enumerate_registered_documents` below shells out to
`cmd/query-api/tools/registrydump`, a small Go program that parses
`cmd/query-api/query_route.go`'s actual AST -- the same source the
running binary compiles from -- and extracts the `registered*Document`
consts plus the `digestByOperation` map that together are this route's
real source of truth (see that file's own doc comments). Nothing in this
module lists operation names; `pytest_generate_tests` parametrizes one
test per document the Go source ITSELF reports having registered, at
collection time, every run. A document added tomorrow needs no change
here.

SHAPE-SANE IS ALSO REFLECTION-DRIVEN, off the SAME document text plus the
real schema (`contracts/graphql/v1/schema.graphql`): rather than
hand-write twelve expected-response-shape assertions (the same class of
drift enumeration avoids, one level down), `_shape_check` parses each
document's own selection set with graphql-core and walks the actual
response tree checking every selected field's response key is present at
the matching nesting level. A field the document selects that goes
missing from a real response is a violation regardless of which document
it is; a NULL on a field the schema itself declares non-nullable is a
violation too. A NULL on a field the schema declares NULLABLE is a
spec-valid response -- reported as a note, not a violation, per chris's
ruling on this file's own first run (see `_shape_check`'s doc comment):
that is a cross-plane PARITY question, which is claim 1's (the dual-run)
job, not this claim's.

Server-launch, EdDSA envelope minting, and registry-fixture scaffolding
are taken from `test_go_api_dual_run_investment.py` (the newest, cleanest
example) with the Python-comparison half dropped entirely -- there is no
Python resolver call anywhere in this file.
"""

from __future__ import annotations

import functools
import json
import os
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid
import warnings
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, cast

import pytest
import sqlalchemy as sa
from graphql import (
    GraphQLList,
    GraphQLNonNull,
    GraphQLSchema,
    build_schema,
)
from graphql import parse as parse_graphql_document
from graphql.language import ast as gql_ast
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from dev_health_ops.api.graphql import principal_envelope
from dev_health_ops.api.graphql.go_api_registry import register_candidate_build
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.licensing.types import LicenseTier
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.models.git import Base as GitBase
from dev_health_ops.models.go_api_registry import CandidateBuild, ProofRun, RoutingState

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")
POSTGRES_TEST_URI = os.environ.get("DEV_HEALTH_POSTGRES_TEST_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI pointed at the REAL local ClickHouse "
        "(org 70d529e0 real synced data) -- never a scratch/CI database for "
        "this file.",
    ),
    pytest.mark.skipif(
        not POSTGRES_TEST_URI,
        reason="Requires DEV_HEALTH_POSTGRES_TEST_URI (admin creds able to "
        "CREATE DATABASE) for the disposable go_api registry scratch db.",
    ),
]

REPO_ROOT = Path(__file__).resolve().parents[3]
QUERY_ROUTE_GO = REPO_ROOT / "cmd" / "query-api" / "query_route.go"
REGISTRYDUMP_DIR = REPO_ROOT / "cmd" / "query-api" / "tools" / "registrydump"

# org `70d529e0-3c06-4597-8480-794fd02328b6` (admin@test.com) -- REAL synced
# data on the shared compose stack. Fixed per the lane brief; never
# generated, never a fixtures-seeded org.
ORG_ID = "70d529e0-3c06-4597-8480-794fd02328b6"

SCHEMA_DIGEST = "sha256:lane-go-api-livelocal-schema-digest"
CANDIDATE_BUILD = "lane-go-api-livelocal-candidate-build"


def _assert_real_local_data_present(clickhouse_uri: str, org_id: str) -> None:
    """Precondition for the `local` evidence label -- codex review round 2
    (2026-08-30) caught that this file accepted ANY nonempty
    `CLICKHOUSE_URI` without ever confirming it actually points at the
    real local stack with `org_id`'s synced data (repro: forwarded
    `CLICKHOUSE_URI=clickhouse://ch:ch@localhost:9000/ci_local_validate`,
    a scratch/CI endpoint, and nothing here rejected it). A misconfigured
    or scratch endpoint would still produce PASS/FAIL results, just
    mislabeled `local` -- silently wrong evidence, not a loud failure.

    Checks for at least one `repos` row for `org_id`: every real synced
    org has repos, and this is a cheap, read-only, single-row COUNT --
    not a deep data-quality check, just "does this look like the org this
    proof claims to be running against."
    """
    sink = ClickHouseMetricsSink(clickhouse_uri)
    try:
        result = sink.client.query(
            "SELECT count() FROM repos WHERE org_id = {org_id:String}",
            parameters={"org_id": org_id},
        )
        count = result.result_rows[0][0] if result.result_rows else 0
    finally:
        sink.close()
    if not count:
        pytest.fail(
            f"LIVE-LOCAL PRECONDITION FAILED: CLICKHOUSE_URI has ZERO "
            f"`repos` rows for org {org_id} -- this does not look like the "
            f"real local stack with synced data. Refusing to run and "
            f"mislabel results as `local`. This check does not inspect "
            f"CLICKHOUSE_URI's value, only whether the org it points at "
            f"actually has data."
        )


@pytest.fixture(scope="session", autouse=True)
def _real_local_data_precondition() -> None:
    """Runs once before any of the 12 parametrized tests actually
    execute (autouse; session-scoped so it is one ClickHouse round trip,
    not twelve). Never invoked for a marker-deselected or env-var-skipped
    run -- pytest only sets up fixtures for tests that are actually
    selected to run, and the empty-list / skip-marked parametrizations
    from `pytest_generate_tests` never reach fixture setup.
    """
    assert CLICKHOUSE_URI is not None  # module skipif already guarantees this
    _assert_real_local_data_present(CLICKHOUSE_URI, ORG_ID)


# --------------------------------------------------------------------------
# Enumeration by reflection -- see module doc comment.
# --------------------------------------------------------------------------


@functools.lru_cache(maxsize=1)
def _enumerate_registered_documents() -> tuple[dict[str, str], ...]:
    """Runs `cmd/query-api/tools/registrydump` against the REAL
    query_route.go and returns its {"operation", "document", "const_name"}
    rows verbatim. Deliberately raises rather than returning an empty
    tuple on any failure -- an operator missing `go`, or a genuine
    registration-source mismatch the tool itself detects (a const with no
    map entry, or vice versa), must fail collection loudly, not silently
    collect zero tests (root AGENTS.md's four verification rules, #4: "a
    measurement that did not happen must FAIL, loudly").
    """
    go = shutil.which("go")
    if go is None:
        raise RuntimeError(
            "go toolchain not on PATH -- required to enumerate registered "
            "documents by reflection over cmd/query-api/query_route.go. "
            "There is deliberately no hand-maintained fallback list."
        )
    result = subprocess.run(
        [go, "run", str(REGISTRYDUMP_DIR), "-file", str(QUERY_ROUTE_GO)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "GOWORK": "off"},
    )
    if result.returncode != 0:
        raise RuntimeError(
            "registrydump failed to enumerate registered documents -- this "
            "names a real gap in the registration source itself (a const "
            "with no digestByOperation entry, or vice versa), NOT a bug in "
            f"this test file:\nstdout={result.stdout}\nstderr={result.stderr}"
        )
    docs = json.loads(result.stdout)
    if not docs:
        raise RuntimeError(
            "registrydump enumerated ZERO registered documents from "
            f"{QUERY_ROUTE_GO} -- either the route registers nothing "
            "(unexpected for this branch) or the tool's map-literal "
            "extraction stopped matching the real source; either way this "
            "is a finding to report, not something to skip past."
        )
    return tuple(docs)


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "registered_doc" not in metafunc.fixturenames:
        return
    # Gate the (go-toolchain-requiring) enumeration behind this module's
    # OWN precondition -- codex review (2026-08-30) caught that an earlier
    # version called _enumerate_registered_documents() unconditionally
    # here, before the module-level `pytestmark` skipif on CLICKHOUSE_URI/
    # POSTGRES_TEST_URI could take effect (skip marks apply at test SETUP,
    # long after pytest_generate_tests has already run at COLLECTION). On
    # a plain "not clickhouse" CI tier with no `go` binary -- an entirely
    # reasonable combination for a runner that only needs Python -- this
    # file would raise a collection ERROR instead of cleanly skipping like
    # its own skipif already promises, potentially failing an unrelated
    # tier for a proof that tier never intended to run. Reproduced by hand
    # (env -i PATH=/usr/bin:/bin, CLICKHOUSE_URI/POSTGRES_TEST_URI unset):
    # `pytest --collect-only` errored the whole file before this fix.
    #
    # When this module's own precondition is already false, the go
    # toolchain is not needed at all -- parametrize with an EMPTY list,
    # which pytest reports as a clean skip, not an error.
    if not CLICKHOUSE_URI or not POSTGRES_TEST_URI:
        metafunc.parametrize("registered_doc", [], ids=[])
        return
    # codex review round 2 (2026-08-30) caught that the fix above still
    # doesn't cover every collection-safe path: a run invoked with
    # `-m 'not clickhouse'` to DESELECT this file's tests can still have
    # CLICKHOUSE_URI/POSTGRES_TEST_URI set (e.g. exported globally by a CI
    # harness for OTHER test files that need them) -- marker-based
    # deselection happens AFTER collection, same timing problem as the
    # skipif fix above one layer up. Reproduced by hand: both env vars
    # set, `go` removed from PATH -> `RuntimeError: go toolchain not on
    # PATH`, collection ERROR, even though `-m 'not clickhouse'` would
    # have excluded every test this file generates anyway.
    #
    # Evaluating the run's `-m` mark expression ourselves to predict
    # deselection would duplicate pytest's own marker engine and could
    # still be wrong. Simpler and correct either way: missing `go` is
    # ALWAYS a clean, visible SKIP now (one item, not zero, so a human
    # reading the report sees exactly why), never a collection-time raise.
    # An operator who genuinely wants this proof to run and is missing
    # `go` sees "1 skipped -- go toolchain not on PATH" in the test
    # report, which is loud enough without being a hard error that can
    # take an unrelated tier down with it.
    if shutil.which("go") is None:
        metafunc.parametrize(
            "registered_doc",
            [
                pytest.param(
                    {"operation": "SKIPPED", "document": "", "const_name": ""},
                    marks=pytest.mark.skip(
                        reason="go toolchain not on PATH -- required to "
                        "enumerate registered documents by reflection over "
                        "query_route.go. This live-local proof did NOT run."
                    ),
                )
            ],
            ids=["go-missing"],
        )
        return
    docs = _enumerate_registered_documents()
    metafunc.parametrize(
        "registered_doc",
        docs,
        ids=[d["operation"] for d in docs],
    )


# --------------------------------------------------------------------------
# Shape-sane checker -- reflection over the SAME document text, see module
# doc comment. Not per-operation; one generic walker for all 12 (and any
# future 13th).
#
# Nullability ruling (chris, on this lane's first live-local run finding
# `investmentBreakdown.evidenceQualityStats` and `investmentFull.sankey.
# coverage` both null): "a null on a field the schema declares nullable is
# a PASS, reported as a note -- not a failure." Claim 2 (this file) asks
# "does this execute and return a sane shape" -- a spec-valid null IS
# sane. Whether the Go plane's null matches what Python would return there
# is a PARITY question, and parity is claim 1's job (the dual-run,
# `test_go_api_dual_run_*.py`); failing here for a legitimate null would
# put a parity assertion inside an execution check, which is exactly the
# thing the two claims exist separately to avoid conflating. A null on a
# NON-nullable field is a different animal -- that is a real contract
# violation (the server broke its own schema) and still fails loudly.
#
# So nullability is read from the schema (`contracts/graphql/v1/
# schema.graphql`, the same contract gqlgen itself is generated from),
# never hand-listed per field -- hand-listing "the two nullable fields we
# found" would reintroduce, one level below the document list, the exact
# maintained-list drift this file's enumerator exists to avoid (CHAOS-4466
# / CHAOS-4495 lost coverage that way at the document level; a per-field
# nullability list would lose it at the field level instead).
# --------------------------------------------------------------------------

SCHEMA_PATH = REPO_ROOT / "contracts" / "graphql" / "v1" / "schema.graphql"


@functools.lru_cache(maxsize=1)
def _load_schema() -> GraphQLSchema:
    return build_schema(SCHEMA_PATH.read_text())


def _response_key(selection: gql_ast.FieldNode) -> str:
    return selection.alias.value if selection.alias else selection.name.value


def _find_fragment_usage(
    selection_set: gql_ast.SelectionSetNode, path: str = "$"
) -> list[str]:
    """Statically scans a selection set -- the DOCUMENT's AST, never
    response data -- for any fragment spread / inline fragment,
    recursively through every nested selection set. Decoupled from
    runtime data on purpose: codex review round 2 (2026-08-30, EXECUTED)
    showed the walker's per-node fragment check only fires when it
    actually descends into non-empty response data for that selection
    set, so a document with a fragment under a field that happens to
    return an EMPTY list (`rows: []`) passed with zero violations even
    though the fragment is structurally unsupported regardless of what
    the response contains. Called once, up front, before any data
    walking -- a document's fragment usage does not depend on what any
    particular response looks like.
    """
    found: list[str] = []
    for selection in selection_set.selections:
        if isinstance(selection, gql_ast.FieldNode):
            if selection.selection_set is not None:
                found.extend(
                    _find_fragment_usage(
                        selection.selection_set, f"{path}.{_response_key(selection)}"
                    )
                )
        else:
            found.append(f"{path}: {type(selection).__name__}")
    return found


@dataclass
class _ShapeCheck:
    violations: list[str]
    notes: list[str]


def _shape_check(document: str, data: Any) -> _ShapeCheck:
    """Walks `document`'s selection set against BOTH the response tree
    `data` and the real schema, starting from the schema's Query type
    (every registered document is a top-level `query`). Returns:

    * `violations` -- a selected field missing from the response, a
      response shape that doesn't match its selection (object/list vs.
      scalar), a field the schema no longer has (schema/document drift),
      or a NULL on a field the schema declares NON-nullable. Any of these
      fails the test.
    * `notes` -- a NULL on a field the schema declares nullable, where the
      document also selects sub-fields under it. Reported so a reader
      still sees it (`investmentFull`'s `sankey.coverage` is a real,
      documented always-nil gap in this port), never silently dropped,
      but NOT a failure per the ruling above.

    Fragments are not expected in any of the 12 documents (confirmed by
    reading every registered*Document const) but are skipped defensively
    rather than mis-walked if one ever appears.
    """
    schema = _load_schema()
    query_type = schema.query_type
    assert query_type is not None, "schema.graphql has no Query type"

    parsed = parse_graphql_document(document)
    operation = next(
        d for d in parsed.definitions if isinstance(d, gql_ast.OperationDefinitionNode)
    )
    result = _ShapeCheck(violations=[], notes=[])

    # Static, data-independent fragment check FIRST (see
    # _find_fragment_usage's doc comment) -- a document's use of
    # fragments is a property of the document, not of whatever the
    # response happens to contain this run.
    fragment_uses = _find_fragment_usage(operation.selection_set)
    if fragment_uses:
        result.violations.extend(
            f"{loc}: document uses a fragment -- _shape_check does not "
            "resolve fragments yet, so this document's shape cannot be "
            "confirmed. Extend _shape_check before claiming this "
            "document shape-sane."
            for loc in fragment_uses
        )
        return result

    def walk(
        selection_set: gql_ast.SelectionSetNode | None,
        node: Any,
        path: str,
        type_: Any,
    ) -> None:
        """Walks one position in the response tree against the ACTUAL
        schema type at that exact position, peeling exactly one
        NonNull/List wrapper per recursive call rather than flattening
        `field_def.type` down to its named type once and reusing a
        single derived boolean for every nesting depth below it.

        codex review round 3 (2026-08-30, EXECUTED, synthetic schema)
        caught that the earlier, flattening version got this wrong two
        ways for a NESTED list (`[[Thing]]` and similar): (1) it computed
        `element_nullable` once from the OUTERMOST list layer and reused
        that same value at every deeper layer, so a null at the second
        list layer was checked against the wrong layer's nullability; (2)
        the earlier version collapsed every wrapper down to the bare named
        type in one step (via a now-removed `_unwrap_to_named_type` helper), so an inner list was walked against the
        eventual OBJECT type's `.fields` instead of being recognized as
        still-a-list at that position -- "runtime list/object shape
        accepted against the wrong schema container". None of the
        current 12 documents nest lists (confirmed by reading every
        registered*Document const), so this was latent, not a live
        false-pass -- fixed here so the SAME logic is correct for every
        depth uniformly, not just depth 1.
        """
        if selection_set is None:
            return
        nullable = not isinstance(type_, GraphQLNonNull)
        inner = type_.of_type if isinstance(type_, GraphQLNonNull) else type_
        if node is None:
            if not nullable:
                result.violations.append(
                    f"{path}: NULL at a NON-nullable schema position "
                    f"({inner}) -- contract violation"
                )
            # A spec-valid null at a NESTED (non-field-level) position --
            # e.g. one element of a nullable-element list -- has no field
            # name to build the richer field-level "note" message chris's
            # ruling covers; that ruling and its note-vs-violation
            # distinction are about FIELD nulls (handled below, in the
            # per-selection loop, which still owns that richer message).
            # A bare nested null needs no note: there's nothing under it
            # this checker could have confirmed either way.
            return
        if isinstance(inner, GraphQLList):
            if not isinstance(node, list):
                result.violations.append(
                    f"{path}: expected a list ({inner}), got "
                    f"{type(node).__name__}={node!r}"
                )
                return
            for i, item in enumerate(node):
                walk(selection_set, item, f"{path}[{i}]", inner.of_type)
            return
        if not isinstance(node, dict):
            result.violations.append(
                f"{path}: expected an object, got {type(node).__name__}={node!r}"
            )
            return
        fields = getattr(inner, "fields", None)
        if fields is None:
            result.violations.append(
                f"{path}: schema type {inner!r} has no fields but the "
                "document selects sub-fields there"
            )
            return
        for selection in selection_set.selections:
            if not isinstance(selection, gql_ast.FieldNode):
                # Unreachable in practice: _shape_check's static
                # _find_fragment_usage pre-check (see its doc comment)
                # already returns early on ANY fragment anywhere in the
                # document, so walk() is never invoked at all for a
                # document that uses one. Kept as a loud defensive
                # assertion rather than a silent `continue`, in case a
                # future refactor calls walk() directly and bypasses the
                # pre-check.
                raise AssertionError(
                    f"{path}: reached a non-field selection "
                    f"({type(selection).__name__}) inside walk() -- the "
                    "static fragment pre-check should have caught this "
                    "before walk() was ever called"
                )
            key = _response_key(selection)
            if key not in node:
                result.violations.append(
                    f"{path}.{key}: MISSING from response (selected in the "
                    "registered document)"
                )
                continue
            field_def = fields.get(selection.name.value)
            if field_def is None:
                result.violations.append(
                    f"{path}.{key}: field not found on schema type "
                    f"{getattr(inner, 'name', inner)!r} -- "
                    "schema/document drift"
                )
                continue
            field_nullable = not isinstance(field_def.type, GraphQLNonNull)
            value = node[key]
            if value is None:
                if not field_nullable:
                    result.violations.append(
                        f"{path}.{key}: NULL on a NON-nullable schema field "
                        f"({getattr(inner, 'name', inner)}."
                        f"{selection.name.value}) -- contract violation"
                    )
                elif selection.selection_set is not None:
                    subfield_names = [
                        _response_key(s)
                        for s in selection.selection_set.selections
                        if isinstance(s, gql_ast.FieldNode)
                    ]
                    result.notes.append(
                        f"{path}.{key}: null (schema-nullable field "
                        f"{getattr(inner, 'name', inner)}."
                        f"{selection.name.value}) -- sub-fields {subfield_names} "
                        "not confirmable. Spec-valid per claim 2's ruling: "
                        "whether the Go plane SHOULD return non-null here is "
                        "a parity question for claim 1 (the dual-run), not "
                        "an execution-shape failure here."
                    )
                else:
                    # Round-5 codex finding (P3, 2026-08-30): a nullable
                    # SCALAR (no sub-selection, e.g. `WorkGraphFlow.
                    # degradedReason: String`) returning null fell through
                    # both branches above -- not a violation (field IS
                    # nullable) and not a note (no selection_set to build
                    # the sub-fields message from) -- so it produced NO
                    # signal at all, contradicting this file's own stated
                    # ruling that every nullable null is reported as a
                    # note. A nullable scalar null is exactly as spec-valid
                    # as a nullable object null; it gets the same note
                    # treatment, just without a sub-fields list (there is
                    # nothing under a scalar to list).
                    result.notes.append(
                        f"{path}.{key}: null (schema-nullable scalar field "
                        f"{getattr(inner, 'name', inner)}."
                        f"{selection.name.value}) -- spec-valid per claim "
                        "2's ruling: whether the Go plane SHOULD return "
                        "non-null here is a parity question for claim 1 "
                        "(the dual-run), not an execution-shape failure "
                        "here."
                    )
                continue
            if selection.selection_set is not None:
                walk(selection.selection_set, value, f"{path}.{key}", field_def.type)

    walk(operation.selection_set, data, "$", query_type)
    return result


# --------------------------------------------------------------------------
# Variable templates. One entry per KNOWN operation name (sourced from the
# real client shapes already proven in the sibling test_go_api_dual_run_*
# files, not invented). This table is the one place per-operation
# knowledge legitimately lives -- it supplies REQUEST ARGUMENTS, not the
# enumeration of WHICH operations exist (that is reflection-driven above).
# An operation with no entry here still runs: it falls back to
# `{"orgId": org_id}` and is flagged separately in the failure message so
# a newly-registered document is caught (loudly) rather than silently
# skipped, even before anyone extends this table.
# --------------------------------------------------------------------------


def _iso_date(d: date) -> str:
    return d.isoformat()


def _iso_utc_z(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _recent_monday(today: date) -> date:
    return today - timedelta(days=today.weekday())


def _build_variables(
    operation: str, *, org_id: str, since: datetime, until: datetime
) -> dict:
    since_date, until_date = since.date(), until.date()
    week_start = _recent_monday(until_date)

    known: dict[str, dict] = {
        "featureFlags": {
            "orgId": org_id,
            "provider": None,
            "project": None,
            "includeArchived": False,
            "limit": 1000,
        },
        "reviewEdges": {
            "input": {
                "orgId": org_id,
                "sinceDate": _iso_date(since_date),
                "untilDate": _iso_date(until_date),
                "repoIds": None,
                "limit": 500,
            }
        },
        "cognitiveLoad": {
            "input": {
                "orgId": org_id,
                "sinceDate": _iso_date(since_date),
                "untilDate": _iso_date(until_date),
                "teamId": None,
                "repoId": None,
            }
        },
        "complexityTimeseries": {
            "input": {
                "orgId": org_id,
                "sinceUtc": _iso_utc_z(since),
                "untilUtc": _iso_utc_z(until),
                "granularity": "DAY",
                "scope": "REPO",
                "repoIds": None,
                "teamIds": None,
                "limit": 500,
            }
        },
        "hotspots": {
            "input": {
                "orgId": org_id,
                "sinceUtc": _iso_utc_z(since),
                "untilUtc": _iso_utc_z(until),
                "repoIds": None,
                "teamIds": None,
                "limit": 50,
            }
        },
        "operatingReview": {
            "orgId": org_id,
            "input": {"weekStart": _iso_date(week_start), "teamId": None},
        },
        "workGraphEdges": {"orgId": org_id, "filters": None},
        "workGraphFlow": {"orgId": org_id, "filters": None},
        "workGraphArtifacts": {"orgId": org_id, "filters": None},
        "flowMatrix": {
            "orgId": org_id,
            "batch": {
                "flowMatrix": {
                    "dimension": "TEAM",
                    "measure": "COUNT",
                    "dateRange": {
                        "startDate": _iso_date(since_date),
                        "endDate": _iso_date(until_date),
                    },
                    "maxNodes": 50,
                    "maxEdges": 200,
                }
            },
        },
        "investmentBreakdown": {
            "orgId": org_id,
            "batch": {
                "breakdowns": [
                    {
                        "dimension": "WORK_TYPE",
                        "measure": "COUNT",
                        "dateRange": {
                            "startDate": _iso_date(since_date),
                            "endDate": _iso_date(until_date),
                        },
                        "topN": 10,
                    }
                ],
                "useInvestment": True,
            },
        },
        "investmentFull": {
            "orgId": org_id,
            "batch": {
                "sankey": {
                    "path": ["REPO", "WORK_TYPE"],
                    "measure": "COUNT",
                    "dateRange": {
                        "startDate": _iso_date(since_date),
                        "endDate": _iso_date(until_date),
                    },
                    "maxNodes": 50,
                    "maxEdges": 200,
                },
                "useInvestment": True,
            },
        },
    }
    return known.get(operation, {"orgId": org_id})


# --------------------------------------------------------------------------
# Scaffolding, taken from test_go_api_dual_run_investment.py with the
# Python-comparison half dropped. See that file for provenance notes on
# each piece.
# --------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def query_api_binary(tmp_path_factory: pytest.TempPathFactory) -> str:
    go = shutil.which("go")
    if go is None:
        pytest.skip("go toolchain not on PATH")
    out = tmp_path_factory.mktemp("query-api-bin") / "query-api"
    result = subprocess.run(
        [go, "build", "-o", str(out), "./cmd/query-api"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "GOWORK": "off"},
    )
    if result.returncode != 0:
        pytest.fail(
            f"go build ./cmd/query-api failed:\n{result.stdout}\n{result.stderr}"
        )
    return str(out)


def _sync_engine(uri: str) -> Engine:
    return sa.create_engine(
        make_url(uri).set(drivername="postgresql+psycopg2"),
        isolation_level="AUTOCOMMIT",
    )


def _create_scratch_postgres_db(admin_uri: str) -> tuple[str, str]:
    db_name = f"lane_go_api_livelocal_{uuid.uuid4().hex}"
    engine = _sync_engine(admin_uri)
    try:
        with engine.connect() as connection:
            connection.exec_driver_sql(f'CREATE DATABASE "{db_name}"')
    except Exception:
        # codex review round 2 (2026-08-30, EXECUTED fault injection): the
        # CREATE DATABASE statement can already have reached and succeeded
        # on the server even though THIS client-side call is about to
        # raise (e.g. the connection drops right after the server acks
        # it). The caller never receives `db_name` in that case, so its
        # own cleanup can never run -- best-effort drop it here, on a
        # FRESH connection (this one may be broken), before propagating
        # the original error. `_drop_scratch_postgres_db` uses `DROP
        # DATABASE IF EXISTS`, so this is a safe no-op on the more common
        # case where CREATE DATABASE never actually landed.
        try:
            _drop_scratch_postgres_db(admin_uri, db_name)
        except Exception:
            pass  # don't mask the original error with a cleanup failure
        raise
    finally:
        engine.dispose()
    base_url = make_url(admin_uri)
    dsn = base_url.set(database=db_name).render_as_string(hide_password=False)
    return db_name, dsn


def _drop_scratch_postgres_db(admin_uri: str, db_name: str) -> None:
    engine = _sync_engine(admin_uri)
    try:
        with engine.connect() as connection:
            connection.exec_driver_sql(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %(db_name)s AND pid <> pg_backend_pid()",
                {"db_name": db_name},
            )
            connection.exec_driver_sql(f'DROP DATABASE IF EXISTS "{db_name}"')
    finally:
        engine.dispose()


@pytest.fixture
def registry_postgres() -> Iterator[dict[str, str]]:
    """A disposable scratch Postgres database, created on the SAME server
    `DEV_HEALTH_POSTGRES_TEST_URI` admin-connects to and dropped at
    teardown -- holds only the go_api_* routing-registry tables (plumbing
    that gates Go-plane reachability), never the org's real data. Same
    pattern every test_go_api_dual_run_*.py file already uses (that
    pattern has the same leak this fixture fixes below -- forwarded, not
    fixed there; out of this PR's scope).

    The DB-drop `finally` wraps EVERYTHING from creation onward, not just
    the `yield` -- codex review (2026-08-30) caught that an earlier
    version left `GitBase.metadata.create_all(...)` outside that
    `finally`'s reach: a raise from `create_all` (a locked-down Postgres
    role, a DDL timeout, anything) would propagate straight out of this
    fixture, past the drop-DB `finally` below it, leaking the scratch
    database on the shared server forever. Confirmed by reading the
    control flow (no fault injection needed: the exception path is
    unambiguous once the drop is outside its scope).
    """
    assert POSTGRES_TEST_URI is not None
    db_name, dsn = _create_scratch_postgres_db(POSTGRES_TEST_URI)
    try:
        sync_engine = _sync_engine(dsn)
        try:
            registry_tables = cast(
                list[sa.Table],
                [CandidateBuild.__table__, RoutingState.__table__, ProofRun.__table__],
            )
            GitBase.metadata.create_all(sync_engine, tables=registry_tables)
        finally:
            sync_engine.dispose()

        base_url = make_url(dsn)
        go_dsn = base_url.set(drivername="postgresql").render_as_string(
            hide_password=False
        )
        async_dsn = base_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        yield {"go": go_dsn, "async": async_dsn}
    finally:
        _drop_scratch_postgres_db(POSTGRES_TEST_URI, db_name)


def _document_digest(document: str) -> str:
    import hashlib

    return hashlib.sha256(document.strip().encode("utf-8")).hexdigest()


async def _seed_candidate_and_enable_canary(
    async_dsn: str, document_digest: str, *, operation: str
) -> None:
    engine = create_async_engine(async_dsn)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await register_candidate_build(
                session,
                schema_digest=SCHEMA_DIGEST,
                document_digest=document_digest,
                selected_operation=operation,
                candidate_build=CANDIDATE_BUILD,
            )
            await session.execute(
                pg_insert(RoutingState)
                .values(
                    schema_digest=SCHEMA_DIGEST,
                    document_digest=document_digest,
                    selected_operation=operation,
                    current_candidate_build=CANDIDATE_BUILD,
                    owner="go",
                    mode="canary",
                    rollout_percentage=100,
                )
                .on_conflict_do_update(
                    index_elements=[
                        "schema_digest",
                        "document_digest",
                        "selected_operation",
                    ],
                    set_={"mode": "canary", "current_candidate_build": CANDIDATE_BUILD},
                )
            )
            await session.commit()
    finally:
        await engine.dispose()


def _mint_envelope(org_id: str) -> tuple[str, dict, str, str]:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.serialization import (
        Encoding,
        NoEncryption,
        PrivateFormat,
    )

    key = Ed25519PrivateKey.generate()
    key_pem = key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    # codex review round 3 (2026-08-30, EXECUTED): this left
    # GO_API_ENVELOPE_PRIVATE_KEY set in the process environment forever
    # (observed before_present=False after_present=True) -- each of the
    # 12 tests overwrote it with a fresh random key and never cleaned up,
    # so anything running later in the SAME pytest process (another test
    # file, a later test in this one) would see a leaked, unrelated
    # Ed25519 key. Save/restore around the two calls that actually need
    # it read from the environment, in a finally so a raise still
    # restores it.
    previous_key = os.environ.get("GO_API_ENVELOPE_PRIVATE_KEY")
    os.environ["GO_API_ENVELOPE_PRIVATE_KEY"] = key_pem
    try:
        user = AuthenticatedUser(
            user_id="55555555-5555-4555-8555-555555555555",
            email="dev@example.com",
            org_id=org_id,
            role="admin",
            is_superuser=False,
            is_superuser_verified=False,
            token_version=3,
        )
        token = principal_envelope.issue_effective_principal_envelope(
            user, tier=LicenseTier.TEAM, licensed_features=["ai_review"]
        )
        jwks = principal_envelope.build_envelope_jwks()
    finally:
        if previous_key is None:
            os.environ.pop("GO_API_ENVELOPE_PRIVATE_KEY", None)
        else:
            os.environ["GO_API_ENVELOPE_PRIVATE_KEY"] = previous_key
    return (
        token,
        jwks,
        principal_envelope.ENVELOPE_ISSUER,
        principal_envelope.ENVELOPE_AUDIENCE,
    )


def _wait_for_ready(base_url: str, timeout_s: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_s
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"{base_url}/readyz", timeout=1) as resp:
                if resp.status == 200:
                    return
        except (urllib.error.URLError, ConnectionError) as exc:
            last_err = exc
        time.sleep(0.1)
    raise TimeoutError(f"query-api did not become ready: {last_err}")


def _post_graphql(base_url: str, token: str, document: str, variables: dict) -> dict:
    body = json.dumps({"query": document, "variables": variables}).encode()
    req = urllib.request.Request(
        f"{base_url}/query",
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


class _RunningGoServer:
    def __init__(self, process: subprocess.Popen, base_url: str) -> None:
        self.process = process
        self.base_url = base_url

    def stop(self) -> None:
        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)


def _go_clickhouse_uri(python_clickhouse_uri: str) -> str:
    from urllib.parse import urlsplit, urlunsplit

    parts = urlsplit(python_clickhouse_uri)
    netloc = parts.netloc.replace(":8123", ":9000")
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def _start_go_server(
    binary: str,
    clickhouse_uri: str,
    registry_uri: str,
    jwks_path: str,
    issuer: str,
    audience: str,
) -> _RunningGoServer:
    clickhouse_uri = _go_clickhouse_uri(clickhouse_uri)
    port = _free_port()
    env = {
        **os.environ,
        "QUERY_API_ADDR": f":{port}",
        "CLICKHOUSE_URI": clickhouse_uri,
        "GO_API_REGISTRY_POSTGRES_URI": registry_uri,
        "GO_API_ENVELOPE_JWKS_PATH": jwks_path,
        "GO_API_ENVELOPE_ISSUER": issuer,
        "GO_API_ENVELOPE_AUDIENCE": audience,
        "GO_API_SCHEMA_DIGEST": SCHEMA_DIGEST,
    }
    process = subprocess.Popen(
        [binary],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    base_url = f"http://127.0.0.1:{port}"
    try:
        _wait_for_ready(base_url)
    except TimeoutError:
        # codex review round 2 (2026-08-30, EXECUTED): `.kill()` alone
        # sends SIGKILL but never reaps the child -- `.stdout.read()`
        # unblocks once the pipe's write end closes on exit, which is NOT
        # the same as the parent calling waitpid() on it. Without `.wait()`
        # the killed query-api process stays a zombie while pytest
        # continues running the other 11 parametrized tests. Mirrors
        # `_RunningGoServer.stop()`'s existing kill+wait pattern below.
        process.kill()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass  # already sent SIGKILL; nothing more to do but not hang
        out = process.stdout.read() if process.stdout else ""
        pytest.fail(f"query-api never became ready:\n{out}")
    return _RunningGoServer(process, base_url)


@pytest.fixture(scope="module")
def jwks_path(tmp_path_factory: pytest.TempPathFactory):
    return tmp_path_factory.mktemp("jwks")


# --------------------------------------------------------------------------
# The proof: one test per document the Go source reports having
# registered (pytest_generate_tests above), against the real local stack.
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_registered_document_executes_on_local_stack(
    query_api_binary: str,
    registry_postgres: dict[str, str],
    jwks_path: Path,
    registered_doc: dict[str, str],
) -> None:
    assert CLICKHOUSE_URI is not None
    operation = registered_doc["operation"]
    document = registered_doc["document"]
    const_name = registered_doc["const_name"]

    until = datetime.now(timezone.utc)
    since = until - timedelta(days=180)
    variables = _build_variables(operation, org_id=ORG_ID, since=since, until=until)

    token, jwks, issuer, audience = _mint_envelope(ORG_ID)
    jwks_file = jwks_path / f"jwks-{operation}.json"
    jwks_file.write_text(json.dumps(jwks))

    document_digest = _document_digest(document)
    await _seed_candidate_and_enable_canary(
        registry_postgres["async"], document_digest, operation=operation
    )

    server = _start_go_server(
        query_api_binary,
        CLICKHOUSE_URI,
        registry_postgres["go"],
        str(jwks_file),
        issuer,
        audience,
    )
    payload: dict | None = None
    transport_exc: Exception | None = None
    try:
        payload = _post_graphql(server.base_url, token, document, variables)
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
        transport_exc = exc
    finally:
        # Stop FIRST, then drain stdout -- the pipe only reaches EOF once
        # the process has exited, so reading it before stop() blocks
        # forever instead of returning what was logged.
        server.stop()
        server_log = server.process.stdout.read() if server.process.stdout else ""

    if transport_exc is not None:
        pytest.fail(
            f"[{operation}] LIVE-LOCAL FIND: transport failure against the real "
            f"local stack (org {ORG_ID}, const {const_name}): {transport_exc}\n"
            f"server log:\n{server_log}"
        )
    assert payload is not None

    # 1. It executes: no GraphQL errors (which is exactly how an
    #    unexecutable-SQL failure like CHAOS-4538's Code 206
    #    ALIAS_REQUIRED surfaces -- gqlgen turns a resolver error into a
    #    `errors[]` entry, HTTP 200 either way). Server stdout/stderr is
    #    included: gqlgen's `errors[].message` alone is often a generic
    #    "ClickHouse row iteration failed" with the real driver error
    #    only in the process log.
    errors = payload.get("errors")
    assert not errors, (
        f"[{operation}] LIVE-LOCAL FIND: GraphQL errors executing the real "
        f"registered document (const {const_name}) against real local data "
        f"(org {ORG_ID}):\n{json.dumps(errors, indent=2)}\n"
        f"server log (last 4000 chars):\n{server_log[-4000:]}"
    )
    assert "data" in payload and payload["data"] is not None, (
        f"[{operation}] LIVE-LOCAL FIND: no `data` in response (const "
        f"{const_name}): {payload}"
    )

    # 2. It is shape-sane: every field the document selects is present in
    #    the response tree, derived from the document's own AST and the
    #    real schema's nullability. A schema-nullable field returning
    #    null is a NOTE (printed, never silently dropped), not a
    #    violation -- see _shape_check's doc comment for the ruling.
    shape = _shape_check(document, payload["data"])
    if shape.notes:
        # codex review round 4 (2026-08-30, EXECUTED): a bare `print()`
        # is captured by pytest's default output capture and only
        # surfaces for a FAILING test -- a repro confirmed a PASSING
        # test with a note produced NO visible "NOTE" text without `-s`.
        # chris's ruling was explicit: "downgrading them to notes must
        # not make them invisible." `warnings.warn` fixes this because
        # pytest always prints its "warnings summary" at the end of a
        # run by default, independent of output capture / `-s` -- a
        # different mechanism than stdout capture entirely.
        warnings.warn(
            f"[{operation}] shape notes (const {const_name}) -- spec-valid "
            "nulls on schema-nullable fields, NOT failures: " + "; ".join(shape.notes),
            stacklevel=2,
        )
    assert not shape.violations, (
        f"[{operation}] LIVE-LOCAL FIND: shape violations (const "
        f"{const_name}) -- response does not carry every field the "
        f"registered document selects, or violates the schema's own "
        f"nullability contract:\n" + "\n".join(shape.violations)
    )

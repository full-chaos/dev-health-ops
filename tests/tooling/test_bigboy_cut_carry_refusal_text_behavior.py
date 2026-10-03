"""bigboy-cut.sh: the pre-roll routing-carry refusal branches match the REAL dho CLI.

D2828/D2829 (CHAOS-7022 r1 self-correction, CHAOS-7023/#3369 r1 P1): branching on carry's
human-readable TEXT was wrong TWICE, in two different ways -- see the commit history on
this file and on ci/bigboy/bigboy-cut.sh. Structural fix: `dho goapi routing carry -json`
prints one `GOAPI_ROUTING_JSON {...}` line with a `reason` field from a small, closed
vocabulary ("carried", "digest_unchanged", "stale_build", "refused", "error"), classified
at the Go call site that knows why -- never guessed from prose.

This module does NOT stub any string. It builds the REAL `dho` binary, starts a REAL local
HTTP server standing in for the deployed query-api's /registry and /buildinfo, and stubs
only `docker` -- rewriting bigboy-cut.sh's `docker compose run venue-tools "<cmd>"`
invocation to run the real binary against that real server instead of the docker-network
hostname `query-api:8090` -- for the two cases bigboy-cut.sh actually branches on
`$CARRY_REASON` for: digest unchanged (no-op) and stale build (repoint-then-retry). Neither
case here needs Postgres: `carry`'s own preflights refuse both before ever opening a
database connection (confirmed by the existing Go integration test asserting the SAME
refusal against an unreachable DSN).

r3 P1 (CHAOS-7022, real, reproduced -- corrects a false claim this docstring used to make):
the "carried" success case was NOT, in fact, unchanged and needing no test -- bigboy-cut.sh's
`[ $CARRY_RC -eq 0 ]` branch (and its retry's `[ $CARRY_RC2 -eq 0 ]`) accepted a zero exit
status alone as proof of a carry, with no check that a `carried` reason (or ANY recognized
reason) actually accompanied it. A docker/compose-layer zero exit with no
`GOAPI_ROUTING_JSON` line at all -- never touching the real `dho` binary -- passed silently.
`test_real_zero_exit_with_no_json_still_aborts_the_cut` below is the missing test: `docker`
stubbed to `exit 0` unconditionally, emitting nothing, never invoking the real CLI at all.
"""

from __future__ import annotations

import http.server
import json
import os
import shutil
import stat
import subprocess
import tempfile
import threading
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CUT = ROOT / "ci" / "bigboy" / "bigboy-cut.sh"

_START_NEEDLE = "ROUTING_ORG=${ROUTING_ORG:-67f1add8-9fcb-4272-addb-044b70c442c8}"
# Unique text (appears exactly once, in the final catch-all `else` branch) -- the outermost
# `fi` right after it closes the WHOLE if/elif/elif/else chain, not one of the two INNER
# `fi`s that close the repoint-then-retry sub-branches earlier in the same block.
_END_NEEDLE = "refused for a reason other than 'no schema change' or a stale build"
_HARNESS_SENTINEL = "HARNESS_REACHED_MIGRATE"

# The exact fabricated fixture bearer the Go integration tests use
# (internal/goapicli/routing/verbs_integration_test.go's verbTestBearer): unsigned,
# accepted by no real verifier, safe to commit. envelopeCredential() only reads this env
# var and forwards it as an Authorization header -- it is never verified client-side (the
# server does that), so a fabricated value is exactly as real here as in the Go tests.
# LoadOperationCatalog refuses an EMPTY array outright ("regenerate it with
# generate_operation_catalog.py"), so `[]` cannot stand in for "no operations relevant to
# this test" the way it can for a JSON-shape-only fixture elsewhere -- one real entry is
# the minimum that loads. targetDocumentDigests recomputes its OWN digest from `document`'s
# text (goapidigest.Document) rather than trusting the file's `digest` field, so that
# field's value is irrelevant here; only a well-formed, non-empty file is required for
# both, since both tests below refuse at Preflight 1/2, long before any row/document is
# actually resolved against the catalog.
_MINIMAL_CATALOG = '[{"operation": "probeOperation", "digest": "irrelevant"}]'
_MINIMAL_DOCUMENTS = (
    '[{"operation": "probeOperation", "document": "query probe { __typename }", '
    '"const_name": "registeredProbeOperationDocument", "digest": "irrelevant"}]'
)

# The stale-build test carries a REAL row, seeded by the Go holder (seedLiveRow) at
# verbTestOperation/carryTestDocument -- this binary's OWN target-side catalog/documents
# must name the SAME operation and document text, or the row is never even ELIGIBLE for
# carry (a document-identity mismatch, a different decision than the one under test).
# UNLIKE _MINIMAL_CATALOG above, the catalog's own `digest` field is NOT ignorable here:
# Carry() cross-checks it against the recomputed document digest and refuses on a mismatch
# ("this image's registered documents and edge catalog disagree") -- found by executing
# this exact test with a placeholder "irrelevant" value and reading the real refusal back.
_STALE_BUILD_OPERATION = (
    "featureFlagTimeseries"  # verbTestOperation, verbs_integration_test.go
)
_STALE_BUILD_DOCUMENT = (
    "query flowMatrix($orgId: String!) { analytics(orgId: $orgId) "
    "{ flowMatrix { nodes { id } } } }"
)  # carryTestDocument, carry_integration_test.go, verbatim
_STALE_BUILD_DOCUMENT_DIGEST = "3b1e0818acf4ae4659152ff300e682ec4ef80ac71e9d25efa32ac9d2952a385d"  # goapidigest.Document(carryTestDocument) -- read off the real dho CLI's own printed
# stderr line for this exact text while developing this test, never hand-computed
_STALE_BUILD_CATALOG = f'[{{"operation": "{_STALE_BUILD_OPERATION}", "digest": "{_STALE_BUILD_DOCUMENT_DIGEST}"}}]'
_STALE_BUILD_DOCUMENTS = (
    f'[{{"operation": "{_STALE_BUILD_OPERATION}", "document": {json.dumps(_STALE_BUILD_DOCUMENT)}, '
    f'"const_name": "registeredFeatureFlagTimeseriesDocument", "digest": "irrelevant"}}]'
)

_FAKE_BEARER = (
    "eyJhbGciOiJFZERTQSIsImtpZCI6ImdvLWFwaS1lbnZlbG9wZS10ZXN0In0."
    "eyJzdWIiOiJiMGExYzJkMy0wMDAwLTQwMDAtODAwMC0wMDAwMDAwMDAwMDEifQ."
    "c2lnbmF0dXJl"
)


def _extract_block() -> str:
    lines = CUT.read_text().splitlines()
    start = next(i for i, ln in enumerate(lines) if _START_NEEDLE in ln)
    matches = [i for i in range(start, len(lines)) if _END_NEEDLE in lines[i]]
    assert len(matches) == 1, (
        f"expected exactly one occurrence of the end needle after line {start}, "
        f"got {len(matches)} -- the extraction anchor is no longer unique"
    )
    end = next(i for i in range(matches[0], len(lines)) if lines[i].strip() == "fi")
    return "\n".join(lines[start : end + 1])


@pytest.fixture(scope="module")
def dho_binary() -> Path:
    """The REAL dho binary, built once for this module. Skips (not fails) the whole
    module if the toolchain cannot build it -- the same shape every other test file in
    this suite uses for an environment precondition it cannot itself provide."""
    if shutil.which("go") is None:
        pytest.skip("go toolchain not available")
    out = Path(tempfile.mkdtemp(prefix="chaos7022-dho-build-")) / "dho"
    result = subprocess.run(
        ["go", "build", "-o", str(out), "./cmd/dho"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=180,
    )
    if result.returncode != 0:
        pytest.fail(f"go build ./cmd/dho failed:\n{result.stdout}\n{result.stderr}")
    return out


class _FakeQueryAPIHandler(http.server.BaseHTTPRequestHandler):
    """Serves /registry and /buildinfo the way the deployed process does -- registry's
    schema_digest and buildinfo's commit are set per-test via class attributes."""

    schema_digest = ""
    document_digest = "3b1e0818acf4ae4659152ff300e682ec4ef80ac71e9d25efa32ac9d2952a385d"
    operation = "featureFlagTimeseries"
    running_commit = "b18e56fa79cfe20ce0f75df148144b832d92be36"
    no_operations = False  # True -> /registry reports zero operations, a real, distinct
    # refusal ("registers no operations") from digest_unchanged/stale_build -- used to
    # prove the catch-all abort branch against the REAL CLI, not a source-text match.

    def _write_json(self, payload: dict) -> None:
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 -- BaseHTTPRequestHandler's own naming
        if self.path == "/registry":
            operations = (
                []
                if self.no_operations
                else [
                    {
                        "operation": self.operation,
                        "document_digest": self.document_digest,
                    }
                ]
            )
            self._write_json(
                {"schema_digest": self.schema_digest, "operations": operations}
            )
            return
        if self.path == "/buildinfo":
            if not self.headers.get("Authorization"):
                self.send_response(401)
                self.end_headers()
                return
            self._write_json(
                {
                    "commit": self.running_commit,
                    "modified": False,
                    "version": "test",
                    "build_time": "2026-09-10T00:00:00Z",
                }
            )
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, *args: object) -> None:  # silence per-request stderr noise
        pass


def _start_fake_query_api(
    *, schema_digest: str, running_commit: str, no_operations: bool = False
) -> http.server.HTTPServer:
    handler = type(
        "_Handler",
        (_FakeQueryAPIHandler,),
        {
            "schema_digest": schema_digest,
            "running_commit": running_commit,
            "no_operations": no_operations,
        },
    )
    server = http.server.HTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


# Same shape as the Go integration test's own control (carry_integration_test.go): the
# digest/build preflights refuse before any Postgres connection is opened, so an unusable
# DSN proves that ordering rather than merely being convenient here.
_UNREACHABLE_POSTGRES_URI = "postgres://nobody@127.0.0.1:1/none"


def _run_harness(
    dho: Path,
    tmp_path: Path,
    *,
    schema_digest: str,
    running_commit: str,
    postgres_uri: str = _UNREACHABLE_POSTGRES_URI,
    catalog_json: str = _MINIMAL_CATALOG,
    documents_json: str = _MINIMAL_DOCUMENTS,
    no_operations: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Runs the REAL bigboy-cut.sh carry block, with `docker` stubbed to invoke the REAL
    dho binary (built by the dho_binary fixture) against a REAL local HTTP server -- never
    a canned string standing in for either."""
    server = _start_fake_query_api(
        schema_digest=schema_digest,
        running_commit=running_commit,
        no_operations=no_operations,
    )
    try:
        # bigboy-cut.sh's own CARRY_ARGS now names -catalog/-documents explicitly, pointed
        # at the tools IMAGE's baked-in absolute paths (/app/go-api/... -- see the comment
        # above CARRY_ARGS in bigboy-cut.sh for why: venue-tools' compose working_dir
        # override makes carry's own relative defaults unreachable there). Those absolute
        # paths exist only inside the real container, so the docker stub below rewrites
        # them to these local fixture files -- the same substitution shape already used for
        # query-api:8090, proving the REAL script's own flags (not a test-side injection).
        catalog_path = tmp_path / "catalog.json"
        catalog_path.write_text(catalog_json)
        documents_path = tmp_path / "documents.json"
        documents_path.write_text(documents_json)

        block = _extract_block()
        stub_bin = tmp_path / "stubbin"
        stub_bin.mkdir(exist_ok=True)
        docker_stub = stub_bin / "docker"
        fake_addr = f"127.0.0.1:{server.server_port}"
        # The real invocation is `docker compose ... run --rm --no-deps -T venue-tools
        # "<sh -c string>"` -- the LAST arg is the command to run inside the container.
        # Rewritten here to run OUTSIDE any container: query-api:8090 -> the fake server,
        # the `dho mint envelope` subshell -> the fixture bearer (never really minted;
        # envelopeCredential() only reads the env var, never verifies it client-side), the
        # bare `dho` invocation -> the real, locally-built binary, and the two image-baked
        # -catalog/-documents paths CARRY_ARGS itself now names -> these local fixtures.
        # -catalog/-documents only ever appear on a `carry` command (REPOINT_ARGS has
        # neither), so the sed substitution below is a harmless no-op on the repoint leg.
        docker_stub.write_text(
            "#!/usr/bin/env bash\n"
            "set -u\n"
            'cmd="${@: -1}"\n'
            f'cmd="${{cmd//query-api:8090/{fake_addr}}}"\n'
            f'cmd=$(printf "%s" "$cmd" | sed -E \'s#GO_API_ROUTING_BEARER=\\$\\(dho mint envelope[^)]*\\)#GO_API_ROUTING_BEARER={_FAKE_BEARER}#\')\n'
            f'cmd="${{cmd/dho goapi/{dho} goapi}}"\n'
            f'cmd=$(printf "%s" "$cmd" | sed "s#/app/go-api/contracts/graphql/v1/go_api_operations.json#{catalog_path}#; s#/app/go-api/documents.json#{documents_path}#")\n'
            'eval "$cmd"\n'
        )
        docker_stub.chmod(docker_stub.stat().st_mode | stat.S_IEXEC)

        rec_prefix = tmp_path / "rec"
        harness = tmp_path / "harness.sh"
        harness.write_text(
            "#!/usr/bin/env bash\n"
            "set -u\n"
            f"REC={rec_prefix}\n"
            "OLD8=aaaaaaaa\n"
            "N8=bbbbbbbb\n"
            'st() { echo "STEP $1 rc=$2"; }\n'
            f"{block}\n"
            f"echo {_HARNESS_SENTINEL}\n"
        )
        harness.chmod(harness.stat().st_mode | stat.S_IEXEC)

        env = {"PATH": f"{stub_bin}:/usr/bin:/bin", "POSTGRES_URI": postgres_uri}
        return subprocess.run(
            ["bash", str(harness)],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
            env=env,
        )
    finally:
        server.shutdown()


def _local_schema_digest(dho: Path, tmp_path: Path) -> str:
    """The schema digest THIS dho binary's own embedded SDL computes -- read off a
    throwaway carry -json call (its own real preflight-2 refusal always names it, in
    `target_schema_digest`), never duplicated in Python against goapidigest's algorithm."""
    probe_server = _start_fake_query_api(
        schema_digest="sha256:0000000000000000000000000000000000000000000000000000000000000000",
        running_commit="irrelevant",
    )
    try:
        catalog_path = tmp_path / "probe-catalog.json"
        catalog_path.write_text(_MINIMAL_CATALOG)
        documents_path = tmp_path / "probe-documents.json"
        documents_path.write_text(_MINIMAL_DOCUMENTS)
        result = subprocess.run(
            [
                str(dho),
                "goapi",
                "routing",
                "carry",
                "-registry-url",
                f"http://127.0.0.1:{probe_server.server_port}/registry",
                "-buildinfo-url",
                f"http://127.0.0.1:{probe_server.server_port}/buildinfo",
                "-catalog",
                str(catalog_path),
                "-documents",
                str(documents_path),
                "-postgres-uri",
                _UNREACHABLE_POSTGRES_URI,
                "-recorded-by",
                "probe",
                "-review-evidence",
                "probe: read this binary's own target schema digest",
                "-json",
            ],
            env={"GO_API_ROUTING_BEARER": _FAKE_BEARER, "PATH": "/usr/bin:/bin"},
            capture_output=True,
            text=True,
            timeout=30,
        )
        for line in result.stdout.splitlines():
            if line.startswith("GOAPI_ROUTING_JSON "):
                payload = json.loads(line.removeprefix("GOAPI_ROUTING_JSON "))
                digest = payload.get("target_schema_digest")
                if digest:
                    return digest
        pytest.fail(
            f"probe carry call never printed a target_schema_digest:\n"
            f"stdout={result.stdout!r}\nstderr={result.stderr!r}"
        )
        return ""  # unreachable, satisfies the type checker
    finally:
        probe_server.shutdown()


def test_real_digest_unchanged_is_treated_as_a_pass_not_an_abort(
    dho_binary: Path, tmp_path: Path
) -> None:
    """The common case: an ordinary cut with no schema change. `carry`'s Preflight 2
    refuses because the fake server reports the digest THIS dho binary itself computes."""
    digest = _local_schema_digest(dho_binary, tmp_path)
    proc = _run_harness(
        dho_binary, tmp_path, schema_digest=digest, running_commit="irrelevant"
    )
    assert proc.returncode == 0, (
        "a REAL digest-unchanged refusal must not abort the cut -- "
        f"got rc={proc.returncode}, stdout={proc.stdout!r} stderr={proc.stderr!r}"
    )
    assert _HARNESS_SENTINEL in proc.stdout, (
        "the harness never reached the line after the carry block on a digest-unchanged "
        "refusal -- the ordinary no-op case is being wrongly aborted"
    )
    assert "STEP routing-carry rc=0" in proc.stdout


@pytest.fixture
def real_stale_row_postgres():
    """A REAL Postgres, with a REAL routing row seeded (candidate_build disagreeing with
    the fixture /buildinfo commit below) -- the shape `carry`'s row-processing loop (not
    an HTTP-only preflight) actually needs to raise ErrCarryBuildNotRunning, which nothing
    short of a real seeded row can trigger.

    Reuses the Go integration suite's OWN schema/seed helpers (seedLiveRow,
    startVerbPostgres -> testcontainers) rather than hand-duplicating DDL in Python: a
    throwaway `go test -run TestSeedStaleRowForBashHarness` holds the container
    open, signaling readiness (and the DSN) through a file since t.Log is buffered until
    the test itself completes -- unusable for a live handoff. Removed once this test and
    its Go-side holder land together.
    """
    work = Path(tempfile.mkdtemp(prefix="chaos7022-stale-build-pg-"))
    dsn_file = work / "dsn.txt"
    sentinel = work / "done"
    proc = subprocess.Popen(
        [
            "go",
            "test",
            "-tags=integration",
            "-run",
            "TestSeedStaleRowForBashHarness",
            "-v",
            "./internal/goapicli/routing/...",
        ],
        cwd=ROOT,
        env={
            **os.environ,
            "SCRATCH_DSN_FILE": str(dsn_file),
            "SCRATCH_SENTINEL": str(sentinel),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        for _ in range(
            300
        ):  # up to 60s, matching the Go side's own container-start budget
            if dsn_file.exists() and dsn_file.stat().st_size > 0:
                break
            if proc.poll() is not None:
                out = proc.stdout.read() if proc.stdout else ""
                pytest.fail(f"the Postgres holder exited before writing a DSN:\n{out}")
            import time as _time

            _time.sleep(0.2)
        else:
            proc.kill()
            pytest.fail(
                "timed out waiting for the Postgres holder to write its DSN file"
            )
        dsn = dsn_file.read_text().strip()
        yield dsn
    finally:
        sentinel.write_text("done")
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
        if proc.returncode not in (0, None):
            out = proc.stdout.read() if proc.stdout else ""
            pytest.fail(
                f"the Postgres holder failed on shutdown (rc={proc.returncode}):\n{out}"
            )


def test_real_stale_build_triggers_the_repoint_then_retry_fallback(
    dho_binary: Path, tmp_path: Path, real_stale_row_postgres: str
) -> None:
    """The documented rev196 exception: a stale-build refusal must repoint then retry
    carry once, not abort on the first refusal. Against a REAL seeded row and a REAL
    /buildinfo reporting a build that row disagrees with, the first carry attempt raises
    the genuine goapiproof.ErrCarryBuildNotRunning (reason="stale_build"); the repoint call
    then rewrites that row's provenance to the live process's OWN reported build, so the
    SECOND (retry) carry attempt sees an up-to-date row and succeeds for real."""
    live_digest = (
        "sha256:29d509cd00000000000000000000000000000000000000000000000000000000"
    )
    running_build = "0000000000000000000000000000000000000000"  # disagrees with seedLiveRow's verbTestBuild
    proc = _run_harness(
        dho_binary,
        tmp_path,
        schema_digest=live_digest,
        running_commit=running_build,
        postgres_uri=real_stale_row_postgres,
        catalog_json=_STALE_BUILD_CATALOG,
        documents_json=_STALE_BUILD_DOCUMENTS,
    )
    assert proc.returncode == 0, (
        "a stale-build refusal, repoint, then a successful retry must not abort -- "
        f"got rc={proc.returncode}, stdout={proc.stdout!r} stderr={proc.stderr!r}"
    )
    assert "pre-roll carry: rows lag the actually-running build" in proc.stdout, (
        f"the stale-build branch never fired -- got stdout={proc.stdout!r} stderr={proc.stderr!r}"
    )
    assert "pre-roll carry: OK after repoint-then-retry" in proc.stdout, (
        "the retry after repoint did not succeed for real -- "
        f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    )
    assert _HARNESS_SENTINEL in proc.stdout


def test_real_unrecognized_refusal_still_aborts_the_cut(
    dho_binary: Path, tmp_path: Path
) -> None:
    """A real refusal outside {digest_unchanged, stale_build} -- here, the deployed
    registry genuinely reports zero operations ("registers no operations", carry's own
    Preflight 1) -- must abort the cut before migrate/up/up-workers ever runs, never fall
    through silently. This proves the final catch-all `else` branch against the REAL CLI's
    own `reason` field, not a source-text match for the literal string "exit 1" (the r2
    finding this test replaces)."""
    proc = _run_harness(
        dho_binary,
        tmp_path,
        schema_digest="sha256:0000000000000000000000000000000000000000000000000000000000000000",
        running_commit="irrelevant",
        no_operations=True,
    )
    assert proc.returncode != 0, (
        "a real, unrecognized carry refusal must abort the cut -- "
        f"got rc={proc.returncode}, stdout={proc.stdout!r} stderr={proc.stderr!r}"
    )
    assert (
        "refused for a reason other than 'no schema change' or a stale build"
        in proc.stderr
    ), f"the catch-all abort branch never fired -- stderr={proc.stderr!r}"
    assert _HARNESS_SENTINEL not in proc.stdout, (
        "the harness reached the line after the carry block -- an unrecognized refusal "
        f"did not actually abort. stdout={proc.stdout!r}"
    )


def test_real_zero_exit_with_no_json_still_aborts_the_cut(tmp_path: Path) -> None:
    """r3 P1 (CHAOS-7022): a zero exit status alone must never be read as a carry. `docker`
    here exits 0 unconditionally and prints nothing for every call -- the compose layer
    "succeeded" but the real `dho` binary never ran, so no `GOAPI_ROUTING_JSON` line, and no
    `carried` reason, ever exists. Before the fix this fell straight into the
    `[ $CARRY_RC -eq 0 ]` branch and reported success; the retry path (same stub, reached via
    a first call returning `stale_build`) has the identical bug at `[ $CARRY_RC2 -eq 0 ]`.
    Both must abort. No dho binary or fake query-api server needed: `docker` never reaches
    either."""
    block = _extract_block()
    stub_bin = tmp_path / "stubbin"
    stub_bin.mkdir(exist_ok=True)
    docker_stub = stub_bin / "docker"
    docker_stub.write_text("#!/usr/bin/env bash\nexit 0\n")
    docker_stub.chmod(docker_stub.stat().st_mode | stat.S_IEXEC)

    rec_prefix = tmp_path / "rec"
    harness = tmp_path / "harness.sh"
    harness.write_text(
        "#!/usr/bin/env bash\n"
        "set -u\n"
        f"REC={rec_prefix}\n"
        "OLD8=aaaaaaaa\n"
        "N8=bbbbbbbb\n"
        'st() { echo "STEP $1 rc=$2"; }\n'
        f"{block}\n"
        f"echo {_HARNESS_SENTINEL}\n"
    )
    harness.chmod(harness.stat().st_mode | stat.S_IEXEC)

    proc = subprocess.run(
        ["bash", str(harness)],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
        env={"PATH": f"{stub_bin}:/usr/bin:/bin"},
    )
    assert proc.returncode != 0, (
        "a zero exit with no GOAPI_ROUTING_JSON line (no evidence a carry happened) must "
        f"abort the cut -- got rc={proc.returncode}, stdout={proc.stdout!r} stderr={proc.stderr!r}"
    )
    assert (
        "refused for a reason other than 'no schema change' or a stale build"
        in proc.stderr
    ), f"the catch-all abort branch never fired -- stderr={proc.stderr!r}"
    assert _HARNESS_SENTINEL not in proc.stdout, (
        "the harness reached the line after the carry block -- a zero exit with no reason "
        f"did not actually abort. stdout={proc.stdout!r}"
    )

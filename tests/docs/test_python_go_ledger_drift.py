"""Python<->Go live-path ledger drift guard (CHAOS-4433).

The ledger published at ``docs/reference/runtime/python-go-live-path-ledger.md``
is the answer to "who writes what today" for every River job kind
(``contracts/jobs/v1/registry.json``), every ``internal/syncdispatchruntime/bridge.go``
route, and every file under ``src/dev_health_ops/workers/*.py``.
``scripts/check_python_go_ledger_docs_drift.py`` fails when the published page
and those producers disagree, mirroring ``tests/docs/test_queue_mapping_drift.py``.

Root cause this guards against (chris, 2026-08-28): two Done tickets
(CHAOS-4323, CHAOS-3716) were read as "ported" while the live writers stayed
Python, because nothing forced a record of producer/writer/state to be kept
in sync with the code. This test is what makes THAT class of drift a hard,
loud CI failure instead of a doc that quietly goes stale.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DRIFT_SCRIPT = ROOT / "scripts" / "check_python_go_ledger_docs_drift.py"
GEN_SCRIPT = ROOT / "scripts" / "gen_python_go_ledger_docs.py"
CANONICAL_DOC = (
    ROOT / "docs" / "reference" / "runtime" / "python-go-live-path-ledger.md"
)

BLOCK_MARKERS = (
    (
        "<!-- BEGIN GENERATED KIND LEDGER -->",
        "<!-- END GENERATED KIND LEDGER -->",
        "render_kind_block",
    ),
    (
        "<!-- BEGIN GENERATED BRIDGE ROUTE LEDGER -->",
        "<!-- END GENERATED BRIDGE ROUTE LEDGER -->",
        "render_route_block",
    ),
    (
        "<!-- BEGIN GENERATED WORKER FILE LEDGER -->",
        "<!-- END GENERATED WORKER FILE LEDGER -->",
        "render_worker_block",
    ),
    (
        "<!-- BEGIN GENERATED SOURCE DISCOVERY LEDGER -->",
        "<!-- END GENERATED SOURCE DISCOVERY LEDGER -->",
        "render_source_discovery_block",
    ),
)


def _load_gen_module() -> types.ModuleType:
    spec = importlib.util.spec_from_file_location(
        "gen_python_go_ledger_docs", GEN_SCRIPT
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_ledger_drift_check_exits_clean() -> None:
    """check_python_go_ledger_docs_drift.py must exit 0 and emit no ERROR lines."""
    assert DRIFT_SCRIPT.is_file(), f"missing drift script: {DRIFT_SCRIPT}"
    result = subprocess.run(
        [sys.executable, str(DRIFT_SCRIPT)],
        check=False,
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"Python<->Go ledger drift check failed:\n{result.stdout}\n{result.stderr}"
    )
    assert "ERROR:" not in result.stdout, (
        f"drift check reported errors:\n{result.stdout}"
    )


def test_ledger_generated_blocks_match_producers() -> None:
    """The three published blocks must match registry.json / bridge.go / workers dir.

    Read-only verification: proves the published page is in sync with its
    producers without writing to disk.
    """
    assert GEN_SCRIPT.is_file(), f"missing gen script: {GEN_SCRIPT}"
    assert CANONICAL_DOC.is_file(), f"missing canonical page: {CANONICAL_DOC}"

    gen = _load_gen_module()
    doc = CANONICAL_DOC.read_text(encoding="utf-8")

    for begin, end, render_fn_name in BLOCK_MARKERS:
        expected_block = getattr(gen, render_fn_name)()
        start = doc.find(begin)
        stop = doc.find(end)
        assert start != -1 and stop > start, (
            f"generated markers {begin}/{end} missing in {CANONICAL_DOC}"
        )
        actual_block = doc[start : stop + len(end)]
        assert actual_block == expected_block, (
            f"Generated block {begin} in docs/reference/runtime/python-go-live-path-ledger.md "
            "is stale. Run 'python scripts/gen_python_go_ledger_docs.py' and commit the result."
        )


def test_every_registry_kind_and_bridge_route_and_worker_file_has_a_curated_row() -> (
    None
):
    """Falsification control: the generator itself must refuse to render on drift.

    Proves the guard can fail, not just pass -- root AGENTS.md's rule that an
    unexercised guard is not evidence it works. Mutates a live producer set in
    memory (never touches disk) and asserts the generator raises.
    """
    gen = _load_gen_module()

    mutated_kinds = gen.load_registry_kinds() | {"a.brand.new.kind.nobody.tracked"}
    try:
        gen._consistency_guard(
            "registry kind(s)", mutated_kinds, set(gen.KIND_LEDGER), ""
        )
        raised = False
    except SystemExit:
        raised = True
    assert raised, "consistency guard did not fail on an untracked new kind"

    mutated_routes = gen.load_bridge_routes() | {
        "/api/internal/worker-sync/brand-new-route"
    }
    try:
        gen._consistency_guard(
            "bridge.go route(s)", mutated_routes, set(gen.BRIDGE_ROUTE_LEDGER), ""
        )
        raised = False
    except SystemExit:
        raised = True
    assert raised, "consistency guard did not fail on an untracked new bridge route"

    mutated_files = gen.load_worker_files() | {"brand_new_worker_module.py"}
    try:
        gen._consistency_guard(
            "src/dev_health_ops/workers/*.py file(s)",
            mutated_files,
            set(gen.WORKER_FILE_LEDGER),
            "",
        )
        raised = False
    except SystemExit:
        raised = True
    assert raised, "consistency guard did not fail on an untracked new worker file"

    # CHAOS-4602: the fourth mechanism -- source discovery is neither a kind,
    # a route, nor a worker file, which is exactly why the three guards above
    # could never have caught it existing at all before this ticket.
    mutated_providers = gen.load_source_discovery_providers() | {"brand_new_provider"}
    try:
        gen._consistency_guard(
            "source-discovery provider(s)",
            mutated_providers,
            set(gen.SOURCE_DISCOVERY_LEDGER),
            "",
        )
        raised = False
    except SystemExit:
        raised = True
    assert raised, (
        "consistency guard did not fail on an untracked new source-discovery provider"
    )


def test_team_item_kinds_native_for_every_provider() -> None:
    """linear/github/gitlab/jira team-item writes must all read as Go-native.

    #1989 (27bef7286, Linear), #1984 (950752653, GitHub), #1985 (5bff38a5a,
    GitLab), and jira's own native collector (`JiraTeamCatalogCollector`)
    complete the set of native Go collectors for `teams` /
    `team_memberships` / `team_project_ownership`. The ledger's
    `sync.team_autoimport` kind row must say so -- citing the three merge
    SHAs -- and must no longer claim jira (or any provider) is still bridge.

    The `state` field is asserted on its own (not the whole row's aggregate
    text), and pinned to a specific attribution -- an earlier codex round
    (2026-08-29) found a substring-anywhere version would still pass a
    swapped or partial attribution, e.g. "native for jira, bridge for
    linear/github/gitlab" or a route that marks only one provider dead.
    """
    gen = _load_gen_module()
    row = gen.KIND_LEDGER["sync.team_autoimport"]
    row_text = " ".join(row.values()).lower()
    state = row.get("state", "").lower()

    for sha in ("27bef7286", "950752653", "5bff38a5a"):
        assert sha in row_text, (
            f"sync.team_autoimport ledger row must cite merge SHA {sha}"
        )
    assert "in progress" not in row_text, (
        "sync.team_autoimport row must not still call 4431/4432/4434 In Progress -- they are Done"
    )

    # Pin the exact per-provider attribution in `state`, not just "some field
    # somewhere mentions the words native/linear/github/gitlab/jira".
    assert "native for linear/github/gitlab/jira" in state, (
        f"sync.team_autoimport state must claim native for every provider (got: {row.get('state')!r})"
    )
    # Reject any lingering bridge attribution for a real import-capable
    # provider (pagerduty/launchdarkly's mechanical no-op reach is fine).
    for provider in ("linear", "github", "gitlab", "jira"):
        assert f"bridge for {provider}" not in state, (
            f"sync.team_autoimport state must not claim {provider} is still bridge"
        )


def test_bridge_routes_marked_dead_or_deleted() -> None:
    """The two team-autoimport bridge routes are dead (or deleted) for every provider.

    `/team-autoimport`'s Python side is deleted outright now that jira (the
    last import-capable provider still reaching it) has its own native
    collector. `/reference-discovery-populate` is deleted entirely -- Go
    caller (`PopulateReferenceDiscovery`/`BridgeDiscoveryExecutor`) and
    Python handler both gone, so neither the generator's route regex nor
    its curated ledger has a row for it any more; a stale curated row here
    is exactly what the generator's own consistency guard would refuse to
    render on.
    """
    gen = _load_gen_module()
    assert (
        "/api/internal/worker-sync/reference-discovery-populate"
        not in gen.load_bridge_routes()
    ), "reference-discovery-populate must have no remaining Go caller in bridge.go"
    assert (
        "/api/internal/worker-sync/reference-discovery-populate"
        not in gen.BRIDGE_ROUTE_LEDGER
    ), "reference-discovery-populate must have no remaining curated ledger row"

    team_autoimport_row = gen.BRIDGE_ROUTE_LEDGER["/api/internal/worker-sync/team-autoimport"]
    team_autoimport_state = team_autoimport_row.get("state", "").lower()
    assert "dead" in team_autoimport_state, (
        f"team-autoimport route state must claim dead (got: {team_autoimport_row.get('state')!r})"
    )
    assert "deleted" in team_autoimport_state, (
        f"team-autoimport route state must say the Python route is deleted (got: {team_autoimport_row.get('state')!r})"
    )
    for provider in ("linear", "github", "gitlab", "jira"):
        assert f"live for {provider}" not in team_autoimport_state, (
            f"team-autoimport route state must not claim {provider} is still live"
        )
    assert (
        team_autoimport_row.get("python_handler", "").upper().startswith("DELETED")
    ), (
        "team-autoimport route's python_handler must say DELETED "
        f"(got: {team_autoimport_row.get('python_handler')!r})"
    )

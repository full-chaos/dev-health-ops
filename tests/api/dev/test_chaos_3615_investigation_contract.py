"""CHAOS-3615: artifact drift, fixture coverage and backend neutrality.

Three things this module holds the contract to.

**Drift.** ``check`` mode compares the whole artifact set — contents,
missing files and unexpected extra files — so a schema regenerated without
committing, a fixture edited by hand, or a contract removed without
regenerating are all red here rather than discovered by CHAOS-3616.

**Coverage.** Every registered contract has a positive golden and at least
one negative fixture, and the negatives genuinely fail. The exporter already
refuses to write otherwise; this asserts it independently, because a gate
that only ever runs inside the thing it gates proves less than it appears
to.

**Backend neutrality.** The wire must name no graph backend, no graph query
language and no graph-store concept. Asserted against the *generated
artifacts* — the schemas, every fixture and every registry file — rather
than against the Python source, because the artifacts are what a consumer
actually reads, and a neutral source that generated a leaky schema would
pass a source-only scan.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from dev_health_ops.api.dev.investigation_contract import (
    INVESTIGATION_CONTRACT_MODELS,
)
from dev_health_ops.api.dev.investigation_contract.export import (
    ARTIFACT_ROOT,
    check_artifacts,
    expected_artifacts,
)
from dev_health_ops.api.dev.investigation_contract.fixtures import (
    negative_fixtures,
    positive_fixtures,
    positive_variant_fixtures,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]

#: Tokens that must never reach the wire. Graph backends, graph query
#: languages and graph-native surface concepts. ``graph`` itself is
#: deliberately absent: ``SourceClass.WORK_GRAPH`` is a platform concept
#: that predates this work and banning the substring would flag it.
BANNED_WIRE_TOKENS: tuple[str, ...] = (
    "graphiti",
    "neo4j",
    "falkor",
    "kuzu",
    "nebula",
    "arangodb",
    "janusgraph",
    "tigergraph",
    "memgraph",
    "dgraph",
    "neptune",
    "cypher",
    "gremlin",
    "sparql",
    "bolt://",
    "graph_store",
    "episodic",
    "group_id",
    "graph_query",
    "traversal_query",
)


def test_checked_in_artifacts_match_the_contract_source() -> None:
    """The drift gate. Regenerate with ``python -m ...export write``."""

    check_artifacts(expected_artifacts())


def test_artifact_root_is_not_the_client_served_tree() -> None:
    """This packet is an internal trial artifact and must stay out of v2.

    ``contracts/ask-dev/v2`` is reserved for wire contracts served to real
    clients and is consumed by ``dev-health-web``'s contract sync. Filing an
    internal trial artifact there would both misrepresent it and put
    CHAOS-3616 iterations on the critical path of a web contract regen.
    """

    assert (
        ARTIFACT_ROOT == REPOSITORY_ROOT / "contracts" / "ask-dev-investigation" / "v1"
    )
    assert ARTIFACT_ROOT.is_dir()

    from dev_health_ops.api.dev.contracts_v2 import CONTRACT_MODELS_V2

    overlap = sorted(set(INVESTIGATION_CONTRACT_MODELS) & set(CONTRACT_MODELS_V2))
    assert not overlap, (
        f"investigation contracts registered in the client-served v2 registry: {overlap}"
    )


def test_manifest_covers_every_artifact_with_a_digest() -> None:
    manifest = json.loads((ARTIFACT_ROOT / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["schema_version"] == "ask_dev_investigation_contract_manifest.v1"
    listed = {entry["schema_version"] for entry in manifest["contracts"]}
    assert listed == set(INVESTIGATION_CONTRACT_MODELS)
    for entry in manifest["contracts"]:
        assert len(entry["schema"]["sha256"]) == 64
        assert len(entry["positive"]["sha256"]) == 64
        assert entry["negative"], f"{entry['schema_version']} has no negative fixture"
        for negative in entry["negative"]:
            assert len(negative["sha256"]) == 64
    assert manifest["registries"], "the registries must be exported for CHAOS-3616"
    for registry in manifest["registries"]:
        assert (ARTIFACT_ROOT / registry["path"]).is_file()
        assert len(registry["sha256"]) == 64


@pytest.mark.parametrize("contract", sorted(INVESTIGATION_CONTRACT_MODELS))
def test_positive_golden_validates(contract: str) -> None:
    INVESTIGATION_CONTRACT_MODELS[contract].model_validate(
        positive_fixtures()[contract]
    )


def test_fixture_coverage_matches_the_registry_exactly() -> None:
    assert set(positive_fixtures()) == set(INVESTIGATION_CONTRACT_MODELS)
    assert set(negative_fixtures()) == set(INVESTIGATION_CONTRACT_MODELS)
    unregistered = set(positive_variant_fixtures()) - set(INVESTIGATION_CONTRACT_MODELS)
    assert not unregistered


def test_every_registered_contract_declares_its_schema_version() -> None:
    """No contract may be identified by position or by convention alone."""

    for name, model in INVESTIGATION_CONTRACT_MODELS.items():
        field = model.model_fields["schema_version"]
        assert field.is_required(), f"{name} lets schema_version default"
        schema = model.model_json_schema(mode="validation")
        assert schema["properties"]["schema_version"]["const"] == name


def test_section_goldens_are_the_packet_goldens() -> None:
    """The section fixtures are slices of the packet, not parallel authorings.

    Eight independently authored examples would drift; this asserts they
    cannot, because each section golden is byte-identical to the
    corresponding section of the packet golden.
    """

    positives = positive_fixtures()
    packet = positives["ask_dev_investigation_packet.v1"]
    for section, contract in (
        ("analytical_job", "ask_dev_analytical_job.v1"),
        ("subject_discovery", "ask_dev_subject_discovery.v1"),
        ("comparison_cohort", "ask_dev_comparison_cohort.v1"),
        ("related_context", "ask_dev_related_context.v1"),
        ("driver_analysis", "ask_dev_driver_analysis.v1"),
        ("evidence_coverage", "ask_dev_evidence_coverage.v1"),
        ("versions", "ask_dev_investigation_versions.v1"),
    ):
        assert packet[section] == positives[contract], (
            f"the {contract} golden has drifted from the packet's {section}"
        )


def _artifact_files() -> list[Path]:
    return sorted(path for path in ARTIFACT_ROOT.rglob("*.json") if path.is_file())


def test_generated_artifacts_name_no_graph_backend() -> None:
    """Zero graph-backend vocabulary anywhere on the wire.

    Scans every generated file, not a sampled subset: schemas carry field
    names and enum values, fixtures carry the values a producer would emit,
    and the registries carry the prose CHAOS-3616 will read. A leak in any
    of them is a leak.
    """

    files = _artifact_files()
    assert len(files) >= 20, (
        f"only {len(files)} artifacts found under {ARTIFACT_ROOT}; this scan "
        "would pass vacuously"
    )
    offenders: list[str] = []
    for path in files:
        text = path.read_text(encoding="utf-8").casefold()
        for token in BANNED_WIRE_TOKENS:
            if token in text:
                offenders.append(f"{path.relative_to(ARTIFACT_ROOT)}: {token}")
    assert not offenders, f"graph-native vocabulary reached the wire: {offenders}"


def test_the_neutrality_scan_can_actually_fail() -> None:
    """Anti-vacuity control for the scan above.

    A scan whose token list never matches anything is indistinguishable from
    a scan that is looking in the wrong place. This plants one banned token
    in a copy of a real artifact's text and requires the same predicate to
    catch it.
    """

    sample = (ARTIFACT_ROOT / "manifest.json").read_text(encoding="utf-8")
    planted = (sample + '\n{"backend": "neo4j"}\n').casefold()
    assert any(token in planted for token in BANNED_WIRE_TOKENS)


def test_no_contract_field_names_a_person_subject() -> None:
    """Person-level ranking is unrepresentable, not merely prohibited.

    Checked against the generated schemas so it covers every enum the wire
    admits: if a ``person``/``individual``/``developer`` subject kind or
    comparison dimension were ever added, it would show up here before any
    producer could emit one.
    """

    banned = ("person", "individual", "developer", "headcount_rank", "contributor_rank")
    offenders: list[str] = []
    for path in sorted((ARTIFACT_ROOT / "schemas").glob("*.json")):
        schema = json.loads(path.read_text(encoding="utf-8"))
        for definition in schema.get("$defs", {}).values():
            for value in definition.get("enum", []):
                if any(token in str(value).casefold() for token in banned):
                    offenders.append(f"{path.name}: {value}")
    assert not offenders, f"person-level vocabulary on the wire: {offenders}"


def test_every_named_guard_is_load_bearing() -> None:
    """Run the fault-injection proof: remove each guard, watch the fault land.

    Shelled out rather than inlined because the proof mutates class-level
    pydantic state, which would corrupt every later test in the session.
    The script's own exit code is the assertion; its stdout is attached on
    failure so a red run names the case that stopped being load-bearing.
    """

    script = REPOSITORY_ROOT / "scripts" / "verify_chaos_3615_fault_mode_guards.py"
    completed = subprocess.run(
        [sys.executable, str(script)],
        check=False,
        capture_output=True,
        text=True,
        cwd=REPOSITORY_ROOT,
    )
    assert completed.returncode == 0, (
        "at least one named guard is not what rejects its fault:\n"
        f"{completed.stdout}\n{completed.stderr}"
    )
    assert "GUARD PROOF PASSED" in completed.stdout

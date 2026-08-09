"""CHAOS-3616: the checked-in corpus artifacts match the code that produces them.

Same drift-gate pattern as the frozen contract's own exporter: the full
artifact set is compared, so a stale file, a missing file and an unexpected
extra file are all failures. Without that, a reviewer reading
``contracts/ask-dev-investigation-corpus/v1`` would be reading a snapshot of
whatever the corpus looked like the last time somebody remembered to
regenerate it.

The other thing checked here is the boundary with the frozen contract root.
Corpus output must not land in ``contracts/ask-dev-investigation/v1``: that
tree's own gate rejects unexpected files, so a stray corpus artifact there
breaks the CHAOS-3615 freeze rather than this one.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dev_health_ops.api.dev.investigation_contract.export import (
    ARTIFACT_ROOT as CONTRACT_ROOT,
)
from dev_health_ops.api.dev.investigation_contract.export import (
    check_artifacts as check_contract_artifacts,
)
from dev_health_ops.api.dev.investigation_contract.export import (
    expected_artifacts as expected_contract_artifacts,
)
from dev_health_ops.api.dev.investigation_corpus.export import (
    ARTIFACT_ROOT,
    FULL_WITNESS_CASE_IDS,
    check_artifacts,
    expected_artifacts,
)
from dev_health_ops.api.dev.investigation_corpus.reference import reference_packet


def test_corpus_artifacts_are_in_sync() -> None:
    check_artifacts(expected_artifacts())


def test_the_corpus_root_is_not_the_frozen_contract_root() -> None:
    assert ARTIFACT_ROOT != CONTRACT_ROOT
    assert CONTRACT_ROOT not in ARTIFACT_ROOT.parents


def test_the_frozen_contract_artifacts_are_undisturbed() -> None:
    """The corpus must not have leaked a file into the 3615 tree.

    Restated as a test here rather than relied on from the contract's own
    suite: this is the failure this package could plausibly cause, so this is
    where a reader looks for it.
    """

    check_contract_artifacts(expected_contract_artifacts())


def test_every_declared_full_witness_is_written_out() -> None:
    for case_id in FULL_WITNESS_CASE_IDS:
        path = ARTIFACT_ROOT / "examples" / "reference" / f"{case_id}.json"
        assert path.exists(), f"declared full witness not exported: {case_id}"
        assert path.read_text(encoding="utf-8").strip()


def test_the_digest_list_covers_every_authored_case() -> None:
    """A digest list that skipped cases would hide drift in exactly those cases."""

    import json

    from dev_health_ops.api.dev.investigation_corpus.cases import authored_cases

    payload = json.loads(
        (ARTIFACT_ROOT / "examples" / "reference_digests.json").read_text(
            encoding="utf-8"
        )
    )
    listed = {item["case_id"] for item in payload["digests"]}
    assert listed == {case.case_id for case in authored_cases()}


def test_a_digest_actually_matches_its_packet() -> None:
    """Otherwise the digest list is decoration rather than a drift gate."""

    import hashlib
    import json

    payload = json.loads(
        (ARTIFACT_ROOT / "examples" / "reference_digests.json").read_text(
            encoding="utf-8"
        )
    )
    by_case = {item["case_id"]: item["sha256"] for item in payload["digests"]}
    case_id = FULL_WITNESS_CASE_IDS[0]
    contents = json.dumps(reference_packet(case_id), indent=2, sort_keys=True) + "\n"
    assert by_case[case_id] == hashlib.sha256(contents.encode("utf-8")).hexdigest()


def test_the_manifest_records_the_authorization_policy() -> None:
    """The one thing a downstream reader must not get wrong.

    A consumer that scored ZERO_UNAUTHORIZED_RESULTS from the packet's own
    ``authorized_entity_ids`` would reproduce the exact gap this corpus
    exists to close, so the manifest says so in the artifact itself.
    """

    import json

    manifest = json.loads((ARTIFACT_ROOT / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["authorization_policy"]["producer_declaration_is_not_evidence"]
    assert manifest["validation_policy"]["schema_only_validation_is_sufficient"] is (
        False
    )


def test_no_corpus_file_landed_in_the_contract_tree() -> None:
    strays = sorted(
        str(path.relative_to(CONTRACT_ROOT))
        for path in Path(CONTRACT_ROOT).rglob("*corpus*")
        if path.is_file()
    )
    assert not strays, f"corpus artifacts inside the frozen contract root: {strays}"


def test_every_counted_world_collection_has_an_artifact() -> None:
    """Adversarial review round 1, finding 6.

    The manifest reported 39 measurements, 3 documents and 3 episodes that the
    artifact tree did not contain, so an artifact-only arm would have run
    against a world missing every staffing fact and both adversarial prose
    sources -- while the Python oracle scored it against the full one. A count
    with no artifact behind it is a reproducibility claim that is false.
    """

    import json

    manifest: dict[str, Any] = json.loads(
        (ARTIFACT_ROOT / "manifest.json").read_text(encoding="utf-8")
    )
    registry_only = {"cases_total", "cases_authored", "oracles", "required_topics"}
    counted = set(manifest["counts"]) - registry_only
    mapped = set(manifest["collection_artifacts"])
    assert counted == mapped, (
        f"counted-but-unexported: {sorted(counted - mapped)}; "
        f"exported-but-uncounted: {sorted(mapped - counted)}"
    )
    for collection, path in manifest["collection_artifacts"].items():
        assert (ARTIFACT_ROOT / path).exists(), f"{collection} -> {path} is missing"


def test_the_exported_collections_carry_the_counted_number_of_records() -> None:
    """A file that exists but is empty would satisfy the parity test alone."""

    import json

    manifest: dict[str, Any] = json.loads(
        (ARTIFACT_ROOT / "manifest.json").read_text(encoding="utf-8")
    )
    key_by_collection = {
        "entities": "entities",
        "relationships": "relationships",
        "evidence_records": "evidence",
        "measurements": "measurements",
        "documents": "documents",
        "episodes": "episodes",
        "principals": "principals",
        "source_feeds": "feeds",
    }
    for collection, path in manifest["collection_artifacts"].items():
        payload = json.loads((ARTIFACT_ROOT / path).read_text(encoding="utf-8"))
        records = payload[key_by_collection[collection]]
        assert len(records) == manifest["counts"][collection], (
            f"{collection}: manifest says {manifest['counts'][collection]}, "
            f"{path} carries {len(records)}"
        )


def test_the_prose_sources_are_exported_in_full() -> None:
    """The injection and the bait cannot be tested against if they are absent."""

    import json

    documents = json.loads(
        (ARTIFACT_ROOT / "world" / "documents.json").read_text(encoding="utf-8")
    )["documents"]
    injected = [item for item in documents if item["contains_injection"]]
    assert injected, "no injected document in the artifact tree"
    assert "ignore your previous instructions" in injected[0]["body"].casefold()

    episodes = json.loads(
        (ARTIFACT_ROOT / "world" / "episodes.json").read_text(encoding="utf-8")
    )["episodes"]
    bait = [item for item in episodes if item["is_adversarial"]]
    assert bait, "no adversarial episode in the artifact tree"
    assert len(bait[0]["summary"]) > 100, (
        "the keyword-stuffed summary was truncated in export, so an "
        "artifact-only arm cannot be baited by it"
    )

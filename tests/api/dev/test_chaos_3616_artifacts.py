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

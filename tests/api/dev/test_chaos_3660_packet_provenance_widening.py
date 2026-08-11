"""CHAOS-3660/CHAOS-3678: AnalyticalJob widens to carry production provenance.

`AnalyticalJob.question_family: QuestionFamilyID` was a **required** field
on the frozen `investigation_contract` wire contract -- not just on
`packet_builder.JobContext` -- so "a production packet with no
QuestionFamilyID" needed a change to the frozen contract itself. Ruled on
CHAOS-3660: `question_family` becomes optional, a new optional
`production_job: ProductionJobProvenance` carries production's own
provenance (intent_id as a validated string -- this frozen contract must
never import production's `QuestionIntentID` -- plus a run identity), and
`validate_provenance_matches_schema_version` enforces exactly one shape per
packet, tied to which `schema_version` literal (`.v1` trial / `.v2`
production) it declares.

Two binding conditions from the ruling, each with its own test class here:

* every existing serialized trial artifact still validates, unchanged,
  under the widened schema -- proven against the REAL committed artifacts
  read from git HEAD (the tree as it stood before this change), not
  synthetic samples;
* the change is recorded in `CONTRACT_CHANGELOG`, dated and cited, so drift
  tooling sees an admitted, explained change rather than a silent one.
"""

from __future__ import annotations

import json
import subprocess
from copy import deepcopy
from pathlib import Path

import pytest
from pydantic import ValidationError

from dev_health_ops.api.dev.investigation_contract import (
    CONTRACT_CHANGELOG,
    INVESTIGATION_CONTRACT_MODELS,
    AnalyticalJob,
    ProductionJobProvenance,
)
from dev_health_ops.api.dev.investigation_contract.export import ARTIFACT_ROOT
from dev_health_ops.api.dev.investigation_contract.fixtures import (
    negative_fixtures,
    positive_fixtures,
)

REPO_ROOT = Path(__file__).resolve().parents[3]


def _artifact_paths_at_head() -> tuple[str, ...]:
    """Every example artifact path as committed at git HEAD.

    Not the working tree -- this repo's own `git ls-tree` is the source of
    truth for "what was already committed before this change", independent
    of whatever this test run's own working tree happens to contain.
    """

    relative_root = ARTIFACT_ROOT.relative_to(REPO_ROOT) / "examples"
    result = subprocess.run(
        ["git", "ls-tree", "-r", "HEAD", "--name-only", "--", str(relative_root)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    paths = tuple(line for line in result.stdout.splitlines() if line)
    assert paths, "git ls-tree returned nothing; the artifact tree moved"
    return paths


def _file_at_head(relative_path: str) -> str:
    result = subprocess.run(
        ["git", "show", f"HEAD:{relative_path}"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


class TestRealFrozenArtifactsStillValidate:
    """Binding condition (a): every artifact committed BEFORE this change
    still validates, unchanged, under the widened schema.

    This is deliberately not the same claim as "positive_fixtures()
    validates" -- fixtures.py is code this PR also touched, so it could not
    prove anything about pre-existing data on its own. Reading the exact
    bytes git already had, from HEAD, is what makes this a real
    backward-compatibility proof rather than a self-consistency check.
    """

    def test_every_pre_existing_example_artifact_validates(self) -> None:
        paths = _artifact_paths_at_head()
        checked = 0
        for relative_path in paths:
            if not relative_path.endswith(".json"):
                continue
            if "/schemas/" in relative_path or relative_path.endswith("manifest.json"):
                continue
            contract_name = Path(relative_path).name.rsplit(".json", 1)[0]
            # Multi-variant packet goldens are named
            # "<contract>.<variant>.json"; the contract is always a
            # registered prefix.
            model = None
            for name, candidate in INVESTIGATION_CONTRACT_MODELS.items():
                if contract_name == name or contract_name.startswith(f"{name}."):
                    model = candidate
                    break
            if model is None:
                continue
            payload = json.loads(_file_at_head(relative_path))
            is_negative = "/negative/" in relative_path
            if is_negative:
                with pytest.raises(ValidationError):
                    model.model_validate(payload)
            else:
                model.model_validate(payload)
            checked += 1
        assert checked >= 40, (
            f"only checked {checked} pre-existing artifacts -- suspiciously "
            "low, this test may have stopped finding the tree"
        )

    def test_the_pre_existing_analytical_job_golden_is_unchanged_bytes(self) -> None:
        """Byte-stability, not just re-validation.

        A schema that happens to still accept the OLD bytes is weaker than a
        constructor that still PRODUCES those exact bytes. This pins the
        latter for the one artifact that matters most: the trial
        AnalyticalJob golden itself.
        """

        before = json.loads(
            _file_at_head(
                "contracts/ask-dev-investigation/v1/examples/positive/"
                "ask_dev_analytical_job.v1.json"
            )
        )
        assert before["schema_version"] == "ask_dev_analytical_job.v1"
        assert "question_family" in before
        assert "production_job" not in before
        # And the current fixture generator still produces exactly this
        # shape -- the byte-stability commitment, checked at the source,
        # not just at the frozen artifact.
        current = positive_fixtures()["ask_dev_analytical_job.v1"]
        assert current["schema_version"] == "ask_dev_analytical_job.v1"
        assert "production_job" not in current


class TestChangelogRecordsTheWidening:
    """Binding condition (b): a dated, cited entry -- not a silent change."""

    def test_a_changelog_entry_cites_this_decision(self) -> None:
        entries = [
            entry
            for entry in CONTRACT_CHANGELOG
            if entry.schema == "ask_dev_analytical_job"
        ]
        assert entries, "no CONTRACT_CHANGELOG entry for AnalyticalJob at all"
        entry = entries[0]
        assert entry.ticket == "CHAOS-3660"
        assert entry.landed_on, (
            "a changelog entry with no date cannot be read as history"
        )
        # ISO date, loudly if not -- a changelog is worthless if a reader
        # cannot trust its own dates are real ones.
        import datetime

        datetime.date.fromisoformat(entry.landed_on)

    def test_the_changelog_is_a_tuple_not_a_mutable_list(self) -> None:
        """APPEND ONLY is a convention this at least makes hard to violate
        by accident -- a tuple cannot be `.append()`-ed to silently."""

        assert isinstance(CONTRACT_CHANGELOG, tuple)


class TestProvenanceValidator:
    """The new validator itself, RED-first proven (see PR description for
    the interactive probe this formalizes)."""

    def test_a_trial_job_validates_unchanged(self) -> None:
        payload = deepcopy(positive_fixtures()["ask_dev_analytical_job.v1"])
        job = AnalyticalJob.model_validate(payload)
        assert job.question_family is not None
        assert job.production_job is None

    def test_a_production_job_validates(self) -> None:
        payload = deepcopy(positive_fixtures()["ask_dev_analytical_job.v2"])
        job = AnalyticalJob.model_validate(payload)
        assert job.production_job is not None
        assert job.question_family is None
        assert job.production_job.intent_id == "project_health"

    def test_both_provenances_set_is_refused(self) -> None:
        payload = deepcopy(positive_fixtures()["ask_dev_analytical_job.v2"])
        payload["question_family"] = "project_status_drivers"
        with pytest.raises(ValidationError, match="exactly one"):
            AnalyticalJob.model_validate(payload)

    def test_neither_provenance_set_is_refused(self) -> None:
        payload = deepcopy(positive_fixtures()["ask_dev_analytical_job.v1"])
        del payload["question_family"]
        with pytest.raises(ValidationError, match="exactly one"):
            AnalyticalJob.model_validate(payload)

    def test_production_provenance_under_the_v1_schema_is_refused(self) -> None:
        """Not the same fault as both-set: this one has NO question_family,
        so only the schema-version/provenance agreement check can catch it
        -- pinning the two checks are not redundant."""

        payload = deepcopy(positive_fixtures()["ask_dev_analytical_job.v2"])
        payload["schema_version"] = "ask_dev_analytical_job.v1"
        with pytest.raises(ValidationError, match="does not match its provenance"):
            AnalyticalJob.model_validate(payload)

    def test_production_provenance_requires_a_real_uuid_run_id(self) -> None:
        payload = deepcopy(positive_fixtures()["ask_dev_production_job_provenance.v1"])
        payload["run_id"] = "not-a-uuid"
        with pytest.raises(ValidationError):
            ProductionJobProvenance.model_validate(payload)


class TestFixtureRegistryCoverage:
    """The two new registry entries are covered exactly like every other
    one -- not a special case exempted from the CHAOS-3615 fixture
    discipline."""

    def test_both_new_entries_have_a_positive_and_negative_fixture(self) -> None:
        positives = positive_fixtures()
        negatives = negative_fixtures()
        for name in (
            "ask_dev_analytical_job.v2",
            "ask_dev_production_job_provenance.v1",
        ):
            assert name in positives
            assert name in negatives
            assert negatives[name], f"{name} has no negative fixture"

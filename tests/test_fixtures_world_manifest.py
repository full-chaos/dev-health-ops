"""CHAOS-3219: shape + cross-reference guards for ask-dev-world.v1's
world.json/subjects.json/sources.json.

Pure-Python, no DB required -- runs in the standard unit tier
(``ci/run_tests.sh``'s ``-m "not benchmark and not clickhouse"`` selection).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dev_health_ops.fixtures.world import (
    WORLD_SCHEMA_VERSION,
    WorldManifest,
    WorldManifestError,
    collect_repo_roster,
    derive_id,
    derive_repo_seed,
    load_world_manifest,
    validate_world_manifest,
)

_WORLD_DIR = (
    Path(__file__).resolve().parents[1]
    / "tests"
    / "acceptance"
    / "world"
    / "ask-dev-world.v1"
)
_MANIFEST_PATH = _WORLD_DIR / "world.json"

_REQUIRED_SUBJECT_CLASSES = (
    "exact",
    "ambiguous",
    "acronym-alias",
    "no-match",
    "deleted",
    "stale-context",
    "partially-resolved",
    "bounded-set",
)
_REQUIRED_SOURCE_STATES = (
    "current",
    "stale",
    "unavailable",
    "unconfigured",
    "no-data",
    "unauthorized",
    "truncated",
    "conflicting",
    "not-applicable",
)


@pytest.fixture(scope="module")
def manifest() -> WorldManifest:
    return load_world_manifest(_MANIFEST_PATH)


def test_checked_in_manifest_loads_and_validates(manifest: WorldManifest) -> None:
    assert manifest.world["schema_version"] == WORLD_SCHEMA_VERSION
    assert manifest.subjects["schema_version"] == WORLD_SCHEMA_VERSION
    assert manifest.sources["schema_version"] == WORLD_SCHEMA_VERSION


def test_three_orgs_with_required_roles(manifest: WorldManifest) -> None:
    roles = {org["alias"]: org["role"] for org in manifest.orgs}
    assert roles == {
        "primary": "primary",
        "sibling": "sibling_tenant",
        "disabled": "disabled_entitlement",
    }


class TestSubjectClassCoverage:
    """A subject class with zero realizing rows must FAIL the guard."""

    @pytest.mark.parametrize("required_class", _REQUIRED_SUBJECT_CLASSES)
    def test_every_required_class_has_a_realizing_subject(
        self, manifest: WorldManifest, required_class: str
    ) -> None:
        present = {s["class"] for s in manifest.subjects["subjects"]}
        assert required_class in present, (
            f"subjects.json has zero rows realizing class={required_class!r}"
        )

    def test_missing_class_fails_validation(self, tmp_path: Path) -> None:
        world = json.loads((_WORLD_DIR / "world.json").read_text())
        subjects = json.loads((_WORLD_DIR / "subjects.json").read_text())
        sources = json.loads((_WORLD_DIR / "sources.json").read_text())

        # Drop every subject realizing the 'deleted' class -- the guard must
        # observe this and fail, not silently accept a thinned registry.
        subjects["subjects"] = [
            s for s in subjects["subjects"] if s["class"] != "deleted"
        ]

        (tmp_path / "world.json").write_text(json.dumps(world))
        (tmp_path / "subjects.json").write_text(json.dumps(subjects))
        (tmp_path / "sources.json").write_text(json.dumps(sources))

        with pytest.raises(WorldManifestError, match="deleted"):
            load_world_manifest(tmp_path / "world.json")


class TestSourceStateCoverage:
    @pytest.mark.parametrize("required_state", _REQUIRED_SOURCE_STATES)
    def test_every_required_state_has_a_realizing_row(
        self, manifest: WorldManifest, required_state: str
    ) -> None:
        present = {m["state"] for m in manifest.sources["matrix"]}
        assert required_state in present, (
            f"sources.json has zero rows realizing state={required_state!r}"
        )

    def test_missing_state_fails_validation(self, tmp_path: Path) -> None:
        world = json.loads((_WORLD_DIR / "world.json").read_text())
        subjects = json.loads((_WORLD_DIR / "subjects.json").read_text())
        sources = json.loads((_WORLD_DIR / "sources.json").read_text())

        sources["matrix"] = [m for m in sources["matrix"] if m["state"] != "stale"]

        (tmp_path / "world.json").write_text(json.dumps(world))
        (tmp_path / "subjects.json").write_text(json.dumps(subjects))
        (tmp_path / "sources.json").write_text(json.dumps(sources))

        with pytest.raises(WorldManifestError, match="stale"):
            load_world_manifest(tmp_path / "world.json")


def test_subject_org_alias_must_exist(tmp_path: Path) -> None:
    world = json.loads((_WORLD_DIR / "world.json").read_text())
    subjects = json.loads((_WORLD_DIR / "subjects.json").read_text())
    sources = json.loads((_WORLD_DIR / "sources.json").read_text())

    subjects["subjects"][0]["org_alias"] = "does-not-exist"

    (tmp_path / "world.json").write_text(json.dumps(world))
    (tmp_path / "subjects.json").write_text(json.dumps(subjects))
    (tmp_path / "sources.json").write_text(json.dumps(sources))

    with pytest.raises(WorldManifestError, match="does-not-exist"):
        load_world_manifest(tmp_path / "world.json")


def test_schema_version_mismatch_rejected(tmp_path: Path) -> None:
    world = json.loads((_WORLD_DIR / "world.json").read_text())
    subjects = json.loads((_WORLD_DIR / "subjects.json").read_text())
    sources = json.loads((_WORLD_DIR / "sources.json").read_text())
    world["schema_version"] = "ask_dev_world.v2"

    (tmp_path / "world.json").write_text(json.dumps(world))
    (tmp_path / "subjects.json").write_text(json.dumps(subjects))
    (tmp_path / "sources.json").write_text(json.dumps(sources))

    with pytest.raises(WorldManifestError, match="schema_version"):
        load_world_manifest(tmp_path / "world.json")


class TestRepoRoster:
    """subjects.json/sources.json content actually drives what the generator
    will seed -- collect_repo_roster is the exact function `fixtures world`
    iterates to decide what to generate, so testing it against the real
    checked-in manifest IS testing "subjects.json cross-checked against what
    the generator actually seeds"."""

    def test_every_named_repo_subject_is_in_the_roster(
        self, manifest: WorldManifest
    ) -> None:
        roster = collect_repo_roster(manifest)
        for subject in manifest.subjects["subjects"]:
            alias = subject.get("org_alias")
            repo = subject.get("repo_full_name")
            if (
                alias
                and repo
                and subject.get("entity_kind") in {"repository", "project"}
            ):
                assert repo in roster.get(alias, set()), (
                    f"subject {subject['id']!r} names repo_full_name={repo!r} "
                    f"but collect_repo_roster() would never generate it"
                )

    def test_ambiguous_candidates_are_in_the_roster(
        self, manifest: WorldManifest
    ) -> None:
        for subject in manifest.subjects["subjects"]:
            if subject["class"] != "ambiguous":
                continue
            roster = collect_repo_roster(manifest)
            for candidate in subject["candidates"]:
                assert candidate in roster[subject["org_alias"]]

    def test_no_match_subject_has_no_realizing_repo(
        self, manifest: WorldManifest
    ) -> None:
        roster = collect_repo_roster(manifest)
        all_repos = {repo for repos in roster.values() for repo in repos}
        no_match = next(
            s for s in manifest.subjects["subjects"] if s["class"] == "no-match"
        )
        for mention in no_match["mentions"]:
            assert mention not in all_repos

    def test_roster_disjoint_across_orgs(self, manifest: WorldManifest) -> None:
        roster = collect_repo_roster(manifest)
        seen: dict[str, str] = {}
        for alias, repos in roster.items():
            for repo in repos:
                assert repo not in seen, (
                    f"repo_full_name={repo!r} claimed by both org "
                    f"{seen.get(repo)!r} and {alias!r}"
                )
                seen[repo] = alias


class TestDeterminism:
    def test_derive_id_is_deterministic(self) -> None:
        a = derive_id(3219000, "ask-dev-world.v1:org:primary")
        b = derive_id(3219000, "ask-dev-world.v1:org:primary")
        assert a == b

    def test_derive_id_differs_by_seed_string(self) -> None:
        a = derive_id(3219000, "org:primary")
        b = derive_id(3219000, "org:sibling")
        assert a != b

    def test_derive_id_namespaced_by_master_seed(self) -> None:
        a = derive_id(1, "org:primary")
        b = derive_id(2, "org:primary")
        assert a != b, "a future world version's ids must not collide with v1's"

    def test_derive_repo_seed_deterministic(self) -> None:
        a = derive_repo_seed(3219000, "primary", "meridian/web-app")
        b = derive_repo_seed(3219000, "primary", "meridian/web-app")
        assert a == b

    def test_derive_repo_seed_differs_by_repo(self) -> None:
        a = derive_repo_seed(3219000, "primary", "meridian/web-app")
        b = derive_repo_seed(3219000, "primary", "meridian/atlas")
        assert a != b

    def test_manifest_org_id_matches_derive_id(self, manifest: WorldManifest) -> None:
        primary = manifest.org("primary")
        assert manifest.org_id("primary") == derive_id(
            manifest.master_seed, primary["id_seed"]
        )


def test_validate_world_manifest_rejects_zero_orgs() -> None:
    manifest = WorldManifest(
        manifest_path=_MANIFEST_PATH,
        world={"schema_version": WORLD_SCHEMA_VERSION, "orgs": []},
        subjects={"schema_version": WORLD_SCHEMA_VERSION, "subjects": []},
        sources={"schema_version": WORLD_SCHEMA_VERSION, "matrix": []},
    )
    with pytest.raises(WorldManifestError, match="zero orgs"):
        validate_world_manifest(manifest)

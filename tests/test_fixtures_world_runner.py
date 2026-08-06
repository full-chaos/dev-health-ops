"""CHAOS-3219: `dev-hops fixtures world` CLI wiring + multi-org mixed-org
guard extension.

Mirrors ``tests/test_fixtures_mixed_org_guard.py``'s style: argparse wiring
assertions plus a monkeypatched ``run_fixtures_generation`` to prove the
per-repo loop actually surfaces a guard refusal instead of silently
continuing past it.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from dev_health_ops.fixtures.runner import register_commands
from dev_health_ops.fixtures.world import (
    WorldManifest,
    _generate_world,
    _generation_namespace,
)

_WORLD_DIR = (
    Path(__file__).resolve().parents[1]
    / "tests"
    / "acceptance"
    / "world"
    / "ask-dev-world.v1"
)


def _world_namespace(**overrides) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    register_commands(parser.add_subparsers(dest="command"))
    args = [
        "fixtures",
        "world",
        "--manifest",
        str(_WORLD_DIR / "world.json"),
        "--sink",
        "clickhouse://stub:8123/stub",
    ]
    ns = parser.parse_args(args)
    for key, value in overrides.items():
        setattr(ns, key, value)
    return ns


def test_world_parser_requires_manifest() -> None:
    parser = argparse.ArgumentParser()
    register_commands(parser.add_subparsers(dest="command"))
    with pytest.raises(SystemExit):
        parser.parse_args(["fixtures", "world", "--sink", "clickhouse://x/y"])


def test_world_parser_defaults() -> None:
    ns = _world_namespace()
    assert ns.verify_digest is False
    assert ns.allow_mixed_org is False
    assert ns.postgres_uri is None
    assert ns.digest_path is None
    assert callable(ns.func)


def test_world_parser_verify_digest_flag() -> None:
    parser = argparse.ArgumentParser()
    register_commands(parser.add_subparsers(dest="command"))
    ns2 = parser.parse_args(
        [
            "fixtures",
            "world",
            "--manifest",
            str(_WORLD_DIR / "world.json"),
            "--verify-digest",
        ]
    )
    assert ns2.verify_digest is True


def test_world_func_is_a_coroutine_function() -> None:
    """cli.py dispatches via `inspect.iscoroutinefunction(func)` ->
    `asyncio.run(func(ns))`; a plain-def wrapper here would silently return
    an un-awaited coroutine and crash with `int(coroutine)` (a real defect
    caught during Lane 1a's own live verification run)."""

    import inspect

    ns = _world_namespace()
    assert inspect.iscoroutinefunction(ns.func)


class TestGenerationNamespace:
    def test_repo_count_one_preserves_exact_repo_name(self) -> None:
        import json
        import uuid

        world = json.loads((_WORLD_DIR / "world.json").read_text())
        subjects = json.loads((_WORLD_DIR / "subjects.json").read_text())
        sources = json.loads((_WORLD_DIR / "sources.json").read_text())
        manifest = WorldManifest(
            manifest_path=_WORLD_DIR / "world.json",
            world=world,
            subjects=subjects,
            sources=sources,
        )
        ns = _generation_namespace(
            manifest,
            org_alias="primary",
            org_id=uuid.uuid4(),
            repo_full_name="meridian/web-app",
            sink="clickhouse://stub/stub",
            postgres_uri="postgresql+asyncpg://stub/world_scratch",
            allow_mixed_org=False,
        )
        assert ns.repo_name == "meridian/web-app"
        assert ns.repo_count == 1

    def test_no_data_probe_repos_use_ordinary_generation_profile(self) -> None:
        """generate_commits/generate_work_items raise on a zero-volume
        profile (`random.randint(1, 0)`) -- the no-data/measured-zero probes
        must generate normally and be zeroed POST-hoc instead (see
        world.NO_DATA_PROBE_REPOS / _run_clickhouse_postprocess)."""
        import json
        import uuid

        world = json.loads((_WORLD_DIR / "world.json").read_text())
        subjects = json.loads((_WORLD_DIR / "subjects.json").read_text())
        sources = json.loads((_WORLD_DIR / "sources.json").read_text())
        manifest = WorldManifest(
            manifest_path=_WORLD_DIR / "world.json",
            world=world,
            subjects=subjects,
            sources=sources,
        )
        ns = _generation_namespace(
            manifest,
            org_alias="primary",
            org_id=uuid.uuid4(),
            repo_full_name="probe/source-no-data",
            sink="clickhouse://stub/stub",
            postgres_uri="postgresql+asyncpg://stub/world_scratch",
            allow_mixed_org=False,
        )
        assert ns.commits_per_day > 0
        assert ns.pr_count > 0

    def test_truncated_probe_gets_elevated_volume(self) -> None:
        import json
        import uuid

        world = json.loads((_WORLD_DIR / "world.json").read_text())
        subjects = json.loads((_WORLD_DIR / "subjects.json").read_text())
        sources = json.loads((_WORLD_DIR / "sources.json").read_text())
        manifest = WorldManifest(
            manifest_path=_WORLD_DIR / "world.json",
            world=world,
            subjects=subjects,
            sources=sources,
        )
        default_ns = _generation_namespace(
            manifest,
            org_alias="primary",
            org_id=uuid.uuid4(),
            repo_full_name="meridian/web-app",
            sink="clickhouse://stub/stub",
            postgres_uri="postgresql+asyncpg://stub/world_scratch",
            allow_mixed_org=False,
        )
        truncated_ns = _generation_namespace(
            manifest,
            org_alias="primary",
            org_id=uuid.uuid4(),
            repo_full_name="probe/source-truncated-workgraph",
            sink="clickhouse://stub/stub",
            postgres_uri="postgresql+asyncpg://stub/world_scratch",
            allow_mixed_org=False,
        )
        assert truncated_ns.pr_count > default_ns.pr_count
        assert truncated_ns.commits_per_day > default_ns.commits_per_day


@pytest.mark.asyncio
async def test_multi_org_world_stops_on_first_mixed_org_refusal(monkeypatch) -> None:
    """Extends the CHAOS-2778 mixed-org guard to the multi-org/multi-repo
    world loop: `run_fixtures_generation` already runs `_ensure_org_
    unpolluted` per call (unchanged, reused verbatim -- see world.py's
    docstring) -- this proves `_generate_world`'s per-repo loop actually
    SURFACES that refusal (stops and returns nonzero) instead of treating a
    guard failure as skippable and moving on to the next repo/org, which
    would silently under-generate the world while still exiting 0."""

    from dev_health_ops.fixtures import world as world_module

    manifest = world_module.load_world_manifest(_WORLD_DIR / "world.json")

    calls: list[str] = []

    async def _fake_run_fixtures_generation(ns: argparse.Namespace) -> int:
        calls.append(ns.repo_name)
        # Simulate MixedOrgError surfaced as exit code 1 by the SECOND repo
        # call (mirrors run_fixtures_generation's own MixedOrgError -> 1
        # contract in runner.py).
        if len(calls) == 2:
            return 1
        return 0

    async def _fake_postgres_phase(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(
        world_module, "run_fixtures_generation", _fake_run_fixtures_generation
    )
    monkeypatch.setattr(world_module, "_run_postgres_phase", _fake_postgres_phase)
    monkeypatch.setenv("POSTGRES_URI", "postgresql+asyncpg://stub/stub")

    ns = _world_namespace()
    rc = await _generate_world(ns, manifest)

    assert rc == 1
    assert len(calls) == 2, (
        "the loop must stop at the first refusal, not silently continue "
        "generating the remaining roster"
    )


# ---------------------------------------------------------------------------
# CHAOS-3463: the world's --postgres-uri must actually be where the world's
# auth rows land.
# ---------------------------------------------------------------------------


class TestWorldAuthSeedTargetsTheGuardedDatabase:
    """`fixtures world --postgres-uri X` validates X through
    `_require_scratch_database` and must then WRITE to X.

    Found live, not by reading: before this fix, `run_fixtures_generation`'s
    auth-seeding branch resolved its Postgres connection from
    `DATABASE_URI`/`POSTGRES_URI` alone, so a `fixtures world` run whose
    `--postgres-uri` named an approved scratch database still created the
    world's org/users/memberships/org_license in whatever database the
    environment happened to name. Reproduced inside the ask-dev acceptance
    stack (org `Meridian` + `admin@devhealth.example` appeared in the SERVING
    `postgres` database); from a direnv dev shell the same call writes into
    dev `devhealth`. The scratch guard was one hop away from the write.
    """

    def test_explicit_ns_uri_wins_over_the_environment(self, monkeypatch) -> None:
        from dev_health_ops.fixtures.runner import resolve_auth_seed_postgres_uri

        monkeypatch.setenv("POSTGRES_URI", "postgresql+asyncpg://env/devhealth")
        monkeypatch.setenv("DATABASE_URI", "postgresql+asyncpg://env/devhealth")
        ns = argparse.Namespace(
            postgres_uri="postgresql+asyncpg://guarded/world_scratch"
        )

        assert (
            resolve_auth_seed_postgres_uri(ns)
            == "postgresql+asyncpg://guarded/world_scratch"
        )

    def test_absent_attribute_still_falls_back_to_the_environment(
        self, monkeypatch
    ) -> None:
        """`fixtures generate` never sets `postgres_uri` -- its resolution
        must be byte-for-byte what it was before CHAOS-3463."""
        from dev_health_ops.fixtures.runner import resolve_auth_seed_postgres_uri

        monkeypatch.setenv("POSTGRES_URI", "postgresql+asyncpg://env/devhealth")
        ns = argparse.Namespace()

        assert (
            resolve_auth_seed_postgres_uri(ns) == "postgresql+asyncpg://env/devhealth"
        )

    def test_explicit_none_falls_back_to_the_environment(self, monkeypatch) -> None:
        """Clause-level, not condition-level: an attribute that EXISTS but is
        None must not shadow the environment (a truthiness bug here would
        silently disable auth seeding entirely rather than misroute it)."""
        from dev_health_ops.fixtures.runner import resolve_auth_seed_postgres_uri

        monkeypatch.setenv("POSTGRES_URI", "postgresql+asyncpg://env/devhealth")
        ns = argparse.Namespace(postgres_uri=None)

        assert (
            resolve_auth_seed_postgres_uri(ns) == "postgresql+asyncpg://env/devhealth"
        )

    def test_no_uri_anywhere_is_still_None(self, monkeypatch) -> None:
        from dev_health_ops.fixtures.runner import resolve_auth_seed_postgres_uri

        monkeypatch.delenv("POSTGRES_URI", raising=False)
        monkeypatch.delenv("DATABASE_URI", raising=False)
        monkeypatch.delenv("DATABASE_URL", raising=False)

        assert resolve_auth_seed_postgres_uri(argparse.Namespace()) is None

    @pytest.mark.asyncio
    async def test_generate_world_threads_its_guarded_uri_into_every_repo_call(
        self, monkeypatch, tmp_path: Path
    ) -> None:
        """The end the guard actually cares about: every per-repo namespace
        `_generate_world` hands to `run_fixtures_generation` carries the
        world's own `--postgres-uri`, so none of them can fall through to the
        environment."""
        from dev_health_ops.fixtures import world as world_module

        manifest = world_module.load_world_manifest(_WORLD_DIR / "world.json")
        seen: list[object] = []

        async def _fake_run_fixtures_generation(ns: argparse.Namespace) -> int:
            seen.append(getattr(ns, "postgres_uri", "<ATTRIBUTE ABSENT>"))
            return 0

        async def _noop(*args, **kwargs):
            return None

        monkeypatch.setattr(
            world_module, "run_fixtures_generation", _fake_run_fixtures_generation
        )
        monkeypatch.setattr(world_module, "_run_postgres_phase", _noop)
        monkeypatch.setattr(world_module, "_run_clickhouse_postprocess", _noop)
        monkeypatch.setattr(world_module, "_run_production_verification", _noop)

        async def _fake_digest(*args, **kwargs):
            return {"digest": "0" * 64, "components": {}}

        monkeypatch.setattr(world_module, "compute_world_digest", _fake_digest)
        monkeypatch.setenv(
            "POSTGRES_URI", "postgresql+asyncpg://env/devhealth_must_not_be_used"
        )

        guarded = "postgresql+asyncpg://guarded/ask_dev_world_scratch"
        # digest_path MUST point at tmp_path. `_generate_world` ends by WRITING
        # the digest, and with digest_path=None that write lands on the real,
        # committed tests/acceptance/world/ask-dev-world.v1/WORLD_DIGEST --
        # this test then replaces the pinned digest with the stub value
        # monkeypatched above, and every subsequent acceptance boot fails
        # because the restored world cannot match a 64-zero digest. Found by
        # Codex adversarial review and reproduced: running this module once was
        # enough to zero the committed pin.
        ns = _world_namespace(
            postgres_uri=guarded, digest_path=str(tmp_path / "WORLD_DIGEST")
        )
        rc = await _generate_world(ns, manifest)

        assert rc == 0
        assert seen, "the world roster generated no repos -- nothing was proven"
        assert set(seen) == {guarded}, (
            "every per-repo generation must seed auth data into the "
            f"guard-approved database, got {sorted(set(map(str, seen)))}"
        )


class TestGenerateOverwriteRealUsersFlag:
    """CHAOS-3463: `fixtures generate` needs an explicit opt-in to seed auth
    data after `world-restore` has populated orgs/users.

    Observed live before this existed: the world restore ran first, the
    CHAOS-2458 guard in `_seed_auth_data` refused ("non-empty auth database"),
    the known-credential superuser was never created, and
    prepare_ask_dev_acceptance.py failed its login with HTTP 401.
    """

    def _generate_namespace(self, *extra: str) -> argparse.Namespace:
        parser = argparse.ArgumentParser()
        register_commands(parser.add_subparsers(dest="command"))
        return parser.parse_args(["fixtures", "generate", *extra])

    def test_flag_is_off_by_default(self) -> None:
        """Every existing caller must keep the guard. A default-on flag would
        silently re-enable writing a known-password superuser into whatever
        database happens to be configured."""
        assert self._generate_namespace().overwrite_real_users is False

    def test_flag_can_be_turned_on(self) -> None:
        assert (
            self._generate_namespace("--overwrite-real-users").overwrite_real_users
            is True
        )

    def test_launcher_passes_it_only_alongside_the_world_restore(self) -> None:
        launcher = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "acceptance"
            / "run_ask_dev_compose.sh"
        ).read_text(encoding="utf-8")
        assert "--overwrite-real-users" in launcher
        # It is only justified because the restore ran first; if the restore
        # were ever removed, this opt-in would be an unexplained weakening.
        assert launcher.index("dev-hops fixtures world-restore") < launcher.index(
            "--overwrite-real-users"
        )


class TestOverwriteRealUsersIsAcceptanceGated:
    """Codex adversarial review (HIGH, confirmed): as a bare CLI flag,
    `--overwrite-real-users` was an unrestricted bypass of the CHAOS-2458
    credential guard -- anyone could point `fixtures generate` at any database
    and merge a known-password superuser into it. Help text saying "demo/CI
    only" is documentation, not a control."""

    def test_flag_refuses_outside_an_acceptance_environment(self, monkeypatch) -> None:
        from dev_health_ops.fixtures.world_snapshot import (
            RestoreRefusedError,
            _require_acceptance_environment,
        )

        monkeypatch.setenv("ENVIRONMENT", "development")
        with pytest.raises(RestoreRefusedError, match="ENVIRONMENT is"):
            _require_acceptance_environment()

    def test_flag_is_allowed_inside_the_acceptance_stack(self, monkeypatch) -> None:
        from dev_health_ops.fixtures.world_snapshot import (
            _require_acceptance_environment,
        )

        monkeypatch.setenv("ENVIRONMENT", "acceptance")
        _require_acceptance_environment()

    @pytest.mark.asyncio
    async def test_a_refused_run_writes_nothing_at_all(self, monkeypatch) -> None:
        """Round 2 of the review found the gate placed AFTER
        `store.insert_teams(...)`: a rejected command had already written
        fixture rows into ClickHouse. A refusal that leaves data behind is not
        a refusal. This proves zero store interaction, not merely a raised
        exception -- `create_store` is replaced by something that fails the
        test if it is ever called.
        """
        from dev_health_ops.fixtures import runner
        from dev_health_ops.fixtures.world_snapshot import RestoreRefusedError

        async def _no_store(*args, **kwargs):  # pragma: no cover -- must never run
            raise AssertionError(
                "a refused --overwrite-real-users run opened the store; the "
                "environment gate must come before any store work"
            )

        monkeypatch.setattr(runner, "run_with_store", _no_store)
        monkeypatch.setenv("ENVIRONMENT", "development")

        ns = argparse.Namespace(
            overwrite_real_users=True,
            sink="clickhouse://stub:8123/stub",
            db_type=None,
        )
        with pytest.raises(RestoreRefusedError):
            await runner.run_fixtures_generation(ns)

    def test_generation_consults_the_gate_only_when_the_flag_is_set(self) -> None:
        """The gate must not fire for the default path -- every existing
        `fixtures generate` caller runs outside an acceptance container and
        must keep working untouched."""
        import inspect

        from dev_health_ops.fixtures import runner

        source = inspect.getsource(runner.run_fixtures_generation)
        gate_index = source.index("_require_acceptance_environment()")
        flag_index = source.index('getattr(ns, "overwrite_real_users", False)')
        store_index = source.index("run_with_store(")
        assert flag_index < gate_index, (
            "the acceptance gate must sit inside the flag check -- outside it, "
            "every ordinary `fixtures generate` run would refuse"
        )
        assert gate_index < store_index, (
            "the gate must run BEFORE the store is opened, so a refusal writes "
            "nothing (Codex round 2)"
        )


# ---------------------------------------------------------------------------
# CHAOS-3463 credential contract
# ---------------------------------------------------------------------------


class TestPasswordForAlias:
    """The single derivation both sides of the contract go through.

    Before this, every world.json user had `password_hash=None`, so no world
    principal could authenticate and the corpus's cross-tenant/entitlement
    cases had no way to be anyone but the superuser (CHAOS-3462 runner lane).
    """

    def test_is_deterministic(self) -> None:
        from dev_health_ops.fixtures.world import password_for_alias

        assert password_for_alias("primary.ordinary") == password_for_alias(
            "primary.ordinary"
        )

    def test_differs_per_alias(self) -> None:
        from dev_health_ops.fixtures.world import password_for_alias

        aliases = [
            "primary.ordinary",
            "sibling.ordinary",
            "primary.degraded-readiness-user",
            "primary.unsupported-model-user",
        ]
        assert len({password_for_alias(a) for a in aliases}) == len(aliases)

    def test_meets_the_api_minimum_length(self) -> None:
        from dev_health_ops.api.services.users import PASSWORD_MIN_LENGTH
        from dev_health_ops.fixtures.world import password_for_alias

        assert len(password_for_alias("primary.ordinary")) >= PASSWORD_MIN_LENGTH

    def test_empty_alias_is_refused(self) -> None:
        from dev_health_ops.fixtures.world import password_for_alias

        with pytest.raises(ValueError):
            password_for_alias("")

    def test_no_literal_password_is_committed_anywhere(self) -> None:
        """The snapshot artifact is committed and gitleaks scans it. The
        derivation must produce the credential at RUNTIME -- a literal in the
        tree would be a finding, and a finding that is genuinely a credential
        for accounts that exist in a running stack.
        """
        from dev_health_ops.fixtures.world import password_for_alias

        secret = password_for_alias("primary.ordinary")
        root = Path(__file__).resolve().parents[1]
        for path in (
            root / "src" / "dev_health_ops" / "fixtures" / "world.py",
            root / "tests" / "acceptance" / "world" / "ask-dev-world.v1" / "world.json",
            root / "scripts" / "acceptance" / "assert_world_principals_can_log_in.py",
        ):
            assert secret not in path.read_text(encoding="utf-8"), (
                f"{path} contains a literal world credential -- it must only ever "
                "be derived at runtime"
            )


class TestWorldUsersAreSeededWithWorkingCredentials:
    def test_every_world_user_gets_a_hash_that_verifies(self) -> None:
        """Round-trips through the API's OWN verifier, so this proves the
        seeded hash is one `/api/v1/auth/login` accepts -- not merely that the
        column is non-NULL."""
        from dev_health_ops.api.services.users import _verify_password
        from dev_health_ops.fixtures.world import (
            _build_auth_fixture,
            load_world_manifest,
            password_for_alias,
        )

        manifest = load_world_manifest(_WORLD_DIR / "world.json")
        fixture = _build_auth_fixture(manifest)
        by_email = {u.email: u for u in fixture["users"]}

        assert by_email, "the world seeded no users at all"
        for user in manifest.world["users"]:
            seeded = by_email[user["email"]]
            assert seeded.password_hash, f"{user['alias']} has no password_hash"
            assert _verify_password(
                password_for_alias(user["alias"]), seeded.password_hash
            ), f"{user['alias']}'s seeded hash does not verify its own password"

    def test_a_wrong_password_does_not_verify(self) -> None:
        """Without this, a verifier that returned True unconditionally would
        satisfy the test above."""
        from dev_health_ops.api.services.users import _verify_password
        from dev_health_ops.fixtures.world import (
            _build_auth_fixture,
            load_world_manifest,
            password_for_alias,
        )

        manifest = load_world_manifest(_WORLD_DIR / "world.json")
        fixture = _build_auth_fixture(manifest)
        seeded = {u.email: u for u in fixture["users"]}
        user = manifest.world["users"][0]
        assert not _verify_password(
            password_for_alias(user["alias"]) + "-wrong",
            seeded[user["email"]].password_hash,
        )

    def test_the_four_corpus_contract_aliases_exist_and_are_seeded(self) -> None:
        """The contract minimum, locked in the unit tier so a world edit that
        drops or renames one fails here rather than at 2am in an evidence run.
        """
        from dev_health_ops.api.services.users import _verify_password
        from dev_health_ops.fixtures.world import (
            CORPUS_CONTRACT_USER_ALIASES,
            _build_auth_fixture,
            load_world_manifest,
            password_for_alias,
        )

        manifest = load_world_manifest(_WORLD_DIR / "world.json")
        by_alias = {u["alias"]: u for u in manifest.world["users"]}
        seeded = {u.email: u for u in _build_auth_fixture(manifest)["users"]}

        assert set(CORPUS_CONTRACT_USER_ALIASES) == {
            "primary.ordinary",
            "sibling.ordinary",
            "primary.degraded-readiness-user",
            "primary.unsupported-model-user",
        }
        for alias in CORPUS_CONTRACT_USER_ALIASES:
            assert alias in by_alias, f"world.json no longer defines {alias}"
            user = by_alias[alias]
            assert _verify_password(
                password_for_alias(alias), seeded[user["email"]].password_hash
            )
            # The org binding the runner hard-fails on.
            assert manifest.org_id(user["org_alias"]), (
                f"{alias} references org_alias {user['org_alias']!r} that "
                "world.json does not define"
            )

    def test_password_hash_stays_inside_the_digest_surface(self) -> None:
        """Binding ruling: the hash is FROZEN by the snapshot and restored
        identically every boot, so keeping it digested is stable AND makes
        credential tampering visible as drift. If a future change excluded it
        as 'volatile', tampering would become invisible."""
        from dev_health_ops.fixtures.world import _volatile_columns_for_table

        assert "password_hash" not in _volatile_columns_for_table("users")

    def test_tampering_with_password_hash_actually_moves_the_digest(self) -> None:
        """The assertion above is structural -- it proves the column is not on
        an exclusion list, not that the digest MOVES when the column changes.
        A future refactor could satisfy it while dropping the column upstream
        of the hash (a narrowed SELECT, a row projection) and credential
        tampering would again be invisible, with the exclusion-set test still
        green. This drives the real row-hashing function and plants the defect.
        """
        from dev_health_ops.fixtures.world import (
            _row_content_key,
            _volatile_columns_for_table,
        )

        volatile = _volatile_columns_for_table("users")
        columns = ["id", "email", "password_hash", "is_active"]
        honest = ("u-1", "ordinary@ask-dev-world-primary.example", "$2b$12$aaa", True)
        tampered = ("u-1", "ordinary@ask-dev-world-primary.example", "$2b$12$bbb", True)

        assert _row_content_key(columns, honest, volatile=volatile) != _row_content_key(
            columns, tampered, volatile=volatile
        ), (
            "swapping password_hash left the hashed row key identical -- the "
            "column is not reaching the digest, so a swapped credential would "
            "restore and verify clean"
        )

        # Negative control: a genuinely EXCLUDED column must not move the key.
        # Without it, the assertion above would also pass for a function that
        # hashes every column handed to it and proves nothing about this one.
        excluded = sorted(volatile)[0]
        control_columns = [*columns, excluded]
        assert _row_content_key(
            control_columns, (*honest, "a"), volatile=volatile
        ) == _row_content_key(control_columns, (*honest, "b"), volatile=volatile), (
            f"{excluded} is on the exclusion list but still moved the row key"
        )

    def test_logging_in_does_not_move_the_users_digest(self) -> None:
        """Regression, found LIVE by the two-boot re-proof (CHAOS-3463).

        `users.last_login_at` is stamped with wall-clock `datetime.now()` on
        every successful authentication. The moment the launcher started
        proving world principals can log in on EVERY boot, those logins mutated
        a digested column: both boots failed `require_world_digest_match` on
        `postgres.users`, each with a different live digest, because each boot
        authenticates at its own instant. No amount of re-minting can stabilise
        a pin that is a function of whether anyone has logged in yet.

        Two rows differing ONLY in `last_login_at` -- the exact shape of
        before-login vs after-login -- must hash identically.
        """
        from dev_health_ops.fixtures.world import (
            _row_content_key,
            _volatile_columns_for_table,
        )

        volatile = _volatile_columns_for_table("users")
        columns = ["id", "email", "password_hash", "last_login_at"]
        before = ("u-1", "ordinary@ask-dev-world-primary.example", "$2b$12$aaa", None)
        after = (
            "u-1",
            "ordinary@ask-dev-world-primary.example",
            "$2b$12$aaa",
            "2026-08-06T19:09:41.795170+00:00",
        )

        assert _row_content_key(columns, before, volatile=volatile) == _row_content_key(
            columns, after, volatile=volatile
        ), (
            "a login moved the users digest -- every acceptance boot would fail "
            "require_world_digest_match on postgres.users, nondeterministically"
        )

    def test_excluding_last_login_at_did_not_also_drop_the_credential(self) -> None:
        """The fix above is an exclusion, and exclusions are how digests go
        quietly blind. Pins the scope: what was dropped is WHEN someone logged
        in, never the credential. Two rows differing only in `password_hash`
        must still hash differently even with `last_login_at` present.
        """
        from dev_health_ops.fixtures.world import (
            _row_content_key,
            _volatile_columns_for_table,
        )

        volatile = _volatile_columns_for_table("users")
        assert "last_login_at" in volatile
        assert "password_hash" not in volatile

        columns = ["id", "password_hash", "last_login_at"]
        stamp = "2026-08-06T19:09:41.795170+00:00"
        assert _row_content_key(
            columns, ("u-1", "$2b$12$aaa", stamp), volatile=volatile
        ) != _row_content_key(
            columns, ("u-1", "$2b$12$SWAPPED", stamp), volatile=volatile
        ), "the last_login_at exclusion also blinded the digest to password_hash"


class TestBootLoginProofFitsTheAuthIpBudget:
    """CHAOS-3490 / Codex P1: the per-boot login proof must leave room for the
    rest of the boot.

    ``AUTH_LOGIN_IP_LIMIT`` is "20/15minutes" keyed by IP
    (``api/middleware/rate_limit.py``). A full acceptance boot
    (``run_ask_dev_compose.sh``) spends that budget on more than this check:
    ``prepare_ask_dev_acceptance.py`` logs in, seven smoke scripts each log in,
    and the Playwright leg logs in again. Proving all ten contract aliases per
    boot pushed the total past 20 and the boot died on a 429 -- and the
    exit-run boot script could not catch it, because it stops before the smoke
    and web legs.

    This pins the arithmetic so the next person to grow the pool sees the
    budget rather than discovering it in a live boot.
    """

    #: api/middleware/rate_limit.py AUTH_LOGIN_IP_LIMIT = "20/15minutes".
    AUTH_LOGIN_IP_LIMIT_PER_15_MIN = 20

    #: Logins a full run_ask_dev_compose.sh boot performs BESIDES this proof:
    #: prepare_ask_dev_acceptance.py (superuser + multi-org provisioning),
    #: seven smoke scripts, and the Playwright backend login.
    OTHER_BOOT_LOGINS = 10

    def test_boot_subset_plus_the_rest_of_the_boot_fits(self) -> None:
        from dev_health_ops.fixtures.world import BOOT_LOGIN_PROOF_ALIASES

        # One login per alias, plus exactly one wrong-password negative control.
        proof_cost = len(BOOT_LOGIN_PROOF_ALIASES) + 1
        assert (
            proof_cost + self.OTHER_BOOT_LOGINS <= self.AUTH_LOGIN_IP_LIMIT_PER_15_MIN
        ), (
            f"the per-boot login proof costs {proof_cost} logins and the rest of "
            f"a full acceptance boot costs about {self.OTHER_BOOT_LOGINS}, which "
            f"exceeds AUTH_LOGIN_IP_LIMIT ({self.AUTH_LOGIN_IP_LIMIT_PER_15_MIN}"
            "/15min, per IP). The boot will 429 partway through. Shrink "
            "BOOT_LOGIN_PROOF_ALIASES rather than raising this number."
        )

    def test_boot_subset_is_a_subset_of_the_contract_aliases(self) -> None:
        from dev_health_ops.fixtures.world import (
            BOOT_LOGIN_PROOF_ALIASES,
            CORPUS_CONTRACT_USER_ALIASES,
        )

        assert set(BOOT_LOGIN_PROOF_ALIASES) <= set(CORPUS_CONTRACT_USER_ALIASES), (
            "the boot subset must be drawn from the contract aliases, or it "
            "proves a principal the corpus never binds to"
        )

    def test_boot_subset_still_covers_both_orgs_and_both_provider_profiles(
        self,
    ) -> None:
        """A cheaper subset must not become a vacuous one. The point of the
        four is coverage of DISTINCT shapes -- a second org, and both
        provider-profile users -- not merely a smaller number."""

        from dev_health_ops.fixtures.world import BOOT_LOGIN_PROOF_ALIASES

        assert "sibling.ordinary" in BOOT_LOGIN_PROOF_ALIASES, "no second org"
        assert "primary.degraded-readiness-user" in BOOT_LOGIN_PROOF_ALIASES
        assert "primary.unsupported-model-user" in BOOT_LOGIN_PROOF_ALIASES
        assert "primary.ordinary" in BOOT_LOGIN_PROOF_ALIASES

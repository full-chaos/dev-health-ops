"""Invariant tests for sync_teams routing (CHAOS-2401).

Pins the two routing paths introduced by CHAOS-2401:

Org-scoped path (``--org <id>`` present):
    provider → _bridge_teams_to_postgres → bridge_teams_to_clickhouse
    - run_with_store is NOT called (no direct ClickHouse write from provider data)
    - _bridge_teams_to_postgres IS called and its count gates the exit code
    - bridge_teams_to_clickhouse IS called with the correct org_id
    - bridge_teams_to_clickhouse failure is fatal after PostgreSQL succeeds

No-org path (``--org`` absent):
    provider → run_with_store (direct ClickHouse write)
    - _bridge_teams_to_postgres is NOT called
    - bridge_teams_to_clickhouse is NOT called
    - run_with_store IS called with org_id=None

Membership invariants (bridge_teams_to_clickhouse):
    - Reads TeamMapping + IdentityMapping from Postgres (org-scoped)
    - Emits identity-resolved members (email, canonical_id, provider logins)
    - Does not read from provider-supplied teams_data directly
"""

from __future__ import annotations

import argparse
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ns(
    *,
    provider: str = "config",
    org: str | None = None,
    allow_empty: bool = False,
    path: str | None = None,
) -> argparse.Namespace:
    ns = argparse.Namespace()
    ns.provider = provider
    ns.org = org
    ns.allow_empty = allow_empty
    ns.path = path
    ns.db = "sqlite+aiosqlite:///:memory:"
    ns.sink = "clickhouse"
    ns.analytics_db = "clickhouse://example.test:8123/default"
    ns.owner = None
    ns.auth = None
    return ns


# ---------------------------------------------------------------------------
# Org-scoped routing invariants
# ---------------------------------------------------------------------------


class TestOrgScopedRouting:
    """Org-scoped sync must go through Postgres-first path."""

    def test_org_scoped_does_not_call_run_with_store(self, tmp_path: Any) -> None:
        """run_with_store must NOT be called for org-scoped syncs.

        The org-scoped path is: _bridge_teams_to_postgres → bridge_teams_to_clickhouse.
        run_with_store (direct ClickHouse write from provider data) must be bypassed.
        """
        import yaml

        from dev_health_ops.providers.teams import sync_teams

        config_file = tmp_path / "teams.yaml"
        config_file.write_text(
            yaml.dump({"teams": [{"team_id": "team-a", "team_name": "Team A"}]})
        )

        run_with_store_called = []

        async def _spy_run_with_store(
            _db_uri: str, _db_type: str, handler: Any, org_id: Any = None
        ) -> int:
            run_with_store_called.append(org_id)
            return 1

        with (
            patch(
                "dev_health_ops.providers.teams._bridge_teams_to_postgres",
                return_value=1,
            ),
            patch(
                "dev_health_ops.providers.team_bridge.bridge_teams_to_clickhouse",
                return_value=1,
            ),
            patch(
                "dev_health_ops.storage.run_with_store",
                side_effect=_spy_run_with_store,
            ),
        ):
            ns = _make_ns(org="org-1", path=str(config_file))
            result = sync_teams(ns)

        assert result == 0
        assert run_with_store_called == [], (
            "run_with_store must not be called on the org-scoped path"
        )

    def test_org_scoped_calls_bridge_to_postgres(self, tmp_path: Any) -> None:
        """_bridge_teams_to_postgres must be called for org-scoped syncs."""
        import yaml

        from dev_health_ops.providers.teams import sync_teams

        config_file = tmp_path / "teams.yaml"
        config_file.write_text(
            yaml.dump({"teams": [{"team_id": "team-a", "team_name": "Team A"}]})
        )

        pg_calls: list[Any] = []

        def _spy_pg(teams_data: list, ns: Any) -> int:
            pg_calls.append((teams_data, ns))
            return len(teams_data)

        with (
            patch(
                "dev_health_ops.providers.teams._bridge_teams_to_postgres",
                side_effect=_spy_pg,
            ),
            patch(
                "dev_health_ops.providers.team_bridge.bridge_teams_to_clickhouse",
                return_value=1,
            ),
        ):
            ns = _make_ns(org="org-1", path=str(config_file))
            result = sync_teams(ns)

        assert result == 0
        assert len(pg_calls) == 1, "_bridge_teams_to_postgres must be called once"

    def test_org_scoped_calls_bridge_to_clickhouse_with_org_id_and_db_url(
        self, tmp_path: Any
    ) -> None:
        """bridge_teams_to_clickhouse must receive org_id and requested DB URL."""
        import yaml

        from dev_health_ops.providers.teams import sync_teams

        config_file = tmp_path / "teams.yaml"
        config_file.write_text(
            yaml.dump({"teams": [{"team_id": "team-a", "team_name": "Team A"}]})
        )

        captured_calls: list[tuple[str | None, str | None, str | None]] = []

        def _spy_ch(
            org_id: str | None = None,
            db_url: str | None = None,
            postgres_db_url: str | None = None,
        ) -> int:
            captured_calls.append((org_id, db_url, postgres_db_url))
            return 1

        with (
            patch(
                "dev_health_ops.providers.teams._bridge_teams_to_postgres",
                return_value=1,
            ),
            patch(
                "dev_health_ops.providers.team_bridge.bridge_teams_to_clickhouse",
                side_effect=_spy_ch,
            ),
        ):
            ns = _make_ns(org="org-42", path=str(config_file))
            result = sync_teams(ns)

        assert result == 0
        assert captured_calls == [
            (
                "org-42",
                "clickhouse://example.test:8123/default",
                "sqlite+aiosqlite:///:memory:",
            )
        ], "bridge_teams_to_clickhouse must receive the org_id and requested db URL"

    def test_org_scoped_postgres_bridge_failure_exits_one(self, tmp_path: Any) -> None:
        """If _bridge_teams_to_postgres raises, exit code must be 1."""
        import yaml

        from dev_health_ops.providers.teams import sync_teams

        config_file = tmp_path / "teams.yaml"
        config_file.write_text(
            yaml.dump({"teams": [{"team_id": "team-a", "team_name": "Team A"}]})
        )

        with patch(
            "dev_health_ops.providers.teams._bridge_teams_to_postgres",
            side_effect=RuntimeError("pg down"),
        ):
            ns = _make_ns(org="org-1", path=str(config_file))
            result = sync_teams(ns)

        assert result == 1

    def test_org_scoped_postgres_bridge_zero_count_exits_one(
        self, tmp_path: Any
    ) -> None:
        """If _bridge_teams_to_postgres returns 0, exit code must be 1."""
        import yaml

        from dev_health_ops.providers.teams import sync_teams

        config_file = tmp_path / "teams.yaml"
        config_file.write_text(
            yaml.dump({"teams": [{"team_id": "team-a", "team_name": "Team A"}]})
        )

        with patch(
            "dev_health_ops.providers.teams._bridge_teams_to_postgres",
            return_value=0,
        ):
            ns = _make_ns(org="org-1", path=str(config_file))
            result = sync_teams(ns)

        assert result == 1

    def test_org_scoped_clickhouse_bridge_failure_exits_one(
        self, tmp_path: Any
    ) -> None:
        import yaml

        from dev_health_ops.providers.teams import sync_teams

        config_file = tmp_path / "teams.yaml"
        config_file.write_text(
            yaml.dump({"teams": [{"team_id": "team-a", "team_name": "Team A"}]})
        )

        with (
            patch(
                "dev_health_ops.providers.teams._bridge_teams_to_postgres",
                return_value=1,
            ),
            patch(
                "dev_health_ops.providers.team_bridge.bridge_teams_to_clickhouse",
                side_effect=RuntimeError("ch down"),
            ),
        ):
            ns = _make_ns(org="org-1", path=str(config_file))
            result = sync_teams(ns)

        assert result == 1

    def test_org_scoped_postgres_bridge_receives_provider_teams(
        self, tmp_path: Any
    ) -> None:
        """_bridge_teams_to_postgres must receive the provider-built teams list."""
        import yaml

        from dev_health_ops.providers.teams import sync_teams

        config_file = tmp_path / "teams.yaml"
        config_file.write_text(
            yaml.dump(
                {
                    "teams": [
                        {"team_id": "eng", "team_name": "Engineering"},
                        {"team_id": "ops", "team_name": "Operations"},
                    ]
                }
            )
        )

        captured: list[Any] = []

        def _spy_pg(teams_data: list, ns: Any) -> int:
            captured.extend(teams_data)
            return len(teams_data)

        with (
            patch(
                "dev_health_ops.providers.teams._bridge_teams_to_postgres",
                side_effect=_spy_pg,
            ),
            patch(
                "dev_health_ops.providers.team_bridge.bridge_teams_to_clickhouse",
                return_value=2,
            ),
        ):
            ns = _make_ns(org="org-1", path=str(config_file))
            sync_teams(ns)

        team_ids = {getattr(t, "id", None) for t in captured}
        assert "eng" in team_ids
        assert "ops" in team_ids


# ---------------------------------------------------------------------------
# No-org routing invariants
# ---------------------------------------------------------------------------


class TestNoOrgRouting:
    """No-org sync must write directly to ClickHouse via run_with_store."""

    def test_no_org_does_not_call_postgres_bridge(self, tmp_path: Any) -> None:
        """_bridge_teams_to_postgres must NOT be called when org is absent."""
        import yaml

        from dev_health_ops.providers.teams import sync_teams

        config_file = tmp_path / "teams.yaml"
        config_file.write_text(
            yaml.dump({"teams": [{"team_id": "team-a", "team_name": "Team A"}]})
        )

        async def _fake_run_with_store(
            _db_uri: str, _db_type: str, handler: Any, org_id: Any = None
        ) -> int:
            class _FakeStore:
                async def ensure_tables(self) -> None:
                    pass

                async def insert_teams(self, teams: list) -> None:
                    pass

                async def get_all_teams(self) -> list:
                    return [SimpleNamespace(id="team-a", org_id=None)]

            return await handler(_FakeStore())

        with (
            patch(
                "dev_health_ops.providers.teams._bridge_teams_to_postgres"
            ) as mock_pg,
            patch(
                "dev_health_ops.providers.team_bridge.bridge_teams_to_clickhouse"
            ) as mock_ch,
            patch(
                "dev_health_ops.storage.run_with_store",
                side_effect=_fake_run_with_store,
            ),
            patch("dev_health_ops.providers.teams.validate_sink"),
            patch(
                "dev_health_ops.providers.teams.resolve_sink_uri",
                return_value="clickhouse://localhost:8123/default",
            ),
            patch(
                "dev_health_ops.providers.teams.detect_db_type",
                return_value="clickhouse",
            ),
        ):
            ns = _make_ns(org=None, path=str(config_file))
            result = sync_teams(ns)

        assert result == 0
        mock_pg.assert_not_called()
        mock_ch.assert_not_called()

    def test_no_org_run_with_store_receives_org_id_none(self, tmp_path: Any) -> None:
        """run_with_store must be called with org_id=None on the no-org path."""
        import yaml

        from dev_health_ops.providers.teams import sync_teams

        config_file = tmp_path / "teams.yaml"
        config_file.write_text(
            yaml.dump({"teams": [{"team_id": "team-a", "team_name": "Team A"}]})
        )

        captured_org_ids: list[Any] = []

        async def _spy_run_with_store(
            _db_uri: str, _db_type: str, handler: Any, org_id: Any = None
        ) -> int:
            captured_org_ids.append(org_id)

            class _FakeStore:
                async def ensure_tables(self) -> None:
                    pass

                async def insert_teams(self, teams: list) -> None:
                    pass

                async def get_all_teams(self) -> list:
                    return [SimpleNamespace(id="team-a", org_id=None)]

            return await handler(_FakeStore())

        with (
            patch(
                "dev_health_ops.storage.run_with_store",
                side_effect=_spy_run_with_store,
            ),
            patch("dev_health_ops.providers.teams.validate_sink"),
            patch(
                "dev_health_ops.providers.teams.resolve_sink_uri",
                return_value="clickhouse://localhost:8123/default",
            ),
            patch(
                "dev_health_ops.providers.teams.detect_db_type",
                return_value="clickhouse",
            ),
            patch("dev_health_ops.providers.teams._bridge_teams_to_postgres"),
        ):
            ns = _make_ns(org=None, path=str(config_file))
            sync_teams(ns)

        assert captured_org_ids == [None], (
            "run_with_store must be called with org_id=None on the no-org path"
        )


# ---------------------------------------------------------------------------
# bridge_teams_to_clickhouse membership invariants
# ---------------------------------------------------------------------------


class TestBridgeTeamsToClickhouseMembership:
    """bridge_teams_to_clickhouse must read from Postgres and resolve identities."""

    def _make_team_mapping(
        self,
        team_id: str = "eng",
        name: str = "Engineering",
        org_id: str = "org-1",
        is_active: bool = True,
        project_keys: Any = None,
        repo_patterns: Any = None,
        description: str | None = None,
        updated_at: Any = None,
    ) -> SimpleNamespace:
        from datetime import datetime, timezone

        return SimpleNamespace(
            team_id=team_id,
            name=name,
            org_id=org_id,
            is_active=is_active,
            project_keys=project_keys,
            repo_patterns=repo_patterns,
            description=description,
            updated_at=updated_at or datetime.now(timezone.utc),
        )

    def _make_identity_mapping(
        self,
        email: str | None = None,
        canonical_id: str | None = None,
        display_name: str | None = None,
        provider_identities: dict | None = None,
        team_ids: list | None = None,
        is_active: bool = True,
        org_id: str = "org-1",
    ) -> SimpleNamespace:
        return SimpleNamespace(
            email=email,
            canonical_id=canonical_id,
            display_name=display_name,
            provider_identities=provider_identities or {},
            team_ids=team_ids or [],
            is_active=is_active,
            org_id=org_id,
        )

    def _make_fake_session(
        self,
        team_mappings: list,
        identity_mappings: list,
    ) -> Any:
        """Build a fake synchronous Postgres session context manager."""
        call_count = [0]

        class _FakeResult:
            def __init__(self, rows: list) -> None:
                self._rows = rows

            def scalars(self) -> _FakeScalars:
                return _FakeScalars(self._rows)

        class _FakeScalars:
            def __init__(self, rows: list) -> None:
                self._rows = rows

            def all(self) -> list:
                return self._rows

        class _FakeSession:
            def __enter__(self) -> _FakeSession:
                return self

            def __exit__(self, *args: Any) -> None:
                pass

            def execute(self, stmt: Any) -> _FakeResult:
                call_count[0] += 1
                if call_count[0] == 1:
                    return _FakeResult(team_mappings)
                return _FakeResult(identity_mappings)

        return _FakeSession()

    def _make_fake_ch_store(self) -> tuple[Any, list]:
        """Return (mock ClickHouseStore class, captured_payloads list)."""
        captured: list[Any] = []

        class _FakeStore:
            async def __aenter__(self) -> _FakeStore:
                return self

            async def __aexit__(self, *args: Any) -> None:
                pass

            async def insert_teams(self, payload: list) -> None:
                captured.extend(payload)

        class _FakeStoreClass:
            def __init__(self, uri: str) -> None:
                pass

            def __new__(cls, uri: str) -> _FakeStore:  # type: ignore[misc]
                return _FakeStore()

        return _FakeStoreClass, captured

    def test_bridge_calls_clickhouse_insert(self) -> None:
        """bridge_teams_to_clickhouse must call ClickHouseStore.insert_teams."""
        from dev_health_ops.providers.team_bridge import bridge_teams_to_clickhouse

        team_mapping = self._make_team_mapping(team_id="eng", org_id="org-1")
        fake_session = self._make_fake_session([team_mapping], [])
        fake_store_cls, captured = self._make_fake_ch_store()

        with (
            patch(
                "dev_health_ops.providers.team_bridge.get_postgres_session_sync",
                return_value=fake_session,
            ),
            patch(
                "dev_health_ops.providers.team_bridge.ClickHouseStore",
                fake_store_cls,
            ),
            patch(
                "dev_health_ops.providers.team_bridge._clickhouse_uri",
                return_value="clickhouse://localhost:8123/default",
            ),
        ):
            count = bridge_teams_to_clickhouse(org_id="org-1")

        assert count == 1
        assert len(captured) == 1
        assert captured[0]["id"] == "eng"
        assert captured[0]["org_id"] == "org-1"

    def test_bridge_uses_requested_postgres_db_url(self) -> None:
        from dev_health_ops.providers.team_bridge import bridge_teams_to_clickhouse

        fake_session = self._make_fake_session([], [])
        fake_store_cls, _captured = self._make_fake_ch_store()

        with (
            patch(
                "dev_health_ops.providers.team_bridge.get_postgres_session_sync_for_uri",
                return_value=fake_session,
            ) as mock_session_for_uri,
            patch(
                "dev_health_ops.providers.team_bridge.ClickHouseStore",
                fake_store_cls,
            ),
            patch(
                "dev_health_ops.providers.team_bridge._clickhouse_uri",
                return_value="clickhouse://localhost:8123/default",
            ),
        ):
            bridge_teams_to_clickhouse(
                org_id="org-1",
                postgres_db_url="sqlite:///semantic.db",
            )

        mock_session_for_uri.assert_called_once_with("sqlite:///semantic.db")

    def test_bridge_requires_clickhouse_uri_not_database_uri(
        self, monkeypatch: Any
    ) -> None:
        from dev_health_ops.providers.team_bridge import _clickhouse_uri

        monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
        monkeypatch.setenv("DATABASE_URI", "postgresql://semantic.example/devhealth")

        try:
            _clickhouse_uri()
        except RuntimeError as exc:
            assert "CLICKHOUSE_URI" in str(exc)
        else:
            raise AssertionError("_clickhouse_uri must not use DATABASE_URI fallback")

    def test_sync_teams_to_analytics_passes_clickhouse_uri(
        self, monkeypatch: Any
    ) -> None:
        from dev_health_ops.workers.product_tasks import sync_teams_to_analytics

        captured: dict[str, str | None] = {}

        def _bridge(org_id: str | None = None, db_url: str | None = None) -> int:
            captured["org_id"] = org_id
            captured["db_url"] = db_url
            return 3

        monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://analytics.example/default")
        monkeypatch.setenv("DATABASE_URI", "postgresql://semantic.example/devhealth")

        with patch(
            "dev_health_ops.providers.team_bridge.bridge_teams_to_clickhouse",
            side_effect=_bridge,
        ):
            result = getattr(sync_teams_to_analytics, "run")(org_id="org-1")

        assert result == {"status": "success", "teams_synced": 3}
        assert captured == {
            "org_id": "org-1",
            "db_url": "clickhouse://analytics.example/default",
        }

    def test_bridge_emits_identity_resolved_members(self) -> None:
        """bridge_teams_to_clickhouse must include email + provider logins in members."""
        from dev_health_ops.providers.team_bridge import bridge_teams_to_clickhouse

        team_mapping = self._make_team_mapping(team_id="eng", org_id="org-1")
        identity = self._make_identity_mapping(
            email="alice@example.com",
            canonical_id="alice@example.com",
            provider_identities={"github": ["alice-gh"], "linear": ["alice-lin"]},
            team_ids=["eng"],
            org_id="org-1",
        )
        fake_session = self._make_fake_session([team_mapping], [identity])
        fake_store_cls, captured = self._make_fake_ch_store()

        with (
            patch(
                "dev_health_ops.providers.team_bridge.get_postgres_session_sync",
                return_value=fake_session,
            ),
            patch(
                "dev_health_ops.providers.team_bridge.ClickHouseStore",
                fake_store_cls,
            ),
            patch(
                "dev_health_ops.providers.team_bridge._clickhouse_uri",
                return_value="clickhouse://localhost:8123/default",
            ),
        ):
            bridge_teams_to_clickhouse(org_id="org-1")

        assert len(captured) == 1
        members = captured[0]["members"]
        assert "alice@example.com" in members, "email must be in members"
        assert "alice-gh" in members, "github login must be in members"
        assert "alice-lin" in members, "linear login must be in members"

    def test_bridge_org_id_scopes_output(self) -> None:
        """bridge_teams_to_clickhouse output rows must carry the correct org_id."""
        from dev_health_ops.providers.team_bridge import bridge_teams_to_clickhouse

        team_mapping = self._make_team_mapping(team_id="eng", org_id="org-99")
        fake_session = self._make_fake_session([team_mapping], [])
        fake_store_cls, captured = self._make_fake_ch_store()

        with (
            patch(
                "dev_health_ops.providers.team_bridge.get_postgres_session_sync",
                return_value=fake_session,
            ),
            patch(
                "dev_health_ops.providers.team_bridge.ClickHouseStore",
                fake_store_cls,
            ),
            patch(
                "dev_health_ops.providers.team_bridge._clickhouse_uri",
                return_value="clickhouse://localhost:8123/default",
            ),
        ):
            bridge_teams_to_clickhouse(org_id="org-99")

        assert captured[0]["org_id"] == "org-99"

    def test_bridge_empty_team_mapping_returns_zero(self) -> None:
        """bridge_teams_to_clickhouse returns 0 when no active TeamMappings exist."""
        from dev_health_ops.providers.team_bridge import bridge_teams_to_clickhouse

        fake_session = self._make_fake_session([], [])
        fake_store_cls, captured = self._make_fake_ch_store()

        with (
            patch(
                "dev_health_ops.providers.team_bridge.get_postgres_session_sync",
                return_value=fake_session,
            ),
            patch(
                "dev_health_ops.providers.team_bridge.ClickHouseStore",
                fake_store_cls,
            ),
            patch(
                "dev_health_ops.providers.team_bridge._clickhouse_uri",
                return_value="clickhouse://localhost:8123/default",
            ),
        ):
            count = bridge_teams_to_clickhouse(org_id="org-1")

        assert count == 0
        assert captured == [], "insert_teams must be called with empty list"

    def test_bridge_member_without_email_uses_display_name(self) -> None:
        """Identity with no email falls back to display_name in members."""
        from dev_health_ops.providers.team_bridge import bridge_teams_to_clickhouse

        team_mapping = self._make_team_mapping(team_id="eng", org_id="org-1")
        identity = self._make_identity_mapping(
            email=None,
            display_name="Dave",
            team_ids=["eng"],
            org_id="org-1",
        )
        fake_session = self._make_fake_session([team_mapping], [identity])
        fake_store_cls, captured = self._make_fake_ch_store()

        with (
            patch(
                "dev_health_ops.providers.team_bridge.get_postgres_session_sync",
                return_value=fake_session,
            ),
            patch(
                "dev_health_ops.providers.team_bridge.ClickHouseStore",
                fake_store_cls,
            ),
            patch(
                "dev_health_ops.providers.team_bridge._clickhouse_uri",
                return_value="clickhouse://localhost:8123/default",
            ),
        ):
            bridge_teams_to_clickhouse(org_id="org-1")

        assert "Dave" in captured[0]["members"]

    def test_bridge_identity_with_email_does_not_use_display_name(self) -> None:
        """Identity with email must NOT include display_name in members."""
        from dev_health_ops.providers.team_bridge import bridge_teams_to_clickhouse

        team_mapping = self._make_team_mapping(team_id="eng", org_id="org-1")
        identity = self._make_identity_mapping(
            email="carol@example.com",
            display_name="Carol",
            team_ids=["eng"],
            org_id="org-1",
        )
        fake_session = self._make_fake_session([team_mapping], [identity])
        fake_store_cls, captured = self._make_fake_ch_store()

        with (
            patch(
                "dev_health_ops.providers.team_bridge.get_postgres_session_sync",
                return_value=fake_session,
            ),
            patch(
                "dev_health_ops.providers.team_bridge.ClickHouseStore",
                fake_store_cls,
            ),
            patch(
                "dev_health_ops.providers.team_bridge._clickhouse_uri",
                return_value="clickhouse://localhost:8123/default",
            ),
        ):
            bridge_teams_to_clickhouse(org_id="org-1")

        members = captured[0]["members"]
        assert "carol@example.com" in members
        assert "Carol" not in members, "display_name must not appear when email is set"

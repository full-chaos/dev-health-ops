"""Tests for Microsoft Teams connector."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dev_health_ops.connectors.teams import (
    MicrosoftGraphClient,
    TeamsChannel,
    TeamsConnector,
    TeamsMessage,
    TeamsTeam,
    TeamsUser,
    _parse_datetime,
    _parse_retry_after,
)


class TestParseDatetime:
    def test_parse_iso_with_z_suffix(self):
        result = _parse_datetime("2024-01-15T10:30:00Z")
        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 10
        assert result.minute == 30

    def test_parse_iso_with_offset(self):
        result = _parse_datetime("2024-01-15T10:30:00+00:00")
        assert result is not None
        assert result.tzinfo is not None

    def test_parse_none(self):
        assert _parse_datetime(None) is None

    def test_parse_invalid(self):
        assert _parse_datetime("not-a-date") is None


class TestParseRetryAfter:
    def test_parse_numeric_header(self):
        response = MagicMock()
        response.headers = {"Retry-After": "30"}
        result = _parse_retry_after(response)
        assert result == 30.0

    def test_parse_missing_header(self):
        response = MagicMock()
        response.headers = {}
        result = _parse_retry_after(response)
        assert result is None

    def test_parse_minimum_value(self):
        response = MagicMock()
        response.headers = {"Retry-After": "0.5"}
        result = _parse_retry_after(response)
        assert result == 1.0


class TestMicrosoftGraphClient:
    @pytest.fixture
    def client(self):
        return MicrosoftGraphClient(
            tenant_id="test-tenant",
            client_id="test-client",
            client_secret="test-secret",
            timeout=30,
        )

    @pytest.mark.asyncio
    async def test_ensure_token_requests_new_token(self, client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "test-token-123",
            "expires_in": 3600,
        }

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_http_client = AsyncMock()
            mock_http_client.post = AsyncMock(return_value=mock_response)
            mock_client_class.return_value = mock_http_client

            token = await client._ensure_token()

            assert token == "test-token-123"
            mock_http_client.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_ensure_token_reuses_valid_token(self, client):
        client._access_token = "existing-token"
        client._token_expires_at = datetime.now(timezone.utc).replace(
            year=datetime.now(timezone.utc).year + 1
        )

        token = await client._ensure_token()

        assert token == "existing-token"

    @pytest.mark.asyncio
    async def test_close_closes_client(self, client):
        mock_http_client = AsyncMock()
        client._client = mock_http_client

        await client.close()

        mock_http_client.aclose.assert_called_once()
        assert client._client is None


class TestTeamsConnector:
    @pytest.fixture
    def connector(self):
        return TeamsConnector(
            tenant_id="test-tenant",
            client_id="test-client",
            client_secret="test-secret",
        )

    @pytest.fixture
    def mock_graph_client(self):
        with patch.object(MicrosoftGraphClient, "get_paginated") as mock_paginated:
            with patch.object(MicrosoftGraphClient, "get") as mock_get:
                with patch.object(MicrosoftGraphClient, "_ensure_token") as mock_token:
                    mock_token.return_value = "test-token"
                    yield mock_paginated, mock_get

    @pytest.mark.asyncio
    async def test_list_teams(self, connector, mock_graph_client):
        mock_paginated, _ = mock_graph_client
        mock_paginated.return_value = [
            {
                "id": "team-1",
                "displayName": "Engineering",
                "description": "Engineering team",
                "visibility": "private",
                "createdDateTime": "2024-01-01T00:00:00Z",
                "webUrl": "https://teams.microsoft.com/team-1",
            },
            {
                "id": "team-2",
                "displayName": "Marketing",
                "description": None,
                "visibility": "public",
            },
        ]

        teams = await connector.list_teams()

        assert len(teams) == 2
        assert teams[0].id == "team-1"
        assert teams[0].display_name == "Engineering"
        assert teams[0].visibility == "private"
        assert teams[1].id == "team-2"
        assert teams[1].display_name == "Marketing"

    @pytest.mark.asyncio
    async def test_list_teams_with_max(self, connector, mock_graph_client):
        mock_paginated, _ = mock_graph_client
        mock_paginated.return_value = [
            {"id": "team-1", "displayName": "Team 1"},
        ]

        await connector.list_teams(max_teams=1)

        mock_paginated.assert_called_once()
        call_kwargs = mock_paginated.call_args[1]
        assert call_kwargs["max_items"] == 1

    @pytest.mark.asyncio
    async def test_get_team(self, connector, mock_graph_client):
        _, mock_get = mock_graph_client
        mock_get.return_value = {
            "id": "team-1",
            "displayName": "Engineering",
            "description": "Engineering team",
            "visibility": "private",
            "createdDateTime": "2024-01-01T00:00:00Z",
        }

        team = await connector.get_team("team-1")

        assert team.id == "team-1"
        assert team.display_name == "Engineering"
        mock_get.assert_called_once()

    @pytest.mark.asyncio
    async def test_list_channels(self, connector, mock_graph_client):
        mock_paginated, _ = mock_graph_client
        mock_paginated.return_value = [
            {
                "id": "channel-1",
                "displayName": "General",
                "description": "General channel",
                "membershipType": "standard",
            },
            {
                "id": "channel-2",
                "displayName": "Private",
                "membershipType": "private",
            },
        ]

        channels = await connector.list_channels("team-1")

        assert len(channels) == 2
        assert channels[0].id == "channel-1"
        assert channels[0].display_name == "General"
        assert channels[0].membership_type == "standard"
        assert channels[1].membership_type == "private"

    @pytest.mark.asyncio
    async def test_list_channel_messages(self, connector, mock_graph_client):
        mock_paginated, _ = mock_graph_client
        mock_paginated.return_value = [
            {
                "id": "msg-1",
                "createdDateTime": "2024-01-15T10:00:00Z",
                "subject": "Hello",
                "body": {"content": "Hello world", "contentType": "text"},
                "from": {
                    "user": {
                        "id": "user-1",
                        "displayName": "John Doe",
                        "email": "john@example.com",
                    }
                },
                "importance": "normal",
                "messageType": "message",
            },
        ]

        messages = await connector.list_channel_messages("team-1", "channel-1")

        assert len(messages) == 1
        assert messages[0].id == "msg-1"
        assert messages[0].subject == "Hello"
        assert messages[0].body_content == "Hello world"
        assert messages[0].from_user is not None
        assert messages[0].from_user.display_name == "John Doe"

    @pytest.mark.asyncio
    async def test_list_team_members(self, connector, mock_graph_client):
        mock_paginated, _ = mock_graph_client
        mock_paginated.return_value = [
            {
                "userId": "user-1",
                "displayName": "John Doe",
                "email": "john@example.com",
            },
            {
                "id": "user-2",
                "displayName": "Jane Smith",
            },
        ]

        members = await connector.list_team_members("team-1")

        assert len(members) == 2
        assert members[0].id == "user-1"
        assert members[0].display_name == "John Doe"
        assert members[1].id == "user-2"

    @pytest.mark.asyncio
    async def test_get_team_with_details(self, connector, mock_graph_client):
        mock_paginated, mock_get = mock_graph_client

        mock_get.return_value = {
            "id": "team-1",
            "displayName": "Engineering",
        }

        mock_paginated.side_effect = [
            [{"id": "channel-1", "displayName": "General"}],
            [{"userId": "user-1", "displayName": "John"}],
        ]

        team = await connector.get_team_with_details(
            "team-1", include_channels=True, include_members=True
        )

        assert team.id == "team-1"
        assert len(team.channels) == 1
        assert len(team.members) == 1

    @pytest.mark.asyncio
    async def test_get_channel_activity_stats(self, connector, mock_graph_client):
        mock_paginated, _ = mock_graph_client
        mock_paginated.return_value = [
            {
                "id": "msg-1",
                "from": {"user": {"id": "user-1", "displayName": "John"}},
            },
            {
                "id": "msg-2",
                "from": {"user": {"id": "user-1", "displayName": "John"}},
            },
            {
                "id": "msg-3",
                "from": {"user": {"id": "user-2", "displayName": "Jane"}},
            },
        ]

        stats = await connector.get_channel_activity_stats("team-1", "channel-1")

        assert stats["message_count"] == 3
        assert stats["unique_authors"] == 2

    @pytest.mark.asyncio
    async def test_context_manager(self, connector):
        with patch.object(connector, "close", new_callable=AsyncMock) as mock_close:
            async with connector as c:
                assert c is connector
            mock_close.assert_called_once()


class TestTeamsConnectorFromEnv:
    def test_from_env_success(self):
        with patch.dict(
            "os.environ",
            {
                "AZURE_TENANT_ID": "test-tenant",
                "AZURE_CLIENT_ID": "test-client",
                "AZURE_CLIENT_SECRET": "test-secret",
            },
        ):
            connector = TeamsConnector.from_env()
            assert connector.client.tenant_id == "test-tenant"
            assert connector.client.client_id == "test-client"

    def test_from_env_missing_vars(self):
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(ValueError, match="Missing required environment"):
                TeamsConnector.from_env()

    def test_from_env_with_optional_vars(self):
        with patch.dict(
            "os.environ",
            {
                "AZURE_TENANT_ID": "test-tenant",
                "AZURE_CLIENT_ID": "test-client",
                "AZURE_CLIENT_SECRET": "test-secret",
                "TEAMS_API_TIMEOUT": "60",
                "TEAMS_USE_BETA_API": "true",
            },
        ):
            connector = TeamsConnector.from_env()
            assert connector.client.timeout == 60
            assert "beta" in connector.client.base_url


class TestTeamsDataModels:
    def test_teams_user_creation(self):
        user = TeamsUser(
            id="user-1",
            display_name="John Doe",
            email="john@example.com",
        )
        assert user.id == "user-1"
        assert user.display_name == "John Doe"

    def test_teams_channel_defaults(self):
        channel = TeamsChannel(id="ch-1", display_name="General")
        assert channel.membership_type == "standard"
        assert channel.description is None

    def test_teams_message_defaults(self):
        message = TeamsMessage(id="msg-1")
        assert message.body_content_type == "text"
        assert message.importance == "normal"
        assert message.message_type == "message"
        assert message.reply_count == 0

    def test_teams_team_with_channels_and_members(self):
        team = TeamsTeam(
            id="team-1",
            display_name="Engineering",
            channels=[TeamsChannel(id="ch-1", display_name="General")],
            members=[TeamsUser(id="user-1", display_name="John")],
        )
        assert len(team.channels) == 1
        assert len(team.members) == 1

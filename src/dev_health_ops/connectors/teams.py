"""
Microsoft Teams connector using Microsoft Graph API.

This connector provides methods to retrieve teams, channels, messages,
and meeting information from Microsoft Teams via the Graph API.

Authentication uses OAuth 2.0 client credentials flow (application permissions).

Required Azure AD App Permissions (Application):
- Team.ReadBasic.All - Read team names and descriptions
- Channel.ReadBasic.All - Read channel names and descriptions
- ChannelMessage.Read.All - Read channel messages (requires admin consent)
- OnlineMeetings.Read.All - Read online meeting details (optional)
- User.Read.All - Read user profiles for member resolution

See: https://learn.microsoft.com/en-us/graph/permissions-reference
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

from dev_health_ops.connectors.exceptions import (
    APIException,
    AuthenticationException,
    RateLimitException,
)
from dev_health_ops.connectors.utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)

# Microsoft Graph API base URL
GRAPH_API_BASE = "https://graph.microsoft.com/v1.0"
GRAPH_API_BETA = "https://graph.microsoft.com/beta"

# OAuth 2.0 token endpoint
OAUTH_TOKEN_URL = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"


@dataclass
class TeamsUser:
    """Represents a Microsoft Teams user."""

    id: str
    display_name: str
    email: Optional[str] = None
    user_principal_name: Optional[str] = None


@dataclass
class TeamsChannel:
    """Represents a Microsoft Teams channel."""

    id: str
    display_name: str
    description: Optional[str] = None
    membership_type: str = "standard"  # standard, private, shared
    created_datetime: Optional[datetime] = None
    web_url: Optional[str] = None


@dataclass
class TeamsMessage:
    """Represents a message in a Teams channel."""

    id: str
    created_datetime: Optional[datetime] = None
    last_modified_datetime: Optional[datetime] = None
    subject: Optional[str] = None
    body_content: Optional[str] = None
    body_content_type: str = "text"  # text, html
    from_user: Optional[TeamsUser] = None
    importance: str = "normal"  # low, normal, high, urgent
    message_type: str = "message"  # message, chatEventMessage, etc.
    web_url: Optional[str] = None
    reply_count: int = 0


@dataclass
class TeamsTeam:
    """Represents a Microsoft Teams team."""

    id: str
    display_name: str
    description: Optional[str] = None
    visibility: str = "private"  # private, public
    created_datetime: Optional[datetime] = None
    web_url: Optional[str] = None
    channels: List[TeamsChannel] = field(default_factory=list)
    members: List[TeamsUser] = field(default_factory=list)


@dataclass
class TeamsMeeting:
    """Represents a Microsoft Teams online meeting."""

    id: str
    subject: Optional[str] = None
    start_datetime: Optional[datetime] = None
    end_datetime: Optional[datetime] = None
    join_web_url: Optional[str] = None
    organizer: Optional[TeamsUser] = None
    participants_count: int = 0


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parse ISO 8601 datetime string from Graph API."""
    if not value:
        return None
    try:
        # Handle both Z suffix and +00:00 format
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _parse_retry_after(response: httpx.Response) -> Optional[float]:
    """Parse Retry-After header from response."""
    retry_after = response.headers.get("Retry-After")
    if not retry_after:
        return None
    try:
        return max(1.0, float(retry_after))
    except (ValueError, TypeError):
        return None


class MicrosoftGraphClient:
    """
    Async HTTP client for Microsoft Graph API with OAuth 2.0 authentication.

    Uses client credentials flow for application-level access.
    """

    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        timeout: int = 30,
        use_beta: bool = False,
    ):
        """
        Initialize Microsoft Graph client.

        :param tenant_id: Azure AD tenant ID.
        :param client_id: Azure AD application (client) ID.
        :param client_secret: Azure AD client secret.
        :param timeout: Request timeout in seconds.
        :param use_beta: Use beta API endpoint for additional features.
        """
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.timeout = timeout
        self.base_url = GRAPH_API_BETA if use_beta else GRAPH_API_BASE

        self._access_token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        return self._client

    async def _ensure_token(self) -> str:
        """Ensure we have a valid access token, refreshing if needed."""
        now = datetime.now(timezone.utc)

        # Check if token is still valid (with 5 minute buffer)
        if (
            self._access_token
            and self._token_expires_at
            and self._token_expires_at > now
        ):
            return self._access_token

        # Request new token
        token_url = OAUTH_TOKEN_URL.format(tenant_id=self.tenant_id)
        client = await self._get_client()

        try:
            response = await client.post(
                token_url,
                data={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "scope": "https://graph.microsoft.com/.default",
                    "grant_type": "client_credentials",
                },
            )

            if response.status_code == 401:
                raise AuthenticationException(
                    "Failed to authenticate with Azure AD. Check client credentials."
                )
            elif response.status_code != 200:
                raise APIException(
                    f"Token request failed: {response.status_code} - {response.text}"
                )

            data = response.json()
            self._access_token = data["access_token"]
            expires_in = int(data.get("expires_in", 3600))
            from datetime import timedelta

            self._token_expires_at = now + timedelta(seconds=expires_in - 300)

            logger.debug("Obtained new access token, expires in %d seconds", expires_in)
            token = self._access_token
            assert token is not None
            return token

        except httpx.RequestError as e:
            raise APIException(f"Token request failed: {e}") from e

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Make an authenticated request to the Graph API.

        :param method: HTTP method (GET, POST, etc.).
        :param endpoint: API endpoint (relative to base URL).
        :param params: Optional query parameters.
        :return: Response JSON data.
        """
        token = await self._ensure_token()
        client = await self._get_client()
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        try:
            response = await client.request(
                method,
                url,
                params=params,
                headers=headers,
            )

            if response.status_code == 401:
                # Token might have expired, clear and retry once
                self._access_token = None
                self._token_expires_at = None
                token = await self._ensure_token()
                headers["Authorization"] = f"Bearer {token}"
                response = await client.request(
                    method,
                    url,
                    params=params,
                    headers=headers,
                )

            if response.status_code == 401:
                raise AuthenticationException(
                    "Authentication failed after token refresh"
                )
            elif response.status_code == 403:
                raise APIException(
                    f"Forbidden: Insufficient permissions for {endpoint}. "
                    "Check Azure AD app permissions."
                )
            elif response.status_code == 404:
                raise APIException(f"Not found: {endpoint}")
            elif response.status_code == 429:
                raise RateLimitException(
                    "Microsoft Graph rate limit exceeded",
                    retry_after_seconds=_parse_retry_after(response),
                )
            elif response.status_code >= 400:
                raise APIException(
                    f"API error: {response.status_code} - {response.text}"
                )

            return response.json()

        except httpx.RequestError as e:
            raise APIException(f"Request failed: {e}") from e

    @retry_with_backoff(
        max_retries=5,
        initial_delay=1.0,
        max_delay=60.0,
        exceptions=(RateLimitException, APIException),
    )
    async def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make a GET request to the Graph API."""
        return await self._request("GET", endpoint, params)

    async def get_paginated(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        max_items: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Make a paginated GET request, following @odata.nextLink.

        :param endpoint: API endpoint.
        :param params: Optional query parameters.
        :param max_items: Maximum number of items to retrieve.
        :return: List of all items across pages.
        """
        items: List[Dict[str, Any]] = []
        next_link: Optional[str] = None

        # Initial request
        data = await self.get(endpoint, params)
        items.extend(data.get("value", []))

        next_link = data.get("@odata.nextLink")

        # Follow pagination
        while next_link and (max_items is None or len(items) < max_items):
            # nextLink is a full URL, extract the path
            if next_link.startswith(self.base_url):
                next_endpoint = next_link[len(self.base_url) :]
            else:
                # Use full URL directly
                client = await self._get_client()
                token = await self._ensure_token()
                response = await client.get(
                    next_link,
                    headers={"Authorization": f"Bearer {token}"},
                )
                if response.status_code != 200:
                    break
                data = response.json()
                items.extend(data.get("value", []))
                next_link = data.get("@odata.nextLink")
                continue

            data = await self.get(next_endpoint)
            items.extend(data.get("value", []))
            next_link = data.get("@odata.nextLink")

        if max_items:
            return items[:max_items]
        return items

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None


class TeamsConnector:
    """
    Microsoft Teams connector using Microsoft Graph API.

    Provides methods to retrieve teams, channels, messages, and meetings
    with automatic pagination, rate limiting, and error handling.
    """

    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        timeout: int = 30,
        use_beta: bool = False,
    ):
        """
        Initialize Teams connector.

        :param tenant_id: Azure AD tenant ID.
        :param client_id: Azure AD application (client) ID.
        :param client_secret: Azure AD client secret.
        :param timeout: Request timeout in seconds.
        :param use_beta: Use beta API for additional features (e.g., meetings).
        """
        self.client = MicrosoftGraphClient(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            timeout=timeout,
            use_beta=use_beta,
        )

    @classmethod
    def from_env(cls) -> "TeamsConnector":
        """
        Create a TeamsConnector from environment variables.

        Required environment variables:
        - AZURE_TENANT_ID: Azure AD tenant ID
        - AZURE_CLIENT_ID: Azure AD application (client) ID
        - AZURE_CLIENT_SECRET: Azure AD client secret

        Optional:
        - TEAMS_API_TIMEOUT: Request timeout in seconds (default: 30)
        - TEAMS_USE_BETA_API: Use beta API (default: false)
        """
        import os

        tenant_id: str | None = os.environ.get("AZURE_TENANT_ID")
        client_id: str | None = os.environ.get("AZURE_CLIENT_ID")
        client_secret: str | None = os.environ.get("AZURE_CLIENT_SECRET")

        if not tenant_id or not client_id or not client_secret:
            raise ValueError(
                "Missing required environment variables: "
                "AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET"
            )

        timeout = int(os.environ.get("TEAMS_API_TIMEOUT", "30"))
        use_beta = os.environ.get("TEAMS_USE_BETA_API", "").lower() in (
            "true",
            "1",
            "yes",
        )

        return cls(
            tenant_id=str(tenant_id),
            client_id=str(client_id),
            client_secret=str(client_secret),
            timeout=timeout,
            use_beta=use_beta,
        )

    async def list_teams(
        self,
        max_teams: Optional[int] = None,
    ) -> List[TeamsTeam]:
        """
        List all teams accessible to the application.

        :param max_teams: Maximum number of teams to retrieve.
        :return: List of TeamsTeam objects.
        """
        logger.info("Fetching teams from Microsoft Graph API")

        teams_data = await self.client.get_paginated(
            "teams",
            params={
                "$select": "id,displayName,description,visibility,createdDateTime,webUrl"
            },
            max_items=max_teams,
        )

        teams = []
        for team_data in teams_data:
            team = TeamsTeam(
                id=team_data.get("id", ""),
                display_name=team_data.get("displayName", ""),
                description=team_data.get("description"),
                visibility=team_data.get("visibility", "private"),
                created_datetime=_parse_datetime(team_data.get("createdDateTime")),
                web_url=team_data.get("webUrl"),
            )
            teams.append(team)
            logger.debug("Retrieved team: %s", team.display_name)

        logger.info("Retrieved %d teams", len(teams))
        return teams

    async def get_team(self, team_id: str) -> TeamsTeam:
        """
        Get a specific team by ID.

        :param team_id: Team ID.
        :return: TeamsTeam object.
        """
        data = await self.client.get(
            f"teams/{team_id}",
            params={
                "$select": "id,displayName,description,visibility,createdDateTime,webUrl"
            },
        )

        return TeamsTeam(
            id=data.get("id", ""),
            display_name=data.get("displayName", ""),
            description=data.get("description"),
            visibility=data.get("visibility", "private"),
            created_datetime=_parse_datetime(data.get("createdDateTime")),
            web_url=data.get("webUrl"),
        )

    async def list_channels(
        self,
        team_id: str,
        max_channels: Optional[int] = None,
    ) -> List[TeamsChannel]:
        """
        List all channels in a team.

        :param team_id: Team ID.
        :param max_channels: Maximum number of channels to retrieve.
        :return: List of TeamsChannel objects.
        """
        logger.debug("Fetching channels for team %s", team_id)

        channels_data = await self.client.get_paginated(
            f"teams/{team_id}/channels",
            params={
                "$select": "id,displayName,description,membershipType,createdDateTime,webUrl"
            },
            max_items=max_channels,
        )

        channels = []
        for channel_data in channels_data:
            channel = TeamsChannel(
                id=channel_data.get("id", ""),
                display_name=channel_data.get("displayName", ""),
                description=channel_data.get("description"),
                membership_type=channel_data.get("membershipType", "standard"),
                created_datetime=_parse_datetime(channel_data.get("createdDateTime")),
                web_url=channel_data.get("webUrl"),
            )
            channels.append(channel)

        logger.debug("Retrieved %d channels for team %s", len(channels), team_id)
        return channels

    async def list_channel_messages(
        self,
        team_id: str,
        channel_id: str,
        max_messages: Optional[int] = None,
        since: Optional[datetime] = None,
    ) -> List[TeamsMessage]:
        """
        List messages in a channel.

        Note: Requires ChannelMessage.Read.All permission with admin consent.

        :param team_id: Team ID.
        :param channel_id: Channel ID.
        :param max_messages: Maximum number of messages to retrieve.
        :param since: Only retrieve messages after this datetime.
        :return: List of TeamsMessage objects.
        """
        logger.debug("Fetching messages for team %s, channel %s", team_id, channel_id)

        params: Dict[str, Any] = {
            "$select": "id,createdDateTime,lastModifiedDateTime,subject,body,from,importance,messageType,webUrl",
            "$orderby": "createdDateTime desc",
        }

        if since:
            params["$filter"] = f"createdDateTime ge {since.isoformat()}"

        messages_data = await self.client.get_paginated(
            f"teams/{team_id}/channels/{channel_id}/messages",
            params=params,
            max_items=max_messages,
        )

        messages = []
        for msg_data in messages_data:
            from_data = msg_data.get("from", {})
            from_user = None
            if from_data and from_data.get("user"):
                user_data = from_data["user"]
                from_user = TeamsUser(
                    id=user_data.get("id", ""),
                    display_name=user_data.get("displayName", ""),
                    email=user_data.get("email"),
                    user_principal_name=user_data.get("userPrincipalName"),
                )

            body = msg_data.get("body", {})
            message = TeamsMessage(
                id=msg_data.get("id", ""),
                created_datetime=_parse_datetime(msg_data.get("createdDateTime")),
                last_modified_datetime=_parse_datetime(
                    msg_data.get("lastModifiedDateTime")
                ),
                subject=msg_data.get("subject"),
                body_content=body.get("content"),
                body_content_type=body.get("contentType", "text"),
                from_user=from_user,
                importance=msg_data.get("importance", "normal"),
                message_type=msg_data.get("messageType", "message"),
                web_url=msg_data.get("webUrl"),
            )
            messages.append(message)

        logger.debug(
            "Retrieved %d messages for team %s, channel %s",
            len(messages),
            team_id,
            channel_id,
        )
        return messages

    async def list_team_members(
        self,
        team_id: str,
        max_members: Optional[int] = None,
    ) -> List[TeamsUser]:
        """
        List members of a team.

        :param team_id: Team ID.
        :param max_members: Maximum number of members to retrieve.
        :return: List of TeamsUser objects.
        """
        logger.debug("Fetching members for team %s", team_id)

        members_data = await self.client.get_paginated(
            f"teams/{team_id}/members",
            max_items=max_members,
        )

        members = []
        for member_data in members_data:
            member = TeamsUser(
                id=member_data.get("userId", member_data.get("id", "")),
                display_name=member_data.get("displayName", ""),
                email=member_data.get("email"),
                user_principal_name=member_data.get("userPrincipalName"),
            )
            members.append(member)

        logger.debug("Retrieved %d members for team %s", len(members), team_id)
        return members

    async def get_team_with_details(
        self,
        team_id: str,
        include_channels: bool = True,
        include_members: bool = True,
    ) -> TeamsTeam:
        """
        Get a team with its channels and members.

        :param team_id: Team ID.
        :param include_channels: Include channel list.
        :param include_members: Include member list.
        :return: TeamsTeam object with channels and members populated.
        """
        team = await self.get_team(team_id)

        if include_channels:
            team.channels = await self.list_channels(team_id)

        if include_members:
            team.members = await self.list_team_members(team_id)

        return team

    async def list_teams_with_details(
        self,
        max_teams: Optional[int] = None,
        include_channels: bool = True,
        include_members: bool = False,
    ) -> List[TeamsTeam]:
        """
        List all teams with their channels and optionally members.

        :param max_teams: Maximum number of teams to retrieve.
        :param include_channels: Include channel list for each team.
        :param include_members: Include member list for each team.
        :return: List of TeamsTeam objects with details.
        """
        teams = await self.list_teams(max_teams=max_teams)

        for team in teams:
            if include_channels:
                team.channels = await self.list_channels(team.id)
            if include_members:
                team.members = await self.list_team_members(team.id)

        return teams

    async def get_channel_activity_stats(
        self,
        team_id: str,
        channel_id: str,
        since: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Get activity statistics for a channel.

        :param team_id: Team ID.
        :param channel_id: Channel ID.
        :param since: Only count messages after this datetime.
        :return: Dictionary with activity statistics.
        """
        messages = await self.list_channel_messages(
            team_id=team_id,
            channel_id=channel_id,
            since=since,
        )

        # Calculate statistics
        unique_authors = set()
        message_count = 0
        reply_count = 0

        for msg in messages:
            message_count += 1
            reply_count += msg.reply_count
            if msg.from_user:
                unique_authors.add(msg.from_user.id)

        return {
            "message_count": message_count,
            "reply_count": reply_count,
            "unique_authors": len(unique_authors),
            "period_start": since.isoformat() if since else None,
            "period_end": datetime.now(timezone.utc).isoformat(),
        }

    async def close(self) -> None:
        """Close the connector and cleanup resources."""
        await self.client.close()

    async def __aenter__(self) -> "TeamsConnector":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Async context manager exit."""
        await self.close()
        return False

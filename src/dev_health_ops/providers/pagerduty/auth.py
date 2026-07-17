"""PagerDuty authorization header strategies."""

from dataclasses import dataclass
from typing import Protocol


class PagerDutyAuth(Protocol):
    """Produces a PagerDuty Authorization header without logging its value."""

    def headers(self) -> dict[str, str]: ...


@dataclass(frozen=True, slots=True)
class OAuthBearerAuth:
    """OAuth access-token authentication."""

    access_token: str

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.access_token}"}


@dataclass(frozen=True, slots=True)
class ApiTokenAuth:
    """Legacy PagerDuty API-token authentication."""

    api_token: str

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Token token={self.api_token}"}

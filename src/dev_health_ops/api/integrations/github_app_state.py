from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import jwt
from jwt.exceptions import InvalidTokenError

from dev_health_ops.api.services.auth import (
    JWT_ALGORITHM,
    JWT_AUDIENCE,
    JWT_ISSUER,
    _get_jwt_secret,
)

GITHUB_APP_INSTALL_PURPOSE = "github_app_install"
GITHUB_APP_STATE_TTL_MINUTES = 15


class GitHubAppStateError(ValueError):
    pass


@dataclass(frozen=True)
class GitHubAppInstallState:
    org_id: str
    jti: str


def mint_github_app_install_state(org_id: str) -> str:
    now = datetime.now(timezone.utc)
    payload: dict[str, Any] = {
        "org_id": org_id,
        "jti": uuid.uuid4().hex,
        "purpose": GITHUB_APP_INSTALL_PURPOSE,
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
        "iat": now,
        "exp": now + timedelta(minutes=GITHUB_APP_STATE_TTL_MINUTES),
    }
    return jwt.encode(payload, _get_jwt_secret(), algorithm=JWT_ALGORITHM)


def verify_github_app_install_state(state: str) -> GitHubAppInstallState:
    try:
        payload = jwt.decode(
            state,
            _get_jwt_secret(),
            algorithms=[JWT_ALGORITHM],
            issuer=JWT_ISSUER,
            audience=JWT_AUDIENCE,
        )
    except InvalidTokenError as exc:
        raise GitHubAppStateError("Invalid GitHub App installation state") from exc

    if payload.get("purpose") != GITHUB_APP_INSTALL_PURPOSE:
        raise GitHubAppStateError("Invalid GitHub App installation state purpose")
    org_id = payload.get("org_id")
    if not isinstance(org_id, str) or not org_id:
        raise GitHubAppStateError("Invalid GitHub App installation organization")
    jti = payload.get("jti")
    if not isinstance(jti, str) or not jti:
        raise GitHubAppStateError("Invalid GitHub App installation state identifier")
    return GitHubAppInstallState(org_id=org_id, jti=jti)

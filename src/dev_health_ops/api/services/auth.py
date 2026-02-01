"""JWT Authentication service for Enterprise Edition.

Provides JWT token creation, validation, and user authentication
for the GraphQL API and REST endpoints.
"""

from __future__ import annotations

import hashlib
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import jwt
from jwt.exceptions import InvalidTokenError


logger = logging.getLogger(__name__)

# JWT configuration
JWT_ALGORITHM = "HS256"
JWT_ACCESS_TOKEN_EXPIRE_MINUTES = 60
JWT_REFRESH_TOKEN_EXPIRE_DAYS = 7


def _get_jwt_secret() -> str:
    secret = os.getenv("JWT_SECRET_KEY")
    if not secret:
        logger.warning(
            "JWT_SECRET_KEY not set, using derived key from SETTINGS_ENCRYPTION_KEY"
        )
        encryption_key = os.getenv("SETTINGS_ENCRYPTION_KEY", "dev-key-not-for-prod")
        secret = hashlib.sha256(encryption_key.encode()).hexdigest()
    return secret


@dataclass
class AuthenticatedUser:
    """Authenticated user information extracted from JWT."""

    user_id: str
    email: str
    org_id: str
    role: str
    is_superuser: bool = False
    username: str | None = None
    full_name: str | None = None

    @property
    def is_admin(self) -> bool:
        return self.role in ("owner", "admin") or self.is_superuser


@dataclass
class TokenPair:
    """Access and refresh token pair."""

    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int = JWT_ACCESS_TOKEN_EXPIRE_MINUTES * 60


class AuthService:
    """JWT authentication service."""

    def __init__(self, secret_key: str | None = None):
        self.secret_key = secret_key or _get_jwt_secret()

    def create_access_token(
        self,
        user_id: str,
        email: str,
        org_id: str,
        role: str = "member",
        is_superuser: bool = False,
        username: str | None = None,
        full_name: str | None = None,
        expires_delta: timedelta | None = None,
    ) -> str:
        """Create a JWT access token."""
        if expires_delta is None:
            expires_delta = timedelta(minutes=JWT_ACCESS_TOKEN_EXPIRE_MINUTES)

        expire = datetime.now(timezone.utc) + expires_delta
        payload = {
            "sub": user_id,
            "email": email,
            "org_id": org_id,
            "role": role,
            "is_superuser": is_superuser,
            "type": "access",
            "exp": expire,
            "iat": datetime.now(timezone.utc),
            "jti": str(uuid.uuid4()),
        }
        if username:
            payload["username"] = username
        if full_name:
            payload["full_name"] = full_name

        return jwt.encode(payload, self.secret_key, algorithm=JWT_ALGORITHM)

    def create_refresh_token(
        self,
        user_id: str,
        org_id: str,
        expires_delta: timedelta | None = None,
    ) -> str:
        """Create a JWT refresh token."""
        if expires_delta is None:
            expires_delta = timedelta(days=JWT_REFRESH_TOKEN_EXPIRE_DAYS)

        expire = datetime.now(timezone.utc) + expires_delta
        payload = {
            "sub": user_id,
            "org_id": org_id,
            "type": "refresh",
            "exp": expire,
            "iat": datetime.now(timezone.utc),
            "jti": str(uuid.uuid4()),
        }

        return jwt.encode(payload, self.secret_key, algorithm=JWT_ALGORITHM)

    def create_token_pair(
        self,
        user_id: str,
        email: str,
        org_id: str,
        role: str = "member",
        is_superuser: bool = False,
        username: str | None = None,
        full_name: str | None = None,
    ) -> TokenPair:
        """Create both access and refresh tokens."""
        access_token = self.create_access_token(
            user_id=user_id,
            email=email,
            org_id=org_id,
            role=role,
            is_superuser=is_superuser,
            username=username,
            full_name=full_name,
        )
        refresh_token = self.create_refresh_token(user_id=user_id, org_id=org_id)
        return TokenPair(access_token=access_token, refresh_token=refresh_token)

    def validate_token(
        self, token: str, token_type: str = "access"
    ) -> dict[str, Any] | None:
        """Validate a JWT token and return its payload."""
        try:
            payload = jwt.decode(
                token,
                self.secret_key,
                algorithms=[JWT_ALGORITHM],
                options={"require": ["exp", "sub", "type"]},
            )

            if payload.get("type") != token_type:
                logger.warning("Token type mismatch: expected %s", token_type)
                return None

            return payload
        except jwt.ExpiredSignatureError:
            logger.debug("Token expired")
            return None
        except InvalidTokenError as e:
            logger.debug("Invalid token: %s", e)
            return None

    def get_authenticated_user(self, token: str) -> AuthenticatedUser | None:
        """Extract authenticated user from access token."""
        payload = self.validate_token(token, token_type="access")
        if not payload:
            return None

        return AuthenticatedUser(
            user_id=payload["sub"],
            email=payload.get("email", ""),
            org_id=payload.get("org_id", ""),
            role=payload.get("role", "member"),
            is_superuser=payload.get("is_superuser", False),
            username=payload.get("username"),
            full_name=payload.get("full_name"),
        )

    def refresh_access_token(
        self,
        refresh_token: str,
        email: str,
        role: str = "member",
        is_superuser: bool = False,
        username: str | None = None,
        full_name: str | None = None,
    ) -> str | None:
        """Create a new access token from a valid refresh token."""
        payload = self.validate_token(refresh_token, token_type="refresh")
        if not payload:
            return None

        return self.create_access_token(
            user_id=payload["sub"],
            email=email,
            org_id=payload["org_id"],
            role=role,
            is_superuser=is_superuser,
            username=username,
            full_name=full_name,
        )


def extract_token_from_header(authorization: str | None) -> str | None:
    """Extract JWT token from Authorization header."""
    if not authorization:
        return None

    parts = authorization.split()
    if len(parts) != 2:
        return None

    scheme, token = parts
    if scheme.lower() != "bearer":
        return None

    return token


_auth_service: AuthService | None = None


def get_auth_service() -> AuthService:
    """Get the global auth service instance."""
    global _auth_service
    if _auth_service is None:
        _auth_service = AuthService()
    return _auth_service

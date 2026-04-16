"""JWT Authentication service for Enterprise Edition.

Provides JWT token creation, validation, and user authentication
for the GraphQL API and REST endpoints.
"""

from __future__ import annotations

import contextvars
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import jwt
from jwt.exceptions import InvalidTokenError

logger = logging.getLogger(__name__)


# Per-request org_id context — set by get_current_user, read by query_dicts.
# This ensures every ClickHouse query is automatically scoped to the
# authenticated user's organization without manual parameter threading.
_current_org_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "current_org_id", default=None
)


def set_current_org_id(org_id: str) -> contextvars.Token[str | None]:
    """Set the org_id for the current request context."""
    return _current_org_id.set(org_id)


def get_current_org_id() -> str | None:
    """Get the org_id for the current request context, or None if unset."""
    return _current_org_id.get(None)


# ─── Impersonation context ──────────────────────────────────────────────────
# Tracks the active impersonation session for the current request.
# Set by ImpersonationMiddleware; read by audit service and permission checks.


@dataclass
class ImpersonationContext:
    target_user_id: str
    target_org_id: str
    target_role: str
    real_user_id: str
    is_active: bool = True


_impersonation_ctx: contextvars.ContextVar[ImpersonationContext | None] = (
    contextvars.ContextVar("impersonation_ctx", default=None)
)


def get_impersonation_context() -> ImpersonationContext | None:
    """Return the active ImpersonationContext for this request, or None."""
    return _impersonation_ctx.get(None)


def set_impersonation_context(
    target_user_id: str,
    target_org_id: str,
    target_role: str,
    real_user_id: str,
) -> contextvars.Token[ImpersonationContext | None]:
    """Activate impersonation context for the current request scope."""
    ctx = ImpersonationContext(
        target_user_id=target_user_id,
        target_org_id=target_org_id,
        target_role=target_role,
        real_user_id=real_user_id,
    )
    return _impersonation_ctx.set(ctx)


def is_impersonating() -> bool:
    """Return True if an active impersonation session is set for this request."""
    ctx = _impersonation_ctx.get(None)
    return ctx is not None and ctx.is_active


# JWT configuration
JWT_ALGORITHM = "HS256"
JWT_ACCESS_TOKEN_EXPIRE_MINUTES = 60
JWT_REFRESH_TOKEN_EXPIRE_DAYS = 7
JWT_ISSUER = os.getenv("JWT_ISSUER", "dev-health-ops")
JWT_AUDIENCE = os.getenv("JWT_AUDIENCE", "dev-health-api")


def _get_jwt_secret() -> str:
    secret = os.getenv("JWT_SECRET_KEY")
    if not secret:
        raise RuntimeError(
            "JWT_SECRET_KEY is required and must be set in the environment. "
            "Derivation from SETTINGS_ENCRYPTION_KEY is no longer supported."
        )
    if len(secret) < 32:
        raise ValueError("JWT secret must be at least 32 characters")
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
    impersonated_by: str | None = None

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
        self.issuer = JWT_ISSUER
        self.audience = JWT_AUDIENCE

        # Key rotation strategy (HS256 shared-secret):
        # 1. Generate a new secret and set it as JWT_SECRET_KEY.
        # 2. Deploy after reducing access/refresh TTLs temporarily, if desired.
        # 3. Keep old service instances alive only for a bounded grace window.
        # 4. After the grace window, retire old instances so old key validation ends.
        #
        # This service signs and validates with one active key from JWT_SECRET_KEY.
        # Rotation is performed operationally by coordinated rollout + key replacement.

    def create_access_token(
        self,
        user_id: str,
        email: str,
        org_id: str = "",
        role: str = "member",
        is_superuser: bool = False,
        username: str | None = None,
        full_name: str | None = None,
        impersonating_user_id: str | None = None,
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
            "iss": self.issuer,
            "aud": self.audience,
            "exp": expire,
            "iat": datetime.now(timezone.utc),
            "jti": str(uuid.uuid4()),
        }
        if username:
            payload["username"] = username
        if full_name:
            payload["full_name"] = full_name
        if impersonating_user_id:
            payload["impersonating_user_id"] = impersonating_user_id

        return jwt.encode(payload, self.secret_key, algorithm=JWT_ALGORITHM)

    def create_refresh_token(
        self,
        user_id: str,
        org_id: str = "",
        family_id: str | None = None,
        expires_delta: timedelta | None = None,
    ) -> str:
        """Create a JWT refresh token."""
        if expires_delta is None:
            expires_delta = timedelta(days=JWT_REFRESH_TOKEN_EXPIRE_DAYS)

        expire = datetime.now(timezone.utc) + expires_delta
        payload = {
            "sub": user_id,
            "org_id": org_id,
            "family_id": family_id or str(uuid.uuid4()),
            "type": "refresh",
            "iss": self.issuer,
            "aud": self.audience,
            "exp": expire,
            "iat": datetime.now(timezone.utc),
            "jti": str(uuid.uuid4()),
        }

        return jwt.encode(payload, self.secret_key, algorithm=JWT_ALGORITHM)

    def create_token_pair(
        self,
        user_id: str,
        email: str,
        org_id: str = "",
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
            # nosemgrep: python.jwt.security.unverified-jwt-decode.unverified-jwt-decode
            # Intentional: peek at claims to check aud/iss presence before full verification
            unverified_payload = jwt.decode(
                token,
                options={
                    "verify_signature": False,
                    "verify_exp": False,
                    "verify_nbf": False,
                    "verify_iat": False,
                    "verify_aud": False,
                    "verify_iss": False,
                },
                algorithms=[JWT_ALGORITHM],
            )

            has_audience = "aud" in unverified_payload
            has_issuer = "iss" in unverified_payload

            decode_kwargs: dict[str, Any] = {
                "key": self.secret_key,
                "algorithms": [JWT_ALGORITHM],
                "options": {
                    "require": ["exp", "sub", "type"],
                    "verify_aud": has_audience,
                    "verify_iss": has_issuer,
                },
            }

            if has_audience:
                decode_kwargs["audience"] = self.audience
            if has_issuer:
                decode_kwargs["issuer"] = self.issuer

            payload = jwt.decode(
                token,
                **decode_kwargs,
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
            impersonated_by=payload.get("impersonating_user_id"),
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

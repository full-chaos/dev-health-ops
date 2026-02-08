"""Ed25519 license key generation and signing.

Output format: ``base64(payload_json).base64(signature)``
"""

from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass
from uuid import uuid4

from nacl.signing import SigningKey

from dev_health_ops.licensing.types import (
    DEFAULT_FEATURES,
    DEFAULT_LIMITS,
    GRACE_DAYS,
    LicenseLimits,
    LicensePayload,
    LicenseTier,
)


@dataclass(frozen=True)
class KeyPair:
    """Base64-encoded Ed25519 key pair (32-byte seed, not 64-byte tweetnacl secret)."""

    public_key: str
    private_key: str


def generate_keypair() -> KeyPair:
    """Generate a new Ed25519 key pair."""
    signing_key = SigningKey.generate()
    return KeyPair(
        public_key=base64.b64encode(signing_key.verify_key.encode()).decode(),
        private_key=base64.b64encode(signing_key.encode()).decode(),
    )


def sign_license(
    private_key_b64: str,
    *,
    org_id: str,
    tier: LicenseTier | str,
    duration_days: int = 365,
    org_name: str | None = None,
    contact_email: str | None = None,
    features: dict[str, bool] | None = None,
    limits: LicenseLimits | None = None,
    grace_days: int | None = None,
    license_id: str | None = None,
    issued_at: int | None = None,
) -> str:
    """Create and sign a license key.

    Raises :class:`ValueError` if *tier* is invalid, *duration_days* is
    non-positive, or the private key cannot be decoded.
    """
    if isinstance(tier, str):
        try:
            tier = LicenseTier(tier.lower())
        except ValueError:
            valid = ", ".join(t.value for t in LicenseTier)
            raise ValueError(f"Invalid tier {tier!r}. Must be one of: {valid}")

    if duration_days <= 0:
        raise ValueError("duration_days must be positive")

    now = issued_at if issued_at is not None else int(time.time())

    payload = LicensePayload(
        iss="fullchaos.studio",
        sub=org_id,
        iat=now,
        exp=now + duration_days * 86400,
        tier=tier,
        features=features if features is not None else DEFAULT_FEATURES[tier],
        limits=limits if limits is not None else DEFAULT_LIMITS[tier],
        grace_days=grace_days if grace_days is not None else GRACE_DAYS[tier],
        org_name=org_name,
        contact_email=contact_email,
        license_id=license_id or str(uuid4()),
    )

    return sign_payload(private_key_b64, payload)


def sign_payload(private_key_b64: str, payload: LicensePayload) -> str:
    """Sign a pre-built :class:`LicensePayload` and return the license string."""
    try:
        private_key_bytes = base64.b64decode(private_key_b64)
        signing_key = SigningKey(private_key_bytes)
    except Exception as e:
        raise ValueError(f"Invalid private key: {e}") from e

    payload_bytes = payload.model_dump_json().encode("utf-8")
    signed = signing_key.sign(payload_bytes)

    payload_b64 = base64.b64encode(payload_bytes).decode()
    signature_b64 = base64.b64encode(signed.signature).decode()

    return f"{payload_b64}.{signature_b64}"

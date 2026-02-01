import base64
import json
import time

import pytest
from nacl.signing import SigningKey

from dev_health_ops.licensing import (
    LicensePayload,
    LicenseTier,
    LicenseValidator,
    LicenseValidationError,
)


def create_test_keypair() -> tuple[str, str]:
    signing_key = SigningKey.generate()
    verify_key = signing_key.verify_key
    return (
        base64.b64encode(verify_key.encode()).decode(),
        base64.b64encode(signing_key.encode()).decode(),
    )


def generate_test_license(
    private_key_b64: str,
    *,
    org_id: str = "test_org",
    tier: str = "team",
    exp_offset: int = 365 * 24 * 60 * 60,
    grace_days: int = 14,
) -> str:
    now = int(time.time())
    payload = {
        "iss": "fullchaos.studio",
        "sub": org_id,
        "iat": now,
        "exp": now + exp_offset,
        "tier": tier,
        "features": {"team_dashboard": True, "sso": tier == "enterprise"},
        "limits": {"users": 25, "repos": 20, "api_rate": 300},
        "grace_days": grace_days,
    }
    payload_bytes = json.dumps(payload).encode()
    payload_b64 = base64.b64encode(payload_bytes).decode()

    private_key_bytes = base64.b64decode(private_key_b64)
    signing_key = SigningKey(private_key_bytes)
    signature = signing_key.sign(payload_bytes).signature
    signature_b64 = base64.b64encode(signature).decode()

    return f"{payload_b64}.{signature_b64}"


class TestLicenseValidator:
    @pytest.fixture
    def keypair(self) -> tuple[str, str]:
        return create_test_keypair()

    @pytest.fixture
    def validator(self, keypair: tuple[str, str]) -> LicenseValidator:
        public_key, _ = keypair
        return LicenseValidator(public_key)

    def test_valid_license(self, validator: LicenseValidator, keypair: tuple[str, str]):
        _, private_key = keypair
        license_str = generate_test_license(private_key)

        result = validator.validate(license_str)

        assert result.valid is True
        assert result.payload is not None
        assert result.payload.sub == "test_org"
        assert result.payload.tier == LicenseTier.TEAM
        assert result.in_grace_period is False

    def test_invalid_format_no_dot(self, validator: LicenseValidator):
        result = validator.validate("invalid_license_no_dot")

        assert result.valid is False
        assert "Invalid license format" in result.error

    def test_invalid_format_multiple_dots(self, validator: LicenseValidator):
        result = validator.validate("a.b.c")

        assert result.valid is False
        assert "Invalid license format" in result.error

    def test_invalid_base64(self, validator: LicenseValidator):
        result = validator.validate("not!valid!base64.also!invalid!")

        assert result.valid is False
        assert "Invalid base64" in result.error

    def test_invalid_signature(self, validator: LicenseValidator):
        other_public, other_private = create_test_keypair()
        license_str = generate_test_license(other_private)

        result = validator.validate(license_str)

        assert result.valid is False
        assert "Invalid signature" in result.error

    def test_tampered_payload(
        self, validator: LicenseValidator, keypair: tuple[str, str]
    ):
        _, private_key = keypair
        license_str = generate_test_license(private_key)
        payload_b64, signature_b64 = license_str.split(".")

        payload_bytes = base64.b64decode(payload_b64)
        payload_dict = json.loads(payload_bytes)
        payload_dict["tier"] = "enterprise"
        tampered_payload = base64.b64encode(json.dumps(payload_dict).encode()).decode()
        tampered_license = f"{tampered_payload}.{signature_b64}"

        result = validator.validate(tampered_license)

        assert result.valid is False
        assert "Invalid signature" in result.error

    def test_expired_license_past_grace(
        self, validator: LicenseValidator, keypair: tuple[str, str]
    ):
        _, private_key = keypair
        license_str = generate_test_license(
            private_key, exp_offset=-100 * 24 * 60 * 60, grace_days=14
        )

        result = validator.validate(license_str)

        assert result.valid is False
        assert "expired" in result.error.lower()

    def test_license_in_grace_period(
        self, validator: LicenseValidator, keypair: tuple[str, str]
    ):
        _, private_key = keypair
        license_str = generate_test_license(
            private_key, exp_offset=-5 * 24 * 60 * 60, grace_days=14
        )

        result = validator.validate(license_str)

        assert result.valid is True
        assert result.in_grace_period is True

    def test_skip_expiry_check(
        self, validator: LicenseValidator, keypair: tuple[str, str]
    ):
        _, private_key = keypair
        license_str = generate_test_license(
            private_key, exp_offset=-100 * 24 * 60 * 60, grace_days=14
        )

        result = validator.validate(license_str, check_expiry=False)

        assert result.valid is True

    def test_has_feature(self, validator: LicenseValidator, keypair: tuple[str, str]):
        _, private_key = keypair
        license_str = generate_test_license(private_key)
        result = validator.validate(license_str)

        assert validator.has_feature(result.payload, "team_dashboard") is True
        assert validator.has_feature(result.payload, "sso") is False
        assert validator.has_feature(result.payload, "nonexistent") is False

    def test_check_limit_within(
        self, validator: LicenseValidator, keypair: tuple[str, str]
    ):
        _, private_key = keypair
        license_str = generate_test_license(private_key)
        result = validator.validate(license_str)

        assert validator.check_limit(result.payload, "users", 10) is True
        assert validator.check_limit(result.payload, "users", 25) is True

    def test_check_limit_exceeded(
        self, validator: LicenseValidator, keypair: tuple[str, str]
    ):
        _, private_key = keypair
        license_str = generate_test_license(private_key)
        result = validator.validate(license_str)

        assert validator.check_limit(result.payload, "users", 30) is False

    def test_invalid_public_key(self):
        with pytest.raises(LicenseValidationError, match="Invalid public key"):
            LicenseValidator("not_a_valid_key")


class TestLicensePayload:
    def test_tier_enum(self):
        assert LicenseTier.COMMUNITY.value == "community"
        assert LicenseTier.TEAM.value == "team"
        assert LicenseTier.ENTERPRISE.value == "enterprise"

    def test_limits_unlimited(self):
        from dev_health_ops.licensing.types import LicenseLimits

        unlimited = LicenseLimits(users=-1, repos=-1, api_rate=-1)
        assert unlimited.is_unlimited("users") is True
        assert unlimited.is_unlimited("repos") is True

        limited = LicenseLimits(users=10, repos=5, api_rate=60)
        assert limited.is_unlimited("users") is False

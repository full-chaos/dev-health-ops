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


class TestLicenseManager:
    @pytest.fixture(autouse=True)
    def reset_manager(self):
        from dev_health_ops.licensing import LicenseManager

        LicenseManager.reset()
        yield
        LicenseManager.reset()

    @pytest.fixture
    def keypair(self) -> tuple[str, str]:
        return create_test_keypair()

    @pytest.fixture
    def team_license(self, keypair: tuple[str, str]) -> tuple[str, str, str]:
        public_key, private_key = keypair
        license_str = generate_test_license(private_key, tier="team")
        return public_key, private_key, license_str

    @pytest.fixture
    def enterprise_license(self, keypair: tuple[str, str]) -> tuple[str, str, str]:
        public_key, private_key = keypair
        license_str = generate_test_license(private_key, tier="enterprise")
        return public_key, private_key, license_str

    def test_singleton_pattern(self):
        from dev_health_ops.licensing import LicenseManager

        m1 = LicenseManager.get_instance()
        m2 = LicenseManager.get_instance()
        assert m1 is m2

    def test_initialize_without_keys(self):
        from dev_health_ops.licensing import LicenseManager

        manager = LicenseManager.initialize()
        assert manager.is_licensed is False
        assert manager.tier == LicenseTier.COMMUNITY

    def test_initialize_with_valid_license(self, team_license: tuple[str, str, str]):
        from dev_health_ops.licensing import LicenseManager

        public_key, _, license_str = team_license
        manager = LicenseManager.initialize(public_key, license_str)

        assert manager.is_licensed is True
        assert manager.tier == LicenseTier.TEAM
        assert manager.payload is not None

    def test_initialize_with_invalid_license(self, keypair: tuple[str, str]):
        from dev_health_ops.licensing import LicenseManager

        public_key, _ = keypair
        manager = LicenseManager.initialize(public_key, "invalid.license")

        assert manager.is_licensed is False
        assert manager.tier == LicenseTier.COMMUNITY

    def test_get_entitlements_unlicensed(self):
        from dev_health_ops.licensing import LicenseManager, get_entitlements

        LicenseManager.initialize()
        entitlements = get_entitlements()

        assert entitlements["tier"] == "community"
        assert entitlements["is_licensed"] is False
        assert "features" in entitlements
        assert "limits" in entitlements

    def test_get_entitlements_licensed(self, team_license: tuple[str, str, str]):
        from dev_health_ops.licensing import LicenseManager, get_entitlements

        public_key, _, license_str = team_license
        LicenseManager.initialize(public_key, license_str)
        entitlements = get_entitlements()

        assert entitlements["tier"] == "team"
        assert entitlements["is_licensed"] is True
        assert entitlements["features"]["team_dashboard"] is True

    def test_has_feature_unlicensed(self):
        from dev_health_ops.licensing import LicenseManager, has_feature

        LicenseManager.initialize()

        assert has_feature("team_dashboard") is False
        assert has_feature("sso") is False

    def test_has_feature_team_license(self, team_license: tuple[str, str, str]):
        from dev_health_ops.licensing import LicenseManager, has_feature

        public_key, _, license_str = team_license
        LicenseManager.initialize(public_key, license_str)

        assert has_feature("team_dashboard") is True
        assert has_feature("sso") is False

    def test_has_feature_enterprise_license(
        self, enterprise_license: tuple[str, str, str]
    ):
        from dev_health_ops.licensing import LicenseManager, has_feature

        public_key, _, license_str = enterprise_license
        LicenseManager.initialize(public_key, license_str)

        assert has_feature("team_dashboard") is True
        assert has_feature("sso") is True

    def test_check_limit_unlicensed(self):
        from dev_health_ops.licensing import LicenseManager, check_limit

        LicenseManager.initialize()

        assert check_limit("users", 3) is True
        assert check_limit("users", 10) is False

    def test_check_limit_licensed(self, team_license: tuple[str, str, str]):
        from dev_health_ops.licensing import LicenseManager, check_limit

        public_key, _, license_str = team_license
        LicenseManager.initialize(public_key, license_str)

        assert check_limit("users", 20) is True
        assert check_limit("users", 25) is True
        assert check_limit("users", 30) is False

    def test_get_limit(self, team_license: tuple[str, str, str]):
        from dev_health_ops.licensing import LicenseManager, get_limit

        public_key, _, license_str = team_license
        LicenseManager.initialize(public_key, license_str)

        assert get_limit("users") == 25
        assert get_limit("repos") == 20

    def test_grace_period_detection(self, keypair: tuple[str, str]):
        from dev_health_ops.licensing import LicenseManager

        public_key, private_key = keypair
        license_str = generate_test_license(
            private_key, exp_offset=-5 * 24 * 60 * 60, grace_days=14
        )
        manager = LicenseManager.initialize(public_key, license_str)

        assert manager.is_licensed is True
        assert manager.in_grace_period is True


class TestRequireFeatureDecorator:
    @pytest.fixture(autouse=True)
    def reset_manager(self):
        from dev_health_ops.licensing import LicenseManager

        LicenseManager.reset()
        yield
        LicenseManager.reset()

    @pytest.fixture
    def keypair(self) -> tuple[str, str]:
        return create_test_keypair()

    def test_sync_function_allowed(self, keypair: tuple[str, str]):
        from dev_health_ops.licensing import LicenseManager, require_feature

        public_key, private_key = keypair
        license_str = generate_test_license(private_key, tier="team")
        LicenseManager.initialize(public_key, license_str)

        @require_feature("team_dashboard", raise_http=False)
        def my_endpoint():
            return "success"

        assert my_endpoint() == "success"

    def test_sync_function_denied(self, keypair: tuple[str, str]):
        from dev_health_ops.licensing import (
            LicenseManager,
            require_feature,
            FeatureNotLicensedError,
        )

        public_key, private_key = keypair
        license_str = generate_test_license(private_key, tier="team")
        LicenseManager.initialize(public_key, license_str)

        @require_feature("sso", raise_http=False)
        def my_endpoint():
            return "success"

        with pytest.raises(FeatureNotLicensedError) as exc_info:
            my_endpoint()
        assert exc_info.value.feature == "sso"

    def test_sync_function_http_error(self, keypair: tuple[str, str]):
        from fastapi import HTTPException

        from dev_health_ops.licensing import LicenseManager, require_feature

        public_key, private_key = keypair
        license_str = generate_test_license(private_key, tier="team")
        LicenseManager.initialize(public_key, license_str)

        @require_feature("sso", raise_http=True)
        def my_endpoint():
            return "success"

        with pytest.raises(HTTPException) as exc_info:
            my_endpoint()
        assert exc_info.value.status_code == 402
        assert exc_info.value.detail["error"] == "feature_not_licensed"

    @pytest.mark.asyncio
    async def test_async_function_allowed(self, keypair: tuple[str, str]):
        from dev_health_ops.licensing import LicenseManager, require_feature

        public_key, private_key = keypair
        license_str = generate_test_license(private_key, tier="team")
        LicenseManager.initialize(public_key, license_str)

        @require_feature("team_dashboard", raise_http=False)
        async def my_async_endpoint():
            return "async success"

        result = await my_async_endpoint()
        assert result == "async success"

    @pytest.mark.asyncio
    async def test_async_function_denied(self, keypair: tuple[str, str]):
        from dev_health_ops.licensing import (
            LicenseManager,
            require_feature,
            FeatureNotLicensedError,
        )

        public_key, private_key = keypair
        license_str = generate_test_license(private_key, tier="team")
        LicenseManager.initialize(public_key, license_str)

        @require_feature("sso", raise_http=False)
        async def my_async_endpoint():
            return "async success"

        with pytest.raises(FeatureNotLicensedError):
            await my_async_endpoint()


class TestRequireLimitDecorator:
    @pytest.fixture(autouse=True)
    def reset_manager(self):
        from dev_health_ops.licensing import LicenseManager

        LicenseManager.reset()
        yield
        LicenseManager.reset()

    @pytest.fixture
    def keypair(self) -> tuple[str, str]:
        return create_test_keypair()

    def test_sync_function_within_limit(self, keypair: tuple[str, str]):
        from dev_health_ops.licensing import LicenseManager, require_limit

        public_key, private_key = keypair
        license_str = generate_test_license(private_key, tier="team")
        LicenseManager.initialize(public_key, license_str)

        user_count = 10

        @require_limit("users", lambda: user_count, raise_http=False)
        def add_user():
            return "user added"

        assert add_user() == "user added"

    def test_sync_function_exceeds_limit(self, keypair: tuple[str, str]):
        from dev_health_ops.licensing import (
            LicenseManager,
            require_limit,
            LimitExceededError,
        )

        public_key, private_key = keypair
        license_str = generate_test_license(private_key, tier="team")
        LicenseManager.initialize(public_key, license_str)

        user_count = 30

        @require_limit("users", lambda: user_count, raise_http=False)
        def add_user():
            return "user added"

        with pytest.raises(LimitExceededError) as exc_info:
            add_user()
        assert exc_info.value.limit_name == "users"
        assert exc_info.value.current == 30
        assert exc_info.value.maximum == 25

    def test_sync_function_http_error(self, keypair: tuple[str, str]):
        from fastapi import HTTPException

        from dev_health_ops.licensing import LicenseManager, require_limit

        public_key, private_key = keypair
        license_str = generate_test_license(private_key, tier="team")
        LicenseManager.initialize(public_key, license_str)

        @require_limit("users", lambda: 30, raise_http=True)
        def add_user():
            return "user added"

        with pytest.raises(HTTPException) as exc_info:
            add_user()
        assert exc_info.value.status_code == 402
        assert exc_info.value.detail["error"] == "limit_exceeded"

    @pytest.mark.asyncio
    async def test_async_function_within_limit(self, keypair: tuple[str, str]):
        from dev_health_ops.licensing import LicenseManager, require_limit

        public_key, private_key = keypair
        license_str = generate_test_license(private_key, tier="team")
        LicenseManager.initialize(public_key, license_str)

        @require_limit("users", lambda: 10, raise_http=False)
        async def async_add_user():
            return "async user added"

        result = await async_add_user()
        assert result == "async user added"

    @pytest.mark.asyncio
    async def test_async_function_exceeds_limit(self, keypair: tuple[str, str]):
        from dev_health_ops.licensing import (
            LicenseManager,
            require_limit,
            LimitExceededError,
        )

        public_key, private_key = keypair
        license_str = generate_test_license(private_key, tier="team")
        LicenseManager.initialize(public_key, license_str)

        @require_limit("users", lambda: 30, raise_http=False)
        async def async_add_user():
            return "async user added"

        with pytest.raises(LimitExceededError):
            await async_add_user()

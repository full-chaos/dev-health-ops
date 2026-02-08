import argparse
import base64
import json
import time

import pytest
from nacl.signing import SigningKey

from dev_health_ops.licensing import (
    KeyPair,
    LicensePayload,
    LicenseTier,
    LicenseValidator,
    LicenseValidationError,
    generate_keypair,
    sign_license,
    sign_payload,
)
from dev_health_ops.licensing.types import (
    DEFAULT_FEATURES,
    DEFAULT_LIMITS,
    GRACE_DAYS,
    LicenseLimits,
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


class TestLicenseAuditLogger:
    @pytest.fixture(autouse=True)
    def reset_all(self):
        from dev_health_ops.licensing import LicenseManager
        from dev_health_ops.licensing.gating import LicenseAuditLogger

        LicenseManager.reset()
        LicenseAuditLogger.reset()
        yield
        LicenseManager.reset()
        LicenseAuditLogger.reset()

    @pytest.fixture
    def keypair(self) -> tuple[str, str]:
        return create_test_keypair()

    @pytest.fixture
    def audit_logger(self):
        from dev_health_ops.licensing.gating import get_license_audit_logger

        return get_license_audit_logger()

    def test_singleton_pattern(self):
        from dev_health_ops.licensing.gating import (
            LicenseAuditLogger,
            get_license_audit_logger,
        )

        logger1 = get_license_audit_logger()
        logger2 = get_license_audit_logger()
        assert logger1 is logger2

    def test_log_validation_success(self, audit_logger):
        entry = audit_logger.log_validation_success(
            license_id="test-license-123",
            tier="team",
            org_id="test-org",
            in_grace_period=False,
        )

        assert entry["action"] == "license_validated"
        assert entry["resource_type"] == "license"
        assert entry["resource_id"] == "test-license-123"
        assert entry["status"] == "success"
        assert entry["changes"]["tier"] == "team"
        assert entry["changes"]["org_id"] == "test-org"
        assert entry["changes"]["in_grace_period"] is False

    def test_log_validation_failure(self, audit_logger):
        entry = audit_logger.log_validation_failure(
            license_id="bad-license",
            error="Invalid signature",
        )

        assert entry["action"] == "license_validation_failed"
        assert entry["resource_type"] == "license"
        assert entry["resource_id"] == "bad-license"
        assert entry["status"] == "failure"
        assert entry["error_message"] == "Invalid signature"

    def test_log_grace_period_entered(self, audit_logger):
        entry = audit_logger.log_grace_period_entered(
            license_id="expiring-license",
            tier="enterprise",
            days_remaining=7,
        )

        assert entry["action"] == "license_grace_period_entered"
        assert entry["resource_type"] == "license"
        assert entry["status"] == "warning"
        assert entry["changes"]["tier"] == "enterprise"
        assert entry["changes"]["days_remaining"] == 7

    def test_log_feature_access_denied(self, audit_logger):
        entry = audit_logger.log_feature_access_denied(
            feature="sso",
            current_tier="team",
            required_tier="enterprise",
        )

        assert entry["action"] == "feature_access_denied"
        assert entry["resource_type"] == "license"
        assert entry["resource_id"] == "sso"
        assert entry["status"] == "failure"
        assert entry["changes"]["feature"] == "sso"
        assert entry["changes"]["current_tier"] == "team"
        assert entry["changes"]["required_tier"] == "enterprise"

    def test_log_limit_exceeded(self, audit_logger):
        entry = audit_logger.log_limit_exceeded(
            limit_name="users",
            current_value=30,
            maximum=25,
            current_tier="team",
        )

        assert entry["action"] == "limit_exceeded"
        assert entry["resource_type"] == "license"
        assert entry["resource_id"] == "users"
        assert entry["status"] == "failure"
        assert entry["changes"]["current_value"] == 30
        assert entry["changes"]["maximum"] == 25

    def test_set_org_id(self):
        import uuid

        from dev_health_ops.licensing.gating import LicenseAuditLogger

        test_uuid = uuid.uuid4()
        LicenseAuditLogger.set_org_id(test_uuid)
        assert LicenseAuditLogger._org_id == test_uuid

        LicenseAuditLogger.set_org_id(str(test_uuid))
        assert LicenseAuditLogger._org_id == test_uuid

        LicenseAuditLogger.set_org_id(None)
        assert LicenseAuditLogger._org_id is None


class TestLicenseAuditIntegration:
    @pytest.fixture(autouse=True)
    def reset_all(self):
        from dev_health_ops.licensing import LicenseManager
        from dev_health_ops.licensing.gating import LicenseAuditLogger

        LicenseManager.reset()
        LicenseAuditLogger.reset()
        yield
        LicenseManager.reset()
        LicenseAuditLogger.reset()

    @pytest.fixture
    def keypair(self) -> tuple[str, str]:
        return create_test_keypair()

    def test_validation_success_logs_audit(self, keypair, caplog):
        import logging

        from dev_health_ops.licensing import LicenseManager

        public_key, private_key = keypair
        license_str = generate_test_license(private_key, tier="team")

        with caplog.at_level(logging.INFO, logger="dev_health_ops.licensing.gating"):
            LicenseManager.initialize(public_key, license_str)

        assert any("license_validated" in record.message for record in caplog.records)
        assert any("team" in record.message for record in caplog.records)

    def test_validation_failure_logs_audit(self, keypair, caplog):
        import logging

        from dev_health_ops.licensing import LicenseManager

        public_key, _ = keypair

        with caplog.at_level(logging.WARNING, logger="dev_health_ops.licensing.gating"):
            LicenseManager.initialize(public_key, "invalid.license")

        assert any(
            "license_validation_failed" in record.message for record in caplog.records
        )

    def test_grace_period_logs_audit(self, keypair, caplog):
        import logging

        from dev_health_ops.licensing import LicenseManager

        public_key, private_key = keypair
        license_str = generate_test_license(
            private_key, exp_offset=-5 * 24 * 60 * 60, grace_days=14
        )

        with caplog.at_level(logging.INFO, logger="dev_health_ops.licensing.gating"):
            LicenseManager.initialize(public_key, license_str)

        assert any(
            "license_grace_period_entered" in record.message
            for record in caplog.records
        )

    def test_feature_denial_logs_audit(self, keypair, caplog):
        import logging

        from dev_health_ops.licensing import LicenseManager, has_feature

        public_key, private_key = keypair
        license_str = generate_test_license(private_key, tier="team")
        LicenseManager.initialize(public_key, license_str)

        with caplog.at_level(logging.WARNING, logger="dev_health_ops.licensing.gating"):
            result = has_feature("sso")

        assert result is False
        assert any(
            "feature_access_denied" in record.message for record in caplog.records
        )

    def test_feature_denial_can_skip_logging(self, keypair, caplog):
        import logging

        from dev_health_ops.licensing import LicenseManager, has_feature

        public_key, private_key = keypair
        license_str = generate_test_license(private_key, tier="team")
        LicenseManager.initialize(public_key, license_str)

        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="dev_health_ops.licensing.gating"):
            result = has_feature("sso", log_denial=False)

        assert result is False
        assert not any(
            "feature_access_denied" in record.message for record in caplog.records
        )

    def test_limit_exceeded_logs_audit(self, keypair, caplog):
        import logging

        from dev_health_ops.licensing import LicenseManager, check_limit

        public_key, private_key = keypair
        license_str = generate_test_license(private_key, tier="team")
        LicenseManager.initialize(public_key, license_str)

        with caplog.at_level(logging.WARNING, logger="dev_health_ops.licensing.gating"):
            result = check_limit("users", 30)

        assert result is False
        assert any("limit_exceeded" in record.message for record in caplog.records)

    def test_limit_exceeded_can_skip_logging(self, keypair, caplog):
        import logging

        from dev_health_ops.licensing import LicenseManager, check_limit

        public_key, private_key = keypair
        license_str = generate_test_license(private_key, tier="team")
        LicenseManager.initialize(public_key, license_str)

        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="dev_health_ops.licensing.gating"):
            result = check_limit("users", 30, log_exceeded=False)

        assert result is False
        assert not any("limit_exceeded" in record.message for record in caplog.records)


class TestGenerateKeypair:
    def test_returns_keypair_dataclass(self):
        kp = generate_keypair()
        assert isinstance(kp, KeyPair)
        assert kp.public_key
        assert kp.private_key

    def test_keys_are_valid_base64(self):
        kp = generate_keypair()
        pub_bytes = base64.b64decode(kp.public_key)
        priv_bytes = base64.b64decode(kp.private_key)
        assert len(pub_bytes) == 32
        assert len(priv_bytes) == 32

    def test_different_keys_each_call(self):
        kp1 = generate_keypair()
        kp2 = generate_keypair()
        assert kp1.public_key != kp2.public_key
        assert kp1.private_key != kp2.private_key


class TestSignLicense:
    @pytest.fixture
    def keypair(self) -> KeyPair:
        return generate_keypair()

    def test_round_trip_with_validator(self, keypair: KeyPair):
        license_str = sign_license(keypair.private_key, org_id="org-123", tier="team")
        validator = LicenseValidator(keypair.public_key)
        result = validator.validate(license_str)

        assert result.valid is True
        assert result.payload is not None
        assert result.payload.sub == "org-123"
        assert result.payload.tier == LicenseTier.TEAM
        assert result.payload.iss == "fullchaos.studio"

    def test_all_tiers(self, keypair: KeyPair):
        for tier in LicenseTier:
            license_str = sign_license(keypair.private_key, org_id="org-1", tier=tier)
            validator = LicenseValidator(keypair.public_key)
            result = validator.validate(license_str)

            assert result.valid is True
            assert result.payload.tier == tier
            assert result.payload.features == DEFAULT_FEATURES[tier]
            assert result.payload.limits == DEFAULT_LIMITS[tier]
            assert result.payload.grace_days == GRACE_DAYS[tier]

    def test_tier_string_normalization(self, keypair: KeyPair):
        license_str = sign_license(
            keypair.private_key, org_id="org-1", tier="ENTERPRISE"
        )
        validator = LicenseValidator(keypair.public_key)
        result = validator.validate(license_str)

        assert result.valid is True
        assert result.payload.tier == LicenseTier.ENTERPRISE

    def test_invalid_tier(self, keypair: KeyPair):
        with pytest.raises(ValueError, match="Invalid tier"):
            sign_license(keypair.private_key, org_id="org-1", tier="platinum")

    def test_invalid_duration(self, keypair: KeyPair):
        with pytest.raises(ValueError, match="duration_days must be positive"):
            sign_license(
                keypair.private_key,
                org_id="org-1",
                tier="team",
                duration_days=0,
            )

    def test_invalid_private_key(self):
        with pytest.raises(ValueError, match="Invalid private key"):
            sign_license("not-a-key", org_id="org-1", tier="team")

    def test_custom_features_and_limits(self, keypair: KeyPair):
        custom_features = {"basic_analytics": True, "sso": True}
        custom_limits = LicenseLimits(users=100, repos=50, api_rate=1000)

        license_str = sign_license(
            keypair.private_key,
            org_id="org-1",
            tier="team",
            features=custom_features,
            limits=custom_limits,
            grace_days=7,
        )
        validator = LicenseValidator(keypair.public_key)
        result = validator.validate(license_str)

        assert result.payload.features == custom_features
        assert result.payload.limits == custom_limits
        assert result.payload.grace_days == 7

    def test_optional_fields(self, keypair: KeyPair):
        license_str = sign_license(
            keypair.private_key,
            org_id="org-1",
            tier="enterprise",
            org_name="Acme Corp",
            contact_email="billing@acme.com",
        )
        validator = LicenseValidator(keypair.public_key)
        result = validator.validate(license_str)

        assert result.payload.org_name == "Acme Corp"
        assert result.payload.contact_email == "billing@acme.com"
        assert result.payload.license_id is not None

    def test_explicit_license_id(self, keypair: KeyPair):
        license_str = sign_license(
            keypair.private_key,
            org_id="org-1",
            tier="team",
            license_id="custom-id-42",
        )
        validator = LicenseValidator(keypair.public_key)
        result = validator.validate(license_str)

        assert result.payload.license_id == "custom-id-42"

    def test_duration_days_sets_expiry(self, keypair: KeyPair):
        now = int(time.time())
        license_str = sign_license(
            keypair.private_key,
            org_id="org-1",
            tier="team",
            duration_days=30,
            issued_at=now,
        )
        validator = LicenseValidator(keypair.public_key)
        result = validator.validate(license_str)

        assert result.payload.iat == now
        assert result.payload.exp == now + 30 * 86400

    def test_tampered_license_rejected(self, keypair: KeyPair):
        license_str = sign_license(keypair.private_key, org_id="org-1", tier="team")
        payload_b64, signature_b64 = license_str.split(".")
        payload_bytes = base64.b64decode(payload_b64)
        payload_dict = json.loads(payload_bytes)
        payload_dict["tier"] = "enterprise"
        tampered_payload = base64.b64encode(json.dumps(payload_dict).encode()).decode()
        tampered_license = f"{tampered_payload}.{signature_b64}"

        validator = LicenseValidator(keypair.public_key)
        result = validator.validate(tampered_license)
        assert result.valid is False
        assert "Invalid signature" in result.error

    def test_wrong_key_rejected(self):
        kp1 = generate_keypair()
        kp2 = generate_keypair()
        license_str = sign_license(kp1.private_key, org_id="org-1", tier="team")
        validator = LicenseValidator(kp2.public_key)
        result = validator.validate(license_str)

        assert result.valid is False
        assert "Invalid signature" in result.error


class TestSignPayload:
    def test_sign_payload_round_trip(self):
        kp = generate_keypair()
        now = int(time.time())
        payload = LicensePayload(
            iss="fullchaos.studio",
            sub="custom-org",
            iat=now,
            exp=now + 86400,
            tier=LicenseTier.ENTERPRISE,
            features=DEFAULT_FEATURES[LicenseTier.ENTERPRISE],
            limits=DEFAULT_LIMITS[LicenseTier.ENTERPRISE],
            grace_days=30,
        )
        license_str = sign_payload(kp.private_key, payload)
        validator = LicenseValidator(kp.public_key)
        result = validator.validate(license_str)

        assert result.valid is True
        assert result.payload.sub == "custom-org"
        assert result.payload.tier == LicenseTier.ENTERPRISE


class TestLicensesCliCommands:
    def test_keygen_returns_keypair(self, capsys):
        from dev_health_ops.api.admin.cli import licenses_keygen_cmd

        ns = argparse.Namespace()
        ret = licenses_keygen_cmd(ns)
        assert ret == 0

        output = capsys.readouterr().out
        assert "PUBLIC_KEY=" in output
        assert "LICENSE_PRIVATE_KEY=" in output

        lines = output.strip().split("\n")
        public_key = lines[0].split("=", 1)[1]
        private_key = lines[1].split("=", 1)[1]

        assert len(base64.b64decode(public_key)) == 32
        assert len(base64.b64decode(private_key)) == 32

    def test_create_requires_private_key(self, monkeypatch, capsys):
        from dev_health_ops.api.admin.cli import licenses_create_cmd

        monkeypatch.delenv("LICENSE_PRIVATE_KEY", raising=False)
        ns = argparse.Namespace(
            org_id="org-1",
            tier="team",
            duration_days=365,
            org_name=None,
            contact_email=None,
        )
        ret = licenses_create_cmd(ns)
        assert ret == 1
        assert "LICENSE_PRIVATE_KEY" in capsys.readouterr().out

    def test_create_produces_valid_license(self, monkeypatch, capsys):
        from dev_health_ops.api.admin.cli import licenses_create_cmd

        kp = generate_keypair()
        monkeypatch.setenv("LICENSE_PRIVATE_KEY", kp.private_key)

        ns = argparse.Namespace(
            org_id="org-42",
            tier="enterprise",
            duration_days=90,
            org_name="Test Corp",
            contact_email="test@test.com",
        )
        ret = licenses_create_cmd(ns)
        assert ret == 0

        license_str = capsys.readouterr().out.strip()
        validator = LicenseValidator(kp.public_key)
        result = validator.validate(license_str)

        assert result.valid is True
        assert result.payload.sub == "org-42"
        assert result.payload.tier == LicenseTier.ENTERPRISE
        assert result.payload.org_name == "Test Corp"

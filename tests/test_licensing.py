import argparse
import base64
import json
import time

import pytest
import pytest_asyncio
from nacl.signing import SigningKey

from dev_health_ops.licensing import (
    KeyPair,
    LicensePayload,
    LicenseTier,
    LicenseValidationError,
    LicenseValidator,
    generate_keypair,
    sign_license,
    sign_payload,
)
from dev_health_ops.licensing.registry import get_features_for_tier
from dev_health_ops.licensing.types import (
    DEFAULT_LIMITS,
    GRACE_DAYS,
    LicenseLimits,
)
from dev_health_ops.licensing.validator import ValidationResult
from tests._helpers import tables_of


def assert_validation_error(result: ValidationResult) -> str:
    error = result.error
    assert error is not None
    return error


def assert_license_payload(result: ValidationResult) -> LicensePayload:
    payload = result.payload
    assert payload is not None
    return payload


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
        assert "Invalid license format" in assert_validation_error(result)

    def test_invalid_format_multiple_dots(self, validator: LicenseValidator):
        result = validator.validate("a.b.c")

        assert result.valid is False
        assert "Invalid license format" in assert_validation_error(result)

    def test_invalid_base64(self, validator: LicenseValidator):
        result = validator.validate("not!valid!base64.also!invalid!")

        assert result.valid is False
        assert "Invalid base64" in assert_validation_error(result)

    def test_invalid_signature(self, validator: LicenseValidator):
        other_public, other_private = create_test_keypair()
        license_str = generate_test_license(other_private)

        result = validator.validate(license_str)

        assert result.valid is False
        assert "Invalid signature" in assert_validation_error(result)

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
        assert "Invalid signature" in assert_validation_error(result)

    def test_expired_license_past_grace(
        self, validator: LicenseValidator, keypair: tuple[str, str]
    ):
        _, private_key = keypair
        license_str = generate_test_license(
            private_key, exp_offset=-100 * 24 * 60 * 60, grace_days=14
        )

        result = validator.validate(license_str)

        assert result.valid is False
        assert "expired" in assert_validation_error(result).lower()

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
        payload = assert_license_payload(result)

        assert validator.has_feature(payload, "team_dashboard") is True
        assert validator.has_feature(payload, "sso") is False
        assert validator.has_feature(payload, "nonexistent") is False

    def test_check_limit_within(
        self, validator: LicenseValidator, keypair: tuple[str, str]
    ):
        _, private_key = keypair
        license_str = generate_test_license(private_key)
        result = validator.validate(license_str)
        payload = assert_license_payload(result)

        assert validator.check_limit(payload, "users", 10) is True
        assert validator.check_limit(payload, "users", 25) is True

    def test_check_limit_exceeded(
        self, validator: LicenseValidator, keypair: tuple[str, str]
    ):
        _, private_key = keypair
        license_str = generate_test_license(private_key)
        result = validator.validate(license_str)
        payload = assert_license_payload(result)

        assert validator.check_limit(payload, "users", 30) is False

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
            FeatureNotLicensedError,
            LicenseManager,
            require_feature,
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
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert detail.get("error") == "feature_not_licensed"

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
            FeatureNotLicensedError,
            LicenseManager,
            require_feature,
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
            LimitExceededError,
            require_limit,
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
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert detail.get("error") == "limit_exceeded"

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
            LimitExceededError,
            require_limit,
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
        for tier in list(LicenseTier):
            license_str = sign_license(keypair.private_key, org_id="org-1", tier=tier)
            validator = LicenseValidator(keypair.public_key)
            result = validator.validate(license_str)
            payload = assert_license_payload(result)

            assert result.valid is True
            assert payload.tier == tier
            assert payload.features == get_features_for_tier(tier)
            assert payload.limits == DEFAULT_LIMITS[tier]
            assert payload.grace_days == GRACE_DAYS[tier]

    def test_tier_string_normalization(self, keypair: KeyPair):
        license_str = sign_license(
            keypair.private_key, org_id="org-1", tier="ENTERPRISE"
        )
        validator = LicenseValidator(keypair.public_key)
        result = validator.validate(license_str)

        assert result.valid is True
        assert assert_license_payload(result).tier == LicenseTier.ENTERPRISE

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

        payload = assert_license_payload(result)
        assert payload.features == custom_features
        assert payload.limits == custom_limits
        assert payload.grace_days == 7

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

        payload = assert_license_payload(result)
        assert payload.org_name == "Acme Corp"
        assert payload.contact_email == "billing@acme.com"
        assert payload.license_id is not None

    def test_explicit_license_id(self, keypair: KeyPair):
        license_str = sign_license(
            keypair.private_key,
            org_id="org-1",
            tier="team",
            license_id="custom-id-42",
        )
        validator = LicenseValidator(keypair.public_key)
        result = validator.validate(license_str)

        assert assert_license_payload(result).license_id == "custom-id-42"

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

        payload = assert_license_payload(result)
        assert payload.iat == now
        assert payload.exp == now + 30 * 86400

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
        assert "Invalid signature" in assert_validation_error(result)

    def test_wrong_key_rejected(self):
        kp1 = generate_keypair()
        kp2 = generate_keypair()
        license_str = sign_license(kp1.private_key, org_id="org-1", tier="team")
        validator = LicenseValidator(kp2.public_key)
        result = validator.validate(license_str)

        assert result.valid is False
        assert "Invalid signature" in assert_validation_error(result)


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
            features=get_features_for_tier(LicenseTier.ENTERPRISE),
            limits=DEFAULT_LIMITS[LicenseTier.ENTERPRISE],
            grace_days=30,
        )
        license_str = sign_payload(kp.private_key, payload)
        validator = LicenseValidator(kp.public_key)
        result = validator.validate(license_str)

        assert result.valid is True
        payload = assert_license_payload(result)
        assert payload.sub == "custom-org"
        assert payload.tier == LicenseTier.ENTERPRISE


class TestTestKeypairAndGenerateTestLicense:
    """Tests for the deterministic TEST_KEYPAIR and generate_test_license() helper."""

    def test_test_keypair_is_deterministic(self):
        from dev_health_ops.licensing import TEST_KEYPAIR as kp1
        from dev_health_ops.licensing.generator import TEST_KEYPAIR as kp2

        assert kp1.public_key == kp2.public_key
        assert kp1.private_key == kp2.private_key

    def test_test_keypair_valid_base64(self):
        from dev_health_ops.licensing import TEST_KEYPAIR

        pub_bytes = base64.b64decode(TEST_KEYPAIR.public_key)
        priv_bytes = base64.b64decode(TEST_KEYPAIR.private_key)
        assert len(pub_bytes) == 32
        assert len(priv_bytes) == 32

    def test_test_keypair_is_keypair_dataclass(self):
        from dev_health_ops.licensing import TEST_KEYPAIR

        assert isinstance(TEST_KEYPAIR, KeyPair)

    def test_generate_test_license_round_trip(self):
        from dev_health_ops.licensing import TEST_KEYPAIR, generate_test_license

        license_str = generate_test_license(org_id="default-org")
        validator = LicenseValidator(TEST_KEYPAIR.public_key)
        result = validator.validate(license_str)

        assert result.valid is True
        assert result.payload is not None
        assert result.payload.sub == "default-org"
        assert result.payload.tier == LicenseTier.ENTERPRISE
        assert result.payload.org_name == "Default Organization"
        assert result.payload.iss == "fullchaos.studio"

    def test_generate_test_license_custom_org(self):
        from dev_health_ops.licensing import TEST_KEYPAIR, generate_test_license

        license_str = generate_test_license(org_id="custom-org", org_name="Custom Org")
        validator = LicenseValidator(TEST_KEYPAIR.public_key)
        result = validator.validate(license_str)

        assert result.valid is True
        payload = assert_license_payload(result)
        assert payload.sub == "custom-org"
        assert payload.org_name == "Custom Org"

    def test_generate_test_license_custom_tier(self):
        from dev_health_ops.licensing import TEST_KEYPAIR, generate_test_license

        license_str = generate_test_license(org_id="test-org", tier=LicenseTier.TEAM)
        validator = LicenseValidator(TEST_KEYPAIR.public_key)
        result = validator.validate(license_str)

        assert result.valid is True
        assert assert_license_payload(result).tier == LicenseTier.TEAM

    def test_generate_test_license_custom_duration(self):
        from dev_health_ops.licensing import TEST_KEYPAIR, generate_test_license

        now = int(time.time())
        license_str = generate_test_license(
            org_id="test-org", duration_days=30, issued_at=now
        )
        validator = LicenseValidator(TEST_KEYPAIR.public_key)
        result = validator.validate(license_str)

        assert result.valid is True
        payload = assert_license_payload(result)
        assert payload.iat == now
        assert payload.exp == now + 30 * 86400

    def test_generate_test_license_default_duration_10_years(self):
        from dev_health_ops.licensing import TEST_KEYPAIR, generate_test_license

        now = int(time.time())
        license_str = generate_test_license(org_id="test-org", issued_at=now)
        validator = LicenseValidator(TEST_KEYPAIR.public_key)
        result = validator.validate(license_str)

        assert assert_license_payload(result).exp == now + 3650 * 86400


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
        payload = assert_license_payload(result)
        assert payload.sub == "org-42"
        assert payload.tier == LicenseTier.ENTERPRISE
        assert payload.org_name == "Test Corp"


class TestGetFeaturesForTier:
    """Tests for the canonical STANDARD_FEATURES registry via get_features_for_tier."""

    def test_community_has_basic_analytics(self):
        features = get_features_for_tier(LicenseTier.COMMUNITY)
        assert features["basic_analytics"] is True

    def test_community_lacks_sso_saml(self):
        features = get_features_for_tier(LicenseTier.COMMUNITY)
        assert features["sso_saml"] is False

    def test_community_lacks_scheduled_jobs(self):
        features = get_features_for_tier(LicenseTier.COMMUNITY)
        assert features["scheduled_jobs"] is False

    def test_team_has_scheduled_jobs(self):
        features = get_features_for_tier(LicenseTier.TEAM)
        assert features["scheduled_jobs"] is True

    def test_team_inherits_community_features(self):
        features = get_features_for_tier(LicenseTier.TEAM)
        assert features["basic_analytics"] is True
        assert features["git_sync"] is True

    def test_team_lacks_sso_saml(self):
        features = get_features_for_tier(LicenseTier.TEAM)
        assert features["sso_saml"] is False

    def test_enterprise_has_sso_saml(self):
        features = get_features_for_tier(LicenseTier.ENTERPRISE)
        assert features["sso_saml"] is True

    def test_enterprise_has_audit_log(self):
        features = get_features_for_tier(LicenseTier.ENTERPRISE)
        assert features["audit_log"] is True

    def test_enterprise_inherits_all_lower_tiers(self):
        features = get_features_for_tier(LicenseTier.ENTERPRISE)
        # community features
        assert features["basic_analytics"] is True
        assert features["git_sync"] is True
        # team features
        assert features["scheduled_jobs"] is True
        assert features["capacity_forecast"] is True

    def test_all_tiers_return_same_key_set(self):
        community = get_features_for_tier(LicenseTier.COMMUNITY)
        team = get_features_for_tier(LicenseTier.TEAM)
        enterprise = get_features_for_tier(LicenseTier.ENTERPRISE)
        assert set(community.keys()) == set(team.keys()) == set(enterprise.keys())

    def test_returns_25_features(self):
        features = get_features_for_tier(LicenseTier.COMMUNITY)
        assert len(features) == 25

    def test_sign_license_uses_canonical_registry(self):
        """sign_license() with no explicit features uses get_features_for_tier."""
        from dev_health_ops.licensing import (
            LicenseValidator,
            generate_keypair,
            sign_license,
        )

        kp = generate_keypair()
        license_str = sign_license(kp.private_key, org_id="org-1", tier="enterprise")
        validator = LicenseValidator(kp.public_key)
        result = validator.validate(license_str)

        assert result.valid is True
        payload = assert_license_payload(result)
        assert payload.features == get_features_for_tier(LicenseTier.ENTERPRISE)
        assert payload.features["sso_saml"] is True

    def test_require_feature_passes_for_enterprise_sso_saml(self):
        """@require_feature works for the canonical sso_saml key at enterprise tier."""
        from dev_health_ops.licensing import (
            LicenseManager,
            generate_keypair,
            generate_test_license,
            require_feature,
        )

        LicenseManager.reset()
        try:
            kp = generate_keypair()
            # generate_test_license defaults to enterprise tier with canonical features
            license_str = generate_test_license(org_id="test-org")
            LicenseManager.initialize(kp.public_key, license_str)

            @require_feature("sso_saml", raise_http=False)
            def protected_endpoint():
                return "ok"

            # Enterprise license should have sso_saml enabled
            # Note: this uses the JWT payload features (from sign_license via
            # generate_test_license), which now uses get_features_for_tier
            # Sign fresh with the correct key so the manager can validate it
            from dev_health_ops.licensing import sign_license

            LicenseManager.reset()
            real_license = sign_license(
                kp.private_key, org_id="org-1", tier="enterprise"
            )
            LicenseManager.initialize(kp.public_key, real_license)

            result = protected_endpoint()
            assert result == "ok"
        finally:
            LicenseManager.reset()


# ---------------------------------------------------------------------------
# G6 (CHAOS-1209) — OrgFeatureOverride updated_by tracking
# ---------------------------------------------------------------------------


class TestOrgFeatureOverrideUpdatedBy:
    """Verify that updated_by is set on override mutations via the admin API."""

    @pytest_asyncio.fixture
    async def session_maker(self, tmp_path):

        from sqlalchemy.ext.asyncio import (
            AsyncSession,
            async_sessionmaker,
            create_async_engine,
        )

        from dev_health_ops.models.git import Base
        from dev_health_ops.models.licensing import (
            FeatureFlag,
            OrgFeatureOverride,
            OrgLicense,
        )
        from dev_health_ops.models.users import Membership, Organization, User

        db_path = tmp_path / "override-updated-by.db"
        engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

        _tables = tables_of(User, Organization, Membership, OrgLicense, FeatureFlag, OrgFeatureOverride)

        async with engine.begin() as conn:
            await conn.run_sync(lambda c: Base.metadata.create_all(c, tables=_tables))

        maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            yield maker
        finally:
            await engine.dispose()

    @pytest_asyncio.fixture
    async def seeded(self, session_maker):
        import uuid

        from dev_health_ops.models.licensing import FeatureFlag
        from dev_health_ops.models.users import Organization, User

        org_id = uuid.uuid4()
        user_id = uuid.uuid4()
        feature_id = uuid.uuid4()

        async with session_maker() as session:
            org = Organization(
                id=org_id, slug="test-org", name="Test Org", tier="enterprise"
            )
            user = User(
                id=user_id, email="super@example.com", is_superuser=True, is_active=True
            )
            ff = FeatureFlag(key="test_feat", name="Test Feat")
            ff.id = feature_id
            session.add_all([org, user, ff])
            await session.commit()

        return {
            "org_id": str(org_id),
            "user_id": str(user_id),
            "feature_id": str(feature_id),
        }

    @pytest_asyncio.fixture
    async def client(self, session_maker, seeded):
        import importlib

        from fastapi import FastAPI
        from httpx import ASGITransport, AsyncClient

        from dev_health_ops.api.services.auth import AuthenticatedUser

        admin_module = importlib.import_module("dev_health_ops.api.admin")
        auth_module = importlib.import_module("dev_health_ops.api.auth.router")

        app = FastAPI()
        app.include_router(admin_module.router)

        current_user = AuthenticatedUser(
            user_id=seeded["user_id"],
            email="super@example.com",
            org_id=seeded["org_id"],
            role="owner",
            is_superuser=True,
        )

        async def _session_override():
            async with session_maker() as session:
                try:
                    yield session
                    await session.commit()
                except Exception:
                    await session.rollback()
                    raise

        app.dependency_overrides[auth_module.get_current_user] = lambda: current_user
        app.dependency_overrides[admin_module.get_session] = _session_override

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c

        app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_create_override_records_created_by(self, client, seeded):
        """POST /feature-overrides sets created_by to the acting superuser."""
        resp = await client.post(
            f"/api/v1/admin/orgs/{seeded['org_id']}/feature-overrides",
            json={
                "feature_id": seeded["feature_id"],
                "is_enabled": True,
                "reason": "trial",
            },
        )
        assert resp.status_code == 201, resp.text
        data = resp.json()
        assert data["created_by"] == seeded["user_id"]
        assert data["updated_by"] is None

    @pytest.mark.asyncio
    async def test_update_override_records_updated_by(self, client, seeded):
        """PATCH /feature-overrides/:id sets updated_by to the acting superuser."""
        create_resp = await client.post(
            f"/api/v1/admin/orgs/{seeded['org_id']}/feature-overrides",
            json={"feature_id": seeded["feature_id"], "is_enabled": True},
        )
        assert create_resp.status_code == 201
        override_id = create_resp.json()["id"]

        patch_resp = await client.patch(
            f"/api/v1/admin/orgs/{seeded['org_id']}/feature-overrides/{override_id}",
            json={"is_enabled": False, "reason": "disabled by admin"},
        )
        assert patch_resp.status_code == 200, patch_resp.text
        data = patch_resp.json()
        assert data["is_enabled"] is False
        assert data["updated_by"] == seeded["user_id"]
        assert data["reason"] == "disabled by admin"

    @pytest.mark.asyncio
    async def test_update_nonexistent_override_returns_404(self, client, seeded):
        """PATCH on a non-existent override returns 404."""
        import uuid

        fake_id = str(uuid.uuid4())
        resp = await client.patch(
            f"/api/v1/admin/orgs/{seeded['org_id']}/feature-overrides/{fake_id}",
            json={"is_enabled": False},
        )
        assert resp.status_code == 404

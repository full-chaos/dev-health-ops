from dev_health_ops.licensing.gating import (
    FeatureNotLicensedError,
    LicenseManager,
    LimitExceededError,
    check_limit,
    get_entitlements,
    get_license_manager,
    get_limit,
    has_feature,
    require_feature,
    require_limit,
)
from dev_health_ops.licensing.generator import (
    TEST_KEYPAIR,
    KeyPair,
    generate_keypair,
    generate_test_license,
    sign_license,
    sign_payload,
)
from dev_health_ops.licensing.registry import (
    STANDARD_FEATURES,
    get_features_for_tier,
)
from dev_health_ops.licensing.types import (
    DEFAULT_LIMITS,
    GRACE_DAYS,
    FeatureCategory,
    LicenseLimits,
    LicensePayload,
    LicenseTier,
)
from dev_health_ops.licensing.validator import (
    LicenseExpiredError,
    LicenseSignatureError,
    LicenseValidationError,
    LicenseValidator,
)

__all__ = [
    # Types
    "LicenseTier",
    "LicenseLimits",
    "LicensePayload",
    "DEFAULT_LIMITS",
    "GRACE_DAYS",
    "FeatureCategory",
    "STANDARD_FEATURES",
    "get_features_for_tier",
    # Validator
    "LicenseValidator",
    "LicenseValidationError",
    "LicenseExpiredError",
    "LicenseSignatureError",
    # Generator
    "KeyPair",
    "TEST_KEYPAIR",
    "generate_keypair",
    "generate_test_license",
    "sign_license",
    "sign_payload",
    # Gating
    "LicenseManager",
    "get_license_manager",
    "get_entitlements",
    "has_feature",
    "check_limit",
    "get_limit",
    "require_feature",
    "require_limit",
    "FeatureNotLicensedError",
    "LimitExceededError",
]

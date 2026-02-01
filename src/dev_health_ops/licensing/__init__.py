from dev_health_ops.licensing.types import (
    LicenseTier,
    LicenseLimits,
    LicensePayload,
    DEFAULT_FEATURES,
    DEFAULT_LIMITS,
    GRACE_DAYS,
)
from dev_health_ops.licensing.validator import (
    LicenseValidator,
    LicenseValidationError,
    LicenseExpiredError,
    LicenseSignatureError,
)
from dev_health_ops.licensing.gating import (
    LicenseManager,
    get_license_manager,
    get_entitlements,
    has_feature,
    check_limit,
    get_limit,
    require_feature,
    require_limit,
    FeatureNotLicensedError,
    LimitExceededError,
)

__all__ = [
    # Types
    "LicenseTier",
    "LicenseLimits",
    "LicensePayload",
    "DEFAULT_FEATURES",
    "DEFAULT_LIMITS",
    "GRACE_DAYS",
    # Validator
    "LicenseValidator",
    "LicenseValidationError",
    "LicenseExpiredError",
    "LicenseSignatureError",
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

from __future__ import annotations

import functools
import logging
import os
import uuid
from typing import Any, Callable, Optional, ParamSpec, TypeVar

from fastapi import HTTPException, status

from dev_health_ops.licensing.types import (
    DEFAULT_FEATURES,
    DEFAULT_LIMITS,
    LicenseLimits,
    LicensePayload,
    LicenseTier,
)
from dev_health_ops.licensing.validator import LicenseValidator, ValidationResult


P = ParamSpec("P")
R = TypeVar("R")

logger = logging.getLogger(__name__)


class LicenseAuditLogger:
    """Audit logger for license-related events.

    This class provides methods to log license validation events for compliance
    evidence. It uses the AuditService pattern but can operate without a database
    session by falling back to structured logging.

    Events logged:
    - License validation (success/failure)
    - Feature access denied
    - Grace period entered
    - Limit exceeded
    """

    _instance: LicenseAuditLogger | None = None
    _org_id: uuid.UUID | None = None

    def __new__(cls) -> LicenseAuditLogger:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_instance(cls) -> LicenseAuditLogger:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Reset the singleton instance (for testing)."""
        cls._instance = None
        cls._org_id = None

    @classmethod
    def set_org_id(cls, org_id: uuid.UUID | str | None) -> None:
        """Set the organization ID for audit logging."""
        if org_id is None:
            cls._org_id = None
        elif isinstance(org_id, str):
            cls._org_id = uuid.UUID(org_id)
        else:
            cls._org_id = org_id

    def _log_event(
        self,
        action: str,
        resource_id: str,
        description: str,
        status: str = "success",
        error_message: Optional[str] = None,
        changes: Optional[dict[str, Any]] = None,
        extra_metadata: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Log a license audit event.

        Returns the audit entry as a dict for testing/verification.
        """
        entry = {
            "action": action,
            "resource_type": "license",
            "resource_id": resource_id,
            "description": description,
            "status": status,
            "error_message": error_message,
            "changes": changes or {},
            "metadata": extra_metadata or {},
            "org_id": str(self._org_id) if self._org_id else None,
        }

        # Log to structured logger for compliance evidence
        log_level = logging.INFO if status == "success" else logging.WARNING
        logger.log(
            log_level,
            "License audit: %s - %s (status=%s, resource_id=%s)",
            action,
            description,
            status,
            resource_id,
            extra={"audit_entry": entry},
        )

        return entry

    def log_validation_success(
        self,
        license_id: str,
        tier: str,
        org_id: Optional[str] = None,
        in_grace_period: bool = False,
    ) -> dict[str, Any]:
        """Log successful license validation."""
        changes = {
            "tier": tier,
            "org_id": org_id,
            "in_grace_period": in_grace_period,
        }
        return self._log_event(
            action="license_validated",
            resource_id=license_id,
            description=f"License validated successfully for tier '{tier}'",
            status="success",
            changes=changes,
        )

    def log_validation_failure(
        self,
        license_id: str,
        error: str,
    ) -> dict[str, Any]:
        """Log failed license validation."""
        return self._log_event(
            action="license_validation_failed",
            resource_id=license_id,
            description="License validation failed",
            status="failure",
            error_message=error,
        )

    def log_grace_period_entered(
        self,
        license_id: str,
        tier: str,
        days_remaining: Optional[int] = None,
    ) -> dict[str, Any]:
        """Log when a license enters grace period."""
        changes = {
            "tier": tier,
            "days_remaining": days_remaining,
        }
        return self._log_event(
            action="license_grace_period_entered",
            resource_id=license_id,
            description=f"License entered grace period (tier: {tier})",
            status="warning",
            changes=changes,
        )

    def log_feature_access_denied(
        self,
        feature: str,
        current_tier: str,
        required_tier: Optional[str] = None,
    ) -> dict[str, Any]:
        """Log when feature access is denied due to licensing."""
        changes = {
            "feature": feature,
            "current_tier": current_tier,
            "required_tier": required_tier,
        }
        return self._log_event(
            action="feature_access_denied",
            resource_id=feature,
            description=f"Feature '{feature}' access denied (current tier: {current_tier})",
            status="failure",
            changes=changes,
        )

    def log_limit_exceeded(
        self,
        limit_name: str,
        current_value: int,
        maximum: int,
        current_tier: str,
    ) -> dict[str, Any]:
        """Log when a license limit is exceeded."""
        changes = {
            "limit_name": limit_name,
            "current_value": current_value,
            "maximum": maximum,
            "current_tier": current_tier,
        }
        return self._log_event(
            action="limit_exceeded",
            resource_id=limit_name,
            description=f"Limit '{limit_name}' exceeded: {current_value}/{maximum}",
            status="failure",
            changes=changes,
        )


def get_license_audit_logger() -> LicenseAuditLogger:
    """Get the singleton LicenseAuditLogger instance."""
    return LicenseAuditLogger.get_instance()


class LicenseManager:
    _instance: LicenseManager | None = None
    _validator: LicenseValidator | None = None
    _license_payload: LicensePayload | None = None
    _validation_result: ValidationResult | None = None

    def __new__(cls) -> LicenseManager:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def initialize(
        cls,
        public_key: str | None = None,
        license_key: str | None = None,
    ) -> LicenseManager:
        instance = cls()
        public_key = public_key or os.environ.get("LICENSE_PUBLIC_KEY")
        license_key = license_key or os.environ.get("LICENSE_KEY")
        audit_logger = get_license_audit_logger()

        if public_key:
            cls._validator = LicenseValidator(public_key)

            if license_key:
                cls._validation_result = cls._validator.validate(license_key)
                license_id = (
                    license_key[:16] + "..." if len(license_key) > 16 else license_key
                )

                if cls._validation_result.valid:
                    cls._license_payload = cls._validation_result.payload
                    tier = (
                        cls._validation_result.payload.tier.value
                        if cls._validation_result.payload
                        else "unknown"
                    )
                    org_id = (
                        cls._validation_result.payload.sub
                        if cls._validation_result.payload
                        else None
                    )

                    audit_logger.log_validation_success(
                        license_id=license_id,
                        tier=tier,
                        org_id=org_id,
                        in_grace_period=cls._validation_result.in_grace_period,
                    )

                    if cls._validation_result.in_grace_period:
                        audit_logger.log_grace_period_entered(
                            license_id=license_id,
                            tier=tier,
                        )
                else:
                    audit_logger.log_validation_failure(
                        license_id=license_id,
                        error=cls._validation_result.error
                        or "Unknown validation error",
                    )

        return instance

    @classmethod
    def reset(cls) -> None:
        cls._instance = None
        cls._validator = None
        cls._license_payload = None
        cls._validation_result = None

    @classmethod
    def get_instance(cls) -> LicenseManager:
        if cls._instance is None:
            cls.initialize()
        return cls._instance

    @property
    def is_licensed(self) -> bool:
        return self._license_payload is not None

    @property
    def tier(self) -> LicenseTier:
        if self._license_payload:
            return self._license_payload.tier
        return LicenseTier.COMMUNITY

    @property
    def in_grace_period(self) -> bool:
        if self._validation_result:
            return self._validation_result.in_grace_period
        return False

    @property
    def payload(self) -> LicensePayload | None:
        return self._license_payload


def get_license_manager() -> LicenseManager:
    return LicenseManager.get_instance()


def get_entitlements() -> dict:
    """Get current license entitlements from the JWT-backed LicenseManager."""
    manager = get_license_manager()
    tier = manager.tier

    if manager.payload:
        features = manager.payload.features
        limits = manager.payload.limits
    else:
        features = DEFAULT_FEATURES[tier]
        limits = DEFAULT_LIMITS[tier]

    return {
        "tier": tier.value,
        "features": features,
        "limits": {
            "users": limits.users,
            "repos": limits.repos,
            "api_rate": limits.api_rate,
        },
        "is_licensed": manager.is_licensed,
        "in_grace_period": manager.in_grace_period,
    }


def has_feature(feature: str, *, log_denial: bool = True) -> bool:
    """Check if a feature is available via the JWT-backed LicenseManager."""
    manager = get_license_manager()

    if manager.payload:
        has_it = manager.payload.features.get(feature, False)
    else:
        has_it = DEFAULT_FEATURES[manager.tier].get(feature, False)

    if not has_it and log_denial:
        audit_logger = get_license_audit_logger()
        audit_logger.log_feature_access_denied(
            feature=feature,
            current_tier=manager.tier.value,
        )

    return has_it


def check_limit(
    limit_name: str, current_value: int, *, log_exceeded: bool = True
) -> bool:
    manager = get_license_manager()

    if manager.payload:
        limit = getattr(manager.payload.limits, limit_name, None)
    else:
        limit = getattr(DEFAULT_LIMITS[manager.tier], limit_name, None)

    if limit is None:
        return False
    if limit == -1:
        return True

    within_limit = current_value <= limit
    if not within_limit and log_exceeded:
        audit_logger = get_license_audit_logger()
        audit_logger.log_limit_exceeded(
            limit_name=limit_name,
            current_value=current_value,
            maximum=limit,
            current_tier=manager.tier.value,
        )

    return within_limit


def get_limit(limit_name: str) -> int:
    manager = get_license_manager()

    if manager.payload:
        return getattr(manager.payload.limits, limit_name, 0)

    return getattr(DEFAULT_LIMITS[manager.tier], limit_name, 0)


class FeatureNotLicensedError(Exception):
    def __init__(self, feature: str, required_tier: str | None = None):
        self.feature = feature
        self.required_tier = required_tier
        msg = f"Feature '{feature}' is not available"
        if required_tier:
            msg += f" (requires {required_tier} tier)"
        super().__init__(msg)


class LimitExceededError(Exception):
    def __init__(self, limit_name: str, current: int, maximum: int):
        self.limit_name = limit_name
        self.current = current
        self.maximum = maximum
        super().__init__(f"Limit '{limit_name}' exceeded: {current}/{maximum}")


def require_feature(
    feature: str,
    *,
    required_tier: str | None = None,
    raise_http: bool = True,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def _deny() -> None:
        current_tier = get_license_manager().tier.value
        if raise_http:
            raise HTTPException(
                status_code=status.HTTP_402_PAYMENT_REQUIRED,
                detail={
                    "error": "feature_not_licensed",
                    "feature": feature,
                    "required_tier": required_tier,
                    "current_tier": current_tier,
                },
            )
        raise FeatureNotLicensedError(feature, required_tier)

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            if not has_feature(feature):
                _deny()
            return func(*args, **kwargs)

        @functools.wraps(func)
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            if not has_feature(feature):
                _deny()
            return await func(*args, **kwargs)

        import inspect

        if inspect.iscoroutinefunction(func):
            async_wrapper._require_feature = feature  # type: ignore[attr-defined]
            return async_wrapper  # type: ignore
        wrapper._require_feature = feature  # type: ignore[attr-defined]
        return wrapper

    return decorator


def require_limit(
    limit_name: str,
    get_current: Callable[[], int],
    *,
    raise_http: bool = True,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            current = get_current()
            if not check_limit(limit_name, current):
                maximum = get_limit(limit_name)
                if raise_http:
                    raise HTTPException(
                        status_code=status.HTTP_402_PAYMENT_REQUIRED,
                        detail={
                            "error": "limit_exceeded",
                            "limit": limit_name,
                            "current": current,
                            "maximum": maximum,
                            "current_tier": get_license_manager().tier.value,
                        },
                    )
                raise LimitExceededError(limit_name, current, maximum)
            return func(*args, **kwargs)

        @functools.wraps(func)
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            current = get_current()
            if not check_limit(limit_name, current):
                maximum = get_limit(limit_name)
                if raise_http:
                    raise HTTPException(
                        status_code=status.HTTP_402_PAYMENT_REQUIRED,
                        detail={
                            "error": "limit_exceeded",
                            "limit": limit_name,
                            "current": current,
                            "maximum": maximum,
                            "current_tier": get_license_manager().tier.value,
                        },
                    )
                raise LimitExceededError(limit_name, current, maximum)
            return await func(*args, **kwargs)

        import inspect

        if inspect.iscoroutinefunction(func):
            return async_wrapper  # type: ignore
        return wrapper

    return decorator

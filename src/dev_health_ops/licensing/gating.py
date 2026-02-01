from __future__ import annotations

import functools
import os
from typing import Callable, ParamSpec, TypeVar

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

        if public_key:
            cls._validator = LicenseValidator(public_key)

            if license_key:
                cls._validation_result = cls._validator.validate(license_key)
                if cls._validation_result.valid:
                    cls._license_payload = cls._validation_result.payload

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


def has_feature(feature: str) -> bool:
    manager = get_license_manager()

    if manager.payload:
        return manager.payload.features.get(feature, False)

    return DEFAULT_FEATURES[manager.tier].get(feature, False)


def check_limit(limit_name: str, current_value: int) -> bool:
    manager = get_license_manager()

    if manager.payload:
        limit = getattr(manager.payload.limits, limit_name, None)
    else:
        limit = getattr(DEFAULT_LIMITS[manager.tier], limit_name, None)

    if limit is None:
        return False
    if limit == -1:
        return True
    return current_value <= limit


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
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            if not has_feature(feature):
                if raise_http:
                    raise HTTPException(
                        status_code=status.HTTP_402_PAYMENT_REQUIRED,
                        detail={
                            "error": "feature_not_licensed",
                            "feature": feature,
                            "required_tier": required_tier,
                            "current_tier": get_license_manager().tier.value,
                        },
                    )
                raise FeatureNotLicensedError(feature, required_tier)
            return func(*args, **kwargs)

        @functools.wraps(func)
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            if not has_feature(feature):
                if raise_http:
                    raise HTTPException(
                        status_code=status.HTTP_402_PAYMENT_REQUIRED,
                        detail={
                            "error": "feature_not_licensed",
                            "feature": feature,
                            "required_tier": required_tier,
                            "current_tier": get_license_manager().tier.value,
                        },
                    )
                raise FeatureNotLicensedError(feature, required_tier)
            return await func(*args, **kwargs)

        import inspect

        if inspect.iscoroutinefunction(func):
            return async_wrapper  # type: ignore
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

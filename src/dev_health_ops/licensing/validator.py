from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass

from nacl.exceptions import BadSignatureError
from nacl.signing import VerifyKey
from pydantic import ValidationError

from dev_health_ops.licensing.types import LicensePayload


class LicenseValidationError(Exception):
    pass


class LicenseExpiredError(LicenseValidationError):
    pass


class LicenseSignatureError(LicenseValidationError):
    pass


@dataclass
class ValidationResult:
    valid: bool
    payload: LicensePayload | None = None
    error: str | None = None
    in_grace_period: bool = False


class LicenseValidator:
    def __init__(self, public_key_base64: str):
        try:
            public_key_bytes = base64.b64decode(public_key_base64)
            self._verify_key = VerifyKey(public_key_bytes)
        except Exception as e:
            raise LicenseValidationError(f"Invalid public key: {e}") from e

    def validate(
        self,
        license_str: str,
        *,
        check_expiry: bool = True,
        current_time: int | None = None,
    ) -> ValidationResult:
        if current_time is None:
            current_time = int(time.time())

        parts = license_str.strip().split(".")
        if len(parts) != 2:
            return ValidationResult(
                valid=False,
                error="Invalid license format: expected <payload>.<signature>",
            )

        payload_b64, signature_b64 = parts

        try:
            payload_bytes = base64.b64decode(payload_b64)
            signature_bytes = base64.b64decode(signature_b64)
        except Exception:
            return ValidationResult(valid=False, error="Invalid base64 encoding")

        try:
            self._verify_key.verify(payload_bytes, signature_bytes)
        except BadSignatureError:
            return ValidationResult(valid=False, error="Invalid signature")

        try:
            payload_dict = json.loads(payload_bytes.decode("utf-8"))
            payload = LicensePayload.model_validate(payload_dict)
        except json.JSONDecodeError:
            return ValidationResult(valid=False, error="Invalid JSON in payload")
        except ValidationError as e:
            return ValidationResult(valid=False, error=f"Invalid payload schema: {e}")

        if check_expiry:
            expiry_status = self._check_expiry(payload, current_time)
            if expiry_status == "expired":
                return ValidationResult(
                    valid=False,
                    payload=payload,
                    error="License expired (past grace period)",
                )
            elif expiry_status == "grace":
                return ValidationResult(
                    valid=True,
                    payload=payload,
                    in_grace_period=True,
                )

        return ValidationResult(valid=True, payload=payload)

    def _check_expiry(self, payload: LicensePayload, current_time: int) -> str:
        if current_time <= payload.exp:
            return "valid"

        grace_end = payload.exp + (payload.grace_days * 24 * 60 * 60)
        if current_time <= grace_end:
            return "grace"

        return "expired"

    def is_expired(
        self, payload: LicensePayload, current_time: int | None = None
    ) -> bool:
        if current_time is None:
            current_time = int(time.time())
        return self._check_expiry(payload, current_time) == "expired"

    def in_grace_period(
        self, payload: LicensePayload, current_time: int | None = None
    ) -> bool:
        if current_time is None:
            current_time = int(time.time())
        return self._check_expiry(payload, current_time) == "grace"

    def has_feature(self, payload: LicensePayload, feature: str) -> bool:
        return payload.features.get(feature, False)

    def check_limit(
        self, payload: LicensePayload, limit_name: str, current_value: int
    ) -> bool:
        limit = getattr(payload.limits, limit_name, None)
        if limit is None:
            return False
        if limit == -1:
            return True
        return current_value <= limit

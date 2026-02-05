from __future__ import annotations

from dev_health_ops.api.utils.numeric import (
    delta_pct,
    safe_float,
    safe_optional_float,
    safe_transform,
)


class TestSafeFloat:
    def test_valid_float(self):
        assert safe_float(3.14) == 3.14

    def test_valid_int(self):
        assert safe_float(42) == 42.0

    def test_valid_string(self):
        assert safe_float("3.14") == 3.14

    def test_invalid_string_returns_default(self):
        assert safe_float("invalid") == 0.0

    def test_none_returns_default(self):
        assert safe_float(None) == 0.0

    def test_custom_default(self):
        assert safe_float("invalid", default=-1.0) == -1.0

    def test_infinity_returns_default(self):
        assert safe_float(float("inf")) == 0.0

    def test_negative_infinity_returns_default(self):
        assert safe_float(float("-inf")) == 0.0

    def test_nan_returns_default(self):
        assert safe_float(float("nan")) == 0.0


class TestSafeOptionalFloat:
    def test_valid_float(self):
        assert safe_optional_float(3.14) == 3.14

    def test_valid_int(self):
        assert safe_optional_float(42) == 42.0

    def test_valid_string(self):
        assert safe_optional_float("3.14") == 3.14

    def test_invalid_string_returns_none(self):
        assert safe_optional_float("invalid") is None

    def test_none_returns_none(self):
        assert safe_optional_float(None) is None

    def test_infinity_returns_none(self):
        assert safe_optional_float(float("inf")) is None

    def test_nan_returns_none(self):
        assert safe_optional_float(float("nan")) is None


class TestSafeTransform:
    def test_identity_transform(self):
        assert safe_transform(lambda x: x, 5.0) == 5.0

    def test_multiply_transform(self):
        assert safe_transform(lambda x: x * 2, 5.0) == 10.0

    def test_divide_transform(self):
        assert safe_transform(lambda x: x / 24.0, 48.0) == 2.0

    def test_transform_returns_infinity_gives_default(self):
        assert safe_transform(lambda x: x / 0.0 if x == 0 else x, 1.0) == 1.0
        result = safe_transform(lambda x: float("inf"), 1.0)
        assert result == 0.0

    def test_transform_returns_nan_gives_default(self):
        result = safe_transform(lambda x: float("nan"), 1.0)
        assert result == 0.0


class TestDeltaPct:
    def test_positive_change(self):
        assert delta_pct(150.0, 100.0) == 50.0

    def test_negative_change(self):
        assert delta_pct(50.0, 100.0) == -50.0

    def test_no_change(self):
        assert delta_pct(100.0, 100.0) == 0.0

    def test_zero_previous_returns_zero(self):
        assert delta_pct(100.0, 0.0) == 0.0

    def test_both_zero_returns_zero(self):
        assert delta_pct(0.0, 0.0) == 0.0

    def test_doubling(self):
        assert delta_pct(200.0, 100.0) == 100.0

    def test_halving(self):
        assert delta_pct(50.0, 100.0) == -50.0

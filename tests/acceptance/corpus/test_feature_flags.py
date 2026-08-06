"""Unit coverage for ``scripts.acceptance.corpus.feature_flags``.

Codex adversarial review (HIGH, confirmed): the first version of this module
proved only that an ORG OVERRIDE row existed and was enabled, and the corpus
then stamped ``measured_wave_3_1_preflight_path`` on every receipt from that
alone. Production does not make that decision -- ``evaluate_org_feature_async``
also fails closed when the FeatureFlag row itself is globally disabled. So a
global kill-switch could leave the runtime on the pre-3292 legacy path while
every receipt certified the Wave 3.1 path had been measured: exactly the
false-confidence class this module exists to eliminate, reproduced inside the
fix for it. ``prepare_ask_dev_acceptance.py`` already guards this
(``required feature flags are globally disabled``); mirroring the smokes'
override sequence had silently dropped it.
"""

from __future__ import annotations

from typing import Any

import pytest

from scripts.acceptance.corpus.feature_flags import (
    WAVE_3_1_FEATURE_KEY,
    FeatureEnablementError,
    enable_wave_3_1,
    verify_wave_3_1_enabled,
)

_ORG = "org-1"
_FLAG_ID = "flag-1"


class _FakeApi:
    """Minimal admin-API double recording the calls the helpers make."""

    def __init__(
        self,
        *,
        flag_globally_enabled: bool = True,
        flag_present: bool = True,
        override_enabled: bool | None = None,
    ) -> None:
        self.flag_globally_enabled = flag_globally_enabled
        self.flag_present = flag_present
        self.override_enabled = override_enabled
        self.calls: list[tuple[str, str]] = []

    def request(
        self, method: str, path: str, payload: dict[str, Any] | None = None
    ) -> Any:
        self.calls.append((method, path))
        if path == "/api/v1/admin/feature-flags":
            if not self.flag_present:
                return []
            return [
                {
                    "id": _FLAG_ID,
                    "key": WAVE_3_1_FEATURE_KEY,
                    "is_enabled": self.flag_globally_enabled,
                }
            ]
        if path.endswith("/feature-overrides"):
            if method == "GET":
                if self.override_enabled is None:
                    return []
                return [
                    {
                        "id": "override-1",
                        "feature_key": WAVE_3_1_FEATURE_KEY,
                        "is_enabled": self.override_enabled,
                    }
                ]
            self.override_enabled = bool((payload or {}).get("is_enabled"))
            return {"id": "override-1", "is_enabled": self.override_enabled}
        if "/feature-overrides/" in path:
            self.override_enabled = bool((payload or {}).get("is_enabled"))
            return {"id": "override-1", "is_enabled": self.override_enabled}
        raise AssertionError(f"unexpected request {method} {path}")


class TestGloballyDisabledFlag:
    """The Codex HIGH finding, as an executable guard."""

    def test_enable_refuses_when_the_flag_is_globally_disabled(self) -> None:
        api = _FakeApi(flag_globally_enabled=False)
        with pytest.raises(FeatureEnablementError, match="globally disabled"):
            enable_wave_3_1(api, org_id=_ORG)

    def test_verify_refuses_when_the_flag_is_globally_disabled(self) -> None:
        """The killer shape: the ORG OVERRIDE says enabled, so the old
        read-back was satisfied -- but production's own
        ``evaluate_org_feature_async`` still resolves the feature as OFF
        because the global row is disabled. Verification that cannot see this
        certifies a legacy-path run as a Wave 3.1 run."""

        api = _FakeApi(flag_globally_enabled=False, override_enabled=True)
        with pytest.raises(FeatureEnablementError, match="globally disabled"):
            verify_wave_3_1_enabled(api, org_id=_ORG)


class TestHappyPath:
    def test_creates_an_override_when_none_exists(self) -> None:
        api = _FakeApi(override_enabled=None)
        enable_wave_3_1(api, org_id=_ORG)
        verify_wave_3_1_enabled(api, org_id=_ORG)
        assert api.override_enabled is True

    def test_is_idempotent_when_already_enabled(self) -> None:
        api = _FakeApi(override_enabled=True)
        enable_wave_3_1(api, org_id=_ORG)
        verify_wave_3_1_enabled(api, org_id=_ORG)
        assert api.override_enabled is True
        assert ("POST", f"/api/v1/admin/orgs/{_ORG}/feature-overrides") not in api.calls

    def test_patches_a_disabled_override(self) -> None:
        api = _FakeApi(override_enabled=False)
        enable_wave_3_1(api, org_id=_ORG)
        verify_wave_3_1_enabled(api, org_id=_ORG)
        assert api.override_enabled is True

    def test_raises_when_the_flag_is_not_registered(self) -> None:
        api = _FakeApi(flag_present=False)
        with pytest.raises(FeatureEnablementError, match="not registered"):
            enable_wave_3_1(api, org_id=_ORG)

"""CHAOS-2670 Phase-0 foundation contracts.

Locks the additive, non-behavior-changing foundation that the parallel streams
build on: the ``AUTH_AUTO_CREATE_ORG_ON_REGISTER`` flag resolver (C5), the
nullable ``RegisterResponse.org_id`` (C5), and the frozen C1/C2 response models.
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.admin.schemas_flat import (
    OnboardingStateResponse,
    SetupStatusResponse,
)
from dev_health_ops.api.auth.config import auth_auto_create_org_on_register
from dev_health_ops.api.auth.routers.register import RegisterResponse


def test_auth_auto_create_org_defaults_true_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AUTH_AUTO_CREATE_ORG_ON_REGISTER", raising=False)
    assert auth_auto_create_org_on_register() is True


@pytest.mark.parametrize("value", ["false", "0", "no", "off", "False", "  OFF "])
def test_auth_auto_create_org_falsy_values_disable(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("AUTH_AUTO_CREATE_ORG_ON_REGISTER", value)
    assert auth_auto_create_org_on_register() is False


@pytest.mark.parametrize("value", ["true", "1", "yes", "on", "TRUE", " On "])
def test_auth_auto_create_org_truthy_values_enable(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("AUTH_AUTO_CREATE_ORG_ON_REGISTER", value)
    assert auth_auto_create_org_on_register() is True


def test_auth_auto_create_org_unknown_falls_back_to_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AUTH_AUTO_CREATE_ORG_ON_REGISTER", "banana")
    assert auth_auto_create_org_on_register() is True
    assert auth_auto_create_org_on_register(default=False) is False


def test_register_response_org_id_is_optional() -> None:
    orgless = RegisterResponse(message="ok", user_id="u1")
    assert orgless.org_id is None
    with_org = RegisterResponse(message="ok", user_id="u1", org_id="o1")
    assert with_org.org_id == "o1"


def test_onboarding_state_response_contract() -> None:
    resp = OnboardingStateResponse(
        needs_onboarding=True,
        org_created=False,
        first_integration_connected=False,
        integration_skipped=False,
        next_step="workspace",
    )
    assert resp.recommended_provider == "github"
    assert resp.org_id is None
    assert resp.blocker is None


def test_setup_status_response_contract_defaults() -> None:
    resp = SetupStatusResponse(
        has_integration=False,
        has_sync_config=False,
        first_sync_started=False,
        next_action="connect_integration",
    )
    assert resp.providers == []
    assert resp.first_sync_completed is False
    assert resp.sync_status == "none"
    assert resp.selected_repositories_count == 0
    assert resp.can_start_sync is False

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from scripts.acceptance.prepare_ask_dev_acceptance import (
    AcceptanceFailure,
    prepare,
)

_ROOT = Path(__file__).resolve().parents[2]
_OVERLAY = _ROOT / "tests" / "acceptance" / "compose.ask-dev.yml"
_LAUNCHER = _ROOT / "scripts" / "acceptance" / "run_ask_dev_compose.sh"
_PREPARE = _ROOT / "scripts" / "acceptance" / "prepare_ask_dev_acceptance.py"
_ORACLE = _ROOT / "tests" / "acceptance" / "ask-dev-oracle.v1.json"


class _ComposeLoader(yaml.SafeLoader):
    pass


_ComposeLoader.add_constructor(
    "!reset", lambda loader, node: loader.construct_sequence(node)
)
_ComposeLoader.add_constructor(
    "!override", lambda loader, node: loader.construct_sequence(node)
)


def _load_overlay() -> dict[str, Any]:
    return yaml.load(_OVERLAY.read_text(encoding="utf-8"), Loader=_ComposeLoader)


def test_scripted_openai_service_is_profiled_internal_and_unpublished() -> None:
    document = _load_overlay()
    service = document["services"]["ask-dev-scripted-openai"]
    assert service["profiles"] == ["ask-dev-acceptance"]
    assert "ports" not in service
    assert service["expose"] == ["8001"]
    assert service["networks"] == ["ask-dev-acceptance"]
    assert document["networks"]["ask-dev-acceptance"]["internal"] is True
    assert service["build"]["context"] == "."
    assert service["entrypoint"] == [
        "python",
        "-m",
        "dev_health_ops.llm.agent.scripted_openai_service",
    ]
    healthcheck = service["healthcheck"]
    assert healthcheck["test"][-1] == "http://localhost:8001/healthz"
    assert healthcheck["retries"] > 1


def test_api_acceptance_configuration_is_exact_and_network_scoped() -> None:
    document = _load_overlay()
    api = document["services"]["api"]
    assert api["environment"] == {
        "ENVIRONMENT": "acceptance",
        "ASK_DEV_LIVE_ACCEPTANCE": "1",
        "LLM_PROVIDER": "openai",
        "ASK_DEV_ACCEPTANCE_OPENAI_BASE_URL": (
            "http://ask-dev-scripted-openai:8001/v1"
        ),
        "ASK_DEV_ACCEPTANCE_OPENAI_API_KEY": "ask-dev-acceptance-local-v1",
        "JWT_SECRET_KEY": "ask-dev-acceptance-jwt-secret-key-v1",
    }
    assert api["networks"] == ["default", "ask-dev-acceptance"]
    assert api["depends_on"]["ask-dev-scripted-openai"] == {
        "condition": "service_healthy"
    }
    assert api["ports"] == ["127.0.0.1:18080:8000"]


def test_web_is_compose_owned_and_points_only_at_the_ops_service() -> None:
    document = _load_overlay()
    web = document["services"]["web"]
    assert web["profiles"] == ["ask-dev-acceptance"]
    assert web["build"]["context"].startswith("${ASK_DEV_WEB_CONTEXT:?")
    assert web["build"]["target"] == "runner"
    assert web["build"]["args"] == {"BACKEND_URL": "http://api:8000"}
    assert web["environment"]["BACKEND_URL"] == "http://api:8000"
    assert web["ports"] == ["127.0.0.1:3002:3000"]
    assert web["depends_on"]["api"] == {"condition": "service_healthy"}


def test_only_api_and_web_publish_host_ports() -> None:
    document = _load_overlay()
    for service_name in ("postgres", "pgbouncer", "clickhouse", "valkey"):
        assert document["services"][service_name]["ports"] == []
    assert "ports" not in document["services"]["ask-dev-scripted-openai"]
    assert document["services"]["api"]["ports"] == ["127.0.0.1:18080:8000"]
    assert document["services"]["web"]["ports"] == ["127.0.0.1:3002:3000"]


def test_launcher_owns_seed_readiness_web_and_fixed_browser_oracle() -> None:
    launcher = _LAUNCHER.read_text(encoding="utf-8")
    assert '"${1:-}" != "--web-root"' in launcher
    assert '"$#" -ne 2' in launcher
    assert "--profile ask-dev-acceptance" in launcher
    assert '--project-name "${project_name}"' in launcher
    assert "down --volumes --remove-orphans" in launcher
    assert "up -d --build --wait" in launcher
    assert "dev-hops fixtures generate" in launcher
    assert "--with-metrics" in launcher
    assert "--with-work-graph" in launcher
    assert "--days 28" in launcher
    assert "prepare_ask_dev_acceptance.py" in launcher
    assert "up -d --build --wait web" in launcher
    assert "ASK_DEV_COMPOSE_WEB_READY=1" in launcher
    assert "oracle_file=" in launcher
    assert "read_oracle_field question" in launcher
    assert "read_oracle_field expected_metric_id" in launcher
    assert "read_oracle_field expected_evidence_entity_fragment" in launcher
    assert "read_oracle_field expected_claim_kind" in launcher
    assert 'ASK_DEV_ACCEPTANCE_QUESTION="${acceptance_question}"' in launcher
    assert '"${web_root}/node_modules/.bin/playwright" test' in launcher
    assert "playwright.ask-dev-acceptance.config.ts" in launcher
    assert "The deterministic acceptance provider grounded" not in launcher
    assert '"$@"' not in launcher
    assert "completed successfully" in launcher


def test_launcher_runs_the_not_found_smoke_before_bringing_up_web() -> None:
    launcher = _LAUNCHER.read_text(encoding="utf-8")
    assert "smoke_ask_dev_not_found.py" in launcher
    prepare_index = launcher.index("prepare_ask_dev_acceptance.py")
    smoke_index = launcher.index("smoke_ask_dev_not_found.py")
    web_index = launcher.index("up -d --build --wait web")
    # CHAOS-3300: proves the not-found original defect reproduction over the
    # live HTTP/SSE API strictly after readiness is established and strictly
    # before web is even brought up -- it needs neither.
    assert prepare_index < smoke_index < web_index
    # Live-validated 2026-08-02: without this, the smoke script's own
    # `from scripts.acceptance...` import fails with ModuleNotFoundError
    # (the script's directory, not ops_root, is on sys.path by default).
    assert 'PYTHONPATH="${ops_root}/src:${ops_root}"' in launcher


def test_oracle_is_versioned_and_requires_exact_grounded_answer_parts() -> None:
    oracle = json.loads(_ORACLE.read_text(encoding="utf-8"))
    assert oracle == {
        "schema_version": "ask_dev_acceptance_oracle.v1",
        "question": (
            "How did completed work change in this scope during the selected time "
            "range, and what evidence supports it?"
        ),
        "expected_metric_id": "items_completed",
        "expected_evidence_entity_fragment": "meridian/web-app",
        "expected_claim_kind": "observed",
        "model": "ask-dev-scripted-v1",
        "required_tool_ids": [
            "query_metric.v1",
            "search_evidence.v1",
            "data_health.v1",
        ],
    }


def test_readiness_bootstrap_uses_public_http_contracts() -> None:
    prepare = _PREPARE.read_text(encoding="utf-8")
    assert "/api/v1/auth/login" in prepare
    assert "/api/v1/admin/feature-flags" in prepare
    assert "/feature-overrides" in prepare
    # CHAOS-3265: platform certification now runs through the Platform Admin
    # surface, not the org-scoped (now-410) /admin/ask-dev/readiness route.
    assert "/api/v1/admin/platform/ask-dev/readiness" in prepare
    assert "/api/v1/dev/capabilities" in prepare
    assert "ask-dev-scripted-v1" in prepare


class _FakeAcceptanceApi:
    token: str | None = None

    def __init__(
        self,
        *,
        disabled_key: str | None = None,
        model: str = "ask-dev-scripted-v1",
    ) -> None:
        self.disabled_key = disabled_key
        self.model = model
        self.calls: list[tuple[str, str, dict[str, Any] | None]] = []

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        self.calls.append((method, path, payload))
        if path == "/api/v1/auth/login":
            return {
                "access_token": "token",
                "user": {
                    "is_superuser": True,
                    "org_id": "0a155cab-8833-42ac-a4ef-0d121725a7b0",
                },
            }
        if path == "/api/v1/admin/feature-flags":
            return [
                {
                    "id": f"id-{key}",
                    "key": key,
                    "is_enabled": key != self.disabled_key,
                }
                for key in (
                    "ask_dev",
                    "ask_dev_contextual_entrypoints",
                )
            ]
        if path.endswith("/feature-overrides") and method == "GET":
            return []
        if path.endswith("/feature-overrides") and method == "POST":
            return {"is_enabled": True}
        if path == "/api/v1/admin/platform/ask-dev/readiness":
            return {
                "schema_version": "platform_ask_dev_readiness.v1",
                "configured": True,
                "provider_label": "OpenAI compatible",
                "model_label": self.model,
                "readiness": "ready",
                "readiness_checked_at": "2026-07-29T00:00:00+00:00",
                "readiness_version": "v1",
                "safe_remediation": None,
            }
        if path == "/api/v1/dev/capabilities":
            return {
                "ask_dev": True,
                "agent_context_runtime": False,
                "can_read": True,
                "can_manage": True,
                "contextual_entrypoints": True,
                "evidence_resolver": True,
                "provider_source": "platform",
                "readiness": "ready",
                "effective_model_label": self.model,
            }
        raise AssertionError(f"unexpected request: {method} {path}")


def test_readiness_bootstrap_enables_all_features_and_proves_capabilities() -> None:
    api = _FakeAcceptanceApi()
    org_id = prepare(api, email="admin@devhealth.example", password="devhealth123")  # type: ignore[arg-type]
    assert org_id == "0a155cab-8833-42ac-a4ef-0d121725a7b0"
    created_features = [
        payload["feature_id"]
        for method, path, payload in api.calls
        if method == "POST" and path.endswith("/feature-overrides") and payload
    ]
    assert created_features == [
        "id-ask_dev",
        "id-ask_dev_contextual_entrypoints",
    ]
    assert api.calls[-2][1] == "/api/v1/admin/platform/ask-dev/readiness"
    assert api.calls[-1][1] == "/api/v1/dev/capabilities"


def test_readiness_bootstrap_fails_closed_on_global_kill_switch() -> None:
    api = _FakeAcceptanceApi(disabled_key="ask_dev")
    with pytest.raises(AcceptanceFailure, match="globally disabled: ask_dev"):
        prepare(api, email="admin@devhealth.example", password="devhealth123")  # type: ignore[arg-type]


def test_readiness_bootstrap_accepts_an_exact_live_profile_model() -> None:
    api = _FakeAcceptanceApi(model="google/gemma-4-e4b")
    org_id = prepare(
        api,  # type: ignore[arg-type]
        email="admin@devhealth.example",
        password="devhealth123",
        expected_model="google/gemma-4-e4b",
    )
    assert org_id == "0a155cab-8833-42ac-a4ef-0d121725a7b0"

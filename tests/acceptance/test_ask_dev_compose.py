from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

from scripts.acceptance.prepare_ask_dev_acceptance import (
    AcceptanceFailure,
    prepare,
    provision_multi_org,
)

_ROOT = Path(__file__).resolve().parents[2]
_OVERLAY = _ROOT / "tests" / "acceptance" / "compose.ask-dev.yml"
_ACR_OVERLAY = _ROOT / "tests" / "acceptance" / "compose.ask-dev-acr.yml"
_BASE_COMPOSE = _ROOT / "compose.yml"
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


def _load_acr_overlay() -> dict[str, Any]:
    return yaml.load(_ACR_OVERLAY.read_text(encoding="utf-8"), Loader=_ComposeLoader)


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
        # CHAOS-3219 Wave 4: the only two Ask Dev settings production code
        # actually reads from the environment (org_policy.py
        # platform_operator_request_limit / ..._cost_limit_microusd).
        # Live-proven 2026-08-05 that pinning these to the documented hard
        # floor (PLATFORM_MONTHLY_REQUEST_LIMIT_MIN=100 /
        # PLATFORM_MONTHLY_COST_LIMIT_MIN_MICROUSD=10_000_000) makes
        # DevMonthlyCostLimitExceeded fire for real -- but the floor broke
        # the existing smoke suite, so the shared default here has headroom
        # instead (still below the operator hard max); Phase 3c's
        # quota-exceeded corpus case should use the floor values, scoped to
        # just that case, not this shared default.
        "ASK_DEV_PLATFORM_MONTHLY_REQUEST_MAX": "1000",
        "ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD": "200000000",
    }
    assert api["networks"] == ["default", "ask-dev-acceptance"]
    assert api["depends_on"]["ask-dev-scripted-openai"] == {
        "condition": "service_healthy"
    }
    # Host port is parameterized so this stack can coexist with the normal
    # dev-health stack (which already publishes 18080). The default must
    # stay 18080, and the bind must stay loopback-only -- a bare "PORT:8000"
    # would expose the acceptance API on every interface.
    assert api["ports"] == ["127.0.0.1:${ASK_DEV_ACCEPTANCE_API_PORT:-18080}:8000"]


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
    assert document["services"]["api"]["ports"] == [
        "127.0.0.1:${ASK_DEV_ACCEPTANCE_API_PORT:-18080}:8000"
    ]
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


# ---------------------------------------------------------------------------
# CHAOS-3219 Wave 4 Phase 1 Lane 1c: required worker/beat, env-hardening,
# optional ACR profile, second-org/disabled-entitlement-org provisioning.
# ---------------------------------------------------------------------------


def test_worker_and_beat_are_required_with_real_healthchecks() -> None:
    document = _load_overlay()
    services = document["services"]
    # Unprofiled (like postgres/api), not gated behind ask-dev-acceptance-acr
    # -- these boot by default, same as the rest of the required set.
    assert "profiles" not in services["worker"]
    assert "profiles" not in services["beat"]

    worker_check = services["worker"]["healthcheck"]
    # "no required job may exit success after skipping substantive work"
    # (plan §5): this must be a real functional receipt, not a bare
    # container-is-running check -- `celery inspect ping` round-trips
    # through the broker to the worker's own consumer loop.
    assert worker_check["test"] == [
        "CMD",
        "celery",
        "-A",
        "dev_health_ops.workers.celery_app",
        "inspect",
        "ping",
    ]
    assert worker_check["retries"] > 1

    beat_check = services["beat"]["healthcheck"]
    # celery beat has no inspect-style RPC; the receipt is that its
    # persistent schedule file is being actively rewritten, not merely that
    # it exists.
    beat_test = beat_check["test"]
    assert beat_test[0] == "CMD-SHELL"
    assert "/tmp/celerybeat-schedule" in beat_test[1]
    assert "-lt 90" in beat_test[1]
    assert beat_check["retries"] > 1


def test_quota_env_documents_what_production_code_actually_reads() -> None:
    """org_policy.py's platform_operator_request_limit / ..._cost_limit_
    microusd (os.getenv) are the ONLY two Ask Dev settings production code
    reads from the environment. Retention (0/30-day) has no env var --
    it's a per-conversation request field plus a settings-DB org default
    (models/dev_persistence.DEV_RETENTION_DAYS, org_policy.
    ASK_DEV_RETENTION_KEY = "ask_dev_retention_days"). This overlay must
    wire the two real env vars and must NOT fabricate a retention env var
    nothing reads -- it must instead document the real settings-DB key so a
    reader knows where retention is actually configured."""
    overlay_text = _OVERLAY.read_text(encoding="utf-8")
    assert "ASK_DEV_PLATFORM_MONTHLY_REQUEST_MAX" in overlay_text
    assert "ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD" in overlay_text
    assert not re.search(r"ASK_DEV_RETENTION_\w*:", overlay_text), (
        "no such env var exists in production -- retention is a settings-DB "
        "org default (ask_dev_retention_days) plus a per-conversation "
        "request field, not an environment variable"
    )
    # Document the real settings-DB key by name so a reader knows how to
    # actually set retention for a corpus case (admin settings API, not env).
    assert "ask_dev_retention_days" in overlay_text


def test_acr_profile_is_off_by_default_and_wired_via_extends() -> None:
    document = _load_acr_overlay()
    services = document["services"]
    for name in ("acr-db-init", "acr-migrate", "acr-api"):
        service = services[name]
        assert service["profiles"] == ["ask-dev-acceptance-acr"]
        assert service["extends"]["service"] == name
        # Reuses the parent repo's authoritative service definition instead
        # of re-declaring it -- cannot silently drift from the source of
        # truth. Default path is configurable, not hardcoded to one
        # operator's checkout layout.
        assert service["extends"]["file"].startswith(
            "${ASK_DEV_ACCEPTANCE_PARENT_COMPOSE:-"
        )
        # The parent's `dev-health` network does not exist in this
        # acceptance project; every extended service must force-replace it.
        assert service["networks"] == ["default"]

    # Never publish acr-api's port from the acceptance overlay -- it must
    # only be reachable over the internal network, matching how
    # ask-dev-scripted-openai and postgres/pgbouncer/clickhouse/valkey are
    # already kept off the host.
    assert services["acr-api"]["ports"] == []
    assert services["acr-api"]["container_name"] == "ask-dev-acceptance-acr-api"

    # The default `ask-dev-acceptance` profile (used by every other test in
    # this module) must not implicitly pull in ACR.
    base_document = _load_overlay()
    assert "acr-api" not in base_document["services"]

    launcher = _LAUNCHER.read_text(encoding="utf-8")
    assert 'acr_armed="${ASK_DEV_ACCEPTANCE_ACR:-0}"' in launcher
    assert '"${acr_armed}" == "1"' in launcher
    assert "ask-dev-acceptance-acr" in launcher
    assert "compose.ask-dev-acr.yml" in launcher


def test_launcher_hardens_compose_interpolation_env_for_every_var_it_boots() -> None:
    """Closed-loop guard for the CHAOS-3219 Phase 0 env-hardening finding:
    every `${VAR}` referenced anywhere in compose.yml or an acceptance
    overlay must either be (a) explicitly unset before the first `docker
    compose` invocation, (b) explicitly exported by the launcher itself
    (a deliberate, launcher-owned value), or (c) namespaced under
    ASK_DEV_ (this launcher's own acceptance-specific knobs, e.g.
    ASK_DEV_ACCEPTANCE_API_PORT, which cannot collide with an unrelated
    ambient dev .env by construction). A future `${NEW_VAR}` added to
    either compose file that isn't triaged into one of these three buckets
    fails this test -- it does not get to silently inherit whatever the
    invoking shell happens to export, the way POSTGRES_DB did.
    """
    launcher = _LAUNCHER.read_text(encoding="utf-8")

    interpolated: set[str] = set()
    for path in (_BASE_COMPOSE, _OVERLAY, _ACR_OVERLAY):
        interpolated |= set(
            re.findall(r"\$\{([A-Z_][A-Z0-9_]*)", path.read_text(encoding="utf-8"))
        )
    assert "POSTGRES_DB" in interpolated, (
        "sanity check: the compose files must still reference ${POSTGRES_DB} "
        "somewhere, or this test would vacuously pass with an empty set"
    )

    unset_match = re.search(r"\nunset \\\n(.*?)\n\nweb_root=", launcher, re.S)
    assert unset_match is not None, "launcher must have a single top-level unset block"
    unset_vars = set(re.findall(r"[A-Z_][A-Z0-9_]*", unset_match.group(1)))
    assert "POSTGRES_DB" in unset_vars

    exported_vars = set(
        re.findall(r"^[ \t]*export ([A-Z_][A-Z0-9_]*)=", launcher, re.M)
    )
    namespaced_vars = {v for v in interpolated if v.startswith("ASK_DEV_")}

    accounted_for = unset_vars | exported_vars | namespaced_vars
    missing = interpolated - accounted_for
    assert not missing, (
        "these ${VAR} references in compose.yml/acceptance overlays are not "
        "unset, exported, or ASK_DEV_-namespaced by the launcher -- an "
        "ambient shell value (e.g. from a direnv-loaded .env) can silently "
        f"reach the acceptance stack's interpolation: {sorted(missing)}"
    )

    # The unset block must run before the first actual `docker compose`
    # invocation ("${compose[@]}" config --quiet) -- otherwise a leaked var
    # could already have been interpolated before the fix takes effect.
    # (Prose in this launcher's own comments mentions the literal words
    # "docker compose" earlier than that, so anchor on the real invocation.)
    assert launcher.index("\nunset \\\n") < launcher.index(
        '"${compose[@]}" config --quiet'
    )


def test_launcher_error_suppression_is_confined_to_failure_diagnostics() -> None:
    """`|| true` anywhere in a REQUIRED boot/test step would let a failed
    command exit success (the exact "required job that boots but processes
    nothing" trap the plan's work-receipt principle exists to catch). The
    only acceptable use is inside report_failure()'s best-effort diagnostic
    dump, which already failed and is just trying to leave useful logs
    behind."""
    launcher = _LAUNCHER.read_text(encoding="utf-8")
    fn_start = launcher.index("report_failure() {")
    fn_end = launcher.index("\n}\n", fn_start)
    report_failure_body = launcher[fn_start:fn_end]

    # Only count "|| true" as CODE if it appears on a non-comment line --
    # this launcher also has a historical comment that quotes the phrase
    # `|| true` in backticks while explaining its *removal*, which is not a
    # live occurrence.
    code_occurrences = [
        m.start()
        for m in re.finditer(re.escape("|| true"), launcher)
        if not launcher[launcher.rfind("\n", 0, m.start()) + 1 : m.start()]
        .lstrip()
        .startswith("#")
    ]
    assert code_occurrences, (
        "sanity check: report_failure's own || true must still be present"
    )
    for pos in code_occurrences:
        assert fn_start <= pos < fn_end, (
            f"'|| true' at offset {pos} falls outside report_failure() -- a "
            "required step may be silently swallowing failure"
        )
    assert "|| true" in report_failure_body


def test_launcher_boots_required_jobs_fleet_and_gates_acr_optionally() -> None:
    launcher = _LAUNCHER.read_text(encoding="utf-8")
    assert (
        "boot_services=(postgres pgbouncer clickhouse valkey migrate ask-dev-scripted-openai api worker beat)"
        in launcher
    )
    assert '"${compose[@]}" up -d --build --wait "${boot_services[@]}"' in launcher
    assert "acr-db-init acr-migrate acr-api" in launcher
    # ACR services are appended to boot_services only inside the arming
    # branch, never unconditionally.
    arm_index = launcher.index('"${acr_armed}" == "1"')
    acr_boot_index = launcher.index("acr-db-init acr-migrate acr-api")
    fi_index = launcher.index("fi\n", arm_index)
    assert arm_index < acr_boot_index < fi_index


def test_launcher_provisions_second_org_and_writes_org_ids_artifact() -> None:
    launcher = _LAUNCHER.read_text(encoding="utf-8")
    assert "org_ids_output=" in launcher
    assert "ASK_DEV_ACCEPTANCE_ORG_IDS_OUTPUT" in launcher
    # Threaded into the same prepare_ask_dev_acceptance.py invocation that
    # already handles the primary org -- no separate second-org script/step
    # to keep in sync with it. Anchor on the quoted invocation path, not the
    # bare filename, since this launcher's own comments also mention the
    # bare filename before the export line.
    prepare_invocation_index = launcher.index(
        '"${ops_root}/scripts/acceptance/prepare_ask_dev_acceptance.py"'
    )
    org_ids_export_index = launcher.index(
        'ASK_DEV_ACCEPTANCE_ORG_IDS_OUTPUT="${org_ids_output}"'
    )
    assert org_ids_export_index < prepare_invocation_index

    prepare_source = _PREPARE.read_text(encoding="utf-8")
    assert "provision_multi_org(" in prepare_source
    assert "def provision_multi_org" in prepare_source


class _MultiOrgFakeAcceptanceApi(_FakeAcceptanceApi):
    """Extends the base fake with org creation + org-scoped login/capability
    responses, keyed by which org_id the caller is currently "logged in
    as" (mirrors the real API: capabilities reflect the token's org)."""

    def __init__(self, *, second_org_enabled: bool = True) -> None:
        super().__init__()
        self._next_org_suffix = 0
        self._created_orgs: dict[str, str] = {}
        self._enabled_orgs: set[str] = {"0a155cab-8833-42ac-a4ef-0d121725a7b0"}
        self._second_org_enabled = second_org_enabled
        self._current_org: str = "0a155cab-8833-42ac-a4ef-0d121725a7b0"

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        if path == "/api/v1/auth/login":
            assert payload is not None
            org_id = payload.get("org_id") or "0a155cab-8833-42ac-a4ef-0d121725a7b0"
            self._current_org = org_id
            self.calls.append((method, path, payload))
            return {
                "access_token": f"token-{org_id}",
                "user": {
                    "id": "11111111-1111-1111-1111-111111111111",
                    "is_superuser": True,
                    "org_id": org_id,
                },
            }
        if path == "/api/v1/admin/orgs" and method == "POST":
            assert payload is not None
            self.calls.append((method, path, payload))
            self._next_org_suffix += 1
            new_org_id = f"org-{self._next_org_suffix}"
            self._created_orgs[new_org_id] = payload["name"]
            return {"id": new_org_id, "name": payload["name"]}
        if path == "/api/v1/dev/capabilities":
            self.calls.append((method, path, payload))
            enabled = self._current_org in self._enabled_orgs or (
                self._current_org in self._created_orgs
                and self._second_org_enabled
                and self._current_org
                == next(iter(self._created_orgs))  # first created == "second org"
            )
            return {
                "ask_dev": enabled,
                "readiness": "ready" if enabled else "disabled",
            }
        if path.endswith("/feature-overrides") and method == "POST":
            self.calls.append((method, path, payload))
            org_id = path.split("/")[-2]
            self._enabled_orgs.add(org_id)
            return {"is_enabled": True}
        return super().request(method, path, payload)


def test_provision_multi_org_enables_second_org_and_verifies_disabled_org() -> None:
    api = _MultiOrgFakeAcceptanceApi()
    second_org_id, disabled_org_id = provision_multi_org(
        api,  # type: ignore[arg-type]
        email="admin@devhealth.example",
        password="devhealth123",
    )
    assert second_org_id == "org-1"
    assert disabled_org_id == "org-2"
    # The disabled org must never have received a feature-overrides POST.
    disabled_overrides = [
        (method, path)
        for method, path, _ in api.calls
        if method == "POST"
        and path == f"/api/v1/admin/orgs/{disabled_org_id}/feature-overrides"
    ]
    assert disabled_overrides == []


def test_provision_multi_org_fails_loud_if_disabled_org_leaks_entitlement() -> None:
    """Negative-control regression guard: if capabilities ever reports the
    never-enabled org as ask_dev-capable (e.g. a global-flag/cache-key
    bug), provision_multi_org must raise -- not silently accept it."""
    api = _MultiOrgFakeAcceptanceApi(second_org_enabled=True)
    # Simulate the leak: enable ask_dev for BOTH created orgs behind the
    # fake's back, same as a real entitlement-isolation bug would.
    original_request = api.request

    def leaking_request(
        method: str, path: str, payload: dict[str, Any] | None = None
    ) -> Any:
        result = original_request(method, path, payload)
        if path == "/api/v1/dev/capabilities" and isinstance(result, dict):
            result = {**result, "ask_dev": True, "readiness": "ready"}
        return result

    api.request = leaking_request  # type: ignore[method-assign]
    with pytest.raises(AcceptanceFailure, match="unexpectedly reports ask_dev"):
        provision_multi_org(
            api,  # type: ignore[arg-type]
            email="admin@devhealth.example",
            password="devhealth123",
        )

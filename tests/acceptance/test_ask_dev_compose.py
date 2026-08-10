from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import uuid
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
_RESOLVE_ACR_SCRIPT = _ROOT / "scripts" / "acceptance" / "resolve_acr_parent_compose.sh"
_WORKER_PROBE_SCRIPT = _ROOT / "scripts" / "acceptance" / "healthcheck_worker_probe.py"
_BEAT_SENTINEL_SCRIPT = (
    _ROOT / "scripts" / "acceptance" / "healthcheck_beat_sentinel.py"
)


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
    probe = "\n".join(str(part) for part in healthcheck["test"])
    assert "http://localhost:8001/healthz" in probe
    assert healthcheck["retries"] > 1
    # CHAOS-3219 Phase 3 (codex adversarial review, MEDIUM, confirmed): this
    # used to assert only that the probe named the URL. /healthz answers 200
    # even with a dead engine, so a status-code-only probe reported the
    # container healthy while it could serve no scripted case at all -- and
    # `api` is gated on this healthcheck, so the whole stack came up and the
    # launcher's smoke/web leg ran against the unscripted heuristic. The
    # probe must inspect the BODY.
    assert "loaded" in probe, (
        "the healthcheck must verify the scripted engine actually LOADED, "
        f"not merely that /healthz answers: {probe!r}"
    )


def test_scripted_provider_can_actually_reach_its_script_directory() -> None:
    """CHAOS-3219 Phase 3. The scripted provider IS the fault/decision matrix
    (D4: "scripted only" is launch-blocking), and it was reaching none of it.

    Measured, not reasoned -- executed against the very image the 2026-08-07
    04:55 armed run used::

        scripts_dir = tests/.../provider-scripts | exists = False
        try_load_engine('legacy_agent') -> None
        load_registry_ids() raises UnmappedCaseError scripts_directory_unavailable

    The chain: this overlay declared no ``volumes:`` at all and set neither
    scripted-provider env var; ``docker/Dockerfile``'s ``api`` target ships
    only the built wheel, no ``tests/`` tree; so ``_scripts_dir()`` resolved a
    RELATIVE path against WORKDIR ``/app`` and missed. ``try_load_engine``
    then swallows that and returns ``None`` by design -- correct for organic
    production traffic, catastrophic here -- and every scripted case fell
    through to the unscripted default heuristic. All 19 scripted cases (10
    faults + 9 decision scripts) recorded ``dev_answer.v1`` COMPLETED and
    PASSED, having exercised no fault at all.

    Nothing failed when the fault matrix stopped existing. This test is why
    it now would.

    The mount supplies the files; the env var makes the lookup EXPLICIT
    rather than leaving it to a relative walk-up from the working directory,
    which is precisely what failed silently. A literal path (not a
    ``${VAR}`` interpolation) is deliberate -- an interpolation would also
    need triaging into ``run_ask_dev_compose.sh``'s unset list, per
    ``test_launcher_hardens_compose_interpolation_env_for_every_var_it_boots``.
    """

    document = _load_overlay()
    service = document["services"]["ask-dev-scripted-openai"]

    scripts_dir = service["environment"]["ASK_DEV_SCRIPTED_PROVIDER_SCRIPTS_DIR"]
    assert scripts_dir, "the scripted provider must be told where its scripts are"

    mounts = service["volumes"]
    matching = [mount for mount in mounts if mount.split(":")[1] == scripts_dir]
    assert matching, (
        f"nothing mounts the declared scripts dir {scripts_dir!r} into the "
        f"container -- the env var would point at an empty path. mounts={mounts!r}"
    )
    assert matching[0].endswith(":ro"), (
        "the corpus fixtures are an oracle the provider reads, never writes -- "
        f"mount them read-only: {matching[0]!r}"
    )

    # Guard against a mount that satisfies the shape while delivering
    # nothing. _OVERLAY is <ops_root>/tests/acceptance/compose.ask-dev.yml,
    # so the compose build context (".") is parents[2].
    host_path = _OVERLAY.parents[2] / matching[0].split(":")[0].lstrip("./")
    assert (host_path / "role-legacy_agent.json").is_file(), (
        f"the mounted host path {host_path} does not contain "
        "role-legacy_agent.json, so this mount would pass the shape check "
        "while still delivering no scripts"
    )


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
        "CONTEXT_FABRIC_GRAPH_READ_ENABLED": "1",
        "CONTEXT_FABRIC_GRAPH_STORE_URI": "redis://graph-trial-store:6379",
        # CHAOS-3532: the QUA ladder, off by default and overridable from the
        # invoking shell. The DEFAULT is the load-bearing half -- an armed
        # corpus run that silently gained a QUA shadow evaluation would
        # change what every existing case measures, and the pre-registered
        # predictions those runs are graded against would be comparing to a
        # different system. Exact-set here on purpose: flipping either
        # default has to be a deliberate edit to this assertion.
        "ASK_DEV_QUA_SHADOW_ENABLED": "${ASK_DEV_QUA_SHADOW_ENABLED:-0}",
        "ASK_DEV_QUA_COMMIT_ENABLED": "${ASK_DEV_QUA_COMMIT_ENABLED:-0}",
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
    assert launcher.count('"${web_root}/node_modules/.bin/playwright" test') == 3
    assert "playwright.ask-dev-graph-acceptance.config.ts" in launcher
    assert "graph-trial-store" in launcher
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


def test_keep_stack_is_opt_in_and_skips_only_the_teardown() -> None:
    """CHAOS-3219 Phase 5's keep-the-stack flag, clause by clause.

    The CI lane runs this launcher and then run_wave4_corpus.sh against the
    same containers, so the stack has to survive the launcher. The risk in any
    "keep the stack" flag is that it becomes a way to skip WORK, so each
    assertion below pins one clause of the guard.

    These are text assertions on a shell script the unit tier cannot execute --
    that is a real limitation and it is why they are clause-level rather than a
    single "the block exists" check. The first version of this test asserted
    only two string ORDERINGS, and adversarial review (2026-08-06) survived
    five separate mutations against it: inverting the comparison so every hand
    run leaked a stack, deleting the real teardown, deleting the early `exit
    0`, deleting `trap - EXIT`, and appending a new guard BELOW the keep block
    so the flag started skipping real work. Every assertion here was written
    against one of those mutations. Behaviour, as opposed to text, is proven by
    the local execution run and by the nightly lane itself.
    """
    launcher = _LAUNCHER.read_text(encoding="utf-8")
    teardown = '"${compose[@]}" down --volumes --remove-orphans'

    # Given the flag, defaulting to off. A developer running this by hand must
    # not leak a stack by not knowing the variable exists.
    assert "ASK_DEV_ACCEPTANCE_KEEP_STACK:-0" in launcher

    # And it must be an EQUALS-1 test. `!= "1"` inverts the flag: the default
    # path would then keep the stack and the CI path would tear it down.
    assert '"${ASK_DEV_ACCEPTANCE_KEEP_STACK:-0}" == "1"' in launcher

    keep_index = launcher.index("ASK_DEV_ACCEPTANCE_KEEP_STACK")
    playwright_index = launcher.index('"${web_root}/node_modules/.bin/playwright" test')

    # Then the flag is read strictly AFTER the last thing the launcher proves.
    # Every guard -- world-restore digest, per-boot login proof, the six
    # ops-side smokes, the web Playwright leg -- runs before this point
    # regardless of the flag, so an armed CI run and a developer's run execute
    # an identical sequence and differ only in whether the containers survive.
    assert playwright_index < keep_index

    # And the real teardown command appears EXACTLY TWICE, and only twice: the
    # pre-boot volume reset that makes the fixture deterministic, and the final
    # teardown. The count is the load-bearing part. A help message that spelled
    # the same command out in prose would make a third occurrence, and would
    # then satisfy both the ordering below and the older
    # `"down --volumes --remove-orphans" in launcher` assertion in
    # test_launcher_owns_seed_readiness_web_and_fixed_browser_oracle -- so
    # DELETING the real final teardown would change no test. That is exactly
    # what the first version of this commit did, and why the retained-stack
    # hint is deliberately spelled `-p ... down -v` instead.
    assert launcher.count(teardown) == 2
    reset_index, final_index = (
        launcher.index(teardown),
        launcher.rindex(teardown),
    )
    assert reset_index < playwright_index < keep_index < final_index

    # And the guarded branch must actually LEAVE the script. Without `exit 0`
    # it falls through to the teardown it exists to skip.
    keep_block = launcher[keep_index:final_index]
    assert "exit 0" in keep_block

    # And it must clear the EXIT trap on the way out. The trap installed
    # earlier dumps `ps` + logs and re-exits; leaving it armed on the keep path
    # turns a successful retained-stack run into a noisy failure.
    assert "trap - EXIT" in keep_block
    # Both exit paths clear it -- the keep path and the default path.
    assert launcher.count("trap - EXIT") == 2

    # And nothing that does WORK may follow the guard. Anything appended below
    # it would be skipped whenever the flag is set, which would make the flag a
    # way to skip a proof rather than a way to keep containers. The tail after
    # the guard is allowed to contain only the teardown and the closing
    # bookkeeping.
    tail = launcher[keep_index:]
    for forbidden in (
        '"${compose[@]}" exec',
        '"${compose[@]}" up',
        "/.venv/bin/python",
        "node_modules/.bin/playwright",
    ):
        assert forbidden not in tail, (
            f"{forbidden!r} runs after the keep-stack guard, so setting "
            "ASK_DEV_ACCEPTANCE_KEEP_STACK=1 now skips it -- the flag may skip the "
            "teardown and nothing else"
        )


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
                    "ask_dev_graph_routing",
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
                "ask_dev_graph_routing": True,
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
        "id-ask_dev_graph_routing",
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

    # Codex finding (HIGH, 2026-08-05): `celery inspect ping` only proves
    # the control-plane RPC answers, not that the task pool executes queued
    # work -- a wedged pool can keep answering ping. Both healthchecks now
    # invoke a dedicated probe script that round-trips a REAL task through a
    # required queue (worker) or reads a worker-written receipt with a
    # bounded timestamp (beat) -- see
    # test_worker_probe_dispatches_a_real_task_through_a_required_queue and
    # test_beat_sentinel_reads_a_freshness_bounded_worker_receipt below for
    # execution-level proof these scripts do what they claim, and
    # test_worker_probe_times_out_against_a_wedged_pool for the RED case.
    worker_check = services["worker"]["healthcheck"]
    assert worker_check["test"] == [
        "CMD",
        "python",
        "/app/scripts/acceptance/healthcheck_worker_probe.py",
    ]
    assert worker_check["retries"] > 1

    beat_check = services["beat"]["healthcheck"]
    assert beat_check["test"] == [
        "CMD",
        "python",
        "/app/scripts/acceptance/healthcheck_beat_sentinel.py",
    ]
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
    # CHAOS-3463 split this in two: the jobs fleet is still REQUIRED, it is
    # just started after `fixtures world-restore` rather than before it.
    # `worker`/`beat` were concurrent writers racing the restore's
    # empty-target precondition (beat dispatches monitor-queue-depths from
    # the moment it starts). Both lists, and the fact that BOTH are brought
    # up, are asserted -- a split that quietly dropped the fleet would leave
    # the "API+jobs" gate framing unmet.
    assert (
        "boot_services=(postgres pgbouncer clickhouse valkey graph-trial-store migrate ask-dev-scripted-openai api)"
        in launcher
    )
    assert "jobs_services=(worker beat)" in launcher
    assert '"${compose[@]}" up -d --build --wait "${boot_services[@]}"' in launcher
    assert '"${compose[@]}" up -d --build --wait "${jobs_services[@]}"' in launcher
    assert launcher.index("dev-hops fixtures world-restore") < launcher.index(
        'up -d --build --wait "${jobs_services[@]}"'
    ), "the jobs fleet must start AFTER the restore, not before it"
    assert "acr-db-init acr-migrate acr-api" in launcher
    # ACR services are appended to boot_services only inside the arming
    # branch, never unconditionally. The arming block now nests its own
    # if/fi (parent-compose path resolution), so anchor the "closed before
    # the block ends" check on `report_failure() {`, which is defined
    # immediately after the whole arming `if` closes -- not on the first
    # bare "fi\n", which now belongs to an inner if/else inside the block.
    arm_index = launcher.index('"${acr_armed}" == "1"')
    acr_boot_index = launcher.index("acr-db-init acr-migrate acr-api")
    report_failure_def_index = launcher.index("report_failure() {")
    assert arm_index < acr_boot_index < report_failure_def_index


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
                "ask_dev_graph_routing": enabled,
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
            result["ask_dev_graph_routing"] = True
        return result

    api.request = leaking_request  # type: ignore[method-assign]
    with pytest.raises(AcceptanceFailure, match="unexpectedly reports ask_dev"):
        provision_multi_org(
            api,  # type: ignore[arg-type]
            email="admin@devhealth.example",
            password="devhealth123",
        )


# ---------------------------------------------------------------------------
# Codex round 2 (2026-08-05): 3 HIGH + 1 MEDIUM findings against 9844d44f7.
# ---------------------------------------------------------------------------

_docker_available = shutil.which("docker") is not None
_requires_docker = pytest.mark.skipif(
    not _docker_available, reason="docker is not available on this machine"
)

#: A structurally-valid but minimal stand-in for the parent dev-health
#: repo's compose.yml -- enough for `extends: service: acr-*` to resolve,
#: without needing the real ACR Go images. Proves PATH RESOLUTION (this
#: finding); the real ACR service shape is proven separately by the live D3
#: boot evidence in the PR description.
_STUB_PARENT_COMPOSE = """\
services:
  acr-db-init:
    image: busybox
    command: ["true"]
  acr-migrate:
    image: busybox
    command: ["true"]
  acr-api:
    image: busybox
    command: ["true"]
    ports:
      - "127.0.0.1:18080:8080"
"""


#: The subset of this repo resolve_acr_parent_compose.sh + a `docker compose
#: config` check actually need. Copied (not the real files read in place)
#: into a fresh, INDEPENDENT git repo per test -- deliberately NOT a
#: `git worktree add` of this real repo: a real worktree's
#: `git rev-parse --git-common-dir` always resolves back to THIS machine's
#: actual dev-health checkout regardless of where the worktree itself is
#: created (correct, desired behavior for the launcher -- proven separately
#: by the direct, no-copy checks below), which makes a worktree of this
#: repo useless for testing against a CONTROLLED fake parent, and
#: unusable in CI (which has no sibling dev-health checkout at all).
_COPIED_OPS_FILES = (
    "compose.yml",
    "tests/acceptance/compose.ask-dev.yml",
    "tests/acceptance/compose.ask-dev-acr.yml",
    "scripts/acceptance/resolve_acr_parent_compose.sh",
)


def _standalone_ops_checkout(root: Path, *, ops_subpath: str) -> Path:
    """A plain (non-worktree) throwaway git repo containing the copied
    files above -- exercises the "canonical layout" case, where
    ``--git-common-dir`` is just the repo's own ``.git``."""

    ops_checkout = root / ops_subpath
    ops_checkout.mkdir(parents=True)
    for relative in _COPIED_OPS_FILES:
        destination = ops_checkout / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(_ROOT / relative, destination)
    subprocess.run(["git", "init", "-q"], cwd=ops_checkout, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.invalid"],
        cwd=ops_checkout,
        check=True,
    )
    subprocess.run(["git", "config", "user.name", "Test"], cwd=ops_checkout, check=True)
    subprocess.run(["git", "add", "-A"], cwd=ops_checkout, check=True)
    subprocess.run(
        ["git", "commit", "-q", "-m", "snapshot"], cwd=ops_checkout, check=True
    )
    return ops_checkout


def _standalone_ops_worktree(root: Path, *, worktree_subpath: str) -> tuple[Path, Path]:
    """A GENUINE `git worktree` of a throwaway "canonical" repo, checked out
    at ``worktree_subpath`` under a SEPARATE temp root -- exercises the
    actual worktree case: `--git-common-dir` from the worktree must resolve
    back to the base repo's location (which owns the sibling compose.yml),
    not to anything derived from the worktree's own path. Using
    `_standalone_ops_checkout` as the base makes this fully self-contained
    (no dependency on this machine's real dev-health checkout), unlike a
    `git worktree add` of the real repo under test, which always resolves
    to that real checkout regardless of the new worktree's location.

    Returns ``(worktree_path, parent_compose_path)``.
    """

    base_root = root / "_base_root"
    base_root.mkdir()
    base_checkout = _standalone_ops_checkout(base_root, ops_subpath="ops")
    parent_compose = base_root / "compose.yml"
    parent_compose.write_text(_STUB_PARENT_COMPOSE)

    worktree_root = root / "_worktree_root"
    worktree_path = worktree_root / worktree_subpath
    worktree_path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["git", "worktree", "add", "--detach", "-q", str(worktree_path), "HEAD"],
        cwd=base_checkout,
        check=True,
    )
    return worktree_path, parent_compose


def _assert_acr_config_resolves(ops_checkout: Path, parent_compose: Path) -> None:
    """Run resolve_acr_parent_compose.sh from ``ops_checkout`` and assert the
    ACR-armed `docker compose config --quiet` it enables actually succeeds --
    the literal Codex repro (worktree layout: `docker compose config --quiet`
    failed outright with the un-fixed relative default)."""

    resolved = subprocess.run(
        [
            "bash",
            str(
                ops_checkout
                / "scripts"
                / "acceptance"
                / "resolve_acr_parent_compose.sh"
            ),
            str(ops_checkout),
        ],
        capture_output=True,
        text=True,
    )
    assert resolved.returncode == 0, resolved.stderr
    resolved_parent, resolved_acr_dev_dir = resolved.stdout.splitlines()
    assert resolved_parent == str(parent_compose.resolve())
    assert Path(resolved_parent).is_file()

    env = {
        "PATH": os.environ.get("PATH", ""),
        "ASK_DEV_WEB_CONTEXT": "/tmp",
        "ASK_DEV_ACCEPTANCE_API_PORT": "18089",
        "BUGSINK_SECRET_KEY": "x",
        "ASK_DEV_ACCEPTANCE_PARENT_COMPOSE": resolved_parent,
        "ASK_DEV_ACCEPTANCE_PARENT_ACR_DEV_DIR": resolved_acr_dev_dir,
        "POSTGRES_USER": "postgres",
        "POSTGRES_PASSWORD": "postgres",
    }
    config = subprocess.run(
        [
            "docker",
            "compose",
            "--project-name",
            f"test-acr-path-{uuid.uuid4().hex[:8]}",
            "--project-directory",
            str(ops_checkout),
            "-f",
            str(ops_checkout / "compose.yml"),
            "-f",
            str(ops_checkout / "tests" / "acceptance" / "compose.ask-dev.yml"),
            "-f",
            str(ops_checkout / "tests" / "acceptance" / "compose.ask-dev-acr.yml"),
            "--profile",
            "ask-dev-acceptance",
            "--profile",
            "ask-dev-acceptance-acr",
            "config",
            "--quiet",
        ],
        capture_output=True,
        text=True,
        env=env,
    )
    assert config.returncode == 0, (
        f"docker compose config --quiet failed from {ops_checkout}: {config.stderr}"
    )
    # The literal failure mode this finding closes: extended services
    # silently disappearing from the merged config rather than erroring.
    full_config = subprocess.run(
        [
            "docker",
            "compose",
            "--project-name",
            f"test-acr-path-{uuid.uuid4().hex[:8]}",
            "--project-directory",
            str(ops_checkout),
            "-f",
            str(ops_checkout / "compose.yml"),
            "-f",
            str(ops_checkout / "tests" / "acceptance" / "compose.ask-dev.yml"),
            "-f",
            str(ops_checkout / "tests" / "acceptance" / "compose.ask-dev-acr.yml"),
            "--profile",
            "ask-dev-acceptance",
            "--profile",
            "ask-dev-acceptance-acr",
            "config",
        ],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )
    parsed = yaml.safe_load(full_config.stdout)
    for service in ("acr-db-init", "acr-migrate", "acr-api"):
        assert service in parsed["services"], (
            f"{service} disappeared from the ACR-armed merged config"
        )


@_requires_docker
def test_resolve_acr_parent_compose_succeeds_from_the_canonical_layout(
    tmp_path: Path,
) -> None:
    """`<checkout>/ops` directly under the parent -- resolve_acr_parent_compose.sh's
    own default path arithmetic, unexercised by a worktree."""

    parent_dir = tmp_path / "dev-health-canonical"
    parent_dir.mkdir()
    (parent_dir / "compose.yml").write_text(_STUB_PARENT_COMPOSE)
    ops_checkout = _standalone_ops_checkout(parent_dir, ops_subpath="ops")
    _assert_acr_config_resolves(ops_checkout, parent_dir / "compose.yml")


@_requires_docker
def test_resolve_acr_parent_compose_succeeds_from_a_worktree_layout(
    tmp_path: Path,
) -> None:
    """The literal Codex repro: `<checkout>/ops-worktrees/<branch>`, one
    level deeper than the canonical layout, AND (the actual failure mode)
    physically located somewhere else entirely on disk -- the un-fixed
    relative default (`../compose.yml` off --project-directory) resolved
    to a nonexistent path regardless of directory depth; the fix instead
    follows `git --git-common-dir` back to wherever the real base checkout
    lives, so it must resolve correctly even when the worktree's own
    parent directory has no compose.yml of its own at all (proven here:
    `worktree_path`'s own directory tree never gets a compose.yml -- only
    the unrelated base checkout's sibling does)."""

    worktree_path, parent_compose = _standalone_ops_worktree(
        tmp_path, worktree_subpath="ops-worktrees/some-branch"
    )
    _assert_acr_config_resolves(worktree_path, parent_compose)


def test_resolve_acr_parent_compose_fails_loud_with_no_sibling_checkout(
    tmp_path: Path,
) -> None:
    """No docker needed: an ops checkout with no sibling dev-health/compose.yml
    at all must fail loud (exit 64) rather than silently resolving to a
    nonexistent path that only breaks later at `docker compose config`."""

    orphan_root = tmp_path / "some-orphan-dir"
    ops_checkout = orphan_root / "ops"
    ops_checkout.mkdir(parents=True)
    shutil.copy2(
        _ROOT / "scripts" / "acceptance" / "resolve_acr_parent_compose.sh",
        ops_checkout / "resolve_acr_parent_compose.sh",
    )
    subprocess.run(["git", "init", "-q"], cwd=ops_checkout, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.invalid"],
        cwd=ops_checkout,
        check=True,
    )
    subprocess.run(["git", "config", "user.name", "Test"], cwd=ops_checkout, check=True)
    subprocess.run(["git", "add", "-A"], cwd=ops_checkout, check=True)
    subprocess.run(
        ["git", "commit", "-q", "-m", "snapshot"], cwd=ops_checkout, check=True
    )

    result = subprocess.run(
        [
            "bash",
            str(ops_checkout / "resolve_acr_parent_compose.sh"),
            str(ops_checkout),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 64
    assert "requires a sibling dev-health checkout" in result.stderr


def test_resolve_acr_parent_compose_honors_an_explicit_override(tmp_path: Path) -> None:
    explicit = tmp_path / "somewhere-else" / "compose.yml"
    explicit.parent.mkdir(parents=True)
    explicit.write_text(_STUB_PARENT_COMPOSE)
    result = subprocess.run(
        ["bash", str(_RESOLVE_ACR_SCRIPT), str(_ROOT)],
        capture_output=True,
        text=True,
        env={
            "PATH": os.environ.get("PATH", ""),
            "ASK_DEV_ACCEPTANCE_PARENT_COMPOSE": str(explicit),
        },
    )
    assert result.returncode == 0, result.stderr
    resolved_parent, _ = result.stdout.splitlines()
    assert resolved_parent == str(explicit.resolve())


def test_resolve_acr_parent_compose_rejects_a_missing_explicit_override() -> None:
    result = subprocess.run(
        ["bash", str(_RESOLVE_ACR_SCRIPT), str(_ROOT)],
        capture_output=True,
        text=True,
        env={
            "PATH": os.environ.get("PATH", ""),
            "ASK_DEV_ACCEPTANCE_PARENT_COMPOSE": "/nonexistent/compose.yml",
        },
    )
    assert result.returncode == 64
    assert "does not exist" in result.stderr


# --- worker/beat probe scripts: static (no-live-infra) coverage. The
# behavioral proof that these actually detect a wedged pool -- Codex's
# explicit "RED-verify by wedging it (pool=solo + blocking task)" -- is a
# live docker-compose proof (see PR description), not a unit test: it needs
# a real broker, a real second worker process, and a real blocking task,
# which is exactly the class of live-infra dependency this repo's other
# acceptance/live tests are already kept out of the fast unit tier for. ---


def test_worker_probe_targets_a_required_queue_with_a_bounded_wait() -> None:
    source = _WORKER_PROBE_SCRIPT.read_text(encoding="utf-8")
    # "monitoring" must be a queue this stack's `worker` service actually
    # consumes (compose.yml's `-Q ...,monitoring`) -- a probe routed to a
    # queue nothing consumes would time out unconditionally, proving
    # nothing about the pool's health either way.
    assert '_PROBE_QUEUE = "monitoring"' in source
    assert (
        '_PROBE_TASK_NAME = "dev_health_ops.workers.tasks.monitor_queue_depths"'
        in source
    )
    assert "task_id=" in source, (
        "must be uniquely identified, not a bare fire-and-forget"
    )
    assert ".get(timeout=" in source, "must actually block for the receipt"


def test_beat_sentinel_reads_a_freshness_bounded_worker_receipt() -> None:
    source = _BEAT_SENTINEL_SCRIPT.read_text(encoding="utf-8")
    assert "CELERY_RESULT_BACKEND" in source
    assert "celery-task-meta-" in source
    assert "_FRESHNESS_WINDOW_SECONDS" in source
    # Matches by result SHAPE, not task name (the default Redis result
    # payload carries no task name) -- assert the actual fingerprint field.
    assert '"queues"' in source


def test_beat_sentinel_date_parsing_handles_naive_and_aware_timestamps() -> None:
    import importlib.util
    from datetime import timezone

    spec = importlib.util.spec_from_file_location(
        "healthcheck_beat_sentinel", _BEAT_SENTINEL_SCRIPT
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    naive = module._parse_date_done("2026-08-05T13:45:28.474000")
    assert naive is not None
    assert naive.tzinfo == timezone.utc

    aware = module._parse_date_done("2026-08-05T13:45:28.474000+00:00")
    assert aware is not None
    assert aware.tzinfo is not None

    assert module._parse_date_done("not-a-timestamp") is None
    assert module._parse_date_done(None) is None


# ---------------------------------------------------------------------------
# CHAOS-3572: ordinary-boot wrong-worktree guard
# ---------------------------------------------------------------------------


def test_launcher_sources_the_shared_container_source_guard() -> None:
    """CHAOS-3572: an ordinary boot has the SAME exposure the mint guard
    (#1582, CHAOS-3544) closed for the one-off mint flow -- `compose.yml` is
    launched with `--project-directory <ops_root>` and bind-mounts that
    directory at /app, so the launcher's own stack can just as easily serve
    a different worktree's source. `docker ps`, the API, and every later
    test all look healthy regardless of which worktree booted the container
    -- nothing else in the launcher would ever report this.

    Sourced from container_source_guard.sh (a shared function), NOT a
    per-entrypoint copy -- CHAOS-3572 explicitly calls out avoiding
    per-entrypoint duplication so a future boot entrypoint (e.g. the
    corpus-lane armed-boot script) inherits the same check rather than
    growing its own that can drift.
    """

    launcher = _LAUNCHER.read_text(encoding="utf-8")
    guard_script = _ROOT / "scripts" / "acceptance" / "container_source_guard.sh"
    assert guard_script.exists(), (
        "the shared guard script must exist for the launcher to source"
    )
    assert 'source "${script_dir}/container_source_guard.sh"' in launcher, (
        "the launcher must source the SHARED guard, not reimplement its own "
        "copy of the signature check"
    )
    assert "container_source_guard_check" in launcher, (
        "the launcher must actually CALL the guard, not merely source the "
        "file that defines it"
    )


def test_launcher_runs_the_container_source_guard_immediately_after_boot() -> None:
    """Ordering is load-bearing, exactly like the world-restore check below:
    the guard must run BEFORE `fixtures world-restore` (or anything else)
    touches the stack -- a mismatch discovered after data has already been
    restored/generated against the wrong container is a mismatch discovered
    too late to matter."""

    launcher = _LAUNCHER.read_text(encoding="utf-8")
    boot_index = launcher.index('up -d --build --wait "${boot_services[@]}"')
    guard_call_index = launcher.index("container_source_guard_check ")
    restore_index = launcher.index("dev-hops fixtures world-restore")

    assert boot_index < guard_call_index < restore_index, (
        "the container-source guard must run immediately after boot and "
        "before the world is restored into the (possibly wrong) container"
    )


def test_launcher_container_source_guard_is_not_gated_by_acr_arming() -> None:
    """The guard call must sit OUTSIDE the `if [[ "${acr_armed}" == "1" ]]`
    block -- an ACR-armed boot has exactly the same bind-mount exposure as a
    plain one, and a guard that only ran when ACR happened to be armed would
    read as coverage it does not have on the (default, far more common)
    unarmed path."""

    launcher = _LAUNCHER.read_text(encoding="utf-8")
    acr_block_start = launcher.index('if [[ "${acr_armed}" == "1" ]]; then')
    acr_block_end = launcher.index("\nfi\n", acr_block_start)
    acr_block = launcher[acr_block_start:acr_block_end]

    assert "container_source_guard_check" not in acr_block, (
        "the guard must run unconditionally, not only inside the ACR-armed branch"
    )
    assert "container_source_guard_check" in launcher


# ---------------------------------------------------------------------------
# CHAOS-3463: world seeding wiring
# ---------------------------------------------------------------------------


def test_launcher_restores_the_pinned_world_before_anything_else_seeds() -> None:
    """Ordering here is load-bearing, not stylistic.

    `fixtures world-restore` refuses to write unless every table its snapshot
    carries is still EMPTY in the target -- that emptiness predicate is what
    makes it impossible to point at a real dev/production database, and it is
    only satisfiable if the restore happens before `fixtures generate` and
    `prepare_ask_dev_acceptance.py` put their own rows in. A future edit that
    moves the restore after either of them turns a fail-closed guard into a
    boot failure; this catches that in the unit tier.
    """

    launcher = _LAUNCHER.read_text(encoding="utf-8")
    assert "dev-hops fixtures world-restore" in launcher

    boot_index = launcher.index('up -d --build --wait "${boot_services[@]}"')
    restore_index = launcher.index("dev-hops fixtures world-restore")
    generate_index = launcher.index("dev-hops fixtures generate")
    # The INVOCATION, not the first mention: `prepare_ask_dev_acceptance.py`
    # is named in a header comment long before it is run, and anchoring on
    # that comment would compare the restore against the wrong position.
    prepare_index = launcher.index("scripts/acceptance/prepare_ask_dev_acceptance.py")

    assert boot_index < restore_index < generate_index < prepare_index


def test_launcher_proves_world_principals_can_log_in_on_every_boot() -> None:
    """Codex adversarial review round 3 (HIGH, confirmed): before this, the
    principal login proof ran ONLY in the one-off mint script.

    The digest guard cannot cover this, and that gap is the reason the step
    exists. WORLD_DIGEST includes `password_hash`, so a verified digest proves
    the credential BYTES are the ones the mint proved a login against -- it
    says nothing about whether the API still ACCEPTS them. A login-path or
    bcrypt-policy regression leaves every restored byte identical, the digest
    green, and no world principal able to authenticate; the corpus then
    silently runs as the superuser and its cross-tenant/entitlement cases stop
    testing what they claim to.

    Asserts the check is wired AND correctly ordered: after the restore (there
    is nothing to authenticate before it) and before the corpus does any work.
    """

    launcher = _LAUNCHER.read_text(encoding="utf-8")
    assert "scripts/acceptance/assert_world_principals_can_log_in.py" in launcher, (
        "the launcher no longer proves world principals can log in on boot -- "
        "a login regression would ride behind a green digest"
    )

    restore_index = launcher.index("dev-hops fixtures world-restore")
    login_index = launcher.index(
        "scripts/acceptance/assert_world_principals_can_log_in.py"
    )
    prepare_index = launcher.index("scripts/acceptance/prepare_ask_dev_acceptance.py")
    assert restore_index < login_index < prepare_index, (
        "the login proof must run after the world is restored and before the "
        "corpus starts, or it proves nothing about the world being served"
    )


def test_boot_login_proof_and_mint_use_the_same_assertion_script() -> None:
    """One script, two callers. If the boot path ever grew its own weaker copy
    of the login check, the mint could keep passing a stricter assertion than
    the one every acceptance run actually relies on, and the divergence would
    be invisible from either side alone.
    """

    launcher = _LAUNCHER.read_text(encoding="utf-8")
    mint_path = _LAUNCHER.parent / "mint_ask_dev_world_snapshot.sh"
    mint = mint_path.read_text(encoding="utf-8")

    script = "scripts/acceptance/assert_world_principals_can_log_in.py"
    assert script in launcher, "the boot path lost the shared login assertion"
    assert script in mint, "the mint lost the shared login assertion"


def test_launcher_restore_targets_the_databases_the_api_actually_serves() -> None:
    """The whole point of CHAOS-3463's B2: the world has to land in the
    ClickHouse `default` / Postgres `postgres` databases the acceptance API
    reads and the corpus runner verifies (see
    test_wave4_corpus_runner_live.py's own `_CONTAINER_*` constants), not in
    some scratch database nothing serves."""

    launcher = _LAUNCHER.read_text(encoding="utf-8")
    restore_block = launcher[launcher.index("dev-hops fixtures world-restore") :]
    restore_block = restore_block[: restore_block.index("dev-hops fixtures generate")]

    assert "clickhouse://ch:ch@clickhouse:8123/default" in restore_block
    assert (
        "postgresql+asyncpg://postgres:postgres@postgres:5432/postgres" in restore_block
    )
    assert "/app/tests/acceptance/world/ask-dev-world.v1/snapshot" in restore_block


def test_launcher_never_mints_the_digest_pin() -> None:
    """`--mint-digest` re-pins WORLD_DIGEST from whatever it just restored.
    On an ordinary boot that would make the digest guard tautological -- it
    would "verify" the world against itself and pass no matter what drifted.
    Minting belongs to the one-off operator flow alone."""

    launcher = _LAUNCHER.read_text(encoding="utf-8")
    assert "--mint-digest" not in launcher

    mint = _ROOT / "scripts" / "acceptance" / "mint_ask_dev_world_snapshot.sh"
    assert mint.exists(), "the mint flow must exist somewhere, just not in the launcher"
    assert "--mint-digest" in mint.read_text(encoding="utf-8")


def test_world_snapshot_artifact_is_committed_and_pairs_with_the_digest() -> None:
    """The artifact and the pin are only ever valid together: the pin is
    re-minted FROM the snapshotted generation. A checkout carrying one
    without the other cannot boot the acceptance stack, so both must be in
    the repo."""

    world_dir = _ROOT / "tests" / "acceptance" / "world" / "ask-dev-world.v1"
    snapshot_dir = world_dir / "snapshot"

    assert (world_dir / "WORLD_DIGEST").exists()
    assert (snapshot_dir / "manifest.json").exists()

    manifest = json.loads((snapshot_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["schema_version"] == "ask_dev_world_snapshot.v1"
    assert manifest["clickhouse"]["tables"], (
        "a world with no ClickHouse rows is not a world"
    )
    assert manifest["postgres"]["tables"], (
        "a world with no Postgres rows is not a world"
    )

    # Every file the manifest names must actually be checked in -- a snapshot
    # whose payload was gitignored would pass a manifest-only check and fail
    # at boot.
    import hashlib

    for store in ("clickhouse", "postgres"):
        for table, entry in manifest[store]["tables"].items():
            path = snapshot_dir / entry["file"]
            assert path.exists(), f"{store} table {table} snapshot file is missing"
            assert hashlib.sha256(path.read_bytes()).hexdigest() == entry["sha256"], (
                f"{store} table {table} snapshot file does not match its manifest hash"
            )


def _unset_block(script: str) -> set[str]:
    """The set of variable names a launcher-style `unset \\ ... ` block clears."""
    body = script.split("\nunset \\\n", 1)[1]
    names: set[str] = set()
    for line in body.splitlines():
        stripped = line.strip()
        names.update(stripped.rstrip("\\").split())
        if not stripped.endswith("\\"):
            break
    return names


def test_mint_script_hardens_the_same_env_as_the_launcher() -> None:
    """Both scripts drive the SAME two compose files, so both are exposed to
    the same `${VAR}` interpolation hazard a direnv-loaded ops/.env creates.
    The launcher's list is already asserted to be a superset of every `${VAR}`
    reference; requiring the mint script's list to be identical extends that
    guarantee to the mint flow without maintaining a second, separately-rotting
    allow-list.
    """

    mint = _ROOT / "scripts" / "acceptance" / "mint_ask_dev_world_snapshot.sh"
    launcher_names = _unset_block(_LAUNCHER.read_text(encoding="utf-8"))
    mint_names = _unset_block(mint.read_text(encoding="utf-8"))

    assert launcher_names, "the launcher's unset block could not be parsed"
    assert mint_names == launcher_names, (
        "mint_ask_dev_world_snapshot.sh and run_ask_dev_compose.sh must unset "
        "exactly the same variables; they differ by "
        f"{launcher_names ^ mint_names}"
    )


def test_mint_script_supplies_the_interpolation_only_variables() -> None:
    """`web` and `bugsink` declare required (`:?`) variables. Compose refuses to
    run ANY command -- even `ps` -- until every `${VAR}` in both files
    interpolates, including for services this flow never starts. Observed: the
    mint script exited 1 on `compose ps` before this."""

    mint = (
        _ROOT / "scripts" / "acceptance" / "mint_ask_dev_world_snapshot.sh"
    ).read_text(encoding="utf-8")
    assert "export ASK_DEV_WEB_CONTEXT=" in mint
    assert "export BUGSINK_SECRET_KEY=" in mint


def test_mint_script_enables_every_profile_required_by_the_acceptance_api() -> None:
    """The W3 API depends on the isolated graph store in the graph-trial profile."""

    mint = (
        _ROOT / "scripts" / "acceptance" / "mint_ask_dev_world_snapshot.sh"
    ).read_text(encoding="utf-8")
    assert "--profile ask-dev-acceptance" in mint
    assert "--profile graph-trial" in mint


def test_mint_script_allows_an_explicit_host_python_for_worktrees() -> None:
    """A clean worktree may intentionally reuse another verified project venv."""

    mint = (
        _ROOT / "scripts" / "acceptance" / "mint_ask_dev_world_snapshot.sh"
    ).read_text(encoding="utf-8")
    assert "ASK_DEV_ACCEPTANCE_PYTHON:-${ops_root}/.venv/bin/python" in mint
    assert '"${python_bin}"' in mint


def test_committed_world_digest_is_a_real_pin_not_a_placeholder() -> None:
    """A stub pin is worse than a missing one: it looks committed and it fails
    every acceptance boot.

    Found by Codex adversarial review and reproduced: `_generate_world` ends by
    WRITING the digest, so a unit test that monkeypatched `compute_world_digest`
    and left `digest_path=None` wrote its 64-zero stub straight onto the real,
    committed `WORLD_DIGEST`. Merely running the unit suite corrupted the
    artifact -- silently, because nothing asserted the pin was real. This is
    that assertion.
    """

    world = _ROOT / "tests" / "acceptance" / "world" / "ask-dev-world.v1"
    pinned = json.loads((world / "WORLD_DIGEST").read_text(encoding="utf-8"))

    digest = pinned.get("digest")
    assert isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest), digest
    assert digest != "0" * 64, "WORLD_DIGEST is the all-zero placeholder, not a pin"

    components = pinned.get("components") or {}
    assert components.get("clickhouse"), "pinned digest has no ClickHouse components"
    assert components.get("postgres"), "pinned digest has no Postgres components"
    # A pin whose every component is an empty table would hash consistently and
    # verify forever while describing nothing.
    row_counts = [
        org.get("row_count", 0)
        for store in components.values()
        for table in store.values()
        for org in table.values()
    ]
    assert sum(row_counts) > 0, "every pinned component is empty -- this pins nothing"


def test_pinned_digest_and_snapshot_artifact_describe_the_same_generation() -> None:
    """Binds the two committed artifacts by an exact value, not a heuristic.

    The mint stamps the digest it computed FROM the restored snapshot into the
    snapshot's own manifest, so the pin and the artifact carry the same 64-hex
    string or they are not a pair. Codex adversarial review round 2 (MEDIUM,
    confirmed): the first version of this test only required pinned row counts
    to be <= snapshot row counts and skipped zero-count tables, which a swapped
    or polluted artifact could satisfy.
    """

    world = _ROOT / "tests" / "acceptance" / "world" / "ask-dev-world.v1"
    pinned = json.loads((world / "WORLD_DIGEST").read_text(encoding="utf-8"))
    manifest = json.loads(
        (world / "snapshot" / "manifest.json").read_text(encoding="utf-8")
    )

    stamped = manifest.get("world_digest")
    assert stamped, (
        "the committed snapshot manifest carries no world_digest -- it was "
        "minted by a build that predates the pin/artifact binding, so nothing "
        "proves it belongs with the committed WORLD_DIGEST. Re-mint."
    )
    assert stamped == pinned["digest"], (
        f"the snapshot was minted for world digest {stamped} but the committed "
        f"pin is {pinned['digest']} -- these are different generations"
    )


def test_committed_snapshot_is_bound_to_the_committed_world_manifest() -> None:
    """The committed artifact must be minted for the committed `world.json`.

    `restore_world` refuses a snapshot whose `world_manifest_contract` is
    absent or disagrees with the manifest, so a committed pair that fails this
    cannot boot the stack at all -- catching it in the unit tier beats
    discovering it 20 minutes into an acceptance run.

    This is the check WORLD_DIGEST structurally cannot make: the digest is
    computed from the restored DATABASE, so editing `world.json` alone (an
    email, a `membership_role`) leaves the rows and the digest identical while
    the manifest and the served world silently diverge. Whoever edits
    `world.json` must re-mint, and this is what says so.
    """

    import sys

    sys.path.insert(0, str(_ROOT / "src"))
    from dev_health_ops.fixtures.world import (
        load_world_manifest,
        world_manifest_contract_hash,
    )

    world_dir = _ROOT / "tests" / "acceptance" / "world" / "ask-dev-world.v1"
    manifest = json.loads(
        (world_dir / "snapshot" / "manifest.json").read_text(encoding="utf-8")
    )

    stamped = manifest.get("world_manifest_contract")
    assert stamped, (
        "the committed snapshot carries no world_manifest_contract -- it was "
        "minted by a build predating that binding, and `world-restore` now "
        "refuses it rather than trusting it. Re-mint."
    )

    world = load_world_manifest(world_dir / "world.json")
    assert stamped == world_manifest_contract_hash(world), (
        "the committed snapshot was minted for a DIFFERENT world.json than the "
        "one committed beside it. An org/user alias, id_seed, email, username, "
        "full_name, membership_role or is_superuser value changed without a "
        "re-mint -- the stack would refuse to boot. Re-mint the snapshot and "
        "the pin together."
    )
    assert manifest.get("world_schema_version") == world.world["schema_version"]
    assert manifest.get("master_seed") == world.master_seed


def test_snapshot_manifest_records_a_schema_fingerprint_for_both_stores() -> None:
    """The restore refuses a snapshot with no fingerprint, so a committed
    artifact that lacks one cannot boot the stack at all. Catching that here
    beats discovering it at boot."""

    manifest = json.loads(
        (
            _ROOT
            / "tests"
            / "acceptance"
            / "world"
            / "ask-dev-world.v1"
            / "snapshot"
            / "manifest.json"
        ).read_text(encoding="utf-8")
    )
    clickhouse = manifest["clickhouse"]["schema_fingerprint"]
    postgres = manifest["postgres"]["schema_fingerprint"]

    assert clickhouse["migrations"], "no ClickHouse migrations recorded"
    assert clickhouse["server_version"]
    assert re.fullmatch(r"[0-9a-f]{64}", clickhouse["catalog_sha256"])
    assert postgres["alembic_heads"], "no alembic heads recorded"
    assert re.fullmatch(r"[0-9a-f]{64}", postgres["catalog_sha256"])


def test_snapshot_manifest_alembic_heads_are_current_for_this_checkout() -> None:
    """CHAOS-3488: the committed snapshot must be minted against THIS
    checkout's migration heads, not merely carry some well-formed heads.

    The sibling test above checks the fingerprint EXISTS and is well-formed.
    That is not the same property, and the difference cost a full Phase 2 exit
    evidence run: #1536 minted its snapshot on a branch that had not picked up
    #1525's ``0087_add_dev_qua_shadow_budget_reservations``, then merged the
    0086-fingerprinted artifact onto a 0087 main. Every existence assertion
    above still passed. The first and only signal was
    ``dev-hops fixtures world-restore`` refusing to boot the acceptance stack
    -- correctly, but at the most expensive possible moment, and with zero
    corpus cases executed.

    Any migration merged AFTER a mint silently invalidates the committed
    artifact, and no CI lane boots this stack (CHAOS-3488), so nothing else
    catches it. This check needs no database and no container: alembic's own
    ``ScriptDirectory`` resolves heads from the versions directory offline, so
    it runs at unit tier and fails RED the moment the pair drifts.

    Heads are compared SORTED because the recorded value comes from
    ``SELECT version_num FROM alembic_version ORDER BY 1``
    (``_postgres_schema_fingerprint``), which is ordering-normalized already;
    sorting both sides keeps this comparing head SETS rather than incidental
    row order.

    DELIBERATELY NOT EXTENDED TO CLICKHOUSE, stated rather than left as a
    silent gap. The same staleness class applies there, but the ClickHouse
    fingerprint records ``schema_migrations.version`` -- the APPLIED ledger --
    and a deferred Python migration is legitimately on disk while never
    appearing in it (``067_operational_ordering_contract.py`` is exactly this
    today). A naive on-disk-equals-recorded check would therefore fail RED
    against a perfectly fresh mint. Encoding the deferral rule here would
    couple this test to migration-runner internals; the honest statement is
    that ClickHouse migration currency is NOT covered by this test and is
    still caught only at boot.
    """

    import sys

    if str(_ROOT / "src") not in sys.path:
        sys.path.insert(0, str(_ROOT / "src"))
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    manifest = json.loads(
        (
            _ROOT
            / "tests"
            / "acceptance"
            / "world"
            / "ask-dev-world.v1"
            / "snapshot"
            / "manifest.json"
        ).read_text(encoding="utf-8")
    )
    recorded = sorted(manifest["postgres"]["schema_fingerprint"]["alembic_heads"])

    script_directory = ScriptDirectory.from_config(Config(str(_ROOT / "alembic.ini")))
    current = sorted(script_directory.get_heads())

    assert recorded == current, (
        f"the committed snapshot was minted against alembic heads {recorded}, "
        f"but this checkout's heads are {current}. The snapshot's bytes are "
        f"only valid against the schema they were taken from, so "
        f"`dev-hops fixtures world-restore` will refuse to boot the acceptance "
        f"stack and no corpus case can execute. A migration landed after the "
        f"mint -- re-mint the snapshot AND WORLD_DIGEST together with "
        f"scripts/acceptance/mint_ask_dev_world_snapshot.sh (they are only "
        f"ever valid as a pair). See CHAOS-3488."
    )


def test_mint_script_refuses_a_container_serving_another_checkout() -> None:
    """CHAOS-3544: the mint must assert the container is serving THIS checkout.

    `fixtures world` runs INSIDE the api container, so the world is generated
    by whatever code that image carries -- not by the checkout the script was
    invoked from. The script previously stated `up -d --build` as a
    prerequisite in a header comment, which is a dead guard: nothing checked
    it and skipping it is invisible.

    That is the worst failure this ticket can produce. Measured on
    2026-08-07, before this assert existed, against the running container:

        container has TTL cap: False
        container has old literal: True

    Minting in that state would have regenerated the decaying world,
    snapshotted it, re-pinned WORLD_DIGEST, and printed "mint: done" -- a
    snapshot that fails its own content oracle again within days, wearing a
    fresh digest that makes it look deliberate.
    """

    mint = (
        _ROOT / "scripts" / "acceptance" / "mint_ask_dev_world_snapshot.sh"
    ).read_text(encoding="utf-8")

    assert "mint: verifying the api container is serving this checkout" in mint, (
        "the mint script must verify the running container serves the "
        "invoking checkout before generating anything"
    )
    assert "REFUSING" in mint and "exit 70" in mint, (
        "and it must REFUSE on mismatch -- warning and continuing would still "
        "produce the snapshot, which is the whole failure"
    )
    assert "up -d --build --wait api" in mint, (
        "the refusal must carry the remedy; an operator who hits this needs "
        "the rebuild command, not a diagnosis"
    )

    # The check must run BEFORE any generation, or it certifies nothing.
    assert mint.index("verifying the api container is serving") < mint.index(
        "dev-hops fixtures world "
    ), "the container-currency check must precede world generation"


def test_acceptance_overlay_wires_the_qua_ladder_off_by_default() -> None:
    """CHAOS-3532: the stack must be ABLE to exercise QUA commit, and must
    not do so unless someone asked for it.

    Before this, neither flag appeared anywhere in the acceptance tooling, so
    the shadow never evaluated and the promotion never engaged -- the stack
    demonstrated pre-CHAOS-3525 behaviour by construction, whatever the code
    did. A live probe against it would have faithfully reproduced the old
    dead-end and been read as a negative result about the fix.

    Both halves are asserted, and the default matters as much as the
    presence: an armed corpus run that silently gained a QUA shadow
    evaluation would change what every existing case measures, and the
    pre-registered predictions those runs are graded against would be
    comparing to a different system.
    """

    overlay = _OVERLAY.read_text(encoding="utf-8")

    for flag in ("ASK_DEV_QUA_SHADOW_ENABLED", "ASK_DEV_QUA_COMMIT_ENABLED"):
        assert f'{flag}: "${{{flag}:-0}}"' in overlay, (
            f"{flag} must be wired into the acceptance overlay, defaulting "
            "OFF and overridable from the invoking shell -- without it the "
            "stack cannot exercise the QUA path at all"
        )


def test_ambient_qua_flags_cannot_arm_the_acceptance_stack() -> None:
    """CHAOS-3532: the QUA flags are CLEARED by the launcher, and armed only
    by its own one-shot opt-in.

    THIS ASSERTION IS THE REVERSE OF THE ONE IT REPLACES, and the reversal was
    forced by reality within the hour. The first version passed the flags
    through from the invoking shell (`${VAR:-0}`) so an operator could arm a
    run, and asserted they must NOT be in the unset list. Then `ops/.env`
    gained `ASK_DEV_QUA_SHADOW_ENABLED=1` / `ASK_DEV_QUA_COMMIT_ENABLED=1`
    for the dev stack, direnv exports that file into every shell under the
    ops tree, and passthrough would therefore have booted EVERY future
    acceptance stack silently ARMED -- changing what every baseline corpus
    case measures, against predictions registered on an unarmed system.

    That is not a hypothetical leftover export. It was the live state of
    every ops shell on this machine, verified directly, while the
    passthrough version of this file was already committed.

    So arming is now a deliberate act at the launcher boundary and nowhere
    else: both names are cleared unconditionally, and only
    `ASK_DEV_ACCEPTANCE_QUA=1` -- the launcher's own knob, translated AFTER
    the clear -- turns them on.
    """

    launcher = _LAUNCHER.read_text(encoding="utf-8")
    unset_match = re.search(r"\nunset \\\n(.*?)\n\nweb_root=", launcher, re.S)
    assert unset_match is not None
    unset_vars = set(re.findall(r"[A-Z_][A-Z0-9_]*", unset_match.group(1)))

    for flag in ("ASK_DEV_QUA_SHADOW_ENABLED", "ASK_DEV_QUA_COMMIT_ENABLED"):
        assert flag in unset_vars, (
            f"{flag} must be CLEARED by the launcher. It is exported by "
            "ops/.env and reaches every shell under the ops tree via direnv, "
            "so leaving it to pass through arms every acceptance stack "
            "booted from a developer shell."
        )

    assert "ASK_DEV_ACCEPTANCE_QUA" in launcher, (
        "the launcher must own a one-shot opt-in; clearing the flags without "
        "one leaves no way to arm a QUA run at all"
    )

    # Ordering is the whole guarantee: translated AFTER the clear, or the
    # clear removes what the translation just set.
    assert launcher.index("\nunset \\\n") < launcher.index(
        "export ASK_DEV_QUA_SHADOW_ENABLED="
    ), "the opt-in translation must run AFTER the unset block, not before"


def test_launcher_runs_the_wave4_access_matrix_with_its_own_arming_contract() -> None:
    """CHAOS-3586 (unblocks CHAOS-3510 / Phase 4 Lane 4d).

    The Wave 4 access matrix is a SECOND playwright invocation with its own
    arming contract. Every assertion here exists because the corresponding
    omission would produce a silently weaker run rather than a failure:

    - a missing config reference means the matrix never runs at all, and the
      launcher still exits 0;
    - a missing ASK_DEV_WAVE4_ACCESS_MATRIX means a launcher predating this
      lane looks like it ran the matrix;
    - a missing ASK_DEV_ACCEPTANCE_ORG_IDS means the entitlement rows cannot
      find the disabled-entitlement tenant;
    - a missing ASK_DEV_ACCEPTANCE_ACR means the non-coupling rows assert
      against an undeclared toggle state.
    """
    launcher = _LAUNCHER.read_text(encoding="utf-8")

    assert "playwright.ask-dev-wave4.config.ts" in launcher
    assert "ASK_DEV_WAVE4_ACCESS_MATRIX=1" in launcher
    assert 'ASK_DEV_ACCEPTANCE_ORG_IDS="${org_ids_output}"' in launcher
    assert 'ASK_DEV_ACCEPTANCE_ACR="${acr_armed}"' in launcher

    # Three SEPARATE invocations, not one with more env. Folding them together
    # would couple the Phase 1 oracle to the access matrix so either could
    # take the other down.
    assert launcher.count('"${web_root}/node_modules/.bin/playwright" test') == 3

    # Ordering: the matrix runs after web is up and after the Phase 1 spec,
    # so a Phase 1 regression is reported against Phase 1 rather than
    # surfacing as a confusing access-matrix failure.
    #
    # Anchor the matrix on its INVOCATION SITE, not on the config filename.
    # The filename now also appears in the presence guard's variable
    # assignment and in prose above it, and anchoring on the string made this
    # assertion silently measure the wrong position the moment the guard
    # landed -- it failed loudly, which is the only reason it was caught.
    web_up_index = launcher.index("up -d --build --wait web")
    acceptance_config_index = launcher.index("playwright.ask-dev-acceptance.config.ts")
    wave4_invocation_index = launcher.index('-c "${wave4_config}"')
    assert web_up_index < acceptance_config_index < wave4_invocation_index

    # The org-ids artifact must be written before it is forwarded. index()
    # raises rather than passing vacuously if either anchor disappears.
    org_ids_written_index = launcher.index(
        'ASK_DEV_ACCEPTANCE_ORG_IDS_OUTPUT="${org_ids_output}"'
    )
    org_ids_forwarded_index = launcher.index(
        'ASK_DEV_ACCEPTANCE_ORG_IDS="${org_ids_output}"'
    )
    assert org_ids_written_index < org_ids_forwarded_index

    # A missing/empty artifact must ABORT, never skip. The matrix asserting
    # nothing about tenants it could not identify is the false green this
    # whole lane exists to prevent.
    assert '[[ ! -s "${org_ids_output}" ]]' in launcher
    preflight_index = launcher.index('[[ ! -s "${org_ids_output}" ]]')
    assert preflight_index < wave4_invocation_index
    assert "|| true" not in launcher[preflight_index:wave4_invocation_index]


def test_launcher_requires_the_wave4_config_and_never_skips_the_matrix() -> None:
    """CHAOS-3510: the wave4 leg is MANDATORY now that the config is on web main.

    History this pins deliberately. The ops half of this feature merged before
    the web half, so an interim revision SKIPPED the leg (with a loud marker)
    when the config was absent -- otherwise every launcher run, including the
    nightly, died on a dangling cross-repo path. That condition is gone:
    playwright.ask-dev-wave4.config.ts is on dev-health-web main.

    The skip is REMOVED rather than left dormant. A skip path that is correct
    today and unreachable tomorrow is how a gate quietly stops gating -- it
    survives precisely because nothing fails when it fires. So this test
    asserts the skip is GONE, not merely that a fail path exists beside it.
    """
    launcher = _LAUNCHER.read_text(encoding="utf-8")

    # Guarded on the file, not on an env flag: a flag would still let a caller
    # arm a config that does not exist.
    assert 'wave4_config="${web_root}/playwright.ask-dev-wave4.config.ts"' in launcher
    assert 'if [[ ! -f "${wave4_config}" ]]; then' in launcher

    # Absent config ABORTS. The marker names the outcome so a log scraper can
    # classify the run without parsing prose.
    assert "WAVE4_ACCESS_MATRIX=FAILED reason=config-absent" in launcher
    assert "WAVE4_ACCESS_MATRIX=RUNNING" in launcher

    # The scaffolding must be GONE, not dormant. This is the assertion that
    # distinguishes "mandatory" from "a fail branch that some other path can
    # still route around".
    assert "WAVE4_ACCESS_MATRIX=NOT_RUN" not in launcher
    assert "proves NOTHING about the Context Fabric" not in launcher

    # The failure branch must actually exit, and do so BEFORE the invocation.
    failed_index = launcher.index("WAVE4_ACCESS_MATRIX=FAILED")
    running_index = launcher.index("WAVE4_ACCESS_MATRIX=RUNNING")
    invocation_index = launcher.index('-c "${wave4_config}"')
    assert failed_index < running_index < invocation_index

    # Scope the abort check to the CONFIG-ABSENT BRANCH ITSELF -- up to its own
    # closing `fi`, not onward to the RUNNING marker.
    #
    # Widening it to [FAILED, RUNNING) is not merely sloppy, it is
    # unfalsifiable: the org-ids preflight sits inside that span and carries
    # its OWN `exit 1`, so deleting this branch's abort still leaves a matching
    # string in the region. A mutation removing it SURVIVED exactly that way.
    # Second time a region assertion in this guard was satisfied by something
    # other than the thing it names -- the region must end where the construct
    # ends.
    absent_branch_end = launcher.index("\nfi\n", failed_index)
    absent_branch = launcher[failed_index:absent_branch_end]
    assert "exit 1" in absent_branch

    # No `else` may re-introduce a continue-anyway path in that branch.
    assert "else" not in absent_branch

    # The leg itself stays mandatory: a real matrix failure must still abort.
    # The slice runs to the END of the invocation block, not merely to the
    # invocation line. An earlier version of this assertion checked only
    # [marker, invocation) and a mutation appending `|| true` to the
    # invocation SURVIVED -- the swallow lands after the anchor, in the region
    # that version never examined. A region assertion is only as good as its
    # region, so that mutation stays in this guard's permanent set.
    tail = launcher[running_index : invocation_index + 400]
    assert "|| true" not in tail
    assert "set +e" not in tail

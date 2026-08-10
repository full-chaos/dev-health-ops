from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import yaml

_ROOT = Path(__file__).resolve().parents[2]
_OVERLAY = _ROOT / "tests" / "acceptance" / "compose.ask-dev-provider-profile.yml"
_LAUNCHER = _ROOT / "scripts" / "acceptance" / "run_ask_dev_provider_profile.sh"
_SMOKE = _ROOT / "scripts" / "acceptance" / "smoke_ask_dev_provider_profile.py"


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


def test_provider_profile_overlay_passes_source_bound_platform_bundles() -> None:
    api = _load_overlay()["services"]["api"]
    environment = api["environment"]
    assert environment["ASK_DEV_LIVE_ACCEPTANCE"] == "0"
    assert environment["LLM_PROVIDER"].startswith("${ASK_DEV_PROFILE_PROVIDER:?")
    assert environment["LLM_MODEL"].startswith("${ASK_DEV_PROFILE_MODEL:?")
    assert environment["LLM_BASE_URL"].startswith("${ASK_DEV_PROFILE_BASE_URL:?")
    assert environment["LOCAL_LLM_BASE_URL"] == ("${ASK_DEV_PROFILE_LOCAL_BASE_URL:-}")
    assert environment["LOCAL_LLM_MODEL"] == "${ASK_DEV_PROFILE_LOCAL_MODEL:-}"
    assert environment["OLLAMA_BASE_URL"] == ("${ASK_DEV_PROFILE_OLLAMA_BASE_URL:-}")
    assert environment["OLLAMA_MODEL"] == "${ASK_DEV_PROFILE_OLLAMA_MODEL:-}"
    assert environment["OLLAMA_API_KEY"] == ("${ASK_DEV_PROFILE_OLLAMA_API_KEY:-}")
    assert environment["LMSTUDIO_BASE_URL"] == ""
    assert environment["OPENAI_API_KEY"] == ""
    assert api["extra_hosts"] == ["host.docker.internal:host-gateway"]
    assert api["ports"] == ["127.0.0.1:18081:8000"]


def test_launcher_has_three_explicit_non_skipping_profiles() -> None:
    launcher = _LAUNCHER.read_text(encoding="utf-8")
    assert "lmstudio-local)" in launcher
    assert "ollama-local)" in launcher
    assert "ollama-cloud)" in launcher
    assert 'provider="local"' in launcher
    assert 'model="${ASK_DEV_PROVIDER_MODEL:-google/gemma-4-e4b}"' in launcher
    assert "http://host.docker.internal:1234/v1" in launcher
    assert "http://host.docker.internal:11434/v1" in launcher
    assert "https://ollama.com/v1" in launcher
    assert "OLLAMA_API_KEY is required" in launcher
    assert "ASK_DEV_PROVIDER_MODEL is required" in launcher
    assert "prepare_ask_dev_acceptance.py" in launcher
    assert "smoke_ask_dev_provider_profile.py" in launcher
    assert "ASK_DEV_ACCEPTANCE_EXPECTED_MODEL" in launcher
    assert "skip" not in launcher.lower()


def test_selected_cloud_profile_fails_before_compose_without_secret() -> None:
    result = subprocess.run(
        [str(_LAUNCHER), "--profile", "ollama-cloud"],
        cwd=_ROOT,
        env={"PATH": "/usr/bin:/bin", "ASK_DEV_PROVIDER_MODEL": "cloud-model"},
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 64
    assert "OLLAMA_API_KEY is required" in result.stderr


def test_selected_ollama_local_profile_fails_before_compose_without_model() -> None:
    result = subprocess.run(
        [str(_LAUNCHER), "--profile", "ollama-local"],
        cwd=_ROOT,
        env={"PATH": "/usr/bin:/bin"},
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 64
    assert "ASK_DEV_PROVIDER_MODEL is required" in result.stderr


def test_launcher_runs_the_shared_container_source_guard_after_boot() -> None:
    """CHAOS-3572: the provider-profile launcher boots its own compose
    project with `--project-directory <ops_root>`, the same bind-mount
    hazard `run_ask_dev_compose.sh` has -- a stack booted from a different
    worktree keeps serving that worktree's source after being handed to
    this one, and nothing else here would report it. Must source the SAME
    shared guard the main launcher uses (not a copy), and must run it after
    the api container boots but before anything reads or writes through it
    (`fixtures generate` here, `fixtures world-restore` in the main
    launcher).
    """

    launcher = _LAUNCHER.read_text(encoding="utf-8")
    guard_script = _ROOT / "scripts" / "acceptance" / "container_source_guard.sh"
    assert guard_script.exists()
    assert 'source "${script_dir}/container_source_guard.sh"' in launcher, (
        "the provider-profile launcher must source the SHARED guard"
    )
    assert "container_source_guard_check" in launcher

    boot_index = launcher.index(
        "up -d --build --wait \\\n  postgres pgbouncer clickhouse valkey migrate api"
    )
    guard_call_index = launcher.index("container_source_guard_check ")
    generate_index = launcher.index("dev-hops fixtures generate")

    assert boot_index < guard_call_index < generate_index, (
        "the guard must run immediately after boot and before fixtures are "
        "generated against a possibly-wrong container"
    )


def test_live_smoke_uses_public_conversation_and_bounded_sse_contract() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert "/api/v1/dev/conversations" in smoke
    assert "text/event-stream" in smoke
    assert "DevStreamEvent.model_validate" in smoke
    assert "validate_stream(events)" in smoke
    assert "StreamEventType.ANSWER_COMPLETED" in smoke
    assert 'answer.model.provider_source == "platform"' in smoke
    assert "answer.model.provider_family == expected_provider" in smoke
    assert 'answer.status.value != "error"' in smoke

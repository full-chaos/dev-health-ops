"""CHAOS-3219 Phase 3: unit coverage for the scripted-engine precondition.

The defect these tests encode really happened: an armed run reported 140
receipts green while the scripted decision/fault engine was not loaded at
all. Every test below is written against a real observed shape, not an
imagined one.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from scripts.acceptance.corpus.compose_context import ComposeContext
from scripts.acceptance.corpus.scripted_engine import (
    ScriptedEngineUnavailableError,
    require_scripted_engine_loaded,
    scripted_engine_status,
)

#: The EXACT body the container returned before the compose fix, captured by
#: executing the image the 2026-08-07 04:55 armed run used.
_OBSERVED_DEGRADED_BODY = {
    "status": "degraded",
    "script": "ask-dev-scripted-v1",
    "scripted_engine": {
        "loaded": False,
        "role": "legacy_agent",
        "reason": (
            "try_load_engine returned None -- the script directory is "
            "unreachable or unreadable from this container"
        ),
    },
}

#: The EXACT body after the fix, same image, mount + env var supplied.
_OBSERVED_READY_BODY = {
    "status": "ready",
    "script": "ask-dev-scripted-v1",
    "scripted_engine": {"loaded": True, "role": "legacy_agent", "cases": 93},
}


def _context() -> ComposeContext:
    return ComposeContext(
        project_name="ask-dev-acceptance",
        project_directory=Path("/tmp/ops"),
        profile="ask-dev-acceptance",
        compose_files=(Path("compose.yml"),),
        api_service="api",
    )


def _runner(stdout: str = "", *, returncode: int = 0, stderr: str = "") -> Any:
    def run(*_args: Any, **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=[], returncode=returncode, stdout=stdout, stderr=stderr
        )

    return run


class TestScriptedEngineStatus:
    def test_reads_the_loaded_shape_observed_after_the_fix(self) -> None:
        status = scripted_engine_status(
            _context(), runner=_runner(json.dumps(_OBSERVED_READY_BODY))
        )
        assert status.loaded is True
        assert status.role == "legacy_agent"
        assert status.cases == 93

    def test_reads_the_degraded_shape_observed_before_the_fix(self) -> None:
        status = scripted_engine_status(
            _context(), runner=_runner(json.dumps(_OBSERVED_DEGRADED_BODY))
        )
        assert status.loaded is False
        assert status.reason is not None
        assert "unreachable" in status.reason

    def test_telemetry_banners_before_the_payload_do_not_break_parsing(self) -> None:
        """The real container prints 'Sentry initialised' and OpenTelemetry
        lines on stdout before anything we asked for. A parser that assumed
        the payload stood alone would fail against the actual stack."""

        noisy = (
            "Sentry initialised\n"
            "OpenTelemetry tracing initialised\n"
            f"{json.dumps(_OBSERVED_READY_BODY)}\n"
        )
        assert scripted_engine_status(_context(), runner=_runner(noisy)).loaded

    @pytest.mark.parametrize(
        ("stdout", "returncode", "match"),
        (
            ("", 1, "exited 1"),
            ("", 0, "produced no output"),
            ("not json at all", 0, "unparseable"),
            ('"a string"', 0, "non-object"),
            ('{"status":"ready"}', 0, "no 'scripted_engine' block"),
        ),
    )
    def test_every_unmeasurable_shape_raises_rather_than_defaulting_to_ok(
        self, stdout: str, returncode: int, match: str
    ) -> None:
        """Rule 4: a measurement that did not happen must FAIL. Every one of
        these could plausibly have been coded as 'assume fine and move on',
        which is how the original defect stayed invisible."""

        with pytest.raises(ScriptedEngineUnavailableError, match=match):
            scripted_engine_status(
                _context(), runner=_runner(stdout, returncode=returncode)
            )

    def test_a_missing_docker_binary_raises_rather_than_passing(self) -> None:
        def run(*_args: Any, **_kwargs: Any) -> Any:
            raise FileNotFoundError("docker")

        with pytest.raises(ScriptedEngineUnavailableError, match="not available"):
            scripted_engine_status(_context(), runner=run)

    def test_a_timeout_raises_rather_than_passing(self) -> None:
        def run(*_args: Any, **_kwargs: Any) -> Any:
            raise subprocess.TimeoutExpired(cmd="probe", timeout=60.0)

        with pytest.raises(ScriptedEngineUnavailableError, match="timed out"):
            scripted_engine_status(_context(), runner=run)

    def test_the_probe_never_inherits_stdin(self) -> None:
        """Same load-bearing hazard db_verify documents: with stdin merely
        inherited this hangs forever under any shell-script driver -- a
        silent hang that burns the stack slot instead of failing."""

        seen: dict[str, Any] = {}

        def run(*_args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
            seen.update(kwargs)
            return subprocess.CompletedProcess(
                args=[], returncode=0, stdout=json.dumps(_OBSERVED_READY_BODY)
            )

        scripted_engine_status(_context(), runner=run)
        assert seen["stdin"] is subprocess.DEVNULL

    def test_the_probe_passes_capital_T_to_avoid_corrupting_stdout(self) -> None:
        seen: dict[str, Any] = {}

        def run(args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
            seen["args"] = args
            return subprocess.CompletedProcess(
                args=[], returncode=0, stdout=json.dumps(_OBSERVED_READY_BODY)
            )

        scripted_engine_status(_context(), runner=run)
        assert "-T" in seen["args"]
        assert "ask-dev-scripted-openai" in seen["args"]


class TestRequireScriptedEngineLoaded:
    def test_passes_when_the_engine_is_loaded_with_scripts(self) -> None:
        status = require_scripted_engine_loaded(
            _context(), runner=_runner(json.dumps(_OBSERVED_READY_BODY))
        )
        assert status.cases == 93

    def test_the_exact_production_failure_stops_the_run(self) -> None:
        """THE regression. This body is what the stack actually returned
        while a full armed run reported green."""

        with pytest.raises(ScriptedEngineUnavailableError) as excinfo:
            require_scripted_engine_loaded(
                _context(), runner=_runner(json.dumps(_OBSERVED_DEGRADED_BODY))
            )
        message = str(excinfo.value)
        assert "NOT loaded" in message
        # The message must name the remedy, not just the symptom -- whoever
        # hits this at 2am should not have to re-derive the diagnosis.
        assert "ASK_DEV_SCRIPTED_PROVIDER_SCRIPTS_DIR" in message
        assert "measured nothing" in message

    def test_loaded_but_zero_cases_is_rejected(self) -> None:
        """The second failure shape, and the reason a bare `loaded` boolean
        is insufficient: a directory that exists but is empty loads
        'successfully' and serves nothing."""

        body = {
            "status": "ready",
            "script": "ask-dev-scripted-v1",
            "scripted_engine": {"loaded": True, "role": "legacy_agent", "cases": 0},
        }
        with pytest.raises(ScriptedEngineUnavailableError, match="only 0 case"):
            require_scripted_engine_loaded(_context(), runner=_runner(json.dumps(body)))

    def test_a_truncated_script_directory_is_rejected(self) -> None:
        body = {
            "status": "ready",
            "script": "ask-dev-scripted-v1",
            "scripted_engine": {"loaded": True, "role": "legacy_agent", "cases": 5},
        }
        with pytest.raises(ScriptedEngineUnavailableError, match="below the required"):
            require_scripted_engine_loaded(
                _context(), runner=_runner(json.dumps(body)), minimum_cases=90
            )

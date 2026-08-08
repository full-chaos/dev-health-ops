"""Unit coverage for ``scripts.acceptance.corpus.db_verify``.

All tests inject a fake ``runner`` (dependency injection, matching
``subprocess.run``'s call shape) -- none of this needs a real Docker
daemon. The actual live ``docker compose exec`` path is exercised only by a
genuinely booted acceptance stack, out of the unit tier's reach by design.
"""

from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.acceptance.corpus.compose_context import ComposeContext
from scripts.acceptance.corpus.db_verify import (
    DbVerifyUnavailableError,
    exec_in_api,
    query_resolution_ledger_via_exec,
    query_transcript_assistant_schema_versions_via_exec,
    verify_world_digest_via_exec,
)


@dataclass(slots=True)
class _FakeResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


def _context() -> ComposeContext:
    return ComposeContext(
        project_name="dev-health-ask-dev-acceptance",
        project_directory=Path("/ops"),
        compose_files=(Path("/ops/compose.yml"),),
        profile="ask-dev-acceptance",
    )


class TestExecInApi:
    def test_success_returns_stdout(self) -> None:
        calls = []

        def runner(args, **kwargs):
            calls.append(args)
            return _FakeResult(returncode=0, stdout="hello\n")

        out = exec_in_api(_context(), ["echo", "hi"], runner=runner)
        assert out == "hello\n"
        assert calls[0][:2] == ["docker", "compose"]
        assert "exec" in calls[0]
        assert "-T" in calls[0]
        assert "api" in calls[0]
        assert calls[0][-2:] == ["echo", "hi"]

    def test_nonzero_exit_raises_with_stderr(self) -> None:
        def runner(args, **kwargs):
            return _FakeResult(returncode=1, stderr="boom")

        with pytest.raises(DbVerifyUnavailableError, match="boom"):
            exec_in_api(_context(), ["false"], runner=runner)

    def test_missing_docker_binary_raises(self) -> None:
        def runner(args, **kwargs):
            raise FileNotFoundError("no docker")

        with pytest.raises(DbVerifyUnavailableError, match="not available"):
            exec_in_api(_context(), ["true"], runner=runner)

    def test_timeout_raises(self) -> None:
        def runner(args, **kwargs):
            raise subprocess.TimeoutExpired(cmd=args, timeout=kwargs.get("timeout", 60))

        with pytest.raises(DbVerifyUnavailableError, match="timed out"):
            exec_in_api(_context(), ["sleep", "999"], runner=runner)

    def test_custom_api_service_name_is_used(self) -> None:
        calls = []

        def runner(args, **kwargs):
            calls.append(args)
            return _FakeResult(returncode=0, stdout="ok")

        context = ComposeContext(
            project_name="p",
            project_directory=Path("/ops"),
            compose_files=(),
            api_service="custom-api",
        )
        exec_in_api(context, ["true"], runner=runner)
        # The service argument immediately follows "-T" -- must be the
        # custom name, never the default "api" literal.
        service_index = calls[0].index("-T") + 1
        assert calls[0][service_index] == "custom-api"


#: Real-shaped (64 lowercase hex char) sha256 hexdigests -- matches what
#: `fixtures/world.py`'s `compute_world_digest` actually produces. Using
#: single-character placeholders here previously masked a real bug (Codex
#: round-3, HIGH): "" == "" also "matches", but is not a validation any
#: strict-format check should accept as a real digest.
_DIGEST_A = "a" * 64
_DIGEST_B = "b" * 64


class TestVerifyWorldDigestViaExec:
    def test_parses_a_matched_result(self) -> None:
        def runner(args, **kwargs):
            return _FakeResult(
                returncode=0,
                stdout=(
                    f'{{"pinned_digest": "{_DIGEST_A}", "live_digest": "{_DIGEST_A}", '
                    '"matched": true, "drifted_components": []}\n'
                ),
            )

        result = verify_world_digest_via_exec(
            _context(),
            manifest_path_in_container="/app/tests/acceptance/world/ask-dev-world.v1/world.json",
            sink="clickhouse://ch:ch@clickhouse:8123/default",
            postgres_uri="postgresql+asyncpg://postgres:postgres@pgbouncer:6432/postgres",
            runner=runner,
        )
        assert result.matched is True
        assert result.pinned_digest == _DIGEST_A

    def test_nonzero_exit_raises(self) -> None:
        def runner(args, **kwargs):
            return _FakeResult(
                returncode=1, stderr="world digest verification raised: boom"
            )

        with pytest.raises(DbVerifyUnavailableError, match="boom"):
            verify_world_digest_via_exec(
                _context(),
                manifest_path_in_container="/app/x/world.json",
                sink="clickhouse://x",
                postgres_uri="postgresql://x",
                runner=runner,
            )

    def test_missing_expected_key_raises(self) -> None:
        def runner(args, **kwargs):
            return _FakeResult(
                returncode=0, stdout=f'{{"pinned_digest": "{_DIGEST_A}"}}\n'
            )

        with pytest.raises(DbVerifyUnavailableError, match="must be a string"):
            verify_world_digest_via_exec(
                _context(),
                manifest_path_in_container="/app/x/world.json",
                sink="clickhouse://x",
                postgres_uri="postgresql://x",
                runner=runner,
            )

    def test_empty_digest_strings_are_rejected_not_treated_as_a_match(self) -> None:
        """Codex round-3, HIGH, confirmed: two empty strings are equal, so
        the previous (type-only) validation let a genuinely empty,
        never-computed pair of digests report `matched=True` -- an
        unverified world silently passing WORLD_DIGEST verification."""

        def runner(args, **kwargs):
            return _FakeResult(
                returncode=0,
                stdout='{"pinned_digest": "", "live_digest": "", "matched": true, '
                '"drifted_components": []}\n',
            )

        with pytest.raises(DbVerifyUnavailableError, match="sha256 digest"):
            verify_world_digest_via_exec(
                _context(),
                manifest_path_in_container="/app/x/world.json",
                sink="clickhouse://x",
                postgres_uri="postgresql://x",
                runner=runner,
            )

    def test_non_hex_digest_is_rejected(self) -> None:
        def runner(args, **kwargs):
            return _FakeResult(
                returncode=0,
                stdout='{"pinned_digest": "not-a-real-digest", '
                f'"live_digest": "{_DIGEST_A}", "matched": true, '
                '"drifted_components": []}\n',
            )

        with pytest.raises(DbVerifyUnavailableError, match="sha256 digest"):
            verify_world_digest_via_exec(
                _context(),
                manifest_path_in_container="/app/x/world.json",
                sink="clickhouse://x",
                postgres_uri="postgresql://x",
                runner=runner,
            )

    def test_matched_is_recomputed_never_trusted_from_the_inner_script(self) -> None:
        """Codex round-2, HIGH, confirmed: a non-bool truthy `matched` value
        (e.g. the STRING "false") used to slip a real digest mismatch past
        this function entirely. `matched` is now always recomputed from the
        two digest strings; the inner script's own claim is never read."""

        def runner(args, **kwargs):
            return _FakeResult(
                returncode=0,
                stdout=(
                    f'{{"pinned_digest": "{_DIGEST_A}", "live_digest": "{_DIGEST_B}", '
                    '"matched": "false", "drifted_components": ["x.y"]}\n'
                ),
            )

        result = verify_world_digest_via_exec(
            _context(),
            manifest_path_in_container="/app/x/world.json",
            sink="clickhouse://x",
            postgres_uri="postgresql://x",
            runner=runner,
        )
        assert result.matched is False

    def test_non_bool_matched_true_string_still_recomputed_correctly(self) -> None:
        def runner(args, **kwargs):
            return _FakeResult(
                returncode=0,
                stdout=(
                    f'{{"pinned_digest": "{_DIGEST_A}", "live_digest": "{_DIGEST_A}", '
                    '"matched": "false", "drifted_components": []}\n'
                ),
            )

        result = verify_world_digest_via_exec(
            _context(),
            manifest_path_in_container="/app/x/world.json",
            sink="clickhouse://x",
            postgres_uri="postgresql://x",
            runner=runner,
        )
        # Even though the inner script's own (bogus) "matched" claim was the
        # string "false", the digests are equal -- the recomputed value
        # must be the real True, never influenced by the untrusted claim.
        assert result.matched is True

    def test_non_string_drifted_components_raises(self) -> None:
        def runner(args, **kwargs):
            return _FakeResult(
                returncode=0,
                stdout=(
                    f'{{"pinned_digest": "{_DIGEST_A}", "live_digest": "{_DIGEST_B}", '
                    '"matched": true, "drifted_components": [1, 2]}\n'
                ),
            )

        with pytest.raises(DbVerifyUnavailableError, match="drifted_components"):
            verify_world_digest_via_exec(
                _context(),
                manifest_path_in_container="/app/x/world.json",
                sink="clickhouse://x",
                postgres_uri="postgresql://x",
                runner=runner,
            )

    def test_unparseable_stdout_raises(self) -> None:
        def runner(args, **kwargs):
            return _FakeResult(returncode=0, stdout="not json at all")

        with pytest.raises(DbVerifyUnavailableError, match="cannot parse"):
            verify_world_digest_via_exec(
                _context(),
                manifest_path_in_container="/app/x/world.json",
                sink="clickhouse://x",
                postgres_uri="postgresql://x",
                runner=runner,
            )

    def test_empty_stdout_raises(self) -> None:
        def runner(args, **kwargs):
            return _FakeResult(returncode=0, stdout="")

        with pytest.raises(DbVerifyUnavailableError, match="no stdout"):
            verify_world_digest_via_exec(
                _context(),
                manifest_path_in_container="/app/x/world.json",
                sink="clickhouse://x",
                postgres_uri="postgresql://x",
                runner=runner,
            )


class TestQueryResolutionLedgerViaExec:
    def test_parses_entries(self) -> None:
        def runner(args, **kwargs):
            return _FakeResult(
                returncode=0,
                stdout=(
                    '{"entries": [{"outcome": "exact_match", "mention_id": "m1", '
                    '"committed_label": "meridian/web-app", '
                    '"committed_canonical_id": "repo-01"}]}\n'
                ),
            )

        entries = query_resolution_ledger_via_exec(
            _context(), run_id="run-1", runner=runner
        )
        assert len(entries) == 1
        assert entries[0].outcome == "exact_match"
        assert entries[0].mention_id == "m1"
        assert entries[0].committed_label == "meridian/web-app"
        assert entries[0].committed_canonical_id == "repo-01"

    def test_empty_ledger_returns_empty_list(self) -> None:
        def runner(args, **kwargs):
            return _FakeResult(returncode=0, stdout='{"entries": []}\n')

        assert (
            query_resolution_ledger_via_exec(_context(), run_id="run-1", runner=runner)
            == []
        )

    def test_missing_entries_key_raises(self) -> None:
        def runner(args, **kwargs):
            return _FakeResult(returncode=0, stdout="{}\n")

        with pytest.raises(DbVerifyUnavailableError, match="no 'entries'"):
            query_resolution_ledger_via_exec(_context(), run_id="run-1", runner=runner)

    def test_run_id_is_passed_through_as_a_string(self) -> None:
        calls = []

        def runner(args, **kwargs):
            calls.append(args)
            return _FakeResult(returncode=0, stdout='{"entries": []}\n')

        import uuid

        run_id = uuid.uuid4()
        query_resolution_ledger_via_exec(_context(), run_id=run_id, runner=runner)
        assert str(run_id) in calls[0]

    def test_non_object_entry_raises_db_verify_unavailable_not_a_bare_type_error(
        self,
    ) -> None:
        """Codex round-2, HIGH, confirmed: indexing a malformed entry used
        to leak a raw KeyError/TypeError instead of the one exception type
        every caller of this module expects."""

        def runner(args, **kwargs):
            return _FakeResult(returncode=0, stdout='{"entries": ["not-an-object"]}\n')

        with pytest.raises(DbVerifyUnavailableError, match="non-object"):
            query_resolution_ledger_via_exec(_context(), run_id="run-1", runner=runner)

    def test_entry_missing_outcome_raises(self) -> None:
        def runner(args, **kwargs):
            return _FakeResult(
                returncode=0, stdout='{"entries": [{"mention_id": "m1"}]}\n'
            )

        with pytest.raises(DbVerifyUnavailableError, match="outcome"):
            query_resolution_ledger_via_exec(_context(), run_id="run-1", runner=runner)

    def test_non_string_committed_label_raises(self) -> None:
        def runner(args, **kwargs):
            return _FakeResult(
                returncode=0,
                stdout=(
                    '{"entries": [{"outcome": "exact_match", "mention_id": "m1", '
                    '"committed_label": 123}]}\n'
                ),
            )

        with pytest.raises(DbVerifyUnavailableError, match="committed_label"):
            query_resolution_ledger_via_exec(_context(), run_id="run-1", runner=runner)


class TestQueryTranscriptAssistantSchemaVersionsViaExec:
    def test_parses_versions(self) -> None:
        def runner(args, **kwargs):
            return _FakeResult(
                returncode=0,
                stdout='{"assistant_schema_versions": ["dev_answer.v2", null]}\n',
            )

        versions = query_transcript_assistant_schema_versions_via_exec(
            _context(), conversation_id="conv-1", runner=runner
        )
        assert versions == ["dev_answer.v2", None]

    def test_empty_result_returns_empty_list(self) -> None:
        def runner(args, **kwargs):
            return _FakeResult(
                returncode=0, stdout='{"assistant_schema_versions": []}\n'
            )

        assert (
            query_transcript_assistant_schema_versions_via_exec(
                _context(), conversation_id="conv-1", runner=runner
            )
            == []
        )

    def test_missing_key_raises(self) -> None:
        def runner(args, **kwargs):
            return _FakeResult(returncode=0, stdout="{}\n")

        with pytest.raises(DbVerifyUnavailableError, match="list of strings/nulls"):
            query_transcript_assistant_schema_versions_via_exec(
                _context(), conversation_id="conv-1", runner=runner
            )

    def test_nonzero_exit_raises(self) -> None:
        def runner(args, **kwargs):
            return _FakeResult(returncode=1, stderr="boom")

        with pytest.raises(DbVerifyUnavailableError, match="boom"):
            query_transcript_assistant_schema_versions_via_exec(
                _context(), conversation_id="conv-1", runner=runner
            )

    def test_non_string_non_null_version_raises(self) -> None:
        def runner(args, **kwargs):
            return _FakeResult(
                returncode=0, stdout='{"assistant_schema_versions": [123]}\n'
            )

        with pytest.raises(DbVerifyUnavailableError, match="list of strings/nulls"):
            query_transcript_assistant_schema_versions_via_exec(
                _context(), conversation_id="conv-1", runner=runner
            )


class TestExecInApiNeverInheritsStdin:
    """CHAOS-3462: ``docker compose exec -T`` must not inherit fd 0.

    Reported by the world-seeding lane, which hit it for real. ``-T``
    suppresses the pseudo-TTY but does NOT close stdin -- compose still
    forwards fd 0 into the container. With stdin merely inherited, the call
    blocks forever whenever the parent's fd 0 is an open pipe nobody closes,
    which is precisely what happens when the runner is driven from a shell
    script instead of an interactive terminal.
    ``scripts/acceptance/run_wave4_corpus.sh`` is exactly such a driver.

    The failure mode is the worst one this lane has: not a red, but a silent
    hang with no output, holding the gate slot while looking like nothing is
    happening. That is the same class of non-signal as the arming false
    green, so it gets a real test rather than a comment.
    """

    def test_the_runner_is_told_not_to_inherit_stdin(self) -> None:
        seen: dict[str, object] = {}

        def runner(args, **kwargs):
            seen.update(kwargs)
            return SimpleNamespace(returncode=0, stdout="ok", stderr="")

        exec_in_api(_context(), ["echo", "hi"], runner=runner)
        assert seen.get("stdin") is subprocess.DEVNULL, (
            "exec_in_api must pass stdin=DEVNULL; inheriting fd 0 hangs the "
            "whole run when driven from a shell script"
        )

    def test_the_hazard_is_real_and_devnull_is_what_fixes_it(self) -> None:
        """Rule 2: observe the guard failing.

        Proves the mechanism rather than asserting a keyword. A child that
        drains stdin -- which is how compose forwards fd 0 -- hangs when
        stdin is inherited from an open pipe, and completes when it is
        DEVNULL. If this ever stops reproducing, the DEVNULL above has
        stopped being load-bearing and the comment should be revisited.
        """

        child = [
            sys.executable,
            "-c",
            "import sys; sys.stdin.read()",
        ]
        # An open pipe nobody closes -- the shell-script driver shape.
        with subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            stdout=subprocess.PIPE,
        ) as holder:
            try:
                with pytest.raises(subprocess.TimeoutExpired):
                    subprocess.run(
                        child,
                        capture_output=True,
                        text=True,
                        timeout=3,
                        stdin=holder.stdout,
                    )
                completed = subprocess.run(
                    child,
                    capture_output=True,
                    text=True,
                    timeout=10,
                    stdin=subprocess.DEVNULL,
                )
                assert completed.returncode == 0
            finally:
                holder.kill()

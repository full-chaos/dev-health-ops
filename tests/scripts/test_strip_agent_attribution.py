"""Tests for ``scripts/strip_agent_attribution.py`` (lefthook commit-msg hook)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT_PATH = _REPO_ROOT / "scripts" / "strip_agent_attribution.py"


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "strip_agent_attribution", _SCRIPT_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["strip_agent_attribution"] = module
    spec.loader.exec_module(module)
    return module


_module = _load_module()
strip_agent_attribution = _module.strip_agent_attribution
main = _module.main


# ---------------------------------------------------------------------------
# strip_agent_attribution()
# ---------------------------------------------------------------------------


def test_strips_sisyphus_trailer_pair() -> None:
    msg = (
        "feat(api): add product telemetry endpoint (CHAOS-1789)\n"
        "\n"
        "Adds POST /api/v1/product-telemetry/events.\n"
        "\n"
        "Ultraworked with [Sisyphus](https://github.com/code-yeongyu/oh-my-openagent)\n"
        "\n"
        "Co-authored-by: Sisyphus <clio-agent@sisyphuslabs.ai>\n"
    )
    cleaned = strip_agent_attribution(msg)
    assert "Sisyphus" not in cleaned
    assert "Ultraworked" not in cleaned
    assert "clio-agent" not in cleaned
    assert cleaned == (
        "feat(api): add product telemetry endpoint (CHAOS-1789)\n"
        "\n"
        "Adds POST /api/v1/product-telemetry/events.\n"
    )


def test_strips_claude_code_trailer() -> None:
    msg = (
        "fix(db): handle BUSYGROUP on consumer group setup\n"
        "\n"
        "🤖 Generated with [Claude Code](https://claude.com/claude-code)\n"
        "\n"
        "Co-authored-by: Claude <noreply@anthropic.com>\n"
    )
    cleaned = strip_agent_attribution(msg)
    assert "Claude" not in cleaned
    assert "🤖" not in cleaned
    assert cleaned.rstrip() == "fix(db): handle BUSYGROUP on consumer group setup"


def test_preserves_real_human_co_author() -> None:
    msg = (
        "feat(metrics): wire DORA dashboard tile (CHAOS-1234)\n"
        "\n"
        "Co-authored-by: Alex Real-Human <alex@example.com>\n"
        "Co-authored-by: Sisyphus <clio-agent@sisyphuslabs.ai>\n"
    )
    cleaned = strip_agent_attribution(msg)
    assert "Alex Real-Human <alex@example.com>" in cleaned
    assert "Sisyphus" not in cleaned


def test_preserves_signed_off_by_and_refs() -> None:
    msg = (
        "fix(api): drop nullable sort key (CHAOS-450)\n"
        "\n"
        "Refs CHAOS-450\n"
        "Closes CHAOS-786\n"
        "Signed-off-by: Real Person <real@example.com>\n"
        "Ultraworked with [Sisyphus](https://github.com/code-yeongyu/oh-my-openagent)\n"
        "Co-authored-by: Sisyphus <clio-agent@sisyphuslabs.ai>\n"
    )
    cleaned = strip_agent_attribution(msg)
    assert "Refs CHAOS-450" in cleaned
    assert "Closes CHAOS-786" in cleaned
    assert "Signed-off-by: Real Person" in cleaned
    assert "Sisyphus" not in cleaned
    assert "Ultraworked" not in cleaned


def test_idempotent_on_clean_message() -> None:
    msg = "chore(deps): bump ruff to 0.6.5\n\nNo behaviour change.\n"
    assert strip_agent_attribution(msg) == msg


def test_collapses_runs_of_blank_lines() -> None:
    msg = (
        "subject\n"
        "\n"
        "body line\n"
        "\n"
        "\n"
        "\n"
        "Ultraworked with [Sisyphus](https://example.com/x)\n"
        "Co-authored-by: Sisyphus <agent@example.com>\n"
    )
    cleaned = strip_agent_attribution(msg)
    assert "\n\n\n" not in cleaned
    assert cleaned.endswith("body line\n")


# ---------------------------------------------------------------------------
# main() — CLI behaviour
# ---------------------------------------------------------------------------


def test_main_rewrites_file_in_place(tmp_path: Path) -> None:
    msg_file = tmp_path / "COMMIT_EDITMSG"
    msg_file.write_text(
        "feat: thing\n"
        "\n"
        "Ultraworked with [Sisyphus](https://example.com/x)\n"
        "Co-authored-by: Sisyphus <agent@example.com>\n"
    )
    rc = main(["strip_agent_attribution.py", str(msg_file)])
    assert rc == 0
    assert msg_file.read_text() == "feat: thing\n"


def test_main_returns_2_on_bad_argv(tmp_path: Path) -> None:
    assert main(["strip_agent_attribution.py"]) == 2
    assert main(["strip_agent_attribution.py", "a", "b"]) == 2


def test_main_returns_1_on_unreadable_file(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist"
    rc = main(["strip_agent_attribution.py", str(missing)])
    assert rc == 1


@pytest.mark.parametrize(
    "agent_line",
    [
        "Co-authored-by: Sisyphus <clio-agent@sisyphuslabs.ai>",
        "Co-authored-by: Sisyphus <other-agent@example.org>",
        "Co-authored-by: Claude <noreply@anthropic.com>",
        "Ultraworked with [SomeAgent](https://example.com)",
    ],
)
def test_strips_known_agent_lines_parametrized(agent_line: str) -> None:
    msg = f"subject\n\nbody\n\n{agent_line}\n"
    cleaned = strip_agent_attribution(msg)
    assert agent_line not in cleaned

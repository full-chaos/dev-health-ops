"""Platform-contract and workspace-AGENTS golden guards (non-public repo contracts).

The platform contract and the generated ``workspace-AGENTS.golden.md`` are machine/repository
contracts, not public product documentation. They were moved out of the public ``docs/`` tree and
are preserved under ``.github/docs-legacy/contributing/``. ``scripts/render_workspace_agents.py``
renders the contract into a workspace ``AGENTS.md``; these tests keep that renderer byte-identical
to the committed golden and protect the documentation-delivery decisions in the contract.
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = (
    ROOT / ".github" / "docs-legacy" / "contributing" / "platform-contract.md"
)
GOLDEN_PATH = (
    ROOT / ".github" / "docs-legacy" / "contributing" / "workspace-AGENTS.golden.md"
)
RENDERER_PATH = ROOT / "scripts" / "render_workspace_agents.py"


def test_platform_contract_preserves_the_documentation_delivery_decisions() -> None:
    assert CONTRACT_PATH.is_file(), f"missing platform contract: {CONTRACT_PATH}"

    contract = CONTRACT_PATH.read_text(encoding="utf-8")

    for required_text in (
        "docs.fullchaos.dev",
        "demo.fullchaos.dev",
        "Cloudflare Workers Static Assets",
        "GitHub Actions",
        "Workers Builds",
        "ClickHouse",
        "WorkUnits are evidence containers",
    ):
        assert required_text in contract


def test_renderer_matches_the_committed_workspace_agents_golden() -> None:
    assert RENDERER_PATH.is_file(), f"missing renderer: {RENDERER_PATH}"
    assert GOLDEN_PATH.is_file(), f"missing renderer golden: {GOLDEN_PATH}"

    result = subprocess.run(
        [sys.executable, str(RENDERER_PATH)],
        check=False,
        cwd=ROOT,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == GOLDEN_PATH.read_text(encoding="utf-8")


def test_renderer_writes_workspace_agents_only_when_explicitly_requested(
    tmp_path: Path,
) -> None:
    assert RENDERER_PATH.is_file(), f"missing renderer: {RENDERER_PATH}"
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir()

    result = subprocess.run(
        [
            sys.executable,
            str(RENDERER_PATH),
            "--workspace-root",
            str(workspace_root),
        ],
        check=False,
        cwd=ROOT,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert (workspace_root / "AGENTS.md").read_text(encoding="utf-8") == (
        GOLDEN_PATH.read_text(encoding="utf-8")
    )

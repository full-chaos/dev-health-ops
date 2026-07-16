#!/usr/bin/env python3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "docs" / "contributing" / "platform-contract.md"


def workspace_root(arguments: list[str]) -> Path | None:
    if not arguments:
        return None
    if len(arguments) == 2 and arguments[0] == "--workspace-root":
        return Path(arguments[1]).resolve()
    raise SystemExit("usage: render_workspace_agents.py [--workspace-root PATH]")


def main(arguments: list[str]) -> int:
    rendered = CONTRACT_PATH.read_text(encoding="utf-8")
    target_root = workspace_root(arguments)
    if target_root is None:
        sys.stdout.write(rendered)
        return 0
    if not target_root.is_dir():
        raise SystemExit(f"workspace root is not a directory: {target_root}")
    (target_root / "AGENTS.md").write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

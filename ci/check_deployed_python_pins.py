"""Fail unless every package in ci/requirements-deployed-python-api.txt is
installed at exactly the pinned version.

The venue oracles run the Python api from the uv environment with the
deployed image's versions overlaid (see .github/workflows/venue-oracles.yml).
An overlay that silently did not apply would leave the oracles proving
parity against the wrong Python, so the step verifies the mechanism, not
just that the install command exited zero.
"""

from __future__ import annotations

import sys
from importlib import metadata
from pathlib import Path

REQUIREMENTS = Path(__file__).with_name("requirements-deployed-python-api.txt")


def main() -> int:
    wrong: list[str] = []
    checked = 0
    for raw in REQUIREMENTS.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        name, _, wanted = line.partition("==")
        checked += 1
        try:
            have = metadata.version(name)
        except metadata.PackageNotFoundError:
            wrong.append(f"{name}: pinned {wanted}, not installed")
            continue
        if have != wanted:
            wrong.append(f"{name}: pinned {wanted}, installed {have}")
    if checked == 0:
        print("no pins read from", REQUIREMENTS, file=sys.stderr)
        return 1
    if wrong:
        print(
            "deployed-version overlay did not apply:",
            *wrong,
            sep="\n  ",
            file=sys.stderr,
        )
        return 1
    print(
        f"deployed-version overlay verified: {checked} packages at the pinned versions"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

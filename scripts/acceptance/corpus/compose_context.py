"""CHAOS-3219 Wave 4 Phase 2 Lane 2a: the docker-compose invocation context
shared by the corpus runner's verification plane (``db_verify.py``).

Team-lead ruling (2026-08-06), binding: the runner's VERIFICATION plane
(WORLD_DIGEST guard, resolution-ledger read) reaches the acceptance
Compose project's database through the CONTAINER boundary
(``docker compose exec -T``) -- never by exposing a host port on
postgres/clickhouse, never by adding a test-only product API endpoint.
Product-facing case execution stays wire-only against the public API; only
these two harness concerns get exec-based DB access.

Every field here duplicates a literal already hardcoded in
``run_ask_dev_compose.sh`` (``project_name``, the two compose file paths,
the ``ask-dev-acceptance`` profile) -- MUST NOT drift from that launcher.
Duplicated rather than imported because the launcher is bash and this is
Python; ``test_compose_context_matches_the_launcher_literals`` (this
package's static-wiring-guard companion) greps both files for the same
literal strings so a future rename in one is caught in the unit tier, not
discovered live.

The "duplicate project-name trap" this repo's own history warns about
(``ops/compose.yml`` project ``dev-health-ops`` vs the parent repo's
``compose.yml`` project ``dev-health`` destroying each other's postgres
pre-CHAOS-3142): every compose invocation here ALWAYS passes an explicit
``--project-name``/``--project-directory``, never relies on Compose's
directory-name-derived default.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

__all__ = ["ComposeContext"]

#: Must match run_ask_dev_compose.sh's `project_name=` literal exactly.
DEFAULT_PROJECT_NAME = "dev-health-ask-dev-acceptance"

#: Must match run_ask_dev_compose.sh's `--profile ask-dev-acceptance` literal.
DEFAULT_PROFILE = "ask-dev-acceptance"

#: This file lives at <ops_root>/scripts/acceptance/corpus/compose_context.py.
_OPS_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True, slots=True)
class ComposeContext:
    project_name: str
    project_directory: Path
    compose_files: tuple[Path, ...]
    profile: str | None = None
    api_service: str = field(default="api")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> ComposeContext:
        """Build the context the launcher itself would use, honoring the
        SAME override env vars ``run_ask_dev_compose.sh`` reads (e.g. a
        developer running two acceptance-shaped stacks side by side)."""

        source = env if env is not None else os.environ
        project_name = source.get(
            "ASK_DEV_ACCEPTANCE_PROJECT_NAME", DEFAULT_PROJECT_NAME
        )
        ops_root = Path(source.get("ASK_DEV_ACCEPTANCE_OPS_ROOT", str(_OPS_ROOT)))
        return cls(
            project_name=project_name,
            project_directory=ops_root,
            compose_files=(
                ops_root / "compose.yml",
                ops_root / "tests" / "acceptance" / "compose.ask-dev.yml",
            ),
            profile=DEFAULT_PROFILE,
        )

    def base_args(self) -> list[str]:
        args = [
            "docker",
            "compose",
            "--project-name",
            self.project_name,
            "--project-directory",
            str(self.project_directory),
        ]
        for compose_file in self.compose_files:
            args += ["-f", str(compose_file)]
        if self.profile is not None:
            args += ["--profile", self.profile]
        return args

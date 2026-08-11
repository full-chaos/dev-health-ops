"""Unit coverage for ``scripts.acceptance.corpus.compose_context``."""

from __future__ import annotations

from pathlib import Path

from scripts.acceptance.corpus.compose_context import (
    DEFAULT_PROFILE,
    DEFAULT_PROJECT_NAME,
    ComposeContext,
)


class TestComposeContextFromEnv:
    def test_defaults_match_the_launcher_literals(self) -> None:
        context = ComposeContext.from_env({})
        assert (
            context.project_name
            == DEFAULT_PROJECT_NAME
            == "dev-health-ask-dev-acceptance"
        )
        assert context.profile == DEFAULT_PROFILE == "ask-dev-acceptance"
        assert context.compose_files[0].name == "compose.yml"
        assert context.compose_files[1] == (
            context.project_directory / "tests" / "acceptance" / "compose.ask-dev.yml"
        )

    def test_project_name_override(self) -> None:
        context = ComposeContext.from_env(
            {"ASK_DEV_ACCEPTANCE_PROJECT_NAME": "custom-project"}
        )
        assert context.project_name == "custom-project"

    def test_ops_root_override(self, tmp_path: Path) -> None:
        context = ComposeContext.from_env(
            {"ASK_DEV_ACCEPTANCE_OPS_ROOT": str(tmp_path)}
        )
        assert context.project_directory == tmp_path
        assert context.compose_files[0] == tmp_path / "compose.yml"


class TestBaseArgs:
    def test_includes_project_name_and_directory_explicitly(self) -> None:
        context = ComposeContext(
            project_name="p",
            project_directory=Path("/ops"),
            compose_files=(Path("/ops/compose.yml"),),
        )
        args = context.base_args()
        assert args[:4] == ["docker", "compose", "--project-name", "p"]
        assert "--project-directory" in args
        assert "/ops" in args

    def test_includes_every_compose_file_with_its_own_flag(self) -> None:
        context = ComposeContext(
            project_name="p",
            project_directory=Path("/ops"),
            compose_files=(Path("/ops/a.yml"), Path("/ops/b.yml")),
        )
        args = context.base_args()
        assert args.count("-f") == 2
        assert "/ops/a.yml" in args
        assert "/ops/b.yml" in args

    def test_includes_profile_when_set(self) -> None:
        context = ComposeContext(
            project_name="p",
            project_directory=Path("/ops"),
            compose_files=(),
            profile="my-profile",
        )
        args = context.base_args()
        assert "--profile" in args
        assert "my-profile" in args

    def test_omits_profile_flag_when_none(self) -> None:
        context = ComposeContext(
            project_name="p",
            project_directory=Path("/ops"),
            compose_files=(),
            profile=None,
        )
        args = context.base_args()
        assert "--profile" not in args


class TestStaysInSyncWithTheLauncher:
    """Static wiring guard (repo convention, ``test_ask_dev_compose.py``'s
    pattern): a future rename of the project name/profile/compose paths in
    ``run_ask_dev_compose.sh`` without updating ``compose_context.py``'s
    defaults must fail in the unit tier, not be discovered against a live
    stack."""

    def test_project_name_literal_matches_the_launcher(self) -> None:
        launcher = (
            Path(__file__).resolve().parents[3]
            / "scripts"
            / "acceptance"
            / "run_ask_dev_compose.sh"
        ).read_text(encoding="utf-8")
        assert (
            f'project_name="{DEFAULT_PROJECT_NAME}-${{RANDOM}}${{RANDOM}}"' in launcher
        )
        assert "ASK_DEV_ACCEPTANCE_PROJECT_NAME" in launcher

    def test_profile_literal_matches_the_launcher(self) -> None:
        launcher = (
            Path(__file__).resolve().parents[3]
            / "scripts"
            / "acceptance"
            / "run_ask_dev_compose.sh"
        ).read_text(encoding="utf-8")
        assert f"--profile {DEFAULT_PROFILE}" in launcher

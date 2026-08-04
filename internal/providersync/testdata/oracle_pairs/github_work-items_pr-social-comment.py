"""Live Python oracle for the GraphQL PR issue-comment adapter."""

from __future__ import annotations

import contextlib
import dataclasses
import io
import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names

with (
    contextlib.redirect_stdout(io.StringIO()),
    contextlib.redirect_stderr(io.StringIO()),
):
    from dev_health_ops.providers.github.client import GitHubWorkClient

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_CLIENT_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/github/client.py"


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    return dataclasses.asdict(GitHubWorkClient._comment_from_graphql(case["raw_node"]))


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/work-items/pr-social-comment",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _CLIENT_SOURCE.read_text(), "GitHubGraphQLComment"
        ),
    )
)

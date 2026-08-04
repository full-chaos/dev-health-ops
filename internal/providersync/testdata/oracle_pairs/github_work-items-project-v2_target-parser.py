"""Live legacy target-parser decision oracle for the durable Go parser."""

from __future__ import annotations

import contextlib
import io
import os
import pathlib
from typing import Any
from unittest.mock import patch

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import (
    RETURN_LITERAL,
    dict_literal_keys,
)

with (
    contextlib.redirect_stdout(io.StringIO()),
    contextlib.redirect_stderr(io.StringIO()),
):
    from dev_health_ops.metrics.work_items import parse_github_projects_v2_env

_THIS_FILE = pathlib.Path(__file__)


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    with patch.dict(os.environ, {"GITHUB_PROJECTS_V2": case["raw"]}, clear=False):
        targets = parse_github_projects_v2_env()
    return {"targets": [f"{org}:{number}" for org, number in targets]}


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/work-items-project-v2/target-parser",
        build_row=_build_row,
        reflected_fields=lambda: dict_literal_keys(
            _THIS_FILE.read_text(), "_build_row", (RETURN_LITERAL,)
        ),
    )
)

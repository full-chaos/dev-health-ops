from __future__ import annotations

import pathlib
import sys
import types
from datetime import datetime

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dict_assigned_keys
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
SOURCE = REPO_ROOT / "src/dev_health_ops/processors/github.py"


class _Repo:
    def __init__(self) -> None:
        self.kwargs = {}

    def get_workflow_runs(self, **kwargs):
        self.kwargs = kwargs
        return []


def _build(case):
    processor = load_live_module(SOURCE)
    safe_archive = types.ModuleType("dev_health_ops.connectors.utils.safe_archive")
    safe_archive.iter_zip_members = lambda *_args, **_kwargs: ()
    sys.modules[safe_archive.__name__] = safe_archive
    repo = _Repo()
    processor._fetch_github_test_artifacts_sync(
        object(),
        repo,
        "acme",
        "api",
        datetime.fromisoformat(case["since_at"].replace("Z", "+00:00")),
        case["default_branch"],
        200,
        datetime.fromisoformat(case["before_at"].replace("Z", "+00:00")),
    )
    return {"branch": repo.kwargs.get("branch"), "created": repo.kwargs.get("created")}


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/tests/selection",
        build_row=_build,
        reflected_fields=lambda: dict_assigned_keys(
            SOURCE.read_text(), "_fetch_github_test_artifacts_sync", "list_kwargs"
        ),
    )
)

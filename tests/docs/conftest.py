from __future__ import annotations

import sys
from pathlib import Path

import pytest

# `scripts/` has no `__init__.py` and pytest.ini only puts `src` on
# sys.path, so tests that import `scripts.docs_publication` as a library
# (rather than invoking it as a subprocess) need the ops repo root on
# sys.path too. Scoped to this conftest so it only affects tests/docs/.

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def pytest_ignore_collect(collection_path: Path, config: pytest.Config) -> bool:
    is_negative_fixture = collection_path.name == "test_missing_issue.py"
    selected_directly = any(
        Path(argument).name == "test_missing_issue.py" for argument in config.args
    )
    return is_negative_fixture and not selected_directly

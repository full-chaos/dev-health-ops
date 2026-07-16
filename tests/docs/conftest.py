from __future__ import annotations

from pathlib import Path

import pytest


def pytest_ignore_collect(collection_path: Path, config: pytest.Config) -> bool:
    is_negative_fixture = collection_path.name == "test_missing_issue.py"
    selected_directly = any(
        Path(argument).name == "test_missing_issue.py" for argument in config.args
    )
    return is_negative_fixture and not selected_directly

from __future__ import annotations

import sys
from pathlib import Path

# `scripts/` has no `__init__.py` and pytest.ini only puts `src` on sys.path, so
# tests that import `scripts.mutation_harness` as a library need the ops repo
# root on sys.path too. Scoped to this conftest, matching tests/docs/conftest.py.

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

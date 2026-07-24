"""Load a named live Python module for a checked-in Go parity oracle.

The Go tests pass source paths so a failure identifies the production contract
being compared.  Those paths must not become an arbitrary-code input: each
oracle admits only its fixed source file, then imports that known module from
this checkout through Python's normal import machinery.
"""

from __future__ import annotations

import importlib
import io
import sys
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]


def load_live_module(source: Path, *, relative_path: str, module_name: str) -> Any:
    """Import the allowlisted production module named by a parity oracle."""
    expected = (ROOT / relative_path).resolve()
    if source.resolve() != expected:
        raise ValueError(f"unexpected oracle source: {source}")

    source_root = str(ROOT / "src")
    if source_root not in sys.path:
        sys.path.insert(0, source_root)
    # Production imports initialize observability in this test process.  Keep
    # that bootstrap chatter out of the JSON-only Go oracle protocol.
    with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
        module = importlib.import_module(module_name)
    module_path = Path(getattr(module, "__file__", "")).resolve()
    if module_path != expected:
        raise RuntimeError(
            f"oracle imported {module_path}, expected checked-out source {expected}"
        )
    return module

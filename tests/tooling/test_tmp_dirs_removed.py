"""A test that makes a temporary directory outside pytest's tmp_path or Go's t.TempDir must remove it.

CHAOS-7185: tests built a ~100 MB binary into a mkdtemp directory per run and never removed it, and a fixed-name
Go build cache under the system temp directory grew by ~850 MB per host. The check below is static and covers
the two ways a test makes such a directory:

* a Python test module that calls ``mkdtemp`` or ``tempfile.gettempdir()`` must also remove what it made
  (``rmtree``, ``atexit.register`` or a finalizer);
* a Go ``_test.go`` file that calls ``os.MkdirTemp`` must also call ``os.RemoveAll``.

Python programs that a Go test embeds as text (frozen oracles) are outside this check: they run only when a
golden is recorded.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SELF = Path(__file__).resolve()
PY_CLEANUP = re.compile(r"\brmtree\b|\batexit\.register\b|\baddfinalizer\b")
GO_CLEANUP = re.compile(r"\bos\.RemoveAll\(")
GO_SKIP_DIRS = {"web", "node_modules", ".git", ".venv", "vendor"}


def _py_makes_temp_dir(source: str) -> bool:
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            name = (
                func.attr
                if isinstance(func, ast.Attribute)
                else getattr(func, "id", "")
            )
            if name in {"mkdtemp", "gettempdir"}:
                return True
    return False


def python_offenders(root: Path) -> list[str]:
    out = []
    for path in sorted((root / "tests").rglob("*.py")):
        if path.resolve() == SELF:
            continue
        source = path.read_text(encoding="utf-8")
        if _py_makes_temp_dir(source) and not PY_CLEANUP.search(source):
            out.append(str(path.relative_to(root)))
    return out


def go_offenders(root: Path) -> list[str]:
    out = []
    for path in sorted(root.rglob("*_test.go")):
        rel = path.relative_to(root)
        if rel.parts[0] in GO_SKIP_DIRS:
            continue
        source = path.read_text(encoding="utf-8")
        if "os.MkdirTemp(" in source and not GO_CLEANUP.search(source):
            out.append(str(rel))
    return out


def test_python_tests_remove_the_temp_dirs_they_make():
    assert python_offenders(ROOT) == []


def test_go_tests_remove_the_temp_dirs_they_make():
    assert go_offenders(ROOT) == []


def test_the_check_reads_something():
    # A scan that matched no file would pass for any tree.
    assert any((ROOT / "tests").rglob("*.py"))
    assert any(ROOT.rglob("*_test.go"))
    assert _py_makes_temp_dir("import tempfile\ntempfile.mkdtemp()\n")


@pytest.mark.parametrize(
    "body",
    [
        "import tempfile\nd = tempfile.mkdtemp(prefix='x-')\n",
        "import tempfile\nfrom pathlib import Path\nC = Path(tempfile.gettempdir()) / 'fixed'\n",
    ],
)
def test_python_check_flags_a_leak_and_accepts_a_cleanup(tmp_path, body):
    (tmp_path / "tests").mkdir()
    module = tmp_path / "tests" / "test_leak.py"
    module.write_text(body, encoding="utf-8")
    assert python_offenders(tmp_path) == ["tests/test_leak.py"]
    module.write_text(
        body + "import atexit, shutil\natexit.register(shutil.rmtree, 'x')\n",
        encoding="utf-8",
    )
    assert python_offenders(tmp_path) == []


def test_go_check_flags_a_leak_and_accepts_a_cleanup(tmp_path):
    source = tmp_path / "pkg" / "leak_test.go"
    source.parent.mkdir()
    source.write_text(
        'package pkg\nvar _, _ = os.MkdirTemp("", "x-")\n', encoding="utf-8"
    )
    assert go_offenders(tmp_path) == ["pkg/leak_test.go"]
    source.write_text(
        'package pkg\nvar d, _ = os.MkdirTemp("", "x-")\nfunc f() { os.RemoveAll(d) }\n',
        encoding="utf-8",
    )
    assert go_offenders(tmp_path) == []

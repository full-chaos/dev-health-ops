"""A test that makes a temporary directory outside pytest's tmp_path or Go's t.TempDir must remove it.

CHAOS-7185: tests built a ~100 MB binary into a mkdtemp directory per run and never removed it, and a fixed-name
Go build cache under the system temp directory grew by ~850 MB per host. The check below is static and covers
the two ways a test makes such a directory:

* a Python test module that calls ``mkdtemp`` or ``tempfile.gettempdir()`` must also remove what it made
  (a real ``rmtree``, ``atexit.register`` or finalizer call, found in the syntax tree).

The Go half (a ``_test.go`` file that calls ``os.MkdirTemp`` must also call ``os.RemoveAll`` in code) moved to
internal/cigates/tmp_dirs_removed_test.go (CHAOS-10178). This Python half stays until the pytest steps go (CHAOS-10180).

Python programs that a Go test embeds as text (frozen oracles) are outside this check: they run only when a
golden is recorded.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SELF = Path(__file__).resolve()
CLEANUP_CALLS = {"rmtree", "addfinalizer"}


def _call_name(call: ast.Call) -> str:
    func = call.func
    return func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")


def _tempfile_aliases(tree: ast.AST) -> dict[str, str]:
    """Local names bound to tempfile.mkdtemp / tempfile.gettempdir by `from tempfile import x as y`."""
    out = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "tempfile":
            for alias in node.names:
                if alias.name in {"mkdtemp", "gettempdir"}:
                    out[alias.asname or alias.name] = alias.name
    return out


def _py_makes_temp_dir(source: str) -> bool:
    """A mkdtemp call, or a gettempdir() that is joined into a path (a read of the name alone makes nothing)."""
    tree = ast.parse(source)
    aliases = _tempfile_aliases(tree)

    def resolved(call: ast.Call) -> str:
        name = _call_name(call)
        return aliases.get(name, name)

    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and resolved(node) == "mkdtemp":
            return True
        joins = (isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div)) or (
            isinstance(node, ast.Call) and _call_name(node) == "join"
        )
        if joins and any(
            isinstance(inner, ast.Call) and resolved(inner) == "gettempdir"
            for inner in ast.walk(node)
        ):
            return True
    return False


def _py_removes(source: str) -> bool:
    """A real call of rmtree, addfinalizer or atexit.register; a comment or a string that names one is not."""
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if _call_name(node) in CLEANUP_CALLS:
            return True
        if (
            isinstance(func, ast.Attribute)
            and func.attr == "register"
            and isinstance(func.value, ast.Name)
            and func.value.id == "atexit"
        ):
            return True
    return False


def python_offenders(root: Path) -> list[str]:
    out = []
    for path in sorted((root / "tests").rglob("*.py")):
        if path.resolve() == SELF:
            continue
        source = path.read_text(encoding="utf-8")
        if _py_makes_temp_dir(source) and not _py_removes(source):
            out.append(str(path.relative_to(root)))
    return out


def test_python_tests_remove_the_temp_dirs_they_make():
    assert python_offenders(ROOT) == []


def test_the_check_reads_something():
    # A scan that matched no file would pass for any tree.
    assert any((ROOT / "tests").rglob("*.py"))
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


@pytest.mark.parametrize(
    "body",
    [
        "import tempfile\nd = tempfile.mkdtemp()\n# shutil.rmtree(d) later\n",
        "import tempfile\nd = tempfile.mkdtemp()\nNOTE = 'rmtree atexit.register'\n",
        "from tempfile import mkdtemp as make\nd = make()\n",
        "from tempfile import mkdtemp\nd = mkdtemp()\n",
        "from tempfile import gettempdir as g\nfrom pathlib import Path\nC = Path(g()) / 'fixed'\n",
        "import os, tempfile\nC = os.path.join(tempfile.gettempdir(), 'fixed')\n",
    ],
)
def test_python_check_is_not_fooled_by_text_or_an_alias(tmp_path, body):
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_leak.py").write_text(body, encoding="utf-8")
    assert python_offenders(tmp_path) == ["tests/test_leak.py"]


def test_python_check_accepts_a_real_cleanup_and_a_read_of_the_temp_dir(tmp_path):
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_clean.py").write_text(
        "import shutil, tempfile\nd = tempfile.mkdtemp()\nshutil.rmtree(d)\n",
        encoding="utf-8",
    )
    (tmp_path / "tests" / "test_read.py").write_text(
        "import tempfile\nprint(tempfile.gettempdir())\n", encoding="utf-8"
    )
    assert python_offenders(tmp_path) == []

"""CHAOS-3617: the arm is optional, removable, contained and deletable.

Four claims the issue makes about the arm's *relationship to the rest of the
system*, each of which would be easy to assert loosely and easy to violate
quietly:

* Graphiti is an optional extra and is never imported at module scope;
* the arm is not reachable from a production import path, nor from Ask Dev,
  ACR or MCP;
* no graph-native surface (Cypher, ontology mutation, deletion, maintenance)
  is exposed anywhere a client could reach;
* org deletion visits the trial store, and does so through the registry.
"""

from __future__ import annotations

import ast
import re
import subprocess
import sys
from pathlib import Path

import pytest

from dev_health_ops.api.services.derived_store_registry import (
    EXTERNAL_DERIVED_STORES,
    DerivedStoreKind,
)
from dev_health_ops.context_fabric.graph_arm import store as store_module
from dev_health_ops.context_fabric.graph_arm.backend import (
    GRAPHITI_EXTRA,
    TELEMETRY_ENV_VAR,
    require_graphiti,
)
from dev_health_ops.context_fabric.graph_arm.readback import READ_ONLY_QUERIES

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ARM_ROOT = _REPO_ROOT / "src" / "dev_health_ops" / "context_fabric"

#: A Cypher node pattern, which no English sentence contains.
_CYPHER_MATCH = re.compile(r"\bMATCH\s*\(")
#: Write and maintenance clauses, word-boundary matched so ``result SET`` in
#: prose cannot fail this and ``SETTINGS`` cannot pass it.
_CYPHER_WRITE = re.compile(r"\b(CREATE|MERGE|DELETE|DETACH|SET|REMOVE|DROP|CALL)\b")


def _string_constants(path: Path) -> list[str]:
    """Every string literal in a module, docstrings excluded.

    Docstrings are excluded deliberately: this module's own prose describes
    the queries it forbids, and a check that cannot tell a docstring from a
    query constant is a check that fails for reasons unrelated to what it
    claims to measure.
    """

    tree = ast.parse(path.read_text())
    docstrings: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(
            node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef
        ):
            body = getattr(node, "body", [])
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                docstrings.add(id(body[0].value))
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
    ]


def _module_level_imports(path: Path) -> set[str]:
    """Every module imported at import time, excluding ``TYPE_CHECKING``."""

    tree = ast.parse(path.read_text())
    names: set[str] = set()

    class Visitor(ast.NodeVisitor):
        def __init__(self) -> None:
            self.depth = 0

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            return  # function bodies are lazy by definition

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
            return

        def visit_If(self, node: ast.If) -> None:
            # `if TYPE_CHECKING:` blocks never execute at runtime.
            test = ast.unparse(node.test)
            if "TYPE_CHECKING" in test:
                return
            self.generic_visit(node)

        def visit_Import(self, node: ast.Import) -> None:
            names.update(alias.name for alias in node.names)

        def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
            if node.module:
                names.add(node.module)

    Visitor().visit(tree)
    return names


class TestGraphitiIsOptional:
    def test_no_arm_module_imports_graphiti_at_module_scope(self) -> None:
        """A module-scope import would make the extra mandatory in practice.

        The whole containment story rests on this: with it, an environment
        without the extra imports the entire package and runs the entire
        non-live suite; without it, every consumer of ``dev_health_ops``
        would need graphiti-core installed.
        """

        offenders: dict[str, set[str]] = {}
        for path in sorted(_ARM_ROOT.rglob("*.py")):
            leaked = {
                name
                for name in _module_level_imports(path)
                if name.split(".")[0] in {"graphiti_core", "falkordb"}
            }
            if leaked:
                offenders[str(path.relative_to(_REPO_ROOT))] = leaked
        assert not offenders, offenders

    def test_the_arm_imports_cleanly_without_graphiti_installed(self) -> None:
        """Proven by blocking the module, not by hoping it is absent.

        The dev environment installs every extra, so "it imported fine here"
        is no evidence at all. This inserts a meta-path finder that makes
        ``graphiti_core`` genuinely unimportable, then imports the arm.
        """

        script = (
            "import sys\n"
            "class Block:\n"
            "    def find_module(self, name, path=None):\n"
            "        return self.find_spec(name, path)\n"
            "    def find_spec(self, name, path=None, target=None):\n"
            "        if name.split('.')[0] in {'graphiti_core', 'falkordb'}:\n"
            "            raise ModuleNotFoundError(name)\n"
            "        return None\n"
            "sys.meta_path.insert(0, Block())\n"
            "import dev_health_ops.context_fabric.graph_arm as arm\n"
            "import dev_health_ops.context_fabric.graph_arm.store\n"
            "import dev_health_ops.context_fabric.graph_arm.packet_builder\n"
            "import dev_health_ops.context_fabric.graph_arm.backend\n"
            "assert 'graphiti_core' not in sys.modules\n"
            "print('ok')\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            cwd=_REPO_ROOT,
            env={
                "PYTHONPATH": str(_REPO_ROOT / "src"),
                "PATH": "/usr/bin:/bin",
                "OTEL_ENABLED": "false",
                "HOME": str(_REPO_ROOT),
            },
        )
        assert result.returncode == 0, result.stderr
        assert "ok" in result.stdout

    def test_the_extra_is_named_in_the_unavailable_error(self) -> None:
        assert GRAPHITI_EXTRA == "context-graph-trial"

    def test_graphiti_is_not_a_default_dependency(self) -> None:
        import tomllib

        data = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text())
        defaults = " ".join(data["project"]["dependencies"]).lower()
        assert "graphiti" not in defaults
        assert "falkordb" not in defaults
        extras = data["project"]["optional-dependencies"]
        assert any("graphiti" in item for item in extras[GRAPHITI_EXTRA])

    def test_the_pinned_version_is_exact(self) -> None:
        """A floating range makes two runs of "the same" arm incomparable."""

        import tomllib

        data = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text())
        pins = data["project"]["optional-dependencies"][GRAPHITI_EXTRA]
        assert any("==" in item for item in pins), pins


class TestNoProductionCoupling:
    def test_no_production_module_imports_the_arm_at_module_scope(self) -> None:
        """One lazy registration point, and no others.

        ``derived_store_registry`` imports the arm inside a function so org
        deletion can visit the trial store; that is the *only* reference the
        production tree is allowed to hold.
        """

        src = _REPO_ROOT / "src" / "dev_health_ops"
        offenders: dict[str, set[str]] = {}
        for path in sorted(src.rglob("*.py")):
            if _ARM_ROOT in path.parents or path == _ARM_ROOT:
                continue
            leaked = {
                name for name in _module_level_imports(path) if "context_fabric" in name
            }
            if leaked:
                offenders[str(path.relative_to(_REPO_ROOT))] = leaked
        assert not offenders, offenders

    def test_the_arm_exposes_no_router_task_or_tool_registration(self) -> None:
        """No FastAPI route, no Celery task, no Ask Dev tool, no MCP surface.

        Graph-native traversal, Cypher, ontology mutation and maintenance
        must not reach Ask Dev or an MCP client, and the cheapest way to
        guarantee that is for the arm to register nothing at all.
        """

        forbidden = (
            "APIRouter",
            "@router.",
            "shared_task",
            "celery_app.task",
            "register_tool",
            "mcp",
        )
        offenders: dict[str, list[str]] = {}
        for path in sorted(_ARM_ROOT.rglob("*.py")):
            text = path.read_text()
            hits = [token for token in forbidden if token in text]
            if hits:
                offenders[str(path.relative_to(_REPO_ROOT))] = hits
        assert not offenders, offenders

    def test_the_declared_query_surface_is_exhaustive(self) -> None:
        """Every Cypher string in the arm is one of the declared three.

        Scanned from the AST's string *constants*, not with a text grep:
        grepping the file matches the prose in its own docstrings, which is
        how a containment check quietly becomes vacuous (or, as here,
        permanently red for the wrong reason).
        """

        declared = set(READ_ONLY_QUERIES)
        stray: dict[str, list[str]] = {}
        for path in sorted(_ARM_ROOT.rglob("*.py")):
            for value in _string_constants(path):
                if _CYPHER_MATCH.search(value) and value not in declared:
                    stray.setdefault(str(path.relative_to(_REPO_ROOT)), []).append(
                        value.strip().splitlines()[0]
                    )
        assert not stray, stray

    def test_no_declared_query_writes_or_maintains(self) -> None:
        """Read-only in the literal sense: no write or DDL clause.

        Word-boundary matched, so ``result SET`` in a comment cannot make
        this fail and ``SETTINGS`` cannot make it pass.
        """

        for query in READ_ONLY_QUERIES:
            assert query.lstrip().upper().startswith("MATCH")
            offending = _CYPHER_WRITE.findall(query.upper())
            assert not offending, (query, offending)


class TestTelemetryContainment:
    def test_telemetry_is_forced_off(self, monkeypatch) -> None:
        """Graphiti's upstream default is *enabled* and posts to PostHog.

        A trial that ingests one organization's project, team and repository
        structure has no business opening an outbound analytics connection,
        so the override is unconditional -- an environment that already said
        ``true`` is overridden too.
        """

        monkeypatch.setenv(TELEMETRY_ENV_VAR, "true")
        pytest.importorskip(
            "graphiti_core", reason="graphiti-core is an optional trial extra"
        )
        require_graphiti()
        import os

        assert os.environ[TELEMETRY_ENV_VAR] == "false"


class TestOrgDeletionRegistration:
    def test_the_trial_store_is_registered_as_an_external_derived_store(self) -> None:
        names = {entry.name for entry in EXTERNAL_DERIVED_STORES}
        assert store_module.TRIAL_DERIVED_STORE_NAME in names

    def test_the_registration_carries_a_real_visit_callable(self) -> None:
        """A registered store with ``visit=None`` only produces a warning.

        The deletion service treats that as "registered but not wired", which
        is a disclosed gap -- not deletion.
        """

        entry = next(
            item
            for item in EXTERNAL_DERIVED_STORES
            if item.name == store_module.TRIAL_DERIVED_STORE_NAME
        )
        assert entry.kind is DerivedStoreKind.EXTERNAL
        assert entry.visit is not None

    def test_the_registry_reaches_the_arm_lazily(self) -> None:
        from dev_health_ops.api.services import derived_store_registry

        assert "context_fabric" not in _module_level_imports(
            Path(derived_store_registry.__file__)
        )

    @pytest.mark.asyncio
    async def test_deletion_is_a_no_op_when_no_trial_store_is_configured(
        self, monkeypatch
    ) -> None:
        """Production today. Org deletion must not be blocked by an optional
        trial store that does not exist in the environment doing the delete."""

        monkeypatch.delenv("CONTEXT_FABRIC_GRAPH_STORE_URI", raising=False)
        entry = next(
            item
            for item in EXTERNAL_DERIVED_STORES
            if item.name == store_module.TRIAL_DERIVED_STORE_NAME
        )
        assert entry.visit is not None
        assert await entry.visit("org_alpha", False) == 0

    @pytest.mark.asyncio
    async def test_an_unreachable_configured_store_does_not_block_deletion(
        self, monkeypatch
    ) -> None:
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_STORE_URI", "falkor://127.0.0.1:1")

        def _boom(*_args, **_kwargs):
            raise store_module.StoreUnavailableError("unreachable")

        monkeypatch.setattr(store_module.GraphArmStore, "for_org", _boom)
        assert await store_module.org_deletion_visit("org_alpha", False) == 0

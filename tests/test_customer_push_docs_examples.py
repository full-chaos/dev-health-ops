"""Drift guard binding the customer-push docs to the record-kind registry (CHAOS-2701).

Every v1 record kind exposed by ``schema_registry.iter_record_kinds()`` must have its
canonical example payload snippet-included (``--8<-- "<kind>.json"``) somewhere under
``docs/customer-push-ingestion/`` -- so adding a new record kind fails CI until the
customer-facing docs show an example for it, and every snippet path a doc references must
resolve to a real example file. Offline; runs in the default unit suite.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from dev_health_ops.api.external_ingest import schema_registry as registry

_REPO_ROOT = Path(__file__).resolve().parents[1]
_DOCS_DIR = _REPO_ROOT / "docs" / "customer-push-ingestion"
# The canonical record-kind example payloads live here (also a snippets base_path).
_EXAMPLES_DIR = (
    _REPO_ROOT / "src" / "dev_health_ops" / "api" / "external_ingest" / "examples"
)
# All pymdownx.snippets base_path dirs (mkdocs.yml) a customer-push doc may
# ``--8<--`` from. Keep in sync with mkdocs.yml's snippets base_path list.
_SNIPPET_BASE_DIRS = (
    _EXAMPLES_DIR,
    _REPO_ROOT / "examples" / "customer-push",
)

# ``--8<-- "pull_request.v1.json"`` (double or single quotes, arbitrary leading whitespace).
_SNIPPET_RE = re.compile(r"""--8<--\s+["']([^"'\n]+)["']""")


def _all_snippet_refs() -> list[tuple[Path, str]]:
    refs: list[tuple[Path, str]] = []
    for md in sorted(_DOCS_DIR.glob("*.md")):
        for target in _SNIPPET_RE.findall(md.read_text()):
            refs.append((md, target))
    return refs


def test_every_record_kind_has_a_docs_example() -> None:
    """A `<kind>.v1.json` snippet include must appear in the customer-push docs."""
    included = {target for _md, target in _all_snippet_refs()}
    missing = [
        f"{kind}.json"
        for kind, _model in registry.iter_record_kinds()
        if f"{kind}.json" not in included
    ]
    assert not missing, (
        "record kinds have no example snippet-included under "
        f'docs/customer-push-ingestion/: {missing}. Add a `--8<-- "<kind>.json"` '
        "include (see examples.md / schemas-and-idempotency.md)."
    )


def test_every_docs_snippet_reference_resolves() -> None:
    """No customer-push doc may reference a snippet file that doesn't exist.

    A snippet resolves if it exists under ANY configured base_path dir (the
    package examples dir or ``examples/customer-push``).
    """
    dangling = [
        f"{md.relative_to(_REPO_ROOT)} -> {target}"
        for md, target in _all_snippet_refs()
        if not any((base / target).is_file() for base in _SNIPPET_BASE_DIRS)
    ]
    assert not dangling, f"docs reference missing snippet files: {dangling}"


@pytest.mark.parametrize("kind", [k for k, _ in registry.iter_record_kinds()])
def test_registry_example_file_exists(kind: str) -> None:
    """The registry's kinds and the on-disk example files stay in lockstep."""
    assert (_EXAMPLES_DIR / f"{kind}.json").is_file(), f"no example file for {kind}"

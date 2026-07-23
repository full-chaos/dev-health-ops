"""Drift guard binding the Customer Push docs to the record-kind registry (CHAOS-2701).

The canonical example payloads are the single source of truth and live under
``src/dev_health_ops/api/external_ingest/examples/`` (one ``<kind>.json`` per v1 record kind).
The canonical Customer Push pages under ``docs/integrate/customer-push/`` direct integrators to
those server-shipped examples rather than inlining 21 copies, so the drift protection is:

* the registry's record kinds and the on-disk example files stay in exact lockstep (adding a new
  record kind fails CI until its canonical example exists, and an orphan example fails too); and
* the canonical Customer Push pages exist and point readers at the maintained per-kind examples; and
* any ``--8<--`` snippet a canonical page does reference must resolve to a real example file.

Offline; runs in the default unit suite.
"""

from __future__ import annotations

import re
from pathlib import Path

from dev_health_ops.api.external_ingest import schema_registry as registry

_REPO_ROOT = Path(__file__).resolve().parents[1]
# Canonical, task-oriented Customer Push pages (public IA).
_DOCS_DIR = _REPO_ROOT / "docs" / "integrate" / "customer-push"
# The canonical record-kind example payloads live here (single source of truth).
_EXAMPLES_DIR = (
    _REPO_ROOT / "src" / "dev_health_ops" / "api" / "external_ingest" / "examples"
)

# ``--8<-- "pull_request.v1.json"`` (double or single quotes, arbitrary leading whitespace).
_SNIPPET_RE = re.compile(r"""--8<--\s+["']([^"'\n]+)["']""")


def _registry_kinds() -> set[str]:
    return {kind for kind, _model in registry.iter_record_kinds()}


def _example_stems() -> set[str]:
    return {path.name.removesuffix(".json") for path in _EXAMPLES_DIR.glob("*.json")}


def _all_snippet_refs() -> list[tuple[Path, str]]:
    refs: list[tuple[Path, str]] = []
    for md in sorted(_DOCS_DIR.glob("*.md")):
        for target in _SNIPPET_RE.findall(md.read_text()):
            refs.append((md, target))
    return refs


def test_registry_and_example_files_stay_in_exact_lockstep() -> None:
    """Every registry record kind has a canonical example file, and vice versa.

    This is the real drift guard: adding a v1 record kind fails until its
    ``<kind>.json`` example exists under the source-owned examples dir, and an
    orphan example with no backing registry kind fails too.
    """
    kinds = _registry_kinds()
    stems = _example_stems()

    missing = sorted(kinds - stems)
    orphan = sorted(stems - kinds)
    assert not missing, (
        "record kinds have no canonical example file under "
        f"src/dev_health_ops/api/external_ingest/examples/: {missing}"
    )
    assert not orphan, (
        f"example files have no backing registry record kind (stale examples): {orphan}"
    )


def test_canonical_customer_push_docs_reference_the_maintained_examples() -> None:
    """The canonical pages must exist and steer integrators to the server-shipped examples.

    We do not inline the 21 example payloads into the public pages (one source of
    truth stays in ``src/.../examples/``); instead the schema-discovery task guide
    must direct integrators to use the server-shipped examples for each record kind.
    """
    assert _DOCS_DIR.is_dir(), f"missing canonical Customer Push docs dir: {_DOCS_DIR}"
    pages = list(_DOCS_DIR.glob("*.md"))
    assert pages, f"no canonical Customer Push pages under {_DOCS_DIR}"

    schema_discovery = _DOCS_DIR / "schema-discovery.md"
    assert schema_discovery.is_file(), (
        f"missing canonical schema-discovery page: {schema_discovery}"
    )
    text = schema_discovery.read_text().lower()
    assert "example" in text and "record kind" in text, (
        "the canonical schema-discovery page no longer directs integrators to the "
        "server-shipped examples for each record kind"
    )


def test_every_docs_snippet_reference_resolves() -> None:
    """No canonical Customer Push page may reference a snippet file that doesn't exist.

    A snippet resolves if it exists under the source-owned examples dir (the single
    source of truth for canonical example payloads).
    """
    dangling = [
        f"{md.relative_to(_REPO_ROOT)} -> {target}"
        for md, target in _all_snippet_refs()
        if not (_EXAMPLES_DIR / target).is_file()
    ]
    assert not dangling, f"docs reference missing snippet files: {dangling}"

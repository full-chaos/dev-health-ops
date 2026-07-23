"""Characterization guard for the investment-materialization operator instructions.

The durable operator instructions for ``investment materialize`` (batch modes, provider batch
support, and the Postgres/ClickHouse data-plane split) have no canonical ``docs/operate/`` page;
they are preserved as source evidence under ``.github/docs-legacy/ops/investment-materialization.md``.
This test pins that archived source so the documented operator contract cannot silently drift.
"""

from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_DOC_PATH = (
    _REPO_ROOT / ".github" / "docs-legacy" / "ops" / "investment-materialization.md"
)


def test_investment_batch_operator_docs_cover_modes_and_providers():
    text = _DOC_PATH.read_text()

    for required in (
        "--llm-batch-mode",
        "sync",
        "auto",
        "provider_batch",
        "openai",
        "qwen",
        "deterministic\nfallback",
        "Postgres",
        "ClickHouse",
    ):
        assert required in text

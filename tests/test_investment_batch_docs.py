from __future__ import annotations

from pathlib import Path


def test_investment_batch_operator_docs_cover_modes_and_providers():
    text = Path("docs/ops/investment-materialization.md").read_text()

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

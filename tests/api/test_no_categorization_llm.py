from __future__ import annotations

from pathlib import Path


def test_api_work_units_no_categorization_imports():
    path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "dev_health_ops"
        / "api"
        / "services"
        / "work_units.py"
    )
    text = path.read_text(encoding="utf-8")
    assert "investment_categorizer" not in text
    assert "categorize_text_bundle" not in text


def test_api_main_no_categorization_imports():
    path = Path(__file__).resolve().parents[2] / "src" / "dev_health_ops" / "api" / "main.py"
    text = path.read_text(encoding="utf-8")
    assert "investment_categorizer" not in text

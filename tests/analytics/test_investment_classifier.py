from pathlib import Path

from dev_health_ops.analytics.investment import InvestmentClassifier


def test_legacy_investment_classifier_fallback_is_assigned(tmp_path: Path) -> None:
    classifier = InvestmentClassifier(tmp_path / "missing-investment-areas.yaml")

    classification = classifier.classify({"labels": [], "component": "", "title": ""})

    assert classification.investment_area == "product"
    assert classification.project_stream == "general"
    assert classification.rule_id == "legacy_default"
    assert classification.confidence == 0.0

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)


@dataclass
class InvestmentClassification:
    investment_area: str
    project_stream: Optional[str]
    confidence: float
    rule_id: str


class InvestmentClassifier:
    def __init__(self, config_path: Path):
        self.rules = self._load_rules(config_path)

    def _load_rules(self, path: Path) -> List[Dict]:
        if not path.exists():
            logger.warning(f"Investment config not found at {path}, using defaults")
            return []
        with open(path, "r") as f:
            data = yaml.safe_load(f)
            # Sort by priority (lower is higher priority)
            return sorted(data.get("rules", []), key=lambda x: x.get("priority", 100))

    def classify(self, artifact: Dict[str, Any]) -> InvestmentClassification:
        """
        Artifact dictionary expectations:
        - labels: List[str]
        - path_prefix: List[str] (for commits/PRs - touched files)
        - component: str
        - epic: str
        - title: str
        """
        for rule in self.rules:
            match = rule.get("match", {})
            output = rule.get("output", {})

            if self._matches(match, artifact):
                return InvestmentClassification(
                    investment_area=output.get("investment_area", "unassigned"),
                    project_stream=output.get("project_stream"),
                    confidence=1.0,
                    rule_id=rule.get("id", "unassigned"),
                )

        return InvestmentClassification(
            investment_area="unassigned",
            project_stream=None,
            confidence=0.0,
            rule_id="unassigned",
        )

    def _matches(self, match_criteria: Dict, artifact: Dict) -> bool:
        if match_criteria.get("always"):
            return True

        # Check Labels
        if "label" in match_criteria:
            artifact_labels = set(lbl.lower() for lbl in artifact.get("labels", []))
            target_labels = set(lbl.lower() for lbl in match_criteria["label"])
            if not artifact_labels.intersection(target_labels):
                return False

        # Check Path Prefix (any match)
        if "path_prefix" in match_criteria:
            # Artifact paths should be a list of strings
            artifact_paths = artifact.get("paths", [])
            target_prefixes = match_criteria["path_prefix"]

            found = False
            for path in artifact_paths:
                for prefix in target_prefixes:
                    if path.startswith(prefix):
                        found = True
                        break
                if found:
                    break
            if not found:
                return False

        # Check Component
        if "component" in match_criteria:
            artifact_component = artifact.get("component")
            if artifact_component not in match_criteria["component"]:
                return False

        return True

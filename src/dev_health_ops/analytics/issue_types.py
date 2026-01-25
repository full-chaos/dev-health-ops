import logging
from pathlib import Path
from typing import Dict

import yaml

logger = logging.getLogger(__name__)

class IssueTypeNormalizer:
    def __init__(self, config_path: Path):
        self.mapping = self._load_config(config_path)

    def _load_config(self, path: Path) -> Dict:
        if not path.exists():
            logger.warning(f"Issue type mapping config not found at {path}, using defaults")
            return {}
        with open(path, "r") as f:
            return yaml.safe_load(f)

    def normalize(self, provider: str, raw_type: str, labels: list[str]) -> str:
        provider = provider.lower()
        
        # Jira: direct mapping from raw_type
        if provider == "jira":
            mapping = self.mapping.get("jira", {})
            return mapping.get(raw_type, "unassigned")

        # GitHub: infer from labels
        if provider == "github":
            mapping = self.mapping.get("github_labels", {})
            # Look for matching labels
            for label in labels:
                norm = mapping.get(label.lower())
                if norm:
                    return norm
            # Fallback or default
            return "unassigned"

        # GitLab: infer from labels (scoped or simple)
        if provider == "gitlab":
            mapping = self.mapping.get("gitlab_labels", {})
            for label in labels:
                norm = mapping.get(label.lower())
                if norm:
                    return norm
            return "unassigned"

        return "unassigned"

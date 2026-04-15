"""Environment and provider field normalization for cross-source joins."""

# Canonical environments
CANONICAL_ENVIRONMENTS = {"production", "staging", "development", "test", "preview"}

# Common aliases → canonical
_ENV_ALIASES: dict[str, str] = {
    "prod": "production",
    "prd": "production",
    "live": "production",
    "stg": "staging",
    "stage": "staging",
    "staging": "staging",
    "dev": "development",
    "develop": "development",
    "development": "development",
    "test": "test",
    "testing": "test",
    "qa": "test",
    "preview": "preview",
    "review": "preview",
}


def canonicalize_environment(raw_env: str, provider: str = "") -> str:
    """Normalize environment string to canonical value.

    - Case-insensitive matching
    - GitLab review/* → preview
    - Unknown values pass through lowercased (never drop data)
    """
    if not raw_env:
        return ""
    normalized = raw_env.strip().lower()

    # GitLab review apps: review/feature-branch → preview
    if normalized.startswith("review/") or normalized.startswith("review-"):
        return "preview"

    return _ENV_ALIASES.get(normalized, normalized)

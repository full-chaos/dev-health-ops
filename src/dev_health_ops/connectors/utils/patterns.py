"""
Common pattern matching utilities for connectors.

These provide fnmatch-style pattern matching for repository/project names
across different providers (GitHub, GitLab, etc.).
"""

import fnmatch


def match_name_pattern(full_name: str, pattern: str) -> bool:
    """
    Match a repository/project full name against a pattern using fnmatch-style matching.

    This is a provider-agnostic utility that can be used by GitHub, GitLab, and
    other connectors for consistent pattern matching behavior.

    :param full_name: Full name (e.g., 'org/repo', 'group/project').
    :param pattern: Pattern to match (e.g., 'org/m*', '*/sync*', 'org/*').
    :return: True if the pattern matches, False otherwise.

    Examples:
        >>> match_name_pattern('chrisgeo/dev-health-ops', 'chrisgeo/m*')
        False
        >>> match_name_pattern('chrisgeo/dev-health-ops', 'chrisgeo/dev*')
        True
        >>> match_name_pattern('anyorg/sync-tool', '*/sync*')
        True
        >>> match_name_pattern('org/repo', 'org/repo')
        True
    """
    return fnmatch.fnmatch(full_name.lower(), pattern.lower())


# Aliases for backward compatibility and semantic clarity
match_repo_pattern = match_name_pattern  # For GitHub
match_project_pattern = match_name_pattern  # For GitLab

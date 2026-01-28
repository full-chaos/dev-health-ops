"""Filter translation logic for GraphQL analytics queries.

This module translates GraphQL FilterInput to SQL predicates, matching
the semantics of the existing REST filter handling in api/services/filtering.py
and api/queries/scopes.py.

Key semantics:
- Empty list/None means "All" - no filtering applied
- Multiple values in a list are ORed (IN clause)
- Different filter dimensions are ANDed together
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ..models.inputs import FilterInput


def translate_scope_filter(
    level: str,
    ids: List[str],
    team_column: str = "team_id",
    repo_column: str = "repo_id",
    author_column: str = "author_id",
) -> Tuple[str, Dict[str, Any]]:
    """Translate scope filter to SQL predicate.

    Args:
        level: Scope level (org, team, repo, developer)
        ids: List of IDs to filter by. Empty means "All" - no filtering.
        team_column: Column name for team filtering
        repo_column: Column name for repo filtering
        author_column: Column name for developer/author filtering

    Returns:
        Tuple of (SQL clause string, params dict)

    Example:
        >>> translate_scope_filter("team", ["team-1", "team-2"])
        (" AND team_id IN %(scope_ids)s", {"scope_ids": ["team-1", "team-2"]})

        >>> translate_scope_filter("team", [])  # Empty means All
        ("", {})
    """
    if not ids:
        return "", {}

    if level == "team":
        return f" AND {team_column} IN %(scope_ids)s", {"scope_ids": ids}
    if level == "repo":
        return f" AND {repo_column} IN %(scope_ids)s", {"scope_ids": ids}
    if level == "developer":
        return f" AND {author_column} IN %(scope_ids)s", {"scope_ids": ids}

    # org or service level - no filtering at data layer
    return "", {}


def translate_work_category_filter(
    categories: List[str],
    use_investment: bool = False,
) -> Tuple[str, Dict[str, Any]]:
    """Translate work category filter to SQL predicate.

    Args:
        categories: List of categories to filter by. Empty means "All".
        use_investment: Whether using investment tables (affects column name)

    Returns:
        Tuple of (SQL clause string, params dict)
    """
    if not categories:
        return "", {}

    if use_investment:
        # For investment queries, filter by theme via subcategory_kv
        # Extract theme from subcategory key: "Theme.Subcategory" -> "Theme"
        return (
            " AND splitByChar('.', subcategory_kv.1)[1] IN %(work_categories)s",
            {"work_categories": categories},
        )
    else:
        # For non-investment queries, use investment_area column
        return " AND investment_area IN %(work_categories)s", {
            "work_categories": categories
        }


def translate_repo_filter(
    repos: List[str],
    repo_column: str = "repo_id",
) -> Tuple[str, Dict[str, Any]]:
    """Translate repo filter to SQL predicate.

    Args:
        repos: List of repo IDs to filter by. Empty means "All".
        repo_column: Column name for repo filtering

    Returns:
        Tuple of (SQL clause string, params dict)
    """
    if not repos:
        return "", {}

    return f" AND {repo_column} IN %(repo_filter_ids)s", {"repo_filter_ids": repos}


def translate_developer_filter(
    developers: List[str],
    author_column: str = "author_id",
) -> Tuple[str, Dict[str, Any]]:
    """Translate developer filter to SQL predicate.

    Args:
        developers: List of developer IDs to filter by. Empty means "All".
        author_column: Column name for author/developer filtering

    Returns:
        Tuple of (SQL clause string, params dict)
    """
    if not developers:
        return "", {}

    return f" AND {author_column} IN %(developer_ids)s", {"developer_ids": developers}


def translate_filters(
    filters: Optional["FilterInput"],
    use_investment: bool = False,
    team_column: str = "team_id",
    repo_column: str = "repo_id",
    author_column: str = "author_id",
) -> Tuple[str, Dict[str, Any]]:
    """Translate a complete FilterInput to SQL predicates.

    Combines all filter dimensions into a single SQL clause string and
    merged params dict. All non-empty filters are ANDed together.

    Args:
        filters: The GraphQL FilterInput, or None for no filtering
        use_investment: Whether using investment tables
        team_column: Column name for team filtering
        repo_column: Column name for repo filtering
        author_column: Column name for author/developer filtering

    Returns:
        Tuple of (SQL clause string, params dict)

    Example:
        >>> translate_filters(FilterInput(
        ...     scope=ScopeFilterInput(level=ScopeLevelInput.TEAM, ids=["team-1"]),
        ...     why=WhyFilterInput(work_category=["Feature Delivery"])
        ... ))
        (
            " AND team_id IN %(scope_ids)s AND investment_area IN %(work_categories)s",
            {"scope_ids": ["team-1"], "work_categories": ["Feature Delivery"]}
        )
    """
    if filters is None:
        return "", {}

    clauses: List[str] = []
    params: Dict[str, Any] = {}

    # Scope filter
    if filters.scope is not None:
        if use_investment and filters.scope.level.value == "team" and filters.scope.ids:
            clauses.append(
                " AND (ut.team_label IN %(scope_ids)s OR ut.team_id IN %(scope_ids)s)"
            )
            params["scope_ids"] = filters.scope.ids
        else:
            clause, scope_params = translate_scope_filter(
                level=filters.scope.level.value,
                ids=filters.scope.ids,
                team_column=team_column,
                repo_column=repo_column,
                author_column=author_column,
            )
            if clause:
                clauses.append(clause)
                params.update(scope_params)

    # Who filter - developers
    if filters.who is not None and filters.who.developers:
        clause, dev_params = translate_developer_filter(
            developers=filters.who.developers,
            author_column=author_column,
        )
        if clause:
            clauses.append(clause)
            params.update(dev_params)

    # What filter - repos
    if filters.what is not None and filters.what.repos:
        clause, repo_params = translate_repo_filter(
            repos=filters.what.repos,
            repo_column=repo_column,
        )
        if clause:
            clauses.append(clause)
            params.update(repo_params)

    # Why filter - work category
    if filters.why is not None and filters.why.work_category:
        clause, cat_params = translate_work_category_filter(
            categories=filters.why.work_category,
            use_investment=use_investment,
        )
        if clause:
            clauses.append(clause)
            params.update(cat_params)

    # Combine all clauses
    filter_clause = "".join(clauses)

    return filter_clause, params

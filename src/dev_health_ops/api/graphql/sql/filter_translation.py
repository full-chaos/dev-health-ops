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

import re
from typing import TYPE_CHECKING, Any

from ..errors import ValidationError

if TYPE_CHECKING:
    from ..models.inputs import FilterInput

# CHAOS-2385/CHAOS-2492: author_email is the only identity column any
# developer/author predicate can ever match (git_commits, git_pull_requests,
# user_metrics_daily, commit_metrics; /api/v1/filters/options populates the
# quick-filter picker with exactly this column). GraphQL input, URL-decoded
# REST query params, and the advanced WhoSection UI (web repo) can all pass
# arbitrary free-form strings (e.g. a raw "alice, bob" string instead of a
# properly split array) -- silently building a predicate against a
# non-email value would just match nothing and look like an unrelated bug
# rather than a bad-input error. Validate format before ANY of who.developers
# / scope.level=developer becomes a predicate (or a rejection).
_EMAIL_PATTERN = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


def _validate_developer_emails(values: list[str], field: str) -> None:
    """Raise ValidationError if any developer filter value isn't an email."""
    invalid = [v for v in values if not _EMAIL_PATTERN.match(v)]
    if invalid:
        raise ValidationError(
            f"{field}.developers must be email addresses (author_email is "
            "the shared identity column across git_commits/"
            "git_pull_requests/user_metrics_daily/commit_metrics); got "
            f"non-email value(s): {invalid!r}",
            field=field,
            value=invalid,
        )


def translate_scope_filter(
    level: str,
    ids: list[str],
    team_column: str = "team_id",
    repo_column: str = "repo_id",
    author_column: str = "author_email",
) -> tuple[str, dict[str, Any]]:
    """Translate scope filter to SQL predicate.

    Args:
        level: Scope level (org, team, repo, developer)
        ids: List of IDs to filter by. Empty means "All" - no filtering.
        team_column: Column name for team filtering
        repo_column: Column name for repo filtering
        author_column: Column name for developer/author filtering (default: real ClickHouse column `author_email`, e.g. git_commits/user_metrics_daily; CHAOS-2385 -- `author_id` does not exist in any ClickHouse table)

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
    categories: list[str],
    use_investment: bool = False,
) -> tuple[str, dict[str, Any]]:
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
    repos: list[str],
    repo_column: str = "repo_id",
) -> tuple[str, dict[str, Any]]:
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
    developers: list[str],
    author_column: str = "author_email",
) -> tuple[str, dict[str, Any]]:
    """Translate developer filter to SQL predicate.

    Args:
        developers: List of developer IDs to filter by. Empty means "All".
        author_column: Column name for author/developer filtering (default: real ClickHouse column `author_email`; CHAOS-2385)

    Returns:
        Tuple of (SQL clause string, params dict)
    """
    if not developers:
        return "", {}

    return f" AND {author_column} IN %(developer_ids)s", {"developer_ids": developers}


def translate_filters(
    filters: FilterInput | None,
    use_investment: bool = False,
    team_column: str = "team_id",
    repo_column: str = "repo_id",
    author_column: str = "author_email",
) -> tuple[str, dict[str, Any]]:
    """Translate a complete FilterInput to SQL predicates.

    Combines all filter dimensions into a single SQL clause string and
    merged params dict. All non-empty filters are ANDed together.

    Args:
        filters: The GraphQL FilterInput, or None for no filtering
        use_investment: Whether using investment tables
        team_column: Column name for team filtering
        repo_column: Column name for repo filtering
        author_column: Column name for author/developer filtering (default: real ClickHouse column `author_email`; CHAOS-2385 -- `author_id` does not exist in any ClickHouse table)

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

    clauses: list[str] = []
    params: dict[str, Any] = {}

    # Scope filter
    if filters.scope is not None:
        if use_investment and filters.scope.level.value == "team" and filters.scope.ids:
            clauses.append(
                " AND (ut.team_label IN %(scope_ids)s OR ut.team_id IN %(scope_ids)s)"
            )
            params["scope_ids"] = filters.scope.ids
        elif filters.scope.level.value == "developer" and filters.scope.ids:
            _validate_developer_emails(filters.scope.ids, field="scope")
            # CHAOS-2385: no ClickHouse table reachable through this generic
            # analytics compiler carries a per-developer breakdown yet --
            # investment_metrics_daily has no author column at all, and
            # work_unit_investments/latest_work_unit_investments doesn't
            # either (CHAOS-2492 adds investment-path support via a
            # companion join, at which point this predicate becomes
            # table-aware instead of unconditional). Reject explicitly
            # rather than emit a predicate against a column that doesn't
            # exist on the resolved source table (mirrors the
            # honest-rejection precedent in compiler.py's
            # _reject_filtered_same_dimension_flow_matrix, CHAOS-2487).
            raise ValidationError(
                "scope.level=developer filtering is not yet supported for "
                "this query; remove the developer scope (CHAOS-2492 tracks "
                "investment-path support).",
                field="scope",
                value="developer",
            )
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
        _validate_developer_emails(filters.who.developers, field="who")
        # CHAOS-2385: same gap as scope.level=developer above -- reject
        # rather than emit a predicate against a nonexistent column.
        raise ValidationError(
            "who.developers filtering is not yet supported for this query; "
            "remove the developer filter (CHAOS-2492 tracks investment-path "
            "support).",
            field="who",
            value="developers",
        )

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

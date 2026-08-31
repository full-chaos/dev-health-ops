package analytics

import (
	"regexp"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// emailPattern ports filter_translation.py's _EMAIL_PATTERN (:31) --
// author_email is the only identity column any developer/author predicate
// can ever match (CHAOS-2385/CHAOS-2492).
var emailPattern = regexp.MustCompile(`^[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+$`)

func validateDeveloperEmails(values []string, field string) error {
	var invalid []string
	for _, v := range values {
		if !emailPattern.MatchString(v) {
			invalid = append(invalid, v)
		}
	}
	if len(invalid) > 0 {
		return newValidationError(field, invalid,
			"%s.developers must be email addresses (author_email is the shared "+
				"identity column across git_commits/git_pull_requests/"+
				"user_metrics_daily/commit_metrics); got non-email value(s): %v",
			field, invalid)
	}
	return nil
}

// filterClause is the Go equivalent of Python's `tuple[str, dict[str,
// Any]]` filter-translation return shape: a SQL fragment (starting with "
// AND ...", or empty) plus its bindings.
type filterClause struct {
	sql      string
	bindings []clickhouse.Binding
}

func emptyFilterClause() filterClause { return filterClause{} }

// translateScopeFilter ports translate_scope_filter (filter_translation.py:48-85).
func translateScopeFilter(level model.ScopeLevelInput, ids []string, teamColumn, repoColumn, authorColumn string) filterClause {
	if len(ids) == 0 {
		return emptyFilterClause()
	}
	switch level {
	case model.ScopeLevelInputTeam:
		return filterClause{sql: " AND " + teamColumn + " IN {scope_ids:Array(String)}", bindings: []clickhouse.Binding{{Name: "scope_ids", Value: ids}}}
	case model.ScopeLevelInputRepo:
		return filterClause{sql: " AND " + repoColumn + " IN {scope_ids:Array(String)}", bindings: []clickhouse.Binding{{Name: "scope_ids", Value: ids}}}
	case model.ScopeLevelInputDeveloper:
		return filterClause{sql: " AND " + authorColumn + " IN {scope_ids:Array(String)}", bindings: []clickhouse.Binding{{Name: "scope_ids", Value: ids}}}
	}
	// org or service level -- no filtering at data layer.
	return emptyFilterClause()
}

// translateWorkCategoryFilter ports translate_work_category_filter
// (filter_translation.py:88-115).
func translateWorkCategoryFilter(categories []string, useInvestment bool) filterClause {
	if len(categories) == 0 {
		return emptyFilterClause()
	}
	if useInvestment {
		return filterClause{
			sql:      " AND splitByChar('.', subcategory_kv.1)[1] IN {work_categories:Array(String)}",
			bindings: []clickhouse.Binding{{Name: "work_categories", Value: categories}},
		}
	}
	return filterClause{
		sql:      " AND investment_area IN {work_categories:Array(String)}",
		bindings: []clickhouse.Binding{{Name: "work_categories", Value: categories}},
	}
}

// translateRepoFilter ports translate_repo_filter (filter_translation.py:118-134).
func translateRepoFilter(repos []string, repoColumn string) filterClause {
	if len(repos) == 0 {
		return emptyFilterClause()
	}
	return filterClause{
		sql:      " AND " + repoColumn + " IN {repo_filter_ids:Array(String)}",
		bindings: []clickhouse.Binding{{Name: "repo_filter_ids", Value: repos}},
	}
}

// filterColumns names the (team, repo, author) columns translateFilters
// substitutes into scope/repo predicates -- the Go equivalent of
// translate_filters' team_column/repo_column/author_column keyword
// defaults (filter_translation.py:159-161).
type filterColumns struct {
	Team   string
	Repo   string
	Author string
}

func defaultFilterColumns() filterColumns {
	return filterColumns{Team: "team_id", Repo: "repo_id", Author: "author_email"}
}

// translateFilters ports translate_filters (filter_translation.py:156-290)
// verbatim, including the deliberate incompleteness this port must NOT
// "fix": filters.how / HowFilterInput.FlowStage is validated as an
// "active filter" by hasActiveFilters (filters.go, mirroring
// compiler.py:268-288) but is NEVER translated into a SQL predicate here
// -- Python's own translate_filters has no `if filters.how` branch at
// all. A same-dimension flow matrix WILL still reject a how-filtered
// request (hasActiveFilters returns true), but an investment-path
// timeseries/breakdown query with an active how filter silently applies
// no how-predicate, exactly as Python does today. This is Python
// behavior that has shipped; porting it faithfully, not filing it as a
// defect (root AGENTS.md: a port copied from a stale/buggy tip is a
// defect only when the bug is ALREADY FIXED on the source tip -- this one
// is not).
//
// use_investment also gates two rejections this function must reproduce
// exactly: scope.level=developer and who.developers are BOTH real errors
// on the non-investment path (filter_translation.py:217-232,260-265) --
// investment_metrics_daily and the testops rollups carry no
// per-developer breakdown at all.
func translateFilters(filters *model.FilterInput, useInvestment bool, cols filterColumns) (filterClause, error) {
	if filters == nil {
		return emptyFilterClause(), nil
	}

	var sql string
	var bindings []clickhouse.Binding

	if filters.Scope != nil {
		switch {
		case useInvestment && filters.Scope.Level == model.ScopeLevelInputTeam && len(filters.Scope.Ids) > 0:
			sql += " AND (ut.team_label IN {scope_ids:Array(String)} OR ut.team_id IN {scope_ids:Array(String)})"
			bindings = append(bindings, clickhouse.Binding{Name: "scope_ids", Value: filters.Scope.Ids})
		case useInvestment && filters.Scope.Level == model.ScopeLevelInputDeveloper && len(filters.Scope.Ids) > 0:
			if err := validateDeveloperEmails(filters.Scope.Ids, "scope"); err != nil {
				return filterClause{}, err
			}
			sql += " AND hasAny(au.author_emails, {scope_ids:Array(String)})"
			bindings = append(bindings, clickhouse.Binding{Name: "scope_ids", Value: filters.Scope.Ids})
		case filters.Scope.Level == model.ScopeLevelInputDeveloper && len(filters.Scope.Ids) > 0:
			if err := validateDeveloperEmails(filters.Scope.Ids, "scope"); err != nil {
				return filterClause{}, err
			}
			return filterClause{}, newValidationError("scope", "developer",
				"scope.level=developer filtering requires an investment query; "+
					"pass useInvestment=true or remove the developer scope.")
		default:
			clause := translateScopeFilter(filters.Scope.Level, filters.Scope.Ids, cols.Team, cols.Repo, cols.Author)
			sql += clause.sql
			bindings = append(bindings, clause.bindings...)
		}
	}

	if filters.Who != nil && len(filters.Who.Developers) > 0 {
		if useInvestment {
			sql += " AND hasAny(au.author_emails, {developer_ids:Array(String)})"
			bindings = append(bindings, clickhouse.Binding{Name: "developer_ids", Value: filters.Who.Developers})
		} else {
			return filterClause{}, newValidationError("who", "developers",
				"who.developers filtering requires an investment query; pass "+
					"useInvestment=true or remove the developer filter.")
		}
	}

	if filters.What != nil && len(filters.What.Repos) > 0 {
		clause := translateRepoFilter(filters.What.Repos, cols.Repo)
		sql += clause.sql
		bindings = append(bindings, clause.bindings...)
	}

	if filters.Why != nil && len(filters.Why.WorkCategory) > 0 {
		clause := translateWorkCategoryFilter(filters.Why.WorkCategory, useInvestment)
		sql += clause.sql
		bindings = append(bindings, clause.bindings...)
	}

	return filterClause{sql: sql, bindings: bindings}, nil
}

// Package security serves the securityAlerts and securityOverview
// GraphQL fields from the ClickHouse security_alerts table joined to
// repos. Both fields share one filter builder so the alert list and the
// aggregates always apply the same predicate to the same rows.
//
// Scope: every statement is org-scoped on both sides of the join, the
// repo row and the alert row each carrying the caller's org, and the org is
// always the caller's authenticated org, never a request argument.
package security

import (
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// openStates are the alert states that count as open. They drive the
// openOnly filter and every "open" aggregate.
var openStates = []string{"open", "detected", "confirmed"}

// severityRank orders alerts critical > high > medium > low > other.
const severityRank = "multiIf(" +
	"sa.severity = 'critical', 4, " +
	"sa.severity = 'high', 3, " +
	"sa.severity = 'medium', 2, " +
	"sa.severity = 'low', 1, " +
	"0)"

// filterClause is a WHERE fragment plus the bindings it references.
type filterClause struct {
	sql      string
	bindings []clickhouse.Binding
}

func lowerAll[T ~string](in []T) []string {
	out := make([]string, 0, len(in))
	for _, v := range in {
		out = append(out, strings.ToLower(string(v)))
	}
	return out
}

// buildFilter returns the WHERE clause for the security_alerts (sa) and
// repos (r) aliases. The org predicate is always present. openOnly wins
// over an explicit state list; empty lists and an empty search apply no
// predicate.
func buildFilter(orgID string, f *model.SecurityAlertFilterInput) filterClause {
	clauses := []string{"r.org_id = {org_id:String}", "sa.org_id = {org_id:String}"}
	bindings := []clickhouse.Binding{{Name: "org_id", Value: orgID}}
	if f != nil {
		if f.OpenOnly {
			clauses = append(clauses, "sa.state IN {open_states:Array(String)}")
			bindings = append(bindings, clickhouse.Binding{Name: "open_states", Value: openStates})
		} else if len(f.States) > 0 {
			clauses = append(clauses, "sa.state IN {states:Array(String)}")
			bindings = append(bindings, clickhouse.Binding{Name: "states", Value: lowerAll(f.States)})
		}
		if len(f.RepoIds) > 0 {
			clauses = append(clauses, "toString(sa.repo_id) IN {repo_ids:Array(String)}")
			bindings = append(bindings, clickhouse.Binding{Name: "repo_ids", Value: f.RepoIds})
		}
		if len(f.Severities) > 0 {
			clauses = append(clauses, "sa.severity IN {severities:Array(String)}")
			bindings = append(bindings, clickhouse.Binding{Name: "severities", Value: lowerAll(f.Severities)})
		}
		if len(f.Sources) > 0 {
			clauses = append(clauses, "sa.source IN {sources:Array(String)}")
			bindings = append(bindings, clickhouse.Binding{Name: "sources", Value: lowerAll(f.Sources)})
		}
		if f.Since != nil {
			d := f.Since.Time().UTC()
			since := time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)
			clauses = append(clauses, "sa.created_at >= {since:DateTime64(3, 'UTC')}")
			bindings = append(bindings, clickhouse.Binding{Name: "since", Value: since})
		}
		if f.Until != nil {
			d := f.Until.Time().UTC()
			until := time.Date(d.Year(), d.Month(), d.Day(), 23, 59, 59, 0, time.UTC)
			clauses = append(clauses, "sa.created_at <= {until:DateTime64(3, 'UTC')}")
			bindings = append(bindings, clickhouse.Binding{Name: "until", Value: until})
		}
		if f.Search != nil && *f.Search != "" {
			clauses = append(clauses,
				"(ilike(sa.title, {search_pattern:String})"+
					" OR ilike(sa.package_name, {search_pattern:String})"+
					" OR ilike(sa.cve_id, {search_pattern:String}))")
			bindings = append(bindings, clickhouse.Binding{Name: "search_pattern", Value: "%" + *f.Search + "%"})
		}
	}
	return filterClause{sql: "WHERE " + strings.Join(clauses, " AND "), bindings: bindings}
}

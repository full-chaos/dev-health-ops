// Package teamkeytables declares the daily tables whose sorting key holds a
// team id and that take the stale-key rule, and builds from one declaration
// the two things that must agree:
//
//   - what the WRITER reads and stores: the live keys of a day and the row of
//     zeros that supersedes a key (package daily, stale_team_keys.go);
//   - what a READER must leave out: a key whose newest row is a row of zeros
//     holds no measure. A reader that sums is right with such a row. A reader
//     that takes an average over rows, or counts rows, would take the row of
//     zeros as a sample of 0; it adds LiveRow (or LiveHaving) to its read.
//
// The package holds declarations and SQL text only. It imports no store and
// no job package, so the query packages can use it.
package teamkeytables

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// ErrInvalid means a declaration or a key value is not usable.
var ErrInvalid = errors.New("teamkeytables: invalid declaration or key value")

// KeyKind is how one key column is read and stored.
type KeyKind int

const (
	// KeyString is a String, LowCardinality(String) or Enum column.
	KeyString KeyKind = iota
	// KeyNullableString is a Nullable(String) column that the sorting key
	// holds as ifNull(column, ''): the empty value is stored as NULL.
	KeyNullableString
	// KeyUUID is a UUID column.
	KeyUUID
	// KeyNullableUUID is a Nullable(UUID) column that the sorting key holds as
	// ifNull(column, the nil id): the nil id is stored as NULL.
	KeyNullableUUID
)

// KeyColumn is one column of a table's sorting key.
type KeyColumn struct {
	Name string
	Kind KeyKind
}

// Table declares one team-keyed daily table for the stale-key rule.
type Table struct {
	// Table is the ClickHouse table.
	Table string
	// Family is the families.json name of the daily family that writes it.
	Family string
	// DayColumn is the Date column of the sorting key.
	DayColumn string
	// Keys are the sorting-key columns other than org_id and DayColumn, in the
	// order of the key.
	Keys []KeyColumn
	// TeamColumn is the key column that holds the team id.
	TeamColumn string
	// Scope are the key columns that bound what one run computes. A run
	// supersedes only keys whose values of these columns are in its scope. An
	// empty Scope is a table that one run computes whole for the day.
	Scope []string
	// Measures are the stored counts and values. A key is live while its
	// newest row holds a value other than 0 in one of them.
	Measures []string
	// NullableMeasures are the Nullable measures. A key is live while its
	// newest row holds a value in one of them.
	NullableMeasures []string
	// Labels are the stored columns that are a name of the key, not a
	// measure (team_name). A row of zeros stores the column default in them.
	Labels []string
	// Configuration are the stored columns that hold the configuration of the
	// compute, not a measure of the key. A row of zeros stores the column
	// default in them, and they do not make a key live.
	Configuration []string
	// Where is a fixed predicate for a table that the family shares with
	// another writer ("" for the whole table).
	Where string
}

// The team-keyed daily tables that take the shared rule. All lists them.
var (
	WorkItemMetricsDaily = Table{
		Table: "work_item_metrics_daily", Family: "work_item", DayColumn: "day",
		Keys:       []KeyColumn{{"provider", KeyString}, {"work_scope_id", KeyString}, {"team_id", KeyString}},
		TeamColumn: "team_id", Scope: []string{"provider", "work_scope_id"}, Labels: []string{"team_name"},
		Measures: []string{
			"items_started", "items_completed", "items_started_unassigned", "items_completed_unassigned",
			"wip_count_end_of_day", "wip_unassigned_end_of_day", "bug_completed_ratio", "story_points_completed",
			"new_bugs_count", "new_items_count", "defect_intro_rate", "wip_congestion_ratio", "predictability_score",
		},
		NullableMeasures: []string{
			"cycle_time_p50_hours", "cycle_time_p90_hours", "lead_time_p50_hours", "lead_time_p90_hours",
			"wip_age_p50_hours", "wip_age_p90_hours",
		},
	}
	WorkItemStateDurationsDaily = Table{
		Table: "work_item_state_durations_daily", Family: "work_item_state", DayColumn: "day",
		Keys: []KeyColumn{
			{"provider", KeyString}, {"work_scope_id", KeyString}, {"team_id", KeyString}, {"status", KeyString},
		},
		TeamColumn: "team_id", Scope: []string{"provider", "work_scope_id"}, Labels: []string{"team_name"},
		Measures: []string{"duration_hours", "items_touched", "avg_wip"},
	}
	EstimateCoverageMetricsDaily = Table{
		Table: "estimate_coverage_metrics_daily", Family: "work_item_estimate", DayColumn: "day",
		Keys: []KeyColumn{
			{"provider", KeyString}, {"work_scope_id", KeyString}, {"team_id", KeyNullableString},
		},
		TeamColumn: "team_id", Scope: []string{"provider", "work_scope_id"}, Labels: []string{"team_name"},
		Measures:         []string{"estimated_count", "unestimated_count", "backlog_size"},
		NullableMeasures: []string{"ratio"},
	}
	TeamMetricsDaily = Table{
		Table: "team_metrics_daily", Family: "team_wellbeing", DayColumn: "day",
		Keys:       []KeyColumn{{"team_id", KeyString}, {"repo_id", KeyString}},
		TeamColumn: "team_id", Scope: []string{"repo_id"}, Labels: []string{"team_name"},
		Measures: []string{
			"commits_count", "after_hours_commits_count", "weekend_commits_count",
			"after_hours_commit_ratio", "weekend_commit_ratio",
		},
	}
	AIImpactMetricsDaily = Table{
		Table: "ai_impact_metrics_daily", Family: "ai_impact", DayColumn: "day",
		Keys: []KeyColumn{
			{"team_id", KeyString}, {"repo_id", KeyUUID}, {"work_type", KeyString}, {"attribution_bucket", KeyString},
		},
		TeamColumn: "team_id", Scope: []string{"repo_id"},
		Measures: []string{
			"prs_total", "prs_merged", "ai_assisted_prs", "agent_created_prs", "human_prs", "unknown_prs",
			"agent_created_pr_count", "rework_prs", "followup_commits_count", "revert_prs", "incidents_count",
			"test_gap_prs", "leverage_prs_component",
		},
		NullableMeasures: []string{
			"ai_assisted_pr_ratio", "cycle_time_avg_hours", "baseline_cycle_time_avg_hours", "ai_cycle_time_delta_hours",
			"reviews_per_pr", "baseline_reviews_per_pr", "ai_review_amplification", "changes_requested_per_pr",
			"rework_drag_rate", "revert_rate", "incident_drag_rate", "test_gap_rate",
			"leverage_cycle_time_component", "leverage_review_component", "leverage_rework_component",
			"leverage_test_component", "leverage_incident_component",
		},
	}
	AIGovernanceCoverageDaily = Table{
		Table: "ai_governance_coverage_daily", Family: "ai_governance", DayColumn: "day",
		Keys:       []KeyColumn{{"team_id", KeyNullableString}, {"repo_id", KeyNullableUUID}},
		TeamColumn: "team_id",
		Measures: []string{
			"ai_artifacts", "declared_artifacts", "human_reviewed_prs", "security_scanned_prs", "in_policy_artifacts",
		},
	}
	TeamCognitiveLoadDaily = Table{
		Table: "team_cognitive_load_daily", Family: "team_cognitive_load", DayColumn: "day",
		Keys:       []KeyColumn{{"team_id", KeyString}},
		TeamColumn: "team_id",
		Measures: []string{
			"pr_interruption_load", "context_spread_count", "review_request_load",
			"contributing_repo_count", "sample_author_count",
		},
		NullableMeasures: []string{"after_hours_commit_ratio", "weekend_commit_ratio"},
	}
	// ic_landscape_rolling_30d holds one point for each (team, map, identity)
	// of a day. Its repo_id is one fixed placeholder id.
	ICLandscapeRolling30d = Table{
		Table: "ic_landscape_rolling_30d", Family: "ic_finalize", DayColumn: "as_of_day",
		Keys: []KeyColumn{
			{"repo_id", KeyUUID}, {"team_id", KeyString}, {"map_name", KeyString}, {"identity_id", KeyString},
		},
		TeamColumn: "team_id",
		Measures: []string{
			"x_raw", "y_raw", "x_norm", "y_norm",
			"churn_loc_30d", "delivery_units_30d", "cycle_p50_30d_hours", "wip_max_30d",
		},
	}
	// compounding_risk_daily holds the team id in scope_id for the rows of
	// scope 'team'. The rows of scope 'repo' belong to another family and hold
	// no team id.
	//
	// The table has no count column, and a real row can hold NULL in every
	// score and input (a team with no input: severity unknown). So the
	// weights and thresholds of the compute are declared as measures: the
	// writer stores them in every real row and they are never 0, and a row of
	// zeros holds 0 in them. They are what tells a row of zeros from a real
	// row with no score.
	CompoundingRiskDailyTeam = Table{
		Table: "compounding_risk_daily", Family: "compounding_risk_team", DayColumn: "day",
		Keys:       []KeyColumn{{"scope", KeyString}, {"scope_id", KeyString}},
		TeamColumn: "scope_id",
		Measures: []string{
			"w_churn", "w_complexity", "w_ownership", "w_review", "threshold_elevated", "threshold_high",
		},
		NullableMeasures: []string{
			"compounding_risk", "churn_norm", "complexity_norm", "ownership_norm", "review_norm",
			"rework_churn", "complexity_delta", "bus_factor", "ownership_gini", "single_owner_ratio",
			"review_latency_p90h",
		},
		// severity is the label of the score. A row of zeros stores its
		// default, 'unknown'.
		Configuration: []string{"severity"},
		Where:         "scope = 'team'",
	}
	TeamComplexityDaily = Table{
		Table: "team_complexity_daily", Family: "team_complexity", DayColumn: "day",
		Keys:       []KeyColumn{{"team_id", KeyString}},
		TeamColumn: "team_id",
		Measures: []string{
			"loc_total", "cyclomatic_total", "cyclomatic_per_kloc", "high_complexity_functions",
			"very_high_complexity_functions", "contributing_repo_count",
		},
	}
)

// All is every table that takes the shared rule. The census of package daily
// (stale_team_keys_census_test.go) holds it against the schema baseline.
func All() []Table {
	return []Table{
		WorkItemMetricsDaily,
		WorkItemStateDurationsDaily,
		EstimateCoverageMetricsDaily,
		TeamMetricsDaily,
		AIImpactMetricsDaily,
		AIGovernanceCoverageDaily,
		TeamCognitiveLoadDaily,
		ICLandscapeRolling30d,
		CompoundingRiskDailyTeam,
		TeamComplexityDaily,
	}
}

// ByTable returns the declaration of a table. A reader that is built for more
// than one table asks here whether its table takes the rule.
func ByTable(name string) (Table, bool) {
	for _, table := range All() {
		if table.Table == name {
			return table, true
		}
	}
	return Table{}, false
}

// Valid reports whether the declaration is usable.
func (table Table) Valid() error {
	if table.Table == "" || table.DayColumn == "" || len(table.Keys) == 0 ||
		len(table.Measures)+len(table.NullableMeasures) == 0 {
		return fmt.Errorf("%w: table %q is not declared", ErrInvalid, table.Table)
	}
	return nil
}

// ReadExpression is the text form of a key column in a read.
func (column KeyColumn) ReadExpression() string {
	switch column.Kind {
	case KeyNullableString:
		return "ifNull(" + column.Name + ", '')"
	case KeyNullableUUID:
		return "toString(ifNull(" + column.Name + ", toUUID('" + uuid.Nil.String() + "')))"
	default:
		return "toString(" + column.Name + ")"
	}
}

// StoredValue is the value that a row of zeros stores for a key column.
func (column KeyColumn) StoredValue(text string) (any, error) {
	switch column.Kind {
	case KeyNullableString:
		if text == "" {
			return (*string)(nil), nil
		}
		return &text, nil
	case KeyUUID:
		id, err := uuid.Parse(text)
		if err != nil {
			return nil, fmt.Errorf("%w: key column %s holds %q, not a uuid", ErrInvalid, column.Name, text)
		}
		return id, nil
	case KeyNullableUUID:
		id, err := uuid.Parse(text)
		if err != nil {
			return nil, fmt.Errorf("%w: key column %s holds %q, not a uuid", ErrInvalid, column.Name, text)
		}
		if id == uuid.Nil {
			return (*uuid.UUID)(nil), nil
		}
		return &id, nil
	default:
		return text, nil
	}
}

// LiveHaving is the test of one key in a GROUP BY over the key columns of the
// raw table: the newest row of the key (by computed_at) holds a measure. It is
// false for a key whose newest row is a row of zeros.
//
// qualifier is the name or the alias of the table in the FROM clause with its
// dot ("work_item_metrics_daily."), or "" for none. A reader whose SELECT
// gives an aggregate the name of its column (argMax(x, computed_at) AS x) MUST
// pass it: ClickHouse resolves a bare x in HAVING to that alias, and the test
// would then hold an aggregate inside an aggregate. A qualified name is the
// column of the table.
func (table Table) LiveHaving(qualifier string) string {
	live := make([]string, 0, len(table.Measures)+len(table.NullableMeasures))
	version := qualifier + "computed_at"
	for _, measure := range table.Measures {
		live = append(live, "argMax("+qualifier+measure+", "+version+") != 0")
	}
	for _, measure := range table.NullableMeasures {
		// The tuple keeps a NULL of the newest row: argMax skips NULL values.
		live = append(live, "isNotNull(tupleElement(argMax(tuple("+qualifier+measure+"), "+version+"), 1))")
	}
	return strings.Join(live, " OR ")
}

// LiveRow is the same test for ONE row that is already the newest row of its
// key (a row of a FINAL read, or of a read that took the newest row of each
// key and kept every measure column). qualifier is the table alias with its
// dot ("m."), or "" for none. The result is in parentheses.
//
// A reader that takes an average over rows, counts rows, or lists the keys of
// the table adds it, so a superseded key is not a sample, a row or a key.
func (table Table) LiveRow(qualifier string) string {
	live := make([]string, 0, len(table.Measures)+len(table.NullableMeasures))
	for _, measure := range table.Measures {
		live = append(live, qualifier+measure+" != 0")
	}
	for _, measure := range table.NullableMeasures {
		live = append(live, qualifier+measure+" IS NOT NULL")
	}
	return "(" + strings.Join(live, " OR ") + ")"
}

// LiveKeysQuery is the read of the keys of one organization and day whose
// newest row holds a measure. Its arguments are the organization id and the
// day.
func (table Table) LiveKeysQuery() string {
	expressions := make([]string, 0, len(table.Keys))
	for _, column := range table.Keys {
		expressions = append(expressions, column.ReadExpression())
	}
	where := ""
	if table.Where != "" {
		where = " AND (" + table.Where + ")"
	}
	keys := strings.Join(expressions, ", ")
	return "SELECT " + keys + " FROM " + table.Table +
		" WHERE org_id = ? AND " + table.DayColumn + " = ?" + where +
		" GROUP BY " + keys + " HAVING " + table.LiveHaving("") +
		" ORDER BY " + keys
}

// ScopeTuple is the values of the scope columns of one key, joined. key holds
// the values of Keys in their order.
func (table Table) ScopeTuple(key []string) string {
	values := make([]string, 0, len(table.Scope))
	for _, name := range table.Scope {
		for index, column := range table.Keys {
			if column.Name == name {
				values = append(values, key[index])
			}
		}
	}
	return strings.Join(values, "\x00")
}

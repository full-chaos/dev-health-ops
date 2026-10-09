package daily

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// # The stale-key rule of the team-keyed daily tables
//
// A daily table whose sorting key holds a team id stores one row for each
// (scope, day, team). The tables are append only: a recompute writes new rows
// and a reader takes the newest row of each KEY. So when a recompute gives a
// day's work to another team id, the row under the old team id is a different
// key and stays the newest row of that key. A read with no team filter then
// counts the day under both ids.
//
// A team id changes for a stored day when a team is set inactive (a carry to
// a keyed id, a retired project-as-team row, an admin delete) and when an item
// moves to another team.
//
// The rule, the same for every such table: a run writes a ROW OF ZEROS over
// each live key of its own scope and day that it did not produce. Live means
// that the newest row of the key holds a measure. A key whose newest row is a
// row of zeros is left alone, so a second run writes nothing.
//
// A row of zeros holds the key, computed_at, 0 in every count and NULL in
// every Nullable measure. It is written only over a key that held a measure,
// never for a day or a team with no data: missing stays missing.
//
// The set of tables is StaleTeamKeyTables. issue_type_metrics_daily and
// investment_metrics_daily hold the same rule in their own writers
// (withIssueTypeMetricsZeroRows, withInvestmentMetricsZeroRows): they are plain
// MergeTree tables and their keys add the repository.

// staleKeyKind is how one key column is read and stored.
type staleKeyKind int

const (
	// staleKeyString is a String, LowCardinality(String) or Enum column.
	staleKeyString staleKeyKind = iota
	// staleKeyNullableString is a Nullable(String) column that the sorting key
	// holds as ifNull(column, ''): the empty value is stored as NULL.
	staleKeyNullableString
	// staleKeyUUID is a UUID column.
	staleKeyUUID
	// staleKeyNullableUUID is a Nullable(UUID) column that the sorting key
	// holds as ifNull(column, the nil id): the nil id is stored as NULL.
	staleKeyNullableUUID
)

// StaleKeyColumn is one column of a table's sorting key.
type StaleKeyColumn struct {
	Name string
	Kind staleKeyKind
}

// StaleKeyTable declares one team-keyed daily table for the stale-key rule.
type StaleKeyTable struct {
	// Table is the ClickHouse table.
	Table string
	// Family is the families.json name of the daily family that writes it.
	Family string
	// DayColumn is the Date column of the sorting key.
	DayColumn string
	// Keys are the sorting-key columns other than org_id and DayColumn, in the
	// order of the key.
	Keys []StaleKeyColumn
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
	// Where is a fixed predicate for a table that the family shares with
	// another writer ("" for the whole table).
	Where string
}

// The team-keyed daily tables that take the shared rule.
var (
	staleKeysWorkItemMetricsDaily = StaleKeyTable{
		Table: "work_item_metrics_daily", Family: "work_item", DayColumn: "day",
		Keys:       []StaleKeyColumn{{"provider", staleKeyString}, {"work_scope_id", staleKeyString}, {"team_id", staleKeyString}},
		TeamColumn: "team_id", Scope: []string{"provider", "work_scope_id"},
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
	staleKeysWorkItemStateDurationsDaily = StaleKeyTable{
		Table: "work_item_state_durations_daily", Family: "work_item_state", DayColumn: "day",
		Keys: []StaleKeyColumn{
			{"provider", staleKeyString}, {"work_scope_id", staleKeyString}, {"team_id", staleKeyString}, {"status", staleKeyString},
		},
		TeamColumn: "team_id", Scope: []string{"provider", "work_scope_id"},
		Measures: []string{"duration_hours", "items_touched", "avg_wip"},
	}
	staleKeysEstimateCoverageMetricsDaily = StaleKeyTable{
		Table: "estimate_coverage_metrics_daily", Family: "work_item_estimate", DayColumn: "day",
		Keys: []StaleKeyColumn{
			{"provider", staleKeyString}, {"work_scope_id", staleKeyString}, {"team_id", staleKeyNullableString},
		},
		TeamColumn: "team_id", Scope: []string{"provider", "work_scope_id"},
		Measures:         []string{"estimated_count", "unestimated_count", "backlog_size"},
		NullableMeasures: []string{"ratio"},
	}
	staleKeysTeamMetricsDaily = StaleKeyTable{
		Table: "team_metrics_daily", Family: "team_wellbeing", DayColumn: "day",
		Keys:       []StaleKeyColumn{{"team_id", staleKeyString}, {"repo_id", staleKeyString}},
		TeamColumn: "team_id", Scope: []string{"repo_id"},
		Measures: []string{
			"commits_count", "after_hours_commits_count", "weekend_commits_count",
			"after_hours_commit_ratio", "weekend_commit_ratio",
		},
	}
	staleKeysAIImpactMetricsDaily = StaleKeyTable{
		Table: "ai_impact_metrics_daily", Family: "ai_impact", DayColumn: "day",
		Keys: []StaleKeyColumn{
			{"team_id", staleKeyString}, {"repo_id", staleKeyUUID}, {"work_type", staleKeyString}, {"attribution_bucket", staleKeyString},
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
	staleKeysAIGovernanceCoverageDaily = StaleKeyTable{
		Table: "ai_governance_coverage_daily", Family: "ai_governance", DayColumn: "day",
		Keys:       []StaleKeyColumn{{"team_id", staleKeyNullableString}, {"repo_id", staleKeyNullableUUID}},
		TeamColumn: "team_id",
		Measures: []string{
			"ai_artifacts", "declared_artifacts", "human_reviewed_prs", "security_scanned_prs", "in_policy_artifacts",
		},
	}
	staleKeysTeamCognitiveLoadDaily = StaleKeyTable{
		Table: "team_cognitive_load_daily", Family: "team_cognitive_load", DayColumn: "day",
		Keys:       []StaleKeyColumn{{"team_id", staleKeyString}},
		TeamColumn: "team_id",
		Measures: []string{
			"pr_interruption_load", "context_spread_count", "review_request_load",
			"contributing_repo_count", "sample_author_count",
		},
		NullableMeasures: []string{"after_hours_commit_ratio", "weekend_commit_ratio"},
	}
	staleKeysTeamComplexityDaily = StaleKeyTable{
		Table: "team_complexity_daily", Family: "team_complexity", DayColumn: "day",
		Keys:       []StaleKeyColumn{{"team_id", staleKeyString}},
		TeamColumn: "team_id",
		Measures: []string{
			"loc_total", "cyclomatic_total", "cyclomatic_per_kloc", "high_complexity_functions",
			"very_high_complexity_functions", "contributing_repo_count",
		},
	}
)

// StaleTeamKeyTables is every table that takes the shared rule. The census
// (stale_team_keys_census_test.go) holds it against the schema baseline.
func StaleTeamKeyTables() []StaleKeyTable {
	return []StaleKeyTable{
		staleKeysWorkItemMetricsDaily,
		staleKeysWorkItemStateDurationsDaily,
		staleKeysEstimateCoverageMetricsDaily,
		staleKeysTeamMetricsDaily,
		staleKeysAIImpactMetricsDaily,
		staleKeysAIGovernanceCoverageDaily,
		staleKeysTeamCognitiveLoadDaily,
		staleKeysTeamComplexityDaily,
	}
}

// staleKey is the values of one key, in the order of StaleKeyTable.Keys. A
// NULL of a Nullable key column is the empty string, and a NULL of a
// Nullable(UUID) key column is the nil id: the forms the sorting key holds.
type staleKey []string

func (key staleKey) text() string { return strings.Join(key, "\x00") }

// staleKeyScope is the scope of one run: the values of StaleKeyTable.Scope of
// every unit the run computed. Nil is the whole organization and day.
type staleKeyScope map[string]struct{}

// newStaleKeyScope builds a scope from the values of the scope columns, one
// tuple for each unit.
func newStaleKeyScope(tuples ...[]string) staleKeyScope {
	scope := make(staleKeyScope, len(tuples))
	for _, tuple := range tuples {
		scope[strings.Join(tuple, "\x00")] = struct{}{}
	}
	return scope
}

// staleKeyConn is what the rule needs of a connection.
type staleKeyConn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
}

func (table StaleKeyTable) valid() error {
	if table.Table == "" || table.DayColumn == "" || len(table.Keys) == 0 ||
		len(table.Measures)+len(table.NullableMeasures) == 0 {
		return fmt.Errorf("%w: stale-key table %q is not declared", ErrInvalidState, table.Table)
	}
	return nil
}

// readExpression is the text form of a key column in the live-key read.
func (column StaleKeyColumn) readExpression() string {
	switch column.Kind {
	case staleKeyNullableString:
		return "ifNull(" + column.Name + ", '')"
	case staleKeyUUID:
		return "toString(" + column.Name + ")"
	case staleKeyNullableUUID:
		return "toString(ifNull(" + column.Name + ", toUUID('" + uuid.Nil.String() + "')))"
	default:
		return "toString(" + column.Name + ")"
	}
}

// storedValue is the value that a zero row stores for a key column.
func (column StaleKeyColumn) storedValue(text string) (any, error) {
	switch column.Kind {
	case staleKeyNullableString:
		if text == "" {
			return (*string)(nil), nil
		}
		return &text, nil
	case staleKeyUUID:
		id, err := uuid.Parse(text)
		if err != nil {
			return nil, fmt.Errorf("%w: key column %s holds %q, not a uuid", ErrInvalidState, column.Name, text)
		}
		return id, nil
	case staleKeyNullableUUID:
		id, err := uuid.Parse(text)
		if err != nil {
			return nil, fmt.Errorf("%w: key column %s holds %q, not a uuid", ErrInvalidState, column.Name, text)
		}
		if id == uuid.Nil {
			return (*uuid.UUID)(nil), nil
		}
		return &id, nil
	default:
		return text, nil
	}
}

// liveKeysQuery is the read of the keys of one organization and day whose
// newest row holds a measure.
func (table StaleKeyTable) liveKeysQuery() string {
	expressions := make([]string, 0, len(table.Keys))
	for _, column := range table.Keys {
		expressions = append(expressions, column.readExpression())
	}
	live := make([]string, 0, len(table.Measures)+len(table.NullableMeasures))
	for _, measure := range table.Measures {
		live = append(live, "argMax("+measure+", computed_at) != 0")
	}
	for _, measure := range table.NullableMeasures {
		// The tuple keeps a NULL of the newest row: argMax skips NULL values.
		live = append(live, "isNotNull(tupleElement(argMax(tuple("+measure+"), computed_at), 1))")
	}
	where := ""
	if table.Where != "" {
		where = " AND (" + table.Where + ")"
	}
	keys := strings.Join(expressions, ", ")
	return "SELECT " + keys + " FROM " + table.Table +
		" WHERE org_id = ? AND " + table.DayColumn + " = ?" + where +
		" GROUP BY " + keys + " HAVING " + strings.Join(live, " OR ") +
		" ORDER BY " + keys
}

// loadLiveStaleKeys returns the live keys of one organization and day.
func loadLiveStaleKeys(
	ctx context.Context, conn staleKeyConn, table StaleKeyTable, organizationID string, day time.Time,
) ([]staleKey, error) {
	rows, err := conn.Query(ctx, table.liveKeysQuery(), organizationID, staleKeyDay(day))
	if err != nil {
		return nil, fmt.Errorf("load %s live keys: %w", table.Table, err)
	}
	defer rows.Close()
	var keys []staleKey
	for rows.Next() {
		values := make([]string, len(table.Keys))
		targets := make([]any, len(values))
		for index := range values {
			targets[index] = &values[index]
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, fmt.Errorf("scan %s live key: %w", table.Table, err)
		}
		keys = append(keys, staleKey(values))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s live keys: %w", table.Table, err)
	}
	return keys, nil
}

func staleKeyDay(day time.Time) time.Time {
	utc := day.UTC()
	return time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
}

// scopeTuple is the values of the scope columns of one key.
func (table StaleKeyTable) scopeTuple(key staleKey) string {
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

// staleKeysOf is the rule itself, with no I/O: the live keys of the run's
// scope that the run did not produce.
func staleKeysOf(table StaleKeyTable, live []staleKey, scope staleKeyScope, produced []staleKey) []staleKey {
	held := make(map[string]struct{}, len(produced))
	for _, key := range produced {
		held[key.text()] = struct{}{}
	}
	var stale []staleKey
	for _, key := range live {
		if _, ok := held[key.text()]; ok {
			continue
		}
		if len(table.Scope) > 0 {
			if _, inScope := scope[table.scopeTuple(key)]; !inScope {
				continue
			}
		}
		stale = append(stale, key)
	}
	sort.Slice(stale, func(left, right int) bool { return stale[left].text() < stale[right].text() })
	return stale
}

// supersedeStaleTeamKeys applies the rule for one table after a run wrote its
// rows: it reads the live keys of the organization and day, keeps the ones of
// the run's scope that the run did not produce, and writes a row of zeros over
// each. It returns the number of rows of zeros.
//
// scope is the values of table.Scope of every unit the run computed; it is
// ignored for a table with no scope column. produced is every key the run
// wrote, in the order of table.Keys.
//
// A failed read is returned: the run then fails and is tried again, and a
// stale key is never taken as "no stale key". The write reports its row count
// on a Send error, because the rows may be stored.
func supersedeStaleTeamKeys(
	ctx context.Context, conn staleKeyConn, table StaleKeyTable, organizationID string, day time.Time,
	scope staleKeyScope, produced []staleKey, computedAt time.Time,
) (int, error) {
	if err := table.valid(); err != nil {
		return 0, err
	}
	if conn == nil || strings.TrimSpace(organizationID) == "" || day.IsZero() || computedAt.IsZero() {
		return 0, ErrInvalidState
	}
	if len(table.Scope) > 0 && len(scope) == 0 {
		// A run with no unit computed nothing: it holds no key to supersede.
		return 0, nil
	}
	live, err := loadLiveStaleKeys(ctx, conn, table, organizationID, day)
	if err != nil {
		return 0, err
	}
	stale := staleKeysOf(table, live, scope, produced)
	if len(stale) == 0 {
		return 0, nil
	}
	names := make([]string, 0, len(table.Keys))
	for _, column := range table.Keys {
		names = append(names, column.Name)
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table.Table+
		" (org_id, "+table.DayColumn+", "+strings.Join(names, ", ")+", computed_at)")
	if err != nil {
		return 0, fmt.Errorf("prepare %s zero rows: %w", table.Table, err)
	}
	dayValue, computedAtUTC := staleKeyDay(day), computedAt.UTC()
	for _, key := range stale {
		values := make([]any, 0, len(key)+3)
		values = append(values, organizationID, dayValue)
		for index, column := range table.Keys {
			value, err := column.storedValue(key[index])
			if err != nil {
				return 0, err
			}
			values = append(values, value)
		}
		values = append(values, computedAtUTC)
		if err := batch.Append(values...); err != nil {
			return 0, fmt.Errorf("append %s zero row: %w", table.Table, err)
		}
	}
	if err := batch.Send(); err != nil {
		return len(stale), fmt.Errorf("send %s zero rows: %w", table.Table, err)
	}
	return len(stale), nil
}

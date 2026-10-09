package teamkeytables

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestEveryDeclaredTableIsValidAndFoundByItsName(t *testing.T) {
	tables := All()
	if len(tables) == 0 {
		t.Fatal("no table is declared")
	}
	seen := map[string]bool{}
	for _, table := range tables {
		if err := table.Valid(); err != nil {
			t.Errorf("%s: %v", table.Table, err)
		}
		if seen[table.Table] {
			t.Errorf("%s is declared twice", table.Table)
		}
		seen[table.Table] = true
		found, ok := ByTable(table.Table)
		if !ok || found.Table != table.Table || found.Family != table.Family {
			t.Errorf("ByTable(%q) = %q, %v", table.Table, found.Table, ok)
		}
	}
	if _, ok := ByTable("repo_metrics_daily"); ok {
		t.Error("ByTable finds a table that takes no rule")
	}
	if err := (Table{Table: "x", DayColumn: "day", Keys: []KeyColumn{{"team_id", KeyString}}}).Valid(); !errors.Is(err, ErrInvalid) {
		t.Errorf("a declaration with no measure is valid: %v", err)
	}
}

// The two forms of the reader predicate, as text: every count and value
// column compared with 0, every Nullable measure tested for a value, and the
// qualifier on every column (the version column too).
func TestTheLivePredicateNamesEveryMeasureWithTheQualifier(t *testing.T) {
	if got, want := EstimateCoverageMetricsDaily.LiveRow("m."),
		"(m.estimated_count != 0 OR m.unestimated_count != 0 OR m.backlog_size != 0 OR m.ratio IS NOT NULL)"; got != want {
		t.Errorf("LiveRow:\n got  %s\n want %s", got, want)
	}
	if got, want := WorkItemStateDurationsDaily.LiveRow(""),
		"(duration_hours != 0 OR items_touched != 0 OR avg_wip != 0)"; got != want {
		t.Errorf("LiveRow with no qualifier:\n got  %s\n want %s", got, want)
	}
	if got, want := EstimateCoverageMetricsDaily.LiveHaving("t."),
		"argMax(t.estimated_count, t.computed_at) != 0 OR argMax(t.unestimated_count, t.computed_at) != 0 OR "+
			"argMax(t.backlog_size, t.computed_at) != 0 OR isNotNull(tupleElement(argMax(tuple(t.ratio), t.computed_at), 1))"; got != want {
		t.Errorf("LiveHaving:\n got  %s\n want %s", got, want)
	}
	if got, want := TeamComplexityDaily.LiveKeysQuery(),
		"SELECT toString(team_id) FROM team_complexity_daily WHERE org_id = ? AND day = ? GROUP BY toString(team_id) HAVING "+
			TeamComplexityDaily.LiveHaving("")+" ORDER BY toString(team_id)"; got != want {
		t.Errorf("LiveKeysQuery:\n got  %s\n want %s", got, want)
	}
	// A table that the family shares with another writer reads its own rows.
	if got, want := CompoundingRiskDailyTeam.LiveKeysQuery(),
		"SELECT toString(scope), toString(scope_id) FROM compounding_risk_daily WHERE org_id = ? AND day = ? AND (scope = 'team') "+
			"GROUP BY toString(scope), toString(scope_id) HAVING "+CompoundingRiskDailyTeam.LiveHaving("")+
			" ORDER BY toString(scope), toString(scope_id)"; got != want {
		t.Errorf("LiveKeysQuery with a fixed predicate:\n got  %s\n want %s", got, want)
	}
}

func TestAKeyColumnIsReadAndStoredByItsKind(t *testing.T) {
	id := uuid.MustParse("00000000-0000-4000-8000-0000000000a1")
	for _, tc := range []struct {
		column KeyColumn
		read   string
		text   string
		stored any
	}{
		{KeyColumn{"team_id", KeyString}, "toString(team_id)", "platform", "platform"},
		{KeyColumn{"team_id", KeyString}, "toString(team_id)", "", ""},
		{KeyColumn{"team_id", KeyNullableString}, "ifNull(team_id, '')", "", (*string)(nil)},
		{KeyColumn{"repo_id", KeyUUID}, "toString(repo_id)", id.String(), id},
		{KeyColumn{"repo_id", KeyNullableUUID}, "toString(ifNull(repo_id, toUUID('00000000-0000-0000-0000-000000000000')))",
			uuid.Nil.String(), (*uuid.UUID)(nil)},
	} {
		if got := tc.column.ReadExpression(); got != tc.read {
			t.Errorf("%+v: ReadExpression = %s, want %s", tc.column, got, tc.read)
		}
		stored, err := tc.column.StoredValue(tc.text)
		if err != nil || stored != tc.stored {
			t.Errorf("%+v: StoredValue(%q) = %#v, %v; want %#v", tc.column, tc.text, stored, err, tc.stored)
		}
	}
	if stored, err := (KeyColumn{"team_id", KeyNullableString}).StoredValue("platform"); err != nil || *(stored.(*string)) != "platform" {
		t.Errorf("a Nullable text key with a value is stored as %#v, %v", stored, err)
	}
	if stored, err := (KeyColumn{"repo_id", KeyNullableUUID}).StoredValue(id.String()); err != nil || *(stored.(*uuid.UUID)) != id {
		t.Errorf("a Nullable uuid key with a value is stored as %#v, %v", stored, err)
	}
	for _, kind := range []KeyKind{KeyUUID, KeyNullableUUID} {
		if _, err := (KeyColumn{"repo_id", kind}).StoredValue("not-a-uuid"); !errors.Is(err, ErrInvalid) {
			t.Errorf("kind %d stores a value that is not a uuid: %v", kind, err)
		}
	}
	if got, want := WorkItemMetricsDaily.ScopeTuple([]string{"linear", "proj", "platform"}), "linear\x00proj"; got != want {
		t.Errorf("ScopeTuple = %q, want %q", got, want)
	}
}

// The marker columns a reader tests: the counts, or the marker of a table
// with no count. A caller cannot change the declaration through the result.
func TestMarkerColumnsAreTheCountsOrTheMarkerOfATableWithNoCount(t *testing.T) {
	if got, want := WorkItemStateDurationsDaily.MarkerColumns(), []string{"items_touched"}; len(got) != 1 || got[0] != want[0] {
		t.Errorf("MarkerColumns = %v, want %v", got, want)
	}
	risk := CompoundingRiskDailyTeam.MarkerColumns()
	if len(CompoundingRiskDailyTeam.Counts) != 0 || len(risk) != 6 || risk[0] != "w_churn" || risk[5] != "threshold_high" {
		t.Errorf("the team risk table has counts %v and marker columns %v, want no count and the six weights and thresholds",
			CompoundingRiskDailyTeam.Counts, risk)
	}
	risk[0] = "changed"
	if CompoundingRiskDailyTeam.Marker[0] != "w_churn" {
		t.Error("a caller changed the declaration through MarkerColumns")
	}
	for _, table := range All() {
		if len(table.MarkerColumns()) == 0 {
			t.Errorf("%s has no marker column", table.Table)
		}
	}
}

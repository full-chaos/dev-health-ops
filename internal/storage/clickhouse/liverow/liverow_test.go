package liverow

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

func TestPredicateIsTheRegistryRuleWithEveryColumnQualified(t *testing.T) {
	// A registry table: the two forms of the registry, under the table name
	// and under an alias.
	if got, want := Predicate("work_item_state_durations_daily", ""),
		"(work_item_state_durations_daily.duration_hours != 0 OR work_item_state_durations_daily.items_touched != 0 OR "+
			"work_item_state_durations_daily.avg_wip != 0)"; got != want {
		t.Fatalf("Predicate = %q, want %q", got, want)
	}
	if got, want := Predicate("team_metrics_daily", "t"), teamkeytables.TeamMetricsDaily.LiveRow("t."); got != want {
		t.Fatalf("Predicate = %q, want the registry row test %q", got, want)
	}
	if got, want := NewestPredicate("work_item_state_durations_daily", "s"),
		"(argMax(s.duration_hours, s.computed_at) != 0 OR argMax(s.items_touched, s.computed_at) != 0 OR "+
			"argMax(s.avg_wip, s.computed_at) != 0)"; got != want {
		t.Fatalf("NewestPredicate = %q, want %q", got, want)
	}
	// A table with the rule in its own writer.
	if got, want := Predicate("issue_type_metrics_daily", ""),
		"(issue_type_metrics_daily.created_count != 0 OR issue_type_metrics_daily.completed_count != 0 OR "+
			"issue_type_metrics_daily.active_count != 0)"; got != want {
		t.Fatalf("Predicate = %q, want %q", got, want)
	}
	if got, want := NewestPredicate("issue_type_metrics_daily", "i"),
		"(argMax(i.created_count, i.computed_at) != 0 OR argMax(i.completed_count, i.computed_at) != 0 OR "+
			"argMax(i.active_count, i.computed_at) != 0)"; got != want {
		t.Fatalf("NewestPredicate = %q, want %q", got, want)
	}
	// No column of any predicate is bare: each name follows a qualifier.
	bare := regexp.MustCompile(`(^|[( ,])[a-z_0-9]+ (!= 0|IS NOT NULL)`)
	for _, table := range Tables() {
		for _, predicate := range []string{Predicate(table, ""), NewestPredicate(table, ""), Predicate(table, "x"), NewestPredicate(table, "x")} {
			if bare.MatchString(predicate) || strings.Contains(predicate, "(computed_at") || strings.Contains(predicate, " computed_at") {
				t.Errorf("%s: a column has no qualifier in %s", table, predicate)
			}
		}
	}
}

// TestEveryRegistryTableHasARuleOrAReason holds this package against the
// registry: each registry table gives a reader a predicate, or it is named in
// noRuleForReaders with the reason a measured row can equal a retraction row
// there. A table added to the registry later is in Tables() at once.
func TestEveryRegistryTableHasARuleOrAReason(t *testing.T) {
	registry := map[string]bool{}
	for _, table := range teamkeytables.All() {
		registry[table.Table] = true
		_, none := noRuleForReaders[table.Table]
		if Registered(table.Table) == none {
			t.Errorf("%s: Registered = %v, in noRuleForReaders = %v", table.Table, Registered(table.Table), none)
		}
		if !none {
			if got, want := Predicate(table.Table, "q"), table.LiveRow("q."); got != want {
				t.Errorf("%s: Predicate = %q, want the registry row test %q", table.Table, got, want)
			}
			if got, want := NewestPredicate(table.Table, "q"), "("+table.LiveHaving("q.")+")"; got != want {
				t.Errorf("%s: NewestPredicate = %q, want the registry key test %q", table.Table, got, want)
			}
		}
	}
	for table := range noRuleForReaders {
		if !registry[table] {
			t.Errorf("stale noRuleForReaders entry %s: not a registry table", table)
		}
	}
	for table := range ownWriterMarkers {
		if registry[table] {
			t.Errorf("%s is in the registry now: delete its ownWriterMarkers entry", table)
		}
	}
	want := []string{
		"ai_governance_coverage_daily", "compounding_risk_daily", "ic_landscape_rolling_30d",
		"investment_metrics_daily", "issue_type_metrics_daily",
		"team_cognitive_load_daily", "team_complexity_daily", "team_metrics_daily",
		"work_item_metrics_daily", "work_item_state_durations_daily",
	}
	if got := Tables(); !reflect.DeepEqual(got, want) {
		t.Errorf("Tables() = %v, want %v", got, want)
	}
}

// TestColumnsAreTheColumnsOfThePredicate holds Columns against the text of
// the two predicate forms: each column it names is tested there, and the
// predicates test no other column.
func TestColumnsAreTheColumnsOfThePredicate(t *testing.T) {
	tested := regexp.MustCompile(`q\.([a-z_0-9]+)`)
	for _, table := range Tables() {
		want := map[string]bool{}
		for _, column := range Columns(table) {
			if want[column] {
				t.Errorf("%s: Columns names %s two times", table, column)
			}
			want[column] = true
		}
		if len(want) == 0 {
			t.Errorf("%s: Columns is empty", table)
		}
		for form, predicate := range map[string]string{"Predicate": Predicate(table, "q"), "NewestPredicate": NewestPredicate(table, "q")} {
			got := map[string]bool{}
			for _, match := range tested.FindAllStringSubmatch(predicate, -1) {
				if match[1] != "computed_at" {
					got[match[1]] = true
				}
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%s: %s tests the columns %v, Columns gives %v", table, form, got, want)
			}
		}
	}
}

func TestATableWithNoRulePanics(t *testing.T) {
	for _, table := range []string{
		"repo_metrics_daily", // not a team-keyed table
		"estimate_coverage_metrics_daily", "ai_impact_metrics_daily",
	} {
		if Registered(table) {
			t.Errorf("%s reads as registered", table)
		}
		for name, call := range map[string]func(){
			"Predicate":       func() { Predicate(table, "") },
			"Columns":         func() { Columns(table) },
			"NewestPredicate": func() { NewestPredicate(table, "") },
		} {
			func() {
				defer func() {
					if recover() == nil {
						t.Errorf("%s(%s) did not panic", name, table)
					}
				}()
				call()
			}()
		}
	}
}

// TestOwnWriterMarkersAreTheLiveKeyColumnsOfTheWriters holds the two tables
// that are not in the registry against the statements with which their own
// daily families find the live keys of a day: the same columns, so the reader
// and the writer agree on what a row of zeros is.
func TestOwnWriterMarkersAreTheLiveKeyColumnsOfTheWriters(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "internal/jobs/metrics/daily/work_item_engine_native_clickhouse.go"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	for table, columns := range ownWriterMarkers {
		var clauses []string
		for _, column := range columns {
			clauses = append(clauses, "argMax("+column+", computed_at) != 0")
		}
		statement := "FROM " + table + "\n"
		at := strings.Index(source, statement)
		if at < 0 {
			t.Errorf("%s: the writer has no live-key read of the table", table)
			continue
		}
		rest := source[at:]
		end := strings.Index(rest, "ORDER BY")
		if end < 0 {
			t.Errorf("%s: the live-key read has no ORDER BY to end at", table)
			continue
		}
		having := rest[strings.Index(rest, "HAVING ")+len("HAVING ") : end]
		got := strings.Join(strings.Fields(having), " ")
		if want := strings.Join(clauses, " OR "); got != want {
			t.Errorf("%s: the writer's live-key test is %q, the reader markers give %q", table, got, want)
		}
	}
}

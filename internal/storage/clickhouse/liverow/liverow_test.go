package liverow

import (
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

func TestPredicateQualifiesEveryMarkerColumn(t *testing.T) {
	if got, want := Predicate("work_item_state_durations_daily", ""),
		"(work_item_state_durations_daily.items_touched != 0)"; got != want {
		t.Fatalf("Predicate = %q, want %q", got, want)
	}
	if got, want := Predicate("team_metrics_daily", "t"),
		"(t.commits_count != 0 OR t.after_hours_commits_count != 0 OR t.weekend_commits_count != 0)"; got != want {
		t.Fatalf("Predicate = %q, want %q", got, want)
	}
	if got, want := NewestPredicate("ai_governance_coverage_daily", "coverage"),
		"argMax((coverage.ai_artifacts != 0 OR coverage.declared_artifacts != 0 OR coverage.human_reviewed_prs != 0 OR "+
			"coverage.security_scanned_prs != 0 OR coverage.in_policy_artifacts != 0), coverage.computed_at)"; got != want {
		t.Fatalf("NewestPredicate = %q, want %q", got, want)
	}
	if got, want := NewestPredicate("work_item_state_durations_daily", ""),
		"argMax((work_item_state_durations_daily.items_touched != 0), work_item_state_durations_daily.computed_at)"; got != want {
		t.Fatalf("NewestPredicate = %q, want %q", got, want)
	}
}

func TestPredicateOfAnUnknownTablePanics(t *testing.T) {
	for name, call := range map[string]func(){
		"Predicate":       func() { Predicate("repo_metrics_daily", "") },
		"NewestPredicate": func() { NewestPredicate("repo_metrics_daily", "") },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s of a table with no rule did not panic", name)
				}
			}()
			call()
		}()
	}
	if Registered("repo_metrics_daily") || MarkerColumns("repo_metrics_daily") != nil {
		t.Fatal("repo_metrics_daily reads as registered")
	}
}

func TestMeasured(t *testing.T) {
	for _, tc := range []struct {
		counts []uint64
		want   bool
	}{
		{nil, false},
		{[]uint64{0, 0, 0}, false},
		{[]uint64{0, 0, 1}, true},
		{[]uint64{5}, true},
	} {
		if got := Measured(tc.counts...); got != tc.want {
			t.Errorf("Measured(%v) = %v, want %v", tc.counts, got, tc.want)
		}
	}
}

var (
	columnPattern  = regexp.MustCompile("`(\\w+)` ((?:[^,(]|\\([^)]*\\))+)")
	orderByPattern = regexp.MustCompile(`ORDER BY \(([^)]*(?:\([^)]*\)[^)]*)*)\)`)
)

// TestMarkerColumnsAreTheCountsOfTheSchema holds the registry against the
// schema baseline of the migration chain. Each marker is a stored column that
// is not Nullable, so a retraction row holds 0 in it. For a table with counts,
// the markers are EXACTLY its unsigned integer columns outside the sorting
// key: a count column added to the table later is a measurement the rule
// would not see, so it fails here until it is registered.
func TestMarkerColumnsAreTheCountsOfTheSchema(t *testing.T) {
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	creates := map[string]string{}
	for _, object := range baseline.Objects {
		creates[object.Name] = object.Create
	}
	for _, table := range Tables() {
		create, ok := creates[table]
		if !ok {
			t.Errorf("%s: no such table in the schema baseline", table)
			continue
		}
		body := create[strings.Index(create, "(")+1 : strings.Index(create, ") ENGINE")]
		types := map[string]string{}
		for _, match := range columnPattern.FindAllStringSubmatch(body, -1) {
			types[match[1]] = strings.TrimSpace(match[2])
		}
		inKey := map[string]bool{}
		if key := orderByPattern.FindStringSubmatch(create); key != nil {
			for _, name := range regexp.MustCompile(`\w+`).FindAllString(key[1], -1) {
				inKey[name] = true
			}
		}
		markers := MarkerColumns(table)
		for _, column := range markers {
			kind, ok := types[column]
			switch {
			case !ok:
				t.Errorf("%s.%s: no such column", table, column)
			case strings.HasPrefix(kind, "Nullable") || strings.HasPrefix(kind, "LowCardinality(Nullable"):
				t.Errorf("%s.%s is %s: a retraction row holds NULL there, not 0", table, column, kind)
			}
		}
		if table == "compounding_risk_daily" {
			continue // markers are the stored configuration; the table has no count
		}
		var counts []string
		for name, kind := range types {
			if strings.HasPrefix(kind, "UInt") && !inKey[name] {
				counts = append(counts, name)
			}
		}
		sort.Strings(counts)
		sorted := append([]string(nil), markers...)
		sort.Strings(sorted)
		if strings.Join(counts, ",") != strings.Join(sorted, ",") {
			t.Errorf("%s: markers %v, want every unsigned count column outside the key %v", table, sorted, counts)
		}
	}
}

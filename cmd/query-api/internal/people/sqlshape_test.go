package people

import (
	"context"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"
)

// TestOrgIDScopesEveryPeopleSearchReadAtTheSameNestingDepthAsFinal pins
// the class ruling this package holds to (see searchPeopleQuery's own doc
// comment): both UNION ALL branches read a ReplacingMergeTree(computed_at)
// table FINAL, with org_id filtered inside the SAME branch, never a
// separate subquery an outer WHERE only narrows afterward. Depth-based
// (sqlshape.Depths), not a bare substring search, matching
// cmd/query-api/internal/quadrant/sqlshape_test.go's own convention: a
// parenthesised expression between a FINAL marker and its org predicate
// would false-positive a naive search without changing nesting depth.
func TestOrgIDScopesEveryPeopleSearchReadAtTheSameNestingDepthAsFinal(t *testing.T) {
	const orgIDPredicate = "org_id = {org_id:String}"
	markers := []string{
		"FROM user_metrics_daily FINAL",
		"FROM work_item_user_metrics_daily FINAL",
	}

	var captured string
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		captured = query
		return &fixtureRowScanner{}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if _, err := BuildSearchResponse(context.Background(), reader, "org-1", SearchParams{Query: "x", Limit: 20, Now: day(2024, 6, 15)}); err != nil {
		t.Fatalf("BuildSearchResponse: %v", err)
	}

	if strings.Contains(captured, "LIMIT 1 BY") {
		t.Fatalf("contains a LIMIT-1-BY dedup subquery shape -- expected a plain FINAL read:\n%s", captured)
	}

	depths := sqlshape.Depths(captured)
	for _, marker := range markers {
		markerIdx := strings.Index(captured, marker)
		if markerIdx < 0 {
			t.Fatalf("expected marker %q in query:\n%s", marker, captured)
		}
		markerDepth := depths[markerIdx]

		rest := captured[markerIdx+len(marker):]
		orgIdx := strings.Index(rest, orgIDPredicate)
		if orgIdx < 0 {
			t.Fatalf("expected %q after %q, got:\n%s", orgIDPredicate, marker, rest)
		}
		absoluteOrgIdx := markerIdx + len(marker) + orgIdx
		orgDepth := depths[absoluteOrgIdx]

		if orgDepth != markerDepth {
			t.Fatalf("org_id predicate sits at nesting depth %d but %q sits at depth %d -- "+
				"org_id must scope the SAME statement as the dedup source, not a subquery that "+
				"dedups the whole table before the tenant filter narrows it",
				orgDepth, marker, markerDepth)
		}
	}
}

// TestDedupTableFinalNeverLimitOneBy pins metricconfig.go's own class
// ruling: dedupTable always answers a plain FINAL read, never the
// reference dedup_from's LIMIT-1-BY subquery shape, for every table this
// package's metric config names.
func TestDedupTableFinalNeverLimitOneBy(t *testing.T) {
	for _, table := range []string{"work_item_user_metrics_daily", "user_metrics_daily", "work_item_cycle_times"} {
		got := dedupTable(table)
		want := table + " FINAL"
		if got != want {
			t.Fatalf("dedupTable(%q) = %q, want %q", table, got, want)
		}
	}
}

// TestFetchPersonMetricValueOrgIDAtSameDepthAsFinal pins the class ruling
// for the generic {table} metric reads (metricqueries.go's
// fetchPersonMetricValue/fetchPersonMetricSeries): org_id sits inside the
// SAME statement as the FINAL-read table, never a LIMIT-1-BY subquery a
// bare org_id predicate only narrows afterward.
func TestFetchPersonMetricValueOrgIDAtSameDepthAsFinal(t *testing.T) {
	var captured string
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		captured = query
		return &scalarFloatScanner{value: 0}, nil
	}}
	if _, err := fetchPersonMetricValue(context.Background(), client, "user_metrics_daily", "loc_touched", "sum", "identity_id", []string{"alice@example.com"}, day(2024, 6, 1), day(2024, 6, 15), "", "org-1"); err != nil {
		t.Fatalf("fetchPersonMetricValue: %v", err)
	}
	if strings.Contains(captured, "LIMIT 1 BY") {
		t.Fatalf("contains a LIMIT-1-BY dedup subquery shape:\n%s", captured)
	}
	const marker = "FROM user_metrics_daily FINAL"
	const orgIDPredicate = "org_id = {org_id:String}"
	markerIdx := strings.Index(captured, marker)
	if markerIdx < 0 {
		t.Fatalf("expected marker %q in query:\n%s", marker, captured)
	}
	depths := sqlshape.Depths(captured)
	markerDepth := depths[markerIdx]
	rest := captured[markerIdx+len(marker):]
	orgIdx := strings.Index(rest, orgIDPredicate)
	if orgIdx < 0 {
		t.Fatalf("expected %q after %q, got:\n%s", orgIDPredicate, marker, rest)
	}
	absoluteOrgIdx := markerIdx + len(marker) + orgIdx
	if depths[absoluteOrgIdx] != markerDepth {
		t.Fatalf("org_id predicate sits at nesting depth %d but %q sits at depth %d", depths[absoluteOrgIdx], marker, markerDepth)
	}
}

// TestFetchPersonBreakdownJoinsReposFinal pins the declared FINAL fix on
// the by_repo breakdown's static join_clause (metricconfig.go's own doc
// comment: the reference's join_clause never reads `repos` FINAL either).
func TestFetchPersonBreakdownJoinsReposFinal(t *testing.T) {
	var captured string
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		captured = query
		return &breakdownRowScanner{}, nil
	}}
	cfg := *personMetrics[1].ByRepo // review_latency's by_repo config
	if _, err := fetchPersonBreakdown(context.Background(), client, cfg, []string{"alice@example.com"}, day(2024, 6, 1), day(2024, 6, 15), "org-1"); err != nil {
		t.Fatalf("fetchPersonBreakdown: %v", err)
	}
	if !strings.Contains(captured, "INNER JOIN repos FINAL ON repos.id = user_metrics_daily.repo_id") {
		t.Fatalf("expected a FINAL repos join, got:\n%s", captured)
	}
}

// TestFetchCoverageWorkItemCycleTimesReadsFinal pins the declared FINAL
// fix on fetch_coverage's two work_item_cycle_times counts (summary.go's
// own doc comment: these ARE correctness-affected by an un-merged
// duplicate row, unlike the countDistinct/max(version) queries in the
// same function).
func TestFetchCoverageWorkItemCycleTimesReadsFinal(t *testing.T) {
	var captured []string
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		captured = append(captured, query)
		if strings.Contains(query, "countIf") {
			return &pairFloatScanner{a: 1, b: 2}, nil
		}
		return &scalarFloatScanner{value: 1}, nil
	}}
	if _, err := fetchCoverage(context.Background(), client, day(2024, 6, 1), day(2024, 6, 15), "org-1"); err != nil {
		t.Fatalf("fetchCoverage: %v", err)
	}
	found := 0
	for _, q := range captured {
		if strings.Contains(q, "countIf") {
			found++
			if !strings.Contains(q, "FROM work_item_cycle_times FINAL") {
				t.Fatalf("countIf query missing FINAL:\n%s", q)
			}
		}
	}
	if found != 2 {
		t.Fatalf("expected 2 countIf queries, got %d", found)
	}
}

// TestFetchPersonWorkMixReadsWorkItemCycleTimesFinal pins the same
// declared fix on person_summary_work_mix.sql's read.
func TestFetchPersonWorkMixReadsWorkItemCycleTimesFinal(t *testing.T) {
	var captured string
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		captured = query
		return &workMixScanner{}, nil
	}}
	if _, err := fetchPersonWorkMix(context.Background(), client, []string{"alice@example.com"}, day(2024, 6, 1), day(2024, 6, 15), "org-1"); err != nil {
		t.Fatalf("fetchPersonWorkMix: %v", err)
	}
	if !strings.Contains(captured, "FROM work_item_cycle_times FINAL") {
		t.Fatalf("expected a FINAL read, got:\n%s", captured)
	}
}

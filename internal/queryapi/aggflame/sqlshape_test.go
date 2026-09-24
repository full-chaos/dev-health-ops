package aggflame

import (
	"context"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"
)

// capturingClient records the last query text sent to it and answers with
// an empty result set -- these sqlshape tests only need the SQL text, not
// any row data.
type capturingClient struct {
	capture *string
}

func (c capturingClient) Query(_ context.Context, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	*c.capture = query
	return &fixtureRowScanner{}, nil
}

// TestOrgIDScopesEveryDedupSourceAtTheSameNestingDepth pins the class
// ruling this package holds to (see aggflame.go's own doc comment): every
// ReplacingMergeTree table this package reads is deduped (FINAL or an
// argMax(...) subquery) with org_id filtered in the SAME statement as the
// dedup source, never a separate outer WHERE that only narrows a
// tenant-blind merge/scan afterward. Depth-based (sqlshape.Depths), not a
// bare substring search, same convention as internal/quadrant's own
// sqlshape test.
func TestOrgIDScopesEveryDedupSourceAtTheSameNestingDepth(t *testing.T) {
	const orgIDPredicate = "org_id = {org_id:String}"

	cases := []struct {
		name    string
		query   string
		markers []string
	}{
		{
			"fetch_cycle_breakdown (work_item_state_durations_daily argMax)",
			cycleBreakdownQueryForTest("", "", ""),
			[]string{"FROM work_item_state_durations_daily"},
		},
		{
			"fetch_code_hotspots (file_metrics_daily argMax)",
			codeHotspotsQueryForTest(""),
			[]string{"FROM file_metrics_daily"},
		},
		{
			"fetch_repo_names (repos FINAL)",
			repoNamesQueryForTest(),
			[]string{"FROM repos FINAL"},
		},
		{
			"fetch_throughput (work_item_cycle_times FINAL + team attribution FINAL)",
			throughputQueryForTest(""),
			[]string{"FROM work_item_cycle_times AS wct FINAL", "FROM work_item_team_attributions FINAL"},
		},
		{
			"fetch_throughput_by_type (work_item_cycle_times FINAL + team attribution FINAL)",
			throughputByTypeQueryForTest(""),
			[]string{"FROM work_item_cycle_times AS wct FINAL", "FROM work_item_team_attributions FINAL"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			depths := sqlshape.Depths(tc.query)
			for _, marker := range tc.markers {
				markerIdx := strings.Index(tc.query, marker)
				if markerIdx < 0 {
					t.Fatalf("expected marker %q in query:\n%s", marker, tc.query)
				}
				markerDepth := depths[markerIdx]

				// Some queries here (fetch_throughput/fetch_throughput_by_type)
				// carry MORE THAN ONE occurrence of orgIDPredicate after a
				// marker -- e.g. the team-attribution subquery's own two
				// org_id checks precede the outer WHERE's -- so this scans
				// every occurrence rather than trusting the first (a naive
				// first-match would false-fail on the subquery's own,
				// correctly-scoped org_id landing before the outer one in
				// source order).
				searchFrom := markerIdx + len(marker)
				foundAtMarkerDepth := false
				for {
					orgIdx := strings.Index(tc.query[searchFrom:], orgIDPredicate)
					if orgIdx < 0 {
						break
					}
					absoluteOrgIdx := searchFrom + orgIdx
					if depths[absoluteOrgIdx] == markerDepth {
						foundAtMarkerDepth = true
						break
					}
					searchFrom = absoluteOrgIdx + len(orgIDPredicate)
				}
				if !foundAtMarkerDepth {
					t.Fatalf("no %q at nesting depth %d (the same depth as %q) -- "+
						"org_id must scope the SAME statement as the dedup source, not a subquery that "+
						"dedups the whole table before the tenant filter narrows it, in:\n%s",
						orgIDPredicate, markerDepth, marker, tc.query)
				}
			}
		})
	}
}

// TestNoLimitOneByDedupShape pins the class ruling's negative half: none
// of this package's ReplacingMergeTree reads may use the LIMIT-1-BY dedup
// idiom (that is Python's clickhouse_dedup.dedup_from()'s own shape for
// file_metrics_daily, deliberately NOT reused here -- see aggflame.go's
// own doc comment).
func TestNoLimitOneByDedupShape(t *testing.T) {
	queries := map[string]string{
		"fetch_code_hotspots": codeHotspotsQueryForTest(""),
	}
	for name, q := range queries {
		if strings.Contains(q, "LIMIT 1 BY") {
			t.Fatalf("%s: contains a LIMIT-1-BY dedup subquery shape -- expected an argMax(...) subquery", name)
		}
	}
}

// The helpers below build the same query strings fetchCycleBreakdown/
// fetchCodeHotspots/fetchRepoNames/fetchThroughput/fetchThroughputByType
// send, without requiring a live QueryClient -- this test file's own
// thin wrapper around each fetch function's query-building, kept in this
// file (not clickhouse.go) since production code has no reason to split
// query construction from execution.

func cycleBreakdownQueryForTest(teamID, provider, workScopeID string) string {
	captured := ""
	client := capturingClient{capture: &captured}
	_, _ = fetchCycleBreakdown(context.Background(), client, "org-1", time.Time{}, time.Time{}, teamID, provider, workScopeID)
	return captured
}

func codeHotspotsQueryForTest(repoID string) string {
	captured := ""
	client := capturingClient{capture: &captured}
	_, _ = fetchCodeHotspots(context.Background(), client, "org-1", time.Time{}, time.Time{}, repoID, 500, 1)
	return captured
}

func repoNamesQueryForTest() string {
	captured := ""
	client := capturingClient{capture: &captured}
	_, _ = fetchRepoNames(context.Background(), client, "org-1", []string{"r1"})
	return captured
}

func throughputQueryForTest(teamID string) string {
	captured := ""
	client := capturingClient{capture: &captured}
	_, _ = fetchThroughput(context.Background(), client, "org-1", time.Time{}, time.Time{}, teamID, 500)
	return captured
}

func throughputByTypeQueryForTest(teamID string) string {
	captured := ""
	client := capturingClient{capture: &captured}
	_, _ = fetchThroughputByType(context.Background(), client, "org-1", time.Time{}, time.Time{}, teamID, 500)
	return captured
}

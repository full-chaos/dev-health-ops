package sankey

import (
	"context"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"
)

// capturingClient records every query text it is asked to run and answers
// with zero rows -- same convention as heatmap/quadrant's own
// capturingClient.
type capturingClient struct {
	queries []string
}

func (c *capturingClient) Query(_ context.Context, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.queries = append(c.queries, query)
	return &fixtureRowScanner{}, nil
}

// TestReplacingMergeTreeReadsAreFinalWithOrgIDInStatement pins the class
// ruling this package holds to (package doc comment): every
// ReplacingMergeTree table read directly by this package (repos,
// work_item_cycle_times, work_item_state_durations_daily,
// file_metrics_daily) is read FINAL, never via a LIMIT-1-BY dedup
// subquery, with org_id filtering the SAME statement as the FINAL source.
func TestReplacingMergeTreeReadsAreFinalWithOrgIDInStatement(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	assertNoLimit1By := func(t *testing.T, query string) {
		t.Helper()
		if strings.Contains(query, "LIMIT 1 BY") {
			t.Fatalf("query contains a LIMIT-1-BY dedup subquery shape -- expected a plain FINAL read:\n%s", query)
		}
	}

	t.Run("resolveRepoID by uuid", func(t *testing.T) {
		c := &capturingClient{}
		_, _, _ = resolveRepoID(ctx, c, "12345678-1234-5678-1234-567812345678", "org-1")
		assertNoLimit1By(t, c.queries[0])
		if !strings.Contains(c.queries[0], "FROM repos FINAL") {
			t.Fatalf("expected FROM repos FINAL:\n%s", c.queries[0])
		}
	})

	t.Run("fetchExpenseCounts", func(t *testing.T) {
		c := &capturingClient{}
		_, _ = fetchExpenseCounts(ctx, c, now, now, "", nil, "org-1")
		assertNoLimit1By(t, c.queries[0])
		if !strings.Contains(c.queries[0], "FROM work_item_metrics_daily FINAL") {
			t.Fatalf("expected FROM work_item_metrics_daily FINAL:\n%s", c.queries[0])
		}
	})

	t.Run("fetchExpenseAbandoned", func(t *testing.T) {
		c := &capturingClient{}
		_, _ = fetchExpenseAbandoned(ctx, c, now, now, "", nil, "org-1")
		assertNoLimit1By(t, c.queries[0])
		if !strings.Contains(c.queries[0], "FROM work_item_cycle_times FINAL") {
			t.Fatalf("expected FROM work_item_cycle_times FINAL:\n%s", c.queries[0])
		}
	})

	t.Run("fetchStateStatusCounts", func(t *testing.T) {
		c := &capturingClient{}
		_, _ = fetchStateStatusCounts(ctx, c, now, now, "", nil, "org-1")
		assertNoLimit1By(t, c.queries[0])
		if !strings.Contains(c.queries[0], "FROM work_item_state_durations_daily FINAL") {
			t.Fatalf("expected FROM work_item_state_durations_daily FINAL:\n%s", c.queries[0])
		}
	})

	t.Run("fetchInvestmentFlowItems repos join", func(t *testing.T) {
		c := &capturingClient{}
		_, _ = fetchInvestmentFlowItems(ctx, c, now, now, "", nil, 60, "org-1")
		query := c.queries[0]
		assertNoLimit1By(t, query)
		joinIdx := strings.Index(query, "LEFT JOIN repos AS r FINAL ON")
		if joinIdx < 0 {
			t.Fatalf("expected LEFT JOIN repos AS r FINAL ON (alias BEFORE FINAL, the only order ClickHouse accepts):\n%s", query)
		}
		if !strings.Contains(query[joinIdx:joinIdx+200], "r.org_id = {org_id:String}") {
			t.Fatalf("expected r.org_id inside the JOIN's own ON clause:\n%s", query)
		}
	})

	t.Run("fetchHotspotRows file_metrics_daily+repos, all three sub-selects", func(t *testing.T) {
		c := &capturingClient{}
		_, _ = fetchHotspotRows(ctx, c, now, now, "", nil, 150, "org-1")
		query := c.queries[0]
		assertNoLimit1By(t, query)
		if got := strings.Count(query, "FROM file_metrics_daily AS metrics FINAL"); got != 3 {
			t.Fatalf("expected 3 FROM file_metrics_daily AS metrics FINAL (alias BEFORE FINAL; two quantile subqueries + main select), got %d:\n%s", got, query)
		}
		if got := strings.Count(query, "INNER JOIN repos AS r FINAL"); got != 3 {
			t.Fatalf("expected 3 INNER JOIN repos AS r FINAL (alias BEFORE FINAL), got %d:\n%s", got, query)
		}
		depths := sqlshape.Depths(query)
		for _, joinIdx := range allIndexes(query, "INNER JOIN repos AS r FINAL") {
			onOrgIdx := strings.Index(query[joinIdx:], "r.org_id = {org_id:String}")
			if onOrgIdx < 0 {
				t.Fatalf("expected r.org_id inside every JOIN's own ON clause:\n%s", query)
			}
			fromIdx := strings.LastIndex(query[:joinIdx], "FROM")
			if depths[fromIdx] != depths[joinIdx+onOrgIdx] {
				t.Fatalf("org_id predicate at depth %d, its own FROM at depth %d -- must scope the SAME statement",
					depths[joinIdx+onOrgIdx], depths[fromIdx])
			}
		}
	})
}

// finalBeforeAliasPattern matches `FINAL AS <alias>` -- ClickHouse accepts
// only `<table> AS <alias> FINAL`, never `<table> FINAL AS <alias>` (Code
// 62 syntax error, confirmed live). A source-text scan, not a
// runtime-captured-query scan, so a NEW hand-written FINAL join added to
// this file in the wrong order fails here regardless of which function it
// lands in, without needing a matching runtime test added by hand.
var finalBeforeAliasPattern = regexp.MustCompile(`\bFINAL\s+AS\s+\w+`)

// TestNoAliasAfterFinalInQueriesSource pins the alias-before-FINAL
// ordering at the source level for this package's queries.go.
func TestNoAliasAfterFinalInQueriesSource(t *testing.T) {
	src, err := os.ReadFile("queries.go")
	if err != nil {
		t.Fatalf("read queries.go: %v -- this trip-wire's own input did not resolve", err)
	}
	if finalCount := strings.Count(string(src), "FINAL"); finalCount == 0 {
		t.Fatalf("queries.go contains no FINAL at all -- this trip-wire's own input moved, was renamed, or the FINAL dedup convention regressed")
	}
	if bad := finalBeforeAliasPattern.FindAll(src, -1); len(bad) > 0 {
		t.Fatalf("queries.go contains %d `<table> FINAL AS <alias>` site(s) -- ClickHouse SYNTAX error (Code 62); alias must come BEFORE FINAL (`<table> AS <alias> FINAL`): %v", len(bad), bad)
	}
}

func allIndexes(s, substr string) []int {
	var out []int
	offset := 0
	for {
		idx := strings.Index(s[offset:], substr)
		if idx < 0 {
			return out
		}
		out = append(out, offset+idx)
		offset += idx + len(substr)
	}
}

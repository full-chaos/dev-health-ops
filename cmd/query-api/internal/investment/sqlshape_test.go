package investment

import (
	"context"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// capturingClient is a fake analytics.QueryClient that records the last
// query/bindings it was asked to run and returns zero rows -- same shape
// investmentexplain's own capturingClient (investmentexplain/
// fakeclient_test.go) establishes for query-composition tests that don't
// need a live ClickHouse.
type capturingClient struct {
	lastQuery    string
	lastBindings []dhclickhouse.Binding
}

func (c *capturingClient) Query(_ context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.lastQuery = statement
	c.lastBindings = bindings
	return &emptyRowScanner{}, nil
}

type emptyRowScanner struct{}

func (emptyRowScanner) Next() bool        { return false }
func (emptyRowScanner) Scan(...any) error { return nil }
func (emptyRowScanner) Err() error        { return nil }
func (emptyRowScanner) Close() error      { return nil }

// TestQueriesComposeFromTheSharedLatestWorkUnitInvestmentsSource pins
// that both of this package's own reads (quality stats, sunburst) go
// through analytics.LatestWorkUnitInvestmentsSource -- never a raw
// `FROM work_unit_investments` -- so both pick up its ReplacingMergeTree
// argMax(tuple(col), computed_at) dedup and its work_unit_supersessions
// exclusion. A raw, un-deduped read of an RMT table, or a second,
// independent copy of this source's dedup logic, is exactly the class of
// defect this test exists to catch before it reaches a live ClickHouse.
func TestQueriesComposeFromTheSharedLatestWorkUnitInvestmentsSource(t *testing.T) {
	const (
		dedupMarker        = "(argMax(tuple(work_unit_type), computed_at)).1 AS work_unit_type"
		supersessionMarker = "work_unit_supersessions"
	)

	cases := []struct {
		name  string
		build func(t *testing.T, client *capturingClient)
	}{
		{
			name: "FetchInvestmentQualityStats",
			build: func(t *testing.T, client *capturingClient) {
				reader, err := NewReader(client)
				if err != nil {
					t.Fatalf("NewReader: %v", err)
				}
				if _, _, err := reader.FetchInvestmentQualityStats(context.Background(), QualityStatsFilters{
					OrgID: "org-1", StartTS: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), EndTS: time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC),
				}); err != nil {
					t.Fatalf("FetchInvestmentQualityStats: %v", err)
				}
			},
		},
		{
			name: "FetchInvestmentSunburst",
			build: func(t *testing.T, client *capturingClient) {
				reader, err := NewReader(client)
				if err != nil {
					t.Fatalf("NewReader: %v", err)
				}
				if _, err := reader.FetchInvestmentSunburst(context.Background(), SunburstFilters{
					OrgID: "org-1", StartTS: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), EndTS: time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC),
				}); err != nil {
					t.Fatalf("FetchInvestmentSunburst: %v", err)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := &capturingClient{}
			tc.build(t, client)
			query := client.lastQuery

			if !strings.Contains(query, dedupMarker) {
				t.Fatalf("%s: expected the shared dedup marker %q, got:\n%s", tc.name, dedupMarker, query)
			}
			if !strings.Contains(query, supersessionMarker) {
				t.Fatalf("%s: expected the shared work_unit_supersessions exclusion, got:\n%s", tc.name, query)
			}
			if strings.Contains(query, "LIMIT 1 BY") {
				t.Fatalf("%s: contains a LIMIT-1-BY dedup shape -- expected argMax/FINAL only", tc.name)
			}
		})
	}
}

// TestSunburstRepoJoinReadsFinal pins the sunburst repo-name join reads
// `repos` (ReplacingMergeTree, sorting key org_id/id) with FINAL and an
// org_id predicate on the SAME join -- never Python's own un-deduped,
// unscoped `LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)`,
// which can fan a sunburst slice out by however many unmerged physical
// versions of a repo row are still live.
func TestSunburstRepoJoinReadsFinal(t *testing.T) {
	client := &capturingClient{}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if _, err := reader.FetchInvestmentSunburst(context.Background(), SunburstFilters{
		OrgID: "org-1", StartTS: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), EndTS: time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("FetchInvestmentSunburst: %v", err)
	}

	const joinMarker = "LEFT JOIN repos AS r FINAL ON toString(r.id) = toString(repo_id) AND r.org_id = {org_id:String}"
	if !strings.Contains(client.lastQuery, joinMarker) {
		t.Fatalf("expected repos join %q, got:\n%s", joinMarker, client.lastQuery)
	}
}

// TestOrgIDScopesTheOuterQueryAtTheSameDepthAsTheSharedSource pins that
// the outer org_id predicate both readers add sits at the SAME
// parenthesis nesting depth as the shared CTE's own org_id filter --
// never inside a narrower subquery an outer WHERE would only filter
// after the fact, the same depth-based guard investmentexplain's own
// sqlshape test uses for its fixed reads.
func TestOrgIDScopesTheOuterQueryAtTheSameDepthAsTheSharedSource(t *testing.T) {
	client := &capturingClient{}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if _, _, err := reader.FetchInvestmentQualityStats(context.Background(), QualityStatsFilters{
		OrgID: "org-1", StartTS: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), EndTS: time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("FetchInvestmentQualityStats: %v", err)
	}

	query := client.lastQuery
	const outerMarker = "AS work_unit_investments"
	const orgPredicate = "work_unit_investments.org_id = {org_id:String}"

	markerIdx := strings.Index(query, outerMarker)
	if markerIdx < 0 {
		t.Fatalf("expected marker %q in query:\n%s", outerMarker, query)
	}
	orgIdx := strings.LastIndex(query, orgPredicate)
	if orgIdx < 0 {
		t.Fatalf("expected %q in query:\n%s", orgPredicate, query)
	}
	if orgIdx < markerIdx {
		t.Fatalf("outer org_id predicate must follow the outer FROM source, got predicate at %d before marker at %d", orgIdx, markerIdx)
	}
}

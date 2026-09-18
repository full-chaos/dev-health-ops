package heatmap

import (
	"context"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"
)

// capturingClient records every query text it is asked to run and
// answers with zero rows -- used only to pull the exact SQL each fetch
// function builds, without needing a query-string-returning variant of
// each one.
type capturingClient struct {
	queries []string
}

func (c *capturingClient) Query(_ context.Context, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.queries = append(c.queries, query)
	return &fixtureRowScanner{}, nil
}

// TestFinalDedupOrgIDScopesEveryHeatmapRead pins the class ruling this
// package holds to (package doc comment): every ReplacingMergeTree table
// this package reads (repos, git_pull_requests, git_commits,
// file_metrics_daily) is read FINAL, with org_id filtered inside the
// JOIN's own ON clause -- the same statement as that FINAL source, never
// a separate subquery an outer WHERE only narrows afterward. Depth-based
// (sqlshape.Depths), not a bare substring search, matching quadrant's own
// copy of this test.
func TestFinalDedupOrgIDScopesEveryHeatmapRead(t *testing.T) {
	const orgIDPredicate = "repos.org_id = {org_id:String}"
	now := time.Now().UTC()
	ctx := context.Background()

	run := func(name string, fn func(c *capturingClient)) {
		t.Run(name, func(t *testing.T) {
			c := &capturingClient{}
			fn(c)
			if len(c.queries) == 0 {
				t.Fatalf("%s: no query captured", name)
			}
			query := c.queries[0]
			if strings.Contains(query, "LIMIT 1 BY") {
				t.Fatalf("%s: contains a LIMIT-1-BY dedup subquery shape -- expected a plain FINAL read:\n%s", name, query)
			}
			joinIdx := strings.Index(query, "INNER JOIN repos FINAL ON")
			if joinIdx < 0 {
				t.Fatalf("%s: expected an INNER JOIN repos FINAL clause:\n%s", name, query)
			}
			orgIdx := strings.Index(query[joinIdx:], orgIDPredicate)
			if orgIdx < 0 {
				t.Fatalf("%s: expected %q inside the JOIN's own ON clause:\n%s", name, orgIDPredicate, query)
			}
			fromIdx := strings.LastIndex(query[:joinIdx], "FROM")
			if fromIdx < 0 {
				t.Fatalf("%s: expected a FROM before the JOIN:\n%s", name, query)
			}
			depths := sqlshape.Depths(query)
			if depths[fromIdx] != depths[joinIdx+orgIdx] {
				t.Fatalf("%s: repos.org_id predicate sits at nesting depth %d, its own FROM sits at %d -- org_id must scope the SAME statement as the FINAL source",
					name, depths[joinIdx+orgIdx], depths[fromIdx])
			}
		})
	}

	run("fetchReviewWaitDensity (git_pull_requests + repos)", func(c *capturingClient) {
		_, _ = fetchReviewWaitDensity(ctx, c, now, now, "", nil, "org-1")
	})
	run("fetchReviewWaitEvidence (git_pull_requests + repos)", func(c *capturingClient) {
		_, _ = fetchReviewWaitEvidence(ctx, c, now, now, 1, 1, "", nil, 1, "org-1")
	})
	run("fetchIndividualActiveHours (git_commits + repos)", func(c *capturingClient) {
		_, _ = fetchIndividualActiveHours(ctx, c, now, now, []string{"a@b.com"}, "org-1")
	})
	run("fetchIndividualActiveEvidence (git_commits + repos)", func(c *capturingClient) {
		_, _ = fetchIndividualActiveEvidence(ctx, c, now, now, 1, 1, []string{"a@b.com"}, 1, "org-1")
	})
	run("fetchHotspotEvidence (file_metrics_daily + repos)", func(c *capturingClient) {
		_, _ = fetchHotspotEvidence(ctx, c, now, now, "repoA:a.go", "", nil, 1, "org-1")
	})

	// fetchRepoTouchpoints and fetchHotspotRisk each run TWO queries (top-N
	// then series); both must carry the shape, so capture and check both
	// via a client returning the top-N with one row (otherwise the
	// second query never runs, matching fetchRepoTouchpoints/
	// fetchHotspotRisk's own "no repos/files -> stop" short circuit).
	t.Run("fetchRepoTouchpoints (git_commits + repos), both queries", func(t *testing.T) {
		c := &topNPassThroughClient{col: "repoA"}
		if _, err := fetchRepoTouchpoints(ctx, c, now, now, "", nil, 1, "org-1"); err != nil {
			t.Fatalf("fetchRepoTouchpoints: %v", err)
		}
		if len(c.queries) != 2 {
			t.Fatalf("got %d queries, want 2 (top-N + series)", len(c.queries))
		}
		for i, query := range c.queries {
			assertFinalDedupShape(t, query, "git_commits", i)
		}
	})
	t.Run("fetchHotspotRisk (file_metrics_daily + repos), both queries", func(t *testing.T) {
		c := &topNPassThroughClient{col: "repoA:a.go"}
		if _, err := fetchHotspotRisk(ctx, c, now, now, "", nil, 1, "org-1"); err != nil {
			t.Fatalf("fetchHotspotRisk: %v", err)
		}
		if len(c.queries) != 2 {
			t.Fatalf("got %d queries, want 2 (top-N + series)", len(c.queries))
		}
		for i, query := range c.queries {
			assertFinalDedupShape(t, query, "file_metrics_daily", i)
		}
	})
}

// assertFinalDedupShape is the run() closure's body above, factored out
// for the two two-query fetch functions.
func assertFinalDedupShape(t *testing.T, query, table string, index int) {
	t.Helper()
	const orgIDPredicate = "repos.org_id = {org_id:String}"
	if strings.Contains(query, "LIMIT 1 BY") {
		t.Fatalf("query[%d]: contains a LIMIT-1-BY dedup subquery shape -- expected a plain FINAL read:\n%s", index, query)
	}
	if !strings.Contains(query, "FROM "+table+" FINAL") {
		t.Fatalf("query[%d]: expected %q read FINAL:\n%s", index, table, query)
	}
	joinIdx := strings.Index(query, "INNER JOIN repos FINAL ON")
	if joinIdx < 0 {
		t.Fatalf("query[%d]: expected an INNER JOIN repos FINAL clause:\n%s", index, query)
	}
	orgIdx := strings.Index(query[joinIdx:], orgIDPredicate)
	if orgIdx < 0 {
		t.Fatalf("query[%d]: expected %q inside the JOIN's own ON clause:\n%s", index, orgIDPredicate, query)
	}
	fromIdx := strings.LastIndex(query[:joinIdx], "FROM")
	depths := sqlshape.Depths(query)
	if depths[fromIdx] != depths[joinIdx+orgIdx] {
		t.Fatalf("query[%d]: repos.org_id predicate sits at nesting depth %d, its own FROM sits at %d",
			index, depths[joinIdx+orgIdx], depths[fromIdx])
	}
}

// TestTopNQueriesOrderByCarriesADeterministicTiebreak pins the fix at
// the SQL text level: fetchRepoTouchpoints' and
// fetchHotspotRisk's own top-N queries each carry a secondary ORDER BY
// key after `total DESC`, so a tie AT the LIMIT boundary resolves to the
// same set of names on every run -- unlike a bare `ORDER BY total DESC`,
// which ClickHouse is free to break differently run to run. This is a
// SQL-text guard only: the set-stability it produces cannot be proven
// from Go alone (ClickHouse decides the tie, not this process), so a
// passing run here says the query CARRIES the tiebreak, not that a live
// ClickHouse tie was ever exercised.
func TestTopNQueriesOrderByCarriesADeterministicTiebreak(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	t.Run("fetchRepoTouchpoints top-N query", func(t *testing.T) {
		c := &capturingClient{}
		if _, err := fetchRepoTouchpoints(ctx, c, now, now, "", nil, 20, "org-1"); err != nil {
			t.Fatalf("fetchRepoTouchpoints: %v", err)
		}
		if len(c.queries) == 0 {
			t.Fatal("no query captured")
		}
		if !strings.Contains(c.queries[0], "ORDER BY total DESC, repo ASC") {
			t.Fatalf("top-N query carries no secondary ORDER BY key:\n%s", c.queries[0])
		}
	})

	t.Run("fetchHotspotRisk top-N query", func(t *testing.T) {
		c := &capturingClient{}
		if _, err := fetchHotspotRisk(ctx, c, now, now, "", nil, 20, "org-1"); err != nil {
			t.Fatalf("fetchHotspotRisk: %v", err)
		}
		if len(c.queries) == 0 {
			t.Fatal("no query captured")
		}
		if !strings.Contains(c.queries[0], "ORDER BY total DESC, file_key ASC") {
			t.Fatalf("top-N query carries no secondary ORDER BY key:\n%s", c.queries[0])
		}
	})
}

// topNPassThroughClient answers the FIRST query (a top-N query selecting
// one string column plus a numeric total) with one row naming col, and
// every subsequent query with zero rows -- enough to make
// fetchRepoTouchpoints/fetchHotspotRisk run their second query instead of
// short-circuiting on an empty top-N result.
type topNPassThroughClient struct {
	col     string
	queries []string
}

func (c *topNPassThroughClient) Query(_ context.Context, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.queries = append(c.queries, query)
	if len(c.queries) == 1 {
		if strings.Contains(query, "git_commits") {
			return &fixtureRowScanner{rows: [][]any{{c.col, uint64(1)}}}, nil
		}
		return &fixtureRowScanner{rows: [][]any{{c.col, 1.0}}}, nil
	}
	return &fixtureRowScanner{}, nil
}

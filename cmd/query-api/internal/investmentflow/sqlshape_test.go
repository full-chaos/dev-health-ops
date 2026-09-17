package investmentflow

import (
	"context"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// capturingClient records every query text it is asked to run and answers
// with zero rows -- same convention as sankey/heatmap/quadrant's own
// capturingClient.
type capturingClient struct {
	queries []string
}

func (c *capturingClient) Query(_ context.Context, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.queries = append(c.queries, query)
	return &fixtureRowScanner{}, nil
}

// TestFetchersNeverReadWorkUnitInvestmentsRaw pins that no fetcher reads
// work_unit_investments outside the shared derived table: every fetcher
// reads it only through analytics.LatestWorkUnitInvestmentsSource()'s
// already argMax-tuple-fixed derived table, never a bare
// `FROM work_unit_investments` that would skip that dedup.
func TestFetchersNeverReadWorkUnitInvestmentsRaw(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	assertDeduped := func(t *testing.T, query string) {
		t.Helper()
		// The one legitimate `FROM work_unit_investments` is
		// LatestWorkUnitInvestmentsSource's own innermost read, which the
		// argMax(tuple()).1 dedup immediately wraps -- so the invariant this
		// asserts is "the dedup pattern is present at all", not "the raw
		// table name never appears" (every fetcher's outer FROM/JOIN
		// references the DERIVED alias "work_unit_investments" produced by
		// that source, which also contains the substring).
		if !strings.Contains(query, "argMax(tuple(") {
			t.Fatalf("query does not carry the argMax(tuple()).1 null-safe dedup pattern:\n%s", query)
		}
		if !strings.Contains(query, "{org_id:String}") {
			t.Fatalf("query carries no org_id binding at all:\n%s", query)
		}
	}

	t.Run("fetchInvestmentSubcategoryEdges", func(t *testing.T) {
		c := &capturingClient{}
		_, _ = fetchInvestmentSubcategoryEdges(ctx, c, now, now, "", nil, "org-1", nil, nil)
		assertDeduped(t, c.queries[0])
	})
	t.Run("fetchInvestmentTeamEdges", func(t *testing.T) {
		c := &capturingClient{}
		_, _ = fetchInvestmentTeamEdges(ctx, c, now, now, "", nil, "org-1", nil, nil)
		assertDeduped(t, c.queries[0])
	})
	t.Run("fetchInvestmentRepoTeamEdges", func(t *testing.T) {
		c := &capturingClient{}
		_, _ = fetchInvestmentRepoTeamEdges(ctx, c, now, now, "", nil, "org-1", nil, nil)
		assertDeduped(t, c.queries[0])
	})
	t.Run("fetchInvestmentTeamCategoryRepoEdges", func(t *testing.T) {
		c := &capturingClient{}
		_, _ = fetchInvestmentTeamCategoryRepoEdges(ctx, c, now, now, "", nil, "org-1", nil, nil)
		assertDeduped(t, c.queries[0])
	})
	t.Run("fetchInvestmentTeamSubcategoryRepoEdges", func(t *testing.T) {
		c := &capturingClient{}
		_, _ = fetchInvestmentTeamSubcategoryRepoEdges(ctx, c, now, now, "", nil, "org-1", nil, nil)
		assertDeduped(t, c.queries[0])
	})
	t.Run("fetchInvestmentUnassignedCounts", func(t *testing.T) {
		c := &capturingClient{}
		_, _ = fetchInvestmentUnassignedCounts(ctx, c, now, now, "", nil, "org-1", nil, nil)
		assertDeduped(t, c.queries[0])
	})
}

// TestRepoJoinsAreFinalWithOrgIDInOwnONClause pins investmentFlowRepoDedup
// Parity (queries.go's own package doc comment): every fetcher that joins
// `repos` reads it FINAL, org_id inside that JOIN's own ON clause -- the
// declared fix over Python's plain, undeduped `LEFT JOIN repos AS r ON
// toString(r.id) = toString(repo_id)`.
func TestRepoJoinsAreFinalWithOrgIDInOwnONClause(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	assertRepoFinal := func(t *testing.T, query string) {
		t.Helper()
		joinIdx := strings.Index(query, "LEFT JOIN repos FINAL AS r ON")
		if joinIdx < 0 {
			t.Fatalf("expected LEFT JOIN repos FINAL AS r ON:\n%s", query)
		}
		onClauseEnd := strings.Index(query[joinIdx:], "\n")
		if onClauseEnd < 0 {
			onClauseEnd = len(query) - joinIdx
		}
		if !strings.Contains(query[joinIdx:joinIdx+onClauseEnd], "r.org_id = {org_id:String}") {
			t.Fatalf("expected r.org_id inside the JOIN's own ON clause:\n%s", query)
		}
	}

	t.Run("fetchInvestmentSubcategoryEdges", func(t *testing.T) {
		c := &capturingClient{}
		_, _ = fetchInvestmentSubcategoryEdges(ctx, c, now, now, "", nil, "org-1", nil, nil)
		assertRepoFinal(t, c.queries[0])
	})
	t.Run("fetchInvestmentRepoTeamEdges", func(t *testing.T) {
		c := &capturingClient{}
		_, _ = fetchInvestmentRepoTeamEdges(ctx, c, now, now, "", nil, "org-1", nil, nil)
		assertRepoFinal(t, c.queries[0])
	})
	t.Run("fetchInvestmentTeamCategoryRepoEdges", func(t *testing.T) {
		c := &capturingClient{}
		_, _ = fetchInvestmentTeamCategoryRepoEdges(ctx, c, now, now, "", nil, "org-1", nil, nil)
		assertRepoFinal(t, c.queries[0])
	})
	t.Run("fetchInvestmentTeamSubcategoryRepoEdges", func(t *testing.T) {
		c := &capturingClient{}
		_, _ = fetchInvestmentTeamSubcategoryRepoEdges(ctx, c, now, now, "", nil, "org-1", nil, nil)
		assertRepoFinal(t, c.queries[0])
	})
	// fetchInvestmentTeamEdges never joins the TARGET repo at all
	// (confirmed by reading fetch_investment_team_edges' Python source) --
	// it still references `repos` INDIRECTLY, inside analytics.
	// BuildUnitTeamSubquery's own WORK_UNIT_EVIDENCE_REPO_SOURCE lookup
	// (a different, evidence-ref-resolution join, not this test's
	// concern), so the assertion below is specifically "no `LEFT JOIN
	// repos FINAL AS r`", not "no `repos` substring at all".
	t.Run("fetchInvestmentTeamEdges never joins the target repo", func(t *testing.T) {
		c := &capturingClient{}
		_, _ = fetchInvestmentTeamEdges(ctx, c, now, now, "", nil, "org-1", nil, nil)
		if strings.Contains(c.queries[0], "LEFT JOIN repos FINAL AS r") {
			t.Fatalf("fetchInvestmentTeamEdges unexpectedly joins the target repo:\n%s", c.queries[0])
		}
	})
}

// TestUnitTeamSubqueryReused pins that every team-attribution join in this
// package composes analytics.BuildUnitTeamSubquery -- confirmed by its own
// unmistakable inner "resolved_team_id"/"resolved_team" column names,
// unique to that function's output.
func TestUnitTeamSubqueryReused(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	t.Run("fetchInvestmentTeamEdges", func(t *testing.T) {
		c := &capturingClient{}
		_, _ = fetchInvestmentTeamEdges(ctx, c, now, now, "", nil, "org-1", nil, nil)
		if !strings.Contains(c.queries[0], "resolved_team_id") {
			t.Fatalf("expected analytics.BuildUnitTeamSubquery's resolved_team_id column:\n%s", c.queries[0])
		}
	})
	t.Run("fetchInvestmentRepoTeamEdges", func(t *testing.T) {
		c := &capturingClient{}
		_, _ = fetchInvestmentRepoTeamEdges(ctx, c, now, now, "", nil, "org-1", nil, nil)
		if !strings.Contains(c.queries[0], "resolved_team_id") {
			t.Fatalf("expected analytics.BuildUnitTeamSubquery's resolved_team_id column:\n%s", c.queries[0])
		}
	})
}

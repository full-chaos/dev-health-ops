package home

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// fetchInvestmentThemeAllocation is the Home tile "Investment by theme" (CHAOS-9195, D5933;
// the wire field is still rework_theme_allocation): the effort over the five canonical themes
// that the Jev categorization writes (work_unit_investments), NOT the legacy rule classifier's
// investment_area keys (D5932: that classifier is removed; before this the tile showed two themes
// covering 2.5% of the completed items on real data). It is the Investment page's own read
// (investmentexplain.Reader.FetchInvestmentThemeEffort: the latest row of each unit, every
// categorization status, the same scope and category clauses), with a unit dated by its end day.
//
//   - allocation: each unit's effort_value weighted by its share of the theme (the unit of the
//     effort is the unit's effort_metric, churn_loc on real data); allocation_pct its share of the
//     response.
//   - churn_loc: the part of that effort measured in changed lines of code.
//   - prs_merged: the pull requests of the units' evidence weighted by the theme share, rounded
//     (the name is the wire's; the evidence of a unit names the pull requests behind it).
//
// DECLARED DIVERGENCE from the reference (which read investment_metrics_daily).
func fetchInvestmentThemeAllocation(ctx context.Context, client QueryClient, f Filters, orgID string, startDay, endDay time.Time, asOf time.Time) ([]ReworkThemeAllocation, error) {
	reader, err := investmentexplain.NewReader(client)
	if err != nil {
		return nil, fmt.Errorf("home: investment by theme: %w", err)
	}
	refs := teamscope.NamedRepoRefs(f.Scope.Level, f.Scope.IDs, f.What.Repos)
	repoIDs, err := resolveRepoIDs(ctx, client, refs, orgID)
	if err != nil {
		return nil, err
	}
	filters := investmentexplain.BreakdownFilters{
		OrgID:      orgID,
		StartTS:    startDay,
		EndTS:      endDay,
		RepoIDs:    repoIDs,
		ReposNamed: len(refs) > 0,
		DatedByEnd: true,
	}
	if f.Scope.Level == "team" && len(f.Scope.IDs) > 0 {
		filters.TeamScopeCondition, filters.TeamScopeBindings = teamscope.RepoCondition(orgID, "repo_id", f.Scope.IDs, asOf)
		filters.TeamIDs, filters.TeamAsOf = f.Scope.IDs, asOf
	}
	filters.Themes, filters.Subcategories = investmentexplain.SplitWorkCategories(f.Why.WorkCategory)

	rows, err := reader.FetchInvestmentThemeEffort(ctx, filters)
	if err != nil {
		return nil, fmt.Errorf("home: investment by theme: %w", err)
	}
	return themeAllocationFromEffort(rows), nil
}

// themeAllocationFromEffort rolls the theme rows into the tile: the canonical themes only, the
// largest effort first (ties by theme), the share of the total in allocation_pct.
func themeAllocationFromEffort(rows []investmentexplain.ThemeEffortRow) []ReworkThemeAllocation {
	kept := make([]investmentexplain.ThemeEffortRow, 0, len(rows))
	total := 0.0
	for _, row := range rows {
		// A zero-weight key of a dense distribution is not a share: the Investment page drops
		// non-positive rows too (investment/response.go), so the tile never lists a 0% theme.
		if !units.IsTheme(row.Theme) || row.Effort <= 0 {
			continue
		}
		kept = append(kept, row)
		total += row.Effort
	}
	sort.SliceStable(kept, func(i, j int) bool {
		if kept[i].Effort != kept[j].Effort {
			return kept[i].Effort > kept[j].Effort
		}
		return kept[i].Theme < kept[j].Theme
	})
	out := make([]ReworkThemeAllocation, 0, len(kept))
	for _, row := range kept {
		pct := 0.0
		if total != 0 {
			pct = row.Effort / total * 100.0
		}
		out = append(out, ReworkThemeAllocation{
			Theme:         row.Theme,
			Label:         investmentThemeLabels[row.Theme],
			Allocation:    row.Effort,
			AllocationPct: pct,
			PRsMerged:     int64(math.Round(row.PRs)),
			ChurnLOC:      int64(math.Round(row.ChurnLOC)),
		})
	}
	return out
}

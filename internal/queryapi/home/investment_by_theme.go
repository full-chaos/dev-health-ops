package home

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// fetchInvestmentThemeAllocation is the Home tile "Investment by theme" (CHAOS-9195, D5933;
// the wire field is still rework_theme_allocation): the work over the five canonical themes, in the
// measure investmentexplain.HomeTileMeasure names (ISSUE_SIZE, CHAOS-9624, D5999: lines of code are
// not a measure of effort), that the Jev categorization writes (work_unit_investments), NOT the legacy rule classifier's
// investment_area keys (D5932: that classifier is removed; before this the tile showed two themes
// covering 2.5% of the completed items on real data). It is the Investment page's own read
// (investmentexplain.Reader.FetchInvestmentThemeEffort: the latest row of each unit, every
// categorization status, the same scope and category clauses), with a unit dated by its end day.
//
//   - allocation: the work of the theme in that measure (each unit counts its share of the theme,
//     one unit counts 1 over the themes and repositories it spreads over); allocation_pct its share
//     of the response. A unit's effort_value is not a weight.
//   - churn_loc: the changed lines of code of the units, an explicit measure of its own (D5999),
//     never the weight of allocation.
//   - prs_merged: the pull requests of the units' evidence weighted by the theme share, rounded
//     (the name is the wire's; the evidence of a unit names the pull requests behind it).
//
// DECLARED DIVERGENCE from the reference (which read investment_metrics_daily).
func fetchInvestmentThemeAllocation(ctx context.Context, client QueryClient, f Filters, orgID string, startDay, endDay time.Time, asOf time.Time) ([]ReworkThemeAllocation, error) {
	return fetchThemeAllocationIn(ctx, client, f, orgID, startDay, endDay, asOf, investmentexplain.HomeTileMeasure)
}

// fetchThemeAllocationIn is the tile read in one named measure. Under ISSUE_SIZE it also reads the sizing
// coverage of the same units (Reader.FetchSizingCoverage, the filters of the theme read), so the tile says how
// much of its share is sized work (CHAOS-9624).
func fetchThemeAllocationIn(ctx context.Context, client QueryClient, f Filters, orgID string, startDay, endDay time.Time, asOf time.Time, measure string) ([]ReworkThemeAllocation, error) {
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
		Measure:    measure,
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
	var sizing *investmentexplain.SizingCoverage
	if measure == investmentexplain.MeasureIssueSize {
		coverage, err := reader.FetchSizingCoverage(ctx, filters)
		if err != nil {
			return nil, fmt.Errorf("home: investment by theme sizing: %w", err)
		}
		sizing = &coverage
	}
	slog.DebugContext(ctx, "home: investment by theme", "measure", measure, "themes", len(rows), "sized_read", sizing != nil)
	return themeAllocationFromEffort(rows, measure, sizing), nil
}

// tileMeasureUnit names the measure on the wire: the SankeyValueUnit of the Investment views.
func tileMeasureUnit(measure string) string {
	if measure == investmentexplain.MeasureIssueSize {
		return "ISSUE_SIZE"
	}
	return "WORK_UNITS"
}

// themeAllocationFromEffort rolls the theme rows into the tile: the canonical themes only, the
// largest value first (ties by theme), the share of the total in allocation_pct.
func themeAllocationFromEffort(rows []investmentexplain.ThemeEffortRow, measure string, sizing *investmentexplain.SizingCoverage) []ReworkThemeAllocation {
	kept := make([]investmentexplain.ThemeEffortRow, 0, len(rows))
	total := 0.0
	for _, row := range rows {
		// A zero-weight key of a dense distribution is not a share: the Investment page drops
		// non-positive rows too (investment/response.go), so the tile never lists a 0% theme.
		if !units.IsTheme(row.Theme) || row.Value <= 0 {
			continue
		}
		kept = append(kept, row)
		total += row.Value
	}
	sort.SliceStable(kept, func(i, j int) bool {
		if kept[i].Value != kept[j].Value {
			return kept[i].Value > kept[j].Value
		}
		return kept[i].Theme < kept[j].Theme
	})
	// The sizing coverage is read over the units of the window, one answer for every row; nil unless the
	// measure is ISSUE_SIZE and the window holds a unit.
	var sizingCoverage *float64
	var unsized, units *int64
	if sizing != nil && measure == investmentexplain.MeasureIssueSize {
		sizingCoverage = sizing.Share()
		if u := sizing.Unsized(); u != nil {
			v := int64(*u)
			unsized = &v
		}
		if sizing.Units > 0 {
			v := int64(sizing.Units)
			units = &v
		}
	}
	out := make([]ReworkThemeAllocation, 0, len(kept))
	for _, row := range kept {
		pct := 0.0
		if total != 0 {
			pct = row.Value / total * 100.0
		}
		out = append(out, ReworkThemeAllocation{
			Theme:         row.Theme,
			Label:         investmentThemeLabels[row.Theme],
			Allocation:    row.Value,
			AllocationPct: pct,
			PRsMerged:     int64(math.Round(row.PRs)),
			ChurnLOC:      int64(math.Round(row.ChurnLOC)),

			Measure:          tileMeasureUnit(measure),
			Denominator:      total,
			SizingCoverage:   sizingCoverage,
			UnsizedUnitCount: unsized,
			WorkUnitCount:    units,
		})
	}
	return out
}

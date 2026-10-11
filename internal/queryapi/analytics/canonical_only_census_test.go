package analytics

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-9195 / CHAOS-9196: the CENSUS of the analytics package for the canonical-only dimensions.
//
// Three review rounds found one class of defect three times: a reader that still took the legacy
// path (investment_metrics_daily / investment_area), or kept the evidence-quality read shut, for a
// THEME or SUBCATEGORY request. This file closes the class two ways:
//
//  1. TestCanonicalOnlyCensus_EveryRequestFormEveryDimension: one table over request form x
//     dimension x batch useInvestment (unset / false / true). Each cell asserts the table read
//     (canonical only, never the legacy daily table or its keys) AND the evidence-quality read
//     where the form has a window to read it over. The three P1s of rounds 1 and 2 are named cells.
//  2. TestCanonicalOnlyCensus_EveryRequestFieldHasARow and TestCanonicalOnlyCensus_BranchSites fail when
//     a request field has no row, or when a branch site on the investment flag appears or goes.

type censusClient struct {
	mu         sync.Mutex
	statements []string
}

func (c *censusClient) Query(_ context.Context, statement string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.mu.Lock()
	c.statements = append(c.statements, statement)
	c.mu.Unlock()
	if strings.Contains(statement, "quality_known_count") && !strings.Contains(statement, "evidence_quality_group_key") {
		return &fakeRowScanner{rows: [][]any{{uint64(10), uint64(10), 0.5, 0.1, uint64(0), uint64(5), uint64(5), uint64(0), uint64(0)}}}, nil
	}
	return &fakeRowScanner{}, nil
}

type censusCell struct {
	name string
	// P1 names the review finding this cell pins (empty when it is a plain cell).
	p1 string
	// build returns the batch for a dimension and the batch flag.
	build func(dim model.DimensionInput, flag *bool) model.AnalyticsRequestInput
	// legacyTableAllowed: another request of the same batch (a REPO one) legitimately reads the legacy daily table.
	legacyTableAllowed bool
	// canonicalReads is the least number of statements on work_unit_investments.
	canonicalReads int
	// quality: the evidence-quality stats read is expected (the batch has a timeseries / breakdown window).
	quality bool
	// group: the evidence-quality group read is expected.
	group bool
}

func censusRange() *model.DateRangeInput {
	return &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07")}
}

func censusCells() []censusCell {
	return []censusCell{
		{name: "timeseries", canonicalReads: 1, quality: true,
			build: func(d model.DimensionInput, f *bool) model.AnalyticsRequestInput {
				return model.AnalyticsRequestInput{UseInvestment: f, Timeseries: []model.TimeseriesRequestInput{tsInput(d, model.MeasureInputCount)}}
			}},
		{name: "breakdown", p1: "r1 P1 #2: quality gate on the batch flag", canonicalReads: 1, quality: true,
			build: func(d model.DimensionInput, f *bool) model.AnalyticsRequestInput {
				return model.AnalyticsRequestInput{UseInvestment: f, Breakdowns: []model.BreakdownRequestInput{bdInput(d, model.MeasureInputCount)}}
			}},
		{name: "sankey_with_coverage", p1: "r1 P1 #1: coverage read the legacy table", canonicalReads: 2,
			build: func(d model.DimensionInput, f *bool) model.AnalyticsRequestInput {
				return model.AnalyticsRequestInput{UseInvestment: f, Sankey: &model.SankeyRequestInput{
					Path: []model.DimensionInput{d, model.DimensionInputRepo}, Measure: model.MeasureInputCount,
					DateRange: censusRange(), MaxNodes: 100, MaxEdges: 500}}
			}},
		{name: "flow_matrix_alone", canonicalReads: 1,
			build: func(d model.DimensionInput, f *bool) model.AnalyticsRequestInput {
				return model.AnalyticsRequestInput{UseInvestment: f, FlowMatrix: &model.FlowMatrixRequestInput{
					Dimension: d, Measure: model.MeasureInputCount, DateRange: censusRange(), MaxNodes: 100, MaxEdges: 500}}
			}},
		{name: "flow_matrix_with_window", p1: "r2 P1: THEME flow matrix kept the quality read shut", legacyTableAllowed: true, canonicalReads: 1, quality: true,
			build: func(d model.DimensionInput, f *bool) model.AnalyticsRequestInput {
				return model.AnalyticsRequestInput{UseInvestment: f,
					Timeseries: []model.TimeseriesRequestInput{tsInput(model.DimensionInputRepo, model.MeasureInputCount)},
					FlowMatrix: &model.FlowMatrixRequestInput{Dimension: d, Measure: model.MeasureInputCount, DateRange: censusRange(), MaxNodes: 100, MaxEdges: 500}}
			}},
		{name: "evidence_quality_group_by", p1: "r2 P1: THEME evidenceQualityGroupBy kept the reads shut", legacyTableAllowed: true, quality: true, group: true,
			build: func(d model.DimensionInput, f *bool) model.AnalyticsRequestInput {
				gb := d
				return model.AnalyticsRequestInput{UseInvestment: f, EvidenceQualityGroupBy: &gb,
					Timeseries: []model.TimeseriesRequestInput{tsInput(model.DimensionInputRepo, model.MeasureInputCount)}}
			}},
		{name: "mixed_batch_with_repo_request", legacyTableAllowed: true, canonicalReads: 1, quality: true,
			build: func(d model.DimensionInput, f *bool) model.AnalyticsRequestInput {
				return model.AnalyticsRequestInput{UseInvestment: f, Breakdowns: []model.BreakdownRequestInput{
					bdInput(d, model.MeasureInputCount), bdInput(model.DimensionInputRepo, model.MeasureInputCount)}}
			}},
	}
}

func TestCanonicalOnlyCensus_EveryRequestFormEveryDimension(t *testing.T) {
	flags := map[string]*bool{"unset": nil, "false": boolPtr(false), "true": boolPtr(true)}
	for _, cell := range censusCells() {
		for _, dim := range []model.DimensionInput{model.DimensionInputTheme, model.DimensionInputSubcategory} {
			for flagName, flag := range flags {
				name := cell.name + "/" + string(dim) + "/useInvestment_" + flagName
				t.Run(name, func(t *testing.T) {
					client := &censusClient{}
					result, err := ResolveSelected(context.Background(), client, "org-1", cell.build(dim, flag), EverythingSelected())
					if err != nil {
						t.Fatalf("%s (%s): %v", name, cell.p1, err)
					}
					canonical, statsReads, groupReads := 0, 0, 0
					for _, s := range client.statements {
						if strings.Contains(s, "work_unit_investments") {
							canonical++
						}
						if strings.Contains(s, "quality_known_count") {
							if strings.Contains(s, "evidence_quality_group_key") {
								groupReads++
							} else {
								statsReads++
							}
						}
						if strings.Contains(s, "evidence_quality_group_key") && (!strings.Contains(s, "work_unit_investments") || strings.Contains(s, "work_item_completion_daily")) {
							t.Errorf("%s (%s): the evidence-quality group statement must read only the canonical categorization:\n%s", name, cell.p1, s)
						}
						if !cell.legacyTableAllowed && strings.Contains(s, "work_item_completion_daily") {
							t.Errorf("%s (%s): a theme or subcategory statement reads the daily completion table:\n%s", name, cell.p1, s)
						}
						// The retired table and the rule classifier's keys are read by NO statement of any cell (CHAOS-9264).
						if strings.Contains(s, "investment_metrics_daily") || strings.Contains(s, "investment_area") || strings.Contains(s, "project_stream") {
							t.Errorf("%s (%s): a statement reads the retired investment_metrics_daily table or a classifier key:\n%s", name, cell.p1, s)
						}
					}
					if canonical < cell.canonicalReads {
						t.Errorf("%s (%s): canonical reads = %d, want >= %d; statements=%d", name, cell.p1, canonical, cell.canonicalReads, len(client.statements))
					}
					if cell.quality && (statsReads == 0 || result.EvidenceQualityStats == nil) {
						t.Errorf("%s (%s): evidence-quality stats missing (reads=%d, result nil=%v)", name, cell.p1, statsReads, result.EvidenceQualityStats == nil)
					}
					if !cell.quality && statsReads != 0 {
						t.Errorf("%s: unexpected evidence-quality stats read (the form has no window)", name)
					}
					if cell.group && groupReads == 0 {
						t.Errorf("%s (%s): evidence-quality group read missing", name, cell.p1)
					}
				})
			}
		}
	}
}

// Every request-bearing field of the batch input has a row above (or is named as not a request).
func TestCanonicalOnlyCensus_EveryRequestFieldHasARow(t *testing.T) {
	covered := map[string]string{
		"Timeseries":             "cell timeseries, mixed_batch_with_repo_request, *_with_window",
		"Breakdowns":             "cell breakdown, mixed_batch_with_repo_request",
		"Sankey":                 "cell sankey_with_coverage",
		"FlowMatrix":             "cells flow_matrix_alone, flow_matrix_with_window",
		"UseInvestment":          "the flag axis of every cell",
		"EvidenceQualityGroupBy": "cell evidence_quality_group_by",
		"Filters":                "not a dimension: the work-category filter on the non-investment path is a named limit of CHAOS-9196",
	}
	typ := reflect.TypeOf(model.AnalyticsRequestInput{})
	var missing []string
	for i := 0; i < typ.NumField(); i++ {
		if _, ok := covered[typ.Field(i).Name]; !ok {
			missing = append(missing, typ.Field(i).Name)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("AnalyticsRequestInput has fields with no census row: %v: add a cell to censusCells", missing)
	}
	if len(covered) != typ.NumField() {
		t.Fatalf("census names %d fields, the input has %d: a field was removed, update the census", len(covered), typ.NumField())
	}
}

// The branch sites on the investment flag in the production source of this package, counted per file.
// A new site (or a removed one) fails here: add a cell to censusCells, then update this table.
func TestCanonicalOnlyCensus_BranchSites(t *testing.T) {
	site := regexp.MustCompile(`\b(useInvestment|UseInvestment|coverageUseInvestment|qualityInvestment)\b|canonicalOnlyDimension\(|resolveUseInvestment\(|pathAutoRoutesToInvestment\(|dbColumn\(`)
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(".", n))
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for _, line := range strings.Split(string(raw), "\n") {
			trim := strings.TrimSpace(line)
			if strings.HasPrefix(trim, "//") {
				continue
			}
			if site.MatchString(line) {
				count++
			}
		}
		if count > 0 {
			got[n] = count
		}
	}
	var names []string
	for n := range got {
		names = append(names, n)
	}
	sort.Strings(names)
	var report []string
	for _, n := range names {
		report = append(report, fmt.Sprintf("%s=%d", n, got[n]))
	}
	t.Logf("branch sites: %s", strings.Join(report, " "))
	if pinned := censusPinnedSites(); !reflect.DeepEqual(got, pinned) {
		t.Fatalf("the branch sites on the investment flag changed.\n got: %v\nwant: %v\nA new site must get a cell in censusCells (request form x dimension), then this table is updated.", got, pinned)
	}
}

// censusPinnedSites is the pinned count per file (comment lines excluded). Roles: resolve.go routes each
// request and the quality gate; investmentquality.go holds the two quality gates; validate.go's dbColumn is
// the one place a dimension maps to a column (the legacy branch holds no theme column); the compile files
// take the flag from the resolver; sankeycoverage.go carries the coverage flag.
func censusPinnedSites() map[string]int {
	return map[string]int{
		"breakdown.go": 6, "catalog.go": 5, "filtertranslation.go": 13, "flowmatrix.go": 9, "investment.go": 5,
		"investmentquality.go": 4, "resolve.go": 33, "sankey.go": 5, "sankeycoverage.go": 19, "sankeygrouped.go": 6,
		"timeseries.go": 4, "validate.go": 5,
	}
}

// The catalog is the one analytics surface outside the batch input: THEME and SUBCATEGORY list their
// values from the canonical categorization, with and without a filter.
func TestCanonicalOnlyCensus_Catalog(t *testing.T) {
	for _, dim := range []model.DimensionInput{model.DimensionInputTheme, model.DimensionInputSubcategory} {
		client := &censusClient{}
		d := dim
		if _, _, err := ResolveCatalog(context.Background(), client, "org-1", &d, nil); err != nil {
			t.Fatalf("%s: %v", dim, err)
		}
		canonical := 0
		for _, s := range client.statements {
			if strings.Contains(s, "work_unit_investments") {
				canonical++
			}
			if strings.Contains(s, "investment_metrics_daily") || strings.Contains(s, "investment_area") || strings.Contains(s, "project_stream") {
				t.Errorf("%s: the catalog read a legacy key:\n%s", dim, s)
			}
		}
		if canonical == 0 {
			t.Errorf("%s: the catalog did not read the canonical categorization; statements=%d", dim, len(client.statements))
		}
	}
}

// CHAOS-9221 / D5947: a TEAM scope is the team's OWNED repositories (the ONE shared teamscope.RepoCondition, read
// over the unit's repository rows), in every analytics theme form: never the work-item attribution vote (`ut` /
// unit_team). One cell per request form and dimension; a cell that reads the vote, or reads no ownership, fails.
func TestCanonicalOnlyCensus_TeamScopeIsTheOwnedRepositories(t *testing.T) {
	team := &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{"team-a"}}}
	for _, cell := range censusCells() {
		for _, dim := range []model.DimensionInput{model.DimensionInputTheme, model.DimensionInputSubcategory} {
			name := cell.name + "/" + string(dim)
			t.Run(name, func(t *testing.T) {
				client := &censusClient{}
				batch := cell.build(dim, nil)
				batch.Filters = team
				if _, err := ResolveSelected(context.Background(), client, "org-1", batch, EverythingSelected()); err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				owned := 0
				for _, s := range client.statements {
					// The sankey COVERAGE statement measures how much effort has an attributed team (AS assigned_team): the
					// vote is the measurement there, a named limit (the coverage definition moves with CHAOS-9221's follow-up);
					// its team SCOPE is still the owned repositories (asserted below).
					if !strings.Contains(s, "AS assigned_team") && (strings.Contains(s, "unit_team") || strings.Contains(s, "ut.team_label") || strings.Contains(s, "ut.team_id")) {
						t.Errorf("%s: a statement scopes the team by the attribution vote:\n%s", name, s)
					}
					if strings.Contains(s, "team_repo_ownership") && strings.Contains(s, "work_unit_repo_effort") {
						owned++
					}
				}
				if owned == 0 {
					t.Errorf("%s: no statement scopes the team by its owned repositories over the unit's repository rows (statements=%d)", name, len(client.statements))
				}
			})
		}
	}
}

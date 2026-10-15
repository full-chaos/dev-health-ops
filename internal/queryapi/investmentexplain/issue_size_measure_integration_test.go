//go:build integration

package investmentexplain

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

// issueSizeSeed is the frozen parameter set of the work units below: the same rows on every run.
const issueSizeSeed = 9619

type issueSizeUnit struct {
	id            string
	provider      string
	from, to      time.Time
	subcategories map[string]float64
	weight        float64
	sized         bool
	repoWeights   map[uuid.UUID]float64
}

// TestIssueSizeMeasureWeighsEachUnitByItsIssueSize pins CHAOS-9619 (D5999) against a real ClickHouse: in the
// ISSUE_SIZE measure every work unit of every provider weighs its newest issue size, 1 when it has none (no size
// row, or size 0), never its lines of code; the shares conserve the unit's weight across subcategories and across
// the repositories it is allocated to; windows dated by their end add up; the Investment page and the explanation
// read the same numbers; and a scope with no size reads exactly the work-unit count.
func TestIssueSizeMeasureWeighsEachUnitByItsIssueSize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := newInvestmentExplainTestClickHouse(ctx, t)
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}

	const org = "investmentexplain-issue-size-org"
	const unsizedOrg = "investmentexplain-issue-size-unsized-org"
	rng := rand.New(rand.NewSource(issueSizeSeed))
	computed := time.Date(2026, 10, 1, 6, 0, 0, 0, time.UTC)
	windowStart := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	windowMid := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	repos := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	providers := []string{"jira", "gitlab", "github", "linear"}
	subcategories := units.SortedSubcategories[:]

	exec := func(statement string, args ...any) {
		t.Helper()
		if err := admin.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	var seeded []issueSizeUnit
	seed := func(orgID string, withSizes bool) []issueSizeUnit {
		var out []issueSizeUnit
		for i := 0; i < 48; i++ {
			first := subcategories[rng.Intn(len(subcategories))]
			second := subcategories[rng.Intn(len(subcategories))]
			share := math.Round(rng.Float64()*100) / 100
			distribution := map[string]float64{first: share}
			distribution[second] += 1 - share
			themes := map[string]float64{}
			for key, value := range distribution {
				themes[units.ThemeOf(key)] += value
			}
			to := windowStart.Add(time.Duration(1+rng.Intn(29*24)) * time.Hour)
			from := to.Add(-time.Duration(rng.Intn(72)) * time.Hour)
			unit := issueSizeUnit{
				id: fmt.Sprintf("%s-wu-%02d", orgID, i), provider: providers[i%len(providers)],
				from: from, to: to, subcategories: distribution, weight: 1, repoWeights: map[uuid.UUID]float64{},
			}
			// Lines of code: large and unrelated to the size, so a read that weighs by them is visibly wrong.
			loc := float64(1 + rng.Intn(5000))
			exec(`INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, org_id)
				VALUES (?, ?, ?, NULL, ?, 'churn_loc', ?, ?, ?, '{}', 0.7, 'moderate', 'ok', '{}', 'v1', 'hash', 'run-1', ?, ?)`,
				unit.id, from, to, unit.provider, loc, themes, distribution, computed, orgID)
			// One or two repository rows, allocated by their effort, the way the materializer writes them.
			repoA, repoB := repos[rng.Intn(len(repos))], repos[rng.Intn(len(repos))]
			efforts := map[uuid.UUID]float64{repoA: float64(1 + rng.Intn(100))}
			if repoB != repoA && i%2 == 0 {
				efforts[repoB] = float64(1 + rng.Intn(100))
			}
			sum := 0.0
			for _, effort := range efforts {
				sum += effort
			}
			for repo, effort := range efforts {
				unit.repoWeights[repo] = effort / sum
				exec(`INSERT INTO work_unit_repo_effort (work_unit_id, repo_id, effort_metric, effort_value, allocation_weight, allocation_source, categorization_run_id, computed_at, org_id) VALUES (?, ?, 'churn_loc', ?, ?, 'pr_refs', 'run-1', ?, ?)`,
					unit.id, repo, effort, effort/sum, computed, orgID)
			}
			if withSizes {
				switch i % 5 {
				case 0: // no size row: unsized
				case 1: // a size row of 0: unsized
					exec(`INSERT INTO work_unit_issue_size (org_id, work_unit_id, issue_size, sized_issue_count, issue_count, categorization_run_id, computed_at) VALUES (?, ?, 0, 0, 2, 'run-1', ?)`, orgID, unit.id, computed)
				case 2, 4:
					unit.weight, unit.sized = float64(1+rng.Intn(8)), true
					exec(`INSERT INTO work_unit_issue_size (org_id, work_unit_id, issue_size, sized_issue_count, issue_count, categorization_run_id, computed_at) VALUES (?, ?, ?, 1, 1, 'run-1', ?)`, orgID, unit.id, unit.weight, computed)
				case 3: // an older size row and a newer one: the newest wins
					unit.weight, unit.sized = float64(1+rng.Intn(5)), true
					exec(`INSERT INTO work_unit_issue_size (org_id, work_unit_id, issue_size, sized_issue_count, issue_count, categorization_run_id, computed_at) VALUES (?, ?, 13, 1, 1, 'run-0', ?)`, orgID, unit.id, computed.Add(-48*time.Hour))
					exec(`INSERT INTO work_unit_issue_size (org_id, work_unit_id, issue_size, sized_issue_count, issue_count, categorization_run_id, computed_at) VALUES (?, ?, ?, 1, 1, 'run-1', ?)`, orgID, unit.id, unit.weight, computed)
				}
			}
			out = append(out, unit)
		}
		return out
	}
	seeded = seed(org, true)
	unsized := seed(unsizedOrg, false)

	inWindow := func(u issueSizeUnit, start, end time.Time, byEnd bool) bool {
		if byEnd {
			return !u.to.Before(start) && u.to.Before(end)
		}
		return u.from.Before(end) && !u.to.Before(start)
	}
	type expectation struct {
		subcategory map[string]float64
		total       float64
	}
	expect := func(list []issueSizeUnit, start, end time.Time, byEnd bool, repo *uuid.UUID, sized bool) expectation {
		want := expectation{subcategory: map[string]float64{}}
		for _, u := range list {
			if !inWindow(u, start, end, byEnd) {
				continue
			}
			weight := 1.0
			if sized {
				weight = u.weight
			}
			if repo != nil {
				weight *= u.repoWeights[*repo]
			}
			for key, share := range u.subcategories {
				// The reader narrows the stored share to Float32 before it sums (FetchInvestmentBreakdown).
				value := float64(float32(share)) * weight
				want.subcategory[key] += value
				want.total += value
			}
		}
		return want
	}
	read := func(orgID, measure string, start, end time.Time, byEnd bool, repo *uuid.UUID) expectation {
		filters := BreakdownFilters{OrgID: orgID, StartTS: start, EndTS: end, DatedByEnd: byEnd, Measure: measure}
		if repo != nil {
			filters.RepoIDs = []string{repo.String()}
			filters.ReposNamed = true
		}
		rows, err := reader.FetchInvestmentBreakdown(ctx, filters)
		if err != nil {
			t.Fatalf("FetchInvestmentBreakdown %s: %v", measure, err)
		}
		got := expectation{subcategory: map[string]float64{}}
		for _, row := range rows {
			got.subcategory[row.Subcategory] += row.Value
			got.total += row.Value
		}
		return got
	}
	same := func(label string, got, want expectation) {
		t.Helper()
		if math.Abs(got.total-want.total) > 1e-6 {
			t.Errorf("%s: total %v, want %v", label, got.total, want.total)
		}
		for key, value := range want.subcategory {
			if math.Abs(got.subcategory[key]-value) > 1e-6 {
				t.Errorf("%s: %s = %v, want %v", label, key, got.subcategory[key], value)
			}
		}
		for key := range got.subcategory {
			if _, ok := want.subcategory[key]; !ok && got.subcategory[key] != 0 {
				t.Errorf("%s: unexpected subcategory %s = %v", label, key, got.subcategory[key])
			}
		}
	}

	// Each unit weighs its size, 1 when unsized; the shares conserve the weight (total = sum of the unit weights).
	sizedRead := read(org, MeasureIssueSize, windowStart, windowEnd, false, nil)
	same("ISSUE_SIZE org", sizedRead, expect(seeded, windowStart, windowEnd, false, nil, true))
	countRead := read(org, MeasureCount, windowStart, windowEnd, false, nil)
	same("COUNT org", countRead, expect(seeded, windowStart, windowEnd, false, nil, false))
	if math.Abs(sizedRead.total-countRead.total) < 1 {
		t.Fatalf("the seeded sizes do not move the measure (ISSUE_SIZE %v, COUNT %v); the test proves nothing", sizedRead.total, countRead.total)
	}
	weights := 0.0
	providersSeen := map[string]bool{}
	for _, u := range seeded {
		if inWindow(u, windowStart, windowEnd, false) {
			weights += u.weight
			providersSeen[u.provider] = true
		}
	}
	if math.Abs(sizedRead.total-weights) > 1e-4 {
		t.Errorf("share conservation: the ISSUE_SIZE total %v is not the sum of the unit weights %v", sizedRead.total, weights)
	}
	if len(providersSeen) != len(providers) {
		t.Fatalf("the window holds units of %d providers, want all %d", len(providersSeen), len(providers))
	}

	// A repository scope reads each unit's allocated share of its weight; over every repository the shares
	// add back to the unit weights.
	acrossRepos := 0.0
	for i := range repos {
		repo := repos[i]
		got := read(org, MeasureIssueSize, windowStart, windowEnd, false, &repo)
		same(fmt.Sprintf("ISSUE_SIZE repo %d", i), got, expect(seeded, windowStart, windowEnd, false, &repo, true))
		acrossRepos += got.total
	}
	if math.Abs(acrossRepos-sizedRead.total) > 1e-4 {
		t.Errorf("the repository scopes add up to %v, the organization reads %v", acrossRepos, sizedRead.total)
	}

	// Windows dated by their end are additive (D6022): one window per unit, no pro rata.
	first := read(org, MeasureIssueSize, windowStart, windowMid, true, nil)
	second := read(org, MeasureIssueSize, windowMid, windowEnd, true, nil)
	whole := read(org, MeasureIssueSize, windowStart, windowEnd, true, nil)
	same("ISSUE_SIZE by end, whole window", whole, expect(seeded, windowStart, windowEnd, true, nil, true))
	if math.Abs(first.total+second.total-whole.total) > 1e-6 || first.total == 0 || second.total == 0 {
		t.Errorf("by-end windows: %v + %v, the whole window reads %v", first.total, second.total, whole.total)
	}

	// With no size in scope the measure is the work-unit count, subcategory by subcategory.
	same("ISSUE_SIZE with no size equals COUNT", read(unsizedOrg, MeasureIssueSize, windowStart, windowEnd, false, nil), read(unsizedOrg, MeasureCount, windowStart, windowEnd, false, nil))
	same("ISSUE_SIZE with no size", read(unsizedOrg, MeasureIssueSize, windowStart, windowEnd, false, nil), expect(unsized, windowStart, windowEnd, false, nil, false))

	// The Investment page reads the same numbers in ISSUE_SIZE, and its coverage names the sized share.
	useInvestment := true
	dateRange := &model.DateRangeInput{StartDate: graphqldate.New(windowStart), EndDate: graphqldate.New(windowEnd)}
	result, err := analytics.Resolve(ctx, client, org, model.AnalyticsRequestInput{
		UseInvestment: &useInvestment,
		Filters:       &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{}}},
		Breakdowns: []model.BreakdownRequestInput{
			{Dimension: model.DimensionInputSubcategory, Measure: model.MeasureInputIssueSize, TopN: 100, DateRange: dateRange},
			{Dimension: model.DimensionInputRepo, Measure: model.MeasureInputIssueSize, TopN: 100, DateRange: dateRange},
		},
		Sankey: &model.SankeyRequestInput{
			Path: []model.DimensionInput{model.DimensionInputTheme, model.DimensionInputSubcategory}, Measure: model.MeasureInputIssueSize,
			DateRange: dateRange, MaxNodes: 100, MaxEdges: 500,
		},
	})
	if err != nil {
		t.Fatalf("page: %v", err)
	}
	page := expectation{subcategory: map[string]float64{}}
	for _, item := range result.Breakdowns[0].Items {
		if item.Value != nil {
			page.subcategory[item.Key] = *item.Value
			page.total += *item.Value
		}
	}
	same("page ISSUE_SIZE", page, sizedRead)
	// By repository the page reads each unit's allocated share of its size; the repositories add back to the total.
	byRepo := 0.0
	for _, item := range result.Breakdowns[1].Items {
		if item.Value != nil {
			byRepo += *item.Value
		}
	}
	if math.Abs(byRepo-sizedRead.total) > 1e-4 {
		t.Errorf("page ISSUE_SIZE by repository adds up to %v, the organization reads %v", byRepo, sizedRead.total)
	}
	if result.Sankey == nil || result.Sankey.Unit != model.SankeyValueUnitIssueSize {
		t.Fatalf("sankey = %+v, want the ISSUE_SIZE unit", result.Sankey)
	}
	coverage := result.Sankey.Coverage
	if coverage == nil || coverage.SizingCoverage == nil || coverage.UnsizedUnitCount == nil {
		t.Fatalf("sankey coverage = %+v, want the sizing fields", coverage)
	}
	var windowUnits, sizedUnits int
	for _, u := range seeded {
		if inWindow(u, windowStart, windowEnd, false) {
			windowUnits++
			if u.sized {
				sizedUnits++
			}
		}
	}
	if *coverage.UnsizedUnitCount != windowUnits-sizedUnits || math.Abs(*coverage.SizingCoverage-float64(sizedUnits)/float64(windowUnits)) > 1e-9 {
		t.Errorf("sizing coverage = (%v, %d unsized), want (%v, %d unsized)", *coverage.SizingCoverage, *coverage.UnsizedUnitCount, float64(sizedUnits)/float64(windowUnits), windowUnits-sizedUnits)
	}

	// The explanation in ISSUE_SIZE: every finding carries the share of the size measure and the sizing coverage.
	completion, err := json.Marshal(map[string]any{
		"summary": "Work appears spread across the mix.",
		"top_findings": []any{map[string]any{
			"finding": "this work appears in the mix",
			"evidence": map[string]any{
				"theme": units.ThemeOf(subcategories[0]), "subcategory": nil, "share_pct": 1.0,
				"delta_pct_points": nil, "evidence_quality_mean": nil, "evidence_quality_band": nil,
			},
		}},
		"confidence": map[string]any{
			"level": "moderate", "quality_mean": 0.55, "quality_stddev": 0.25,
			"band_mix": map[string]any{"high": 1, "moderate": 0, "low": 1, "very_low": 0, "unknown": 0}, "drivers": []any{},
		},
		"what_to_check_next": []any{map[string]any{"action": "Open the evidence", "why": "It shows the mix", "where": "Evidence panel"}},
		"anti_claims":        []any{"This does not rank people."},
	})
	if err != nil {
		t.Fatal(err)
	}
	var calls int
	explanation, err := reader.ExplainInvestmentMix(ctx, nil, alwaysAvailable, countingComplete(string(completion), &calls), ExplainInvestmentMixOptions{
		OrgID: org, StartTS: windowStart, EndTS: windowEnd, ScopeLevel: "org", Measure: MeasureIssueSize,
		LLMProvider: "openai", Now: explainFixtureNow, FiltersForCacheKey: defaultMetricFilterForCacheKey(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(explanation.TopFindings) == 0 {
		t.Fatal("the explanation has no finding")
	}
	for _, finding := range explanation.TopFindings {
		evidence := finding.Evidence
		if evidence.Measure != MeasureIssueSize || evidence.Denominator == nil || math.Abs(*evidence.Denominator-sizedRead.total) > 1e-3 {
			t.Errorf("finding %s: measure %q, denominator %v; want ISSUE_SIZE over %v", evidence.Theme, evidence.Measure, evidence.Denominator, sizedRead.total)
		}
		if evidence.SizingCoverage == nil || evidence.UnsizedUnitCount == nil ||
			math.Abs(*evidence.SizingCoverage-float64(sizedUnits)/float64(windowUnits)) > 1e-9 || *evidence.UnsizedUnitCount != windowUnits-sizedUnits {
			t.Errorf("finding %s: sizing (%v, %v), want (%v, %d)", evidence.Theme, evidence.SizingCoverage, evidence.UnsizedUnitCount, float64(sizedUnits)/float64(windowUnits), windowUnits-sizedUnits)
		}
	}
}

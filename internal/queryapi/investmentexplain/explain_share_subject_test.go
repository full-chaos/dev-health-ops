package investmentexplain

import (
	"strings"
	"testing"
)

const shareSubjectConfidence = `"confidence": {"level": "moderate", "quality_mean": null, "quality_stddev": null, "band_mix": {"high": 0, "moderate": 0, "low": 0, "very_low": 0, "unknown": 0}, "drivers": []}, "what_to_check_next": [], "anti_claims": []`

func shareSubjectCompletion(finding, theme, subcategory string) string {
	return `{"summary": "A summary.", "top_findings": [{"finding": "` + finding + `", "evidence": {"theme": "` + theme +
		`", "subcategory": ` + subcategory + `, "share_pct": 0, "delta_pct_points": null, "evidence_quality_mean": null, "evidence_quality_band": null}}], ` +
		shareSubjectConfidence + `}`
}

func shareSubjectOptions() ParseOptions {
	return ParseOptions{
		ThemeSharesPct: map[string]float64{"quality": 44.6, "feature_delivery": 18.0, "maintenance": 16.0},
		SubcategorySharesPct: map[string]float64{
			"quality.bugfix": 32.0, "quality.testing": 9.0, "quality.reliability": 3.0,
			"feature_delivery.roadmap": 16.0, "feature_delivery.enablement": 2.0,
			"maintenance.debt": 9.0,
		},
		ShareNoun: "work",
	}
}

func TestParseFindingRepairsAThemeShareOnASubcategorySentence(t *testing.T) {
	cases := []struct {
		name            string
		finding         string
		theme           string
		wantSubcategory string
		wantSuffix      string
		wantShare       float64
	}{
		{"bugfix under quality", "Quality / Reliability leads the mix, with Bugfix the largest listed subcategory", "quality", "quality.bugfix", "(~32% of work).", 32.0},
		{"roadmap under feature delivery", "Feature Delivery is the second-largest theme, with Roadmap its largest listed subcategory", "feature_delivery", "feature_delivery.roadmap", "(~16% of work).", 16.0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := ParseInvestmentMixResponse(shareSubjectCompletion(tc.finding, tc.theme, "null"), shareSubjectOptions())
			if result.Output == nil {
				t.Fatalf("rejected: rule=%s path=%s", result.ReasonRule, result.ReasonPath)
			}
			got := result.Output.TopFindings[0]
			if got.Evidence.Subcategory == nil || *got.Evidence.Subcategory != tc.wantSubcategory {
				t.Fatalf("evidence subcategory = %v, want %s", got.Evidence.Subcategory, tc.wantSubcategory)
			}
			if got.Evidence.SharePct != tc.wantShare {
				t.Fatalf("share = %v, want the subcategory's %v", got.Evidence.SharePct, tc.wantShare)
			}
			if !strings.HasSuffix(got.Finding, tc.wantSuffix) {
				t.Fatalf("finding %q does not end with %q", got.Finding, tc.wantSuffix)
			}
			if result.RepairedFindings != 1 {
				t.Fatalf("repaired findings = %d, want 1", result.RepairedFindings)
			}
		})
	}
}

func TestParseFindingNamingSeveralSubcategoriesKeepsNoPercent(t *testing.T) {
	result := ParseInvestmentMixResponse(
		shareSubjectCompletion("Quality / Reliability appears split between Bugfix and Testing work", "quality", "null"), shareSubjectOptions())
	if result.Output == nil {
		t.Fatalf("rejected: rule=%s", result.ReasonRule)
	}
	got := result.Output.TopFindings[0]
	if strings.Contains(got.Finding, "%") || got.Evidence.Subcategory != nil {
		t.Fatalf("finding %q subcategory %v: want no percent and no subcategory", got.Finding, got.Evidence.Subcategory)
	}
	if result.RepairedFindings != 0 {
		t.Fatalf("repaired findings = %d, want 0", result.RepairedFindings)
	}
}

func TestParseFindingKeepsTheThemeShareWhenTheSentenceNamesOnlyTheTheme(t *testing.T) {
	cases := []struct {
		name    string
		finding string
		theme   string
		suffix  string
	}{
		{"theme only", "Quality / Reliability leads the mix", "quality", "(~45% of work)."},
		{"theme label holds a subcategory word", "Maintenance / Tech Debt appears substantial", "maintenance", "(~16% of work)."},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := ParseInvestmentMixResponse(shareSubjectCompletion(tc.finding, tc.theme, "null"), shareSubjectOptions())
			if result.Output == nil {
				t.Fatalf("rejected: rule=%s", result.ReasonRule)
			}
			got := result.Output.TopFindings[0]
			if !strings.HasSuffix(got.Finding, tc.suffix) || got.Evidence.Subcategory != nil || result.RepairedFindings != 0 {
				t.Fatalf("finding %q subcategory %v repaired %d", got.Finding, got.Evidence.Subcategory, result.RepairedFindings)
			}
		})
	}
}

func TestParseFindingKeepsAnExplicitSubcategory(t *testing.T) {
	result := ParseInvestmentMixResponse(
		shareSubjectCompletion("Bugfix leans to the largest listed subcategory", "quality", `"quality.bugfix"`), shareSubjectOptions())
	if result.Output == nil || result.RepairedFindings != 0 || !strings.HasSuffix(result.Output.TopFindings[0].Finding, "(~32% of work).") {
		t.Fatalf("explicit subcategory changed: %+v", result)
	}
}

func TestMixCacheKeyCarriesTheParserVersion(t *testing.T) {
	themes, subcategories := newOrderedFloatMap(), newOrderedFloatMap()
	themes.Add("quality", 10)
	subcategories.Add("quality.bugfix", 10)
	filters, ok := mixCacheFilters(ExplainInvestmentMixOptions{OrgID: "o"}, themes, subcategories, "v1/1").(map[string]any)
	if !ok || filters["parser"] != explainParserVersion {
		t.Fatalf("mix cache filters = %#v, want parser %q", filters, explainParserVersion)
	}
	withParser, err := ComputeCacheKey(CacheKeyInput{Filters: filters, OrgID: "o"})
	if err != nil {
		t.Fatal(err)
	}
	delete(filters, "parser")
	withoutParser, err := ComputeCacheKey(CacheKeyInput{Filters: filters, OrgID: "o"})
	if err != nil {
		t.Fatal(err)
	}
	if withParser == withoutParser {
		t.Fatal("an explanation cached before the parser version joined the key is still served")
	}
}

func TestParseFindingKeepsTheThemeShareWhenAThemeLabelPartIsAlsoASubcategory(t *testing.T) {
	cases := []struct {
		name    string
		finding string
		theme   string
		suffix  string
	}{
		{"reliability shorthand for the quality theme", "The Reliability theme remains steady", "quality", "(~45% of work)."},
		{"tech debt shorthand for the maintenance theme", "The Tech Debt theme remains steady", "maintenance", "(~16% of work)."},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := ParseInvestmentMixResponse(shareSubjectCompletion(tc.finding, tc.theme, "null"), shareSubjectOptions())
			if result.Output == nil {
				t.Fatalf("rejected: rule=%s", result.ReasonRule)
			}
			got := result.Output.TopFindings[0]
			if !strings.HasSuffix(got.Finding, tc.suffix) || got.Evidence.Subcategory != nil || result.RepairedFindings != 0 {
				t.Fatalf("finding %q subcategory %v repaired %d: want the theme share, no repair", got.Finding, got.Evidence.Subcategory, result.RepairedFindings)
			}
		})
	}
}

package investmentexplain

import (
	"context"
	"math"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

// statementRecorder keeps the statement of the breakdown read.
type statementRecorder struct {
	mixAgreementClient
	breakdownStatement *string
}

func (c statementRecorder) Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	if strings.Contains(statement, "subcategory_kv.1 AS subcategory") {
		*c.breakdownStatement = statement
	}
	return c.mixAgreementClient.Query(ctx, statement, bindings)
}

func TestExplainReadsTheMeasureOfTheRequest(t *testing.T) {
	cases := []struct {
		measure string
		want    string
		notWant string
	}{
		{"COUNT", "sum(subcategory_kv.2 * 1.0 * 1.0)", "subcategory_kv.2 * effort_value"},
		{"THROUGHPUT", "sum(subcategory_kv.2 * 1.0 * 1.0)", "subcategory_kv.2 * effort_value"},
		{"", "sum(subcategory_kv.2 * 1.0 * 1.0 * unit_size_weight)", "subcategory_kv.2 * effort_value"},
		{"ISSUE_SIZE", "sum(subcategory_kv.2 * 1.0 * 1.0 * unit_size_weight)", "subcategory_kv.2 * effort_value"},
	}
	for _, tc := range cases {
		t.Run("measure "+tc.measure, func(t *testing.T) {
			var statement string
			reader, err := NewReader(statementRecorder{mixAgreementClient: mixAgreementClient{breakdown: mixRows()}, breakdownStatement: &statement})
			if err != nil {
				t.Fatal(err)
			}
			opts := explainOpts()
			opts.Measure = tc.measure
			var calls int
			if _, err := reader.ExplainInvestmentMix(context.Background(), nil, alwaysAvailable, countingComplete(completionWithKeys(64), &calls), opts); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(statement, tc.want) {
				t.Errorf("breakdown statement lacks %q:\n%s", tc.want, statement)
			}
			if tc.notWant != "" && strings.Contains(statement, tc.notWant) {
				t.Errorf("breakdown statement holds %q for measure %s:\n%s", tc.notWant, tc.measure, statement)
			}
		})
	}
}

func TestExplainNeverWeighsByEffortValue(t *testing.T) {
	for _, measure := range []string{"", "COUNT", "THROUGHPUT", "EFFORT", "bogus"} {
		var statement string
		reader, err := NewReader(statementRecorder{mixAgreementClient: mixAgreementClient{breakdown: mixRows()}, breakdownStatement: &statement})
		if err != nil {
			t.Fatal(err)
		}
		opts := explainOpts()
		opts.Measure = measure
		var calls int
		if _, err := reader.ExplainInvestmentMix(context.Background(), nil, alwaysAvailable, countingComplete(completionWithKeys(64), &calls), opts); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(statement, "subcategory_kv.2 * effort_value") || strings.Contains(statement, "'churn_loc'") {
			t.Errorf("measure %q reads effort_value or churn_loc:\n%s", measure, statement)
		}
	}
}

func TestExplainEvidenceCarriesMeasureNumeratorAndDenominator(t *testing.T) {
	reader, err := NewReader(mixAgreementClient{breakdown: mixRows()})
	if err != nil {
		t.Fatal(err)
	}
	opts := explainOpts()
	opts.Measure = "COUNT"
	var calls int
	got, err := reader.ExplainInvestmentMix(context.Background(), nil, alwaysAvailable, countingComplete(completionWithKeys(64), &calls), opts)
	if err != nil {
		t.Fatal(err)
	}
	evidence := got.TopFindings[0].Evidence
	if evidence.Measure != "COUNT" || evidence.Numerator == nil || evidence.Denominator == nil {
		t.Fatalf("evidence = %+v, want measure COUNT with numerator and denominator", evidence)
	}
	if *evidence.Numerator != 10.0 || math.Abs(*evidence.Denominator-100.0) > 1e-9 {
		t.Errorf("numerator/denominator = %v/%v, want 10/100 (feature_delivery.roadmap over the theme total)", *evidence.Numerator, *evidence.Denominator)
	}
	if want := roundOneDecimal(*evidence.Numerator / *evidence.Denominator * 100); evidence.SharePct != want {
		t.Errorf("share_pct = %v, want numerator/denominator = %v", evidence.SharePct, want)
	}
	if !strings.Contains(got.TopFindings[0].Finding, "~10% of work") {
		t.Errorf("finding = %q, want the share in the noun of the COUNT measure", got.TopFindings[0].Finding)
	}
}

func TestExplainCacheKeyCarriesTheMeasure(t *testing.T) {
	themes, subcategories := buildInvestmentBreakdownDistributions([]BreakdownRow{{Subcategory: "quality.bugfix", Theme: "quality", Value: 5}})
	key := func(measure string) string {
		opts := explainOpts()
		opts.Measure = measure
		k, err := ComputeCacheKey(CacheKeyInput{Filters: mixCacheFilters(opts, themes, subcategories, "v1/4"), OrgID: "o"})
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	if key("COUNT") != key("THROUGHPUT") {
		t.Error("THROUGHPUT is COUNT on the investment path and must share its key")
	}
}

func TestExplainPromptNamesTheTotalAfterTheMeasure(t *testing.T) {
	for measure, want := range map[string]string{"COUNT": `"total_work"`} {
		reader, err := NewReader(mixAgreementClient{breakdown: mixRows()})
		if err != nil {
			t.Fatal(err)
		}
		opts := explainOpts()
		opts.Measure = measure
		var prompt string
		complete := func(_ context.Context, _, _, fullPrompt string) (categorize.CompletionResult, string, string, error) {
			prompt = fullPrompt
			return categorize.CompletionResult{Text: completionWithKeys(64), Model: "fake"}, "openai", "gpt-5-mini", nil
		}
		if _, err := reader.ExplainInvestmentMix(context.Background(), nil, alwaysAvailable, complete, opts); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(prompt, want) || strings.Contains(prompt, "total_effort") {
			t.Errorf("measure %s: prompt total key wrong (want %s, no total_effort)", measure, want)
		}
	}
}

func TestInvalidFallbackNamesTheMeasure(t *testing.T) {
	for measure, want := range map[string]string{"COUNT": "Work appears concentrated in Velocity"} {
		fallback := invalidLLMOutputExplanation([]keyValue{{Key: "velocity", Value: 8}, {Key: "quality", Value: 2}}, 10, "unknown", nil, nil, BandMix{}, nil, "work")
		if !strings.HasPrefix(fallback.TopFindings[0].Finding, want) || strings.Contains(fallback.Summary, "effort") {
			t.Errorf("measure %s: fallback = %q / %q, want %q and no effort", measure, fallback.TopFindings[0].Finding, fallback.Summary, want)
		}
	}
}

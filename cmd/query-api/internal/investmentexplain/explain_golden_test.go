package investmentexplain

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

// fixtureRowScanner is a RowScanner over a fixed slice of pre-built rows,
// each a []any matching one query's Scan destination count/order.
type fixtureRowScanner struct {
	rows  [][]any
	index int
}

func (s *fixtureRowScanner) Next() bool {
	if s.index >= len(s.rows) {
		return false
	}
	s.index++
	return true
}

func (s *fixtureRowScanner) Scan(dest ...any) error {
	row := s.rows[s.index-1]
	for i, d := range dest {
		switch typed := d.(type) {
		case *string:
			*typed, _ = row[i].(string)
		case **string:
			if v, ok := row[i].(*string); ok {
				*typed = v
			} else {
				*typed = nil
			}
		case *float64:
			*typed, _ = row[i].(float64)
		case **float64:
			if v, ok := row[i].(*float64); ok {
				*typed = v
			} else {
				*typed = nil
			}
		case *time.Time:
			*typed, _ = row[i].(time.Time)
		case *int:
			*typed, _ = row[i].(int)
		default:
			// Not needed by this test's fixtures.
		}
	}
	return nil
}

func (s *fixtureRowScanner) Err() error   { return nil }
func (s *fixtureRowScanner) Close() error { return nil }

func strPtr(s string) *string     { return &s }
func floatPtr(f float64) *float64 { return &f }

// explainFixtureClient dispatches on distinctive SQL substrings, matching
// generate_explain_investment_mix_golden.py's fixture rows exactly (same
// field values, same source).
type explainFixtureClient struct{}

var explainFixtureNow = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func (explainFixtureClient) Query(_ context.Context, statement string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	switch {
	case strings.Contains(statement, "ARRAY JOIN CAST(subcategory_distribution_json") && strings.Contains(statement, "subcategory_kv.1 AS subcategory"):
		// FetchInvestmentBreakdown
		return &fixtureRowScanner{rows: [][]any{
			{"velocity.feature", "velocity", 40.0},
			{"quality.bugfix", "quality", 10.0},
		}}, nil

	case strings.Contains(statement, "ORDER BY effort_value DESC, work_unit_id ASC"):
		// FetchWorkUnitInvestments
		return &fixtureRowScanner{rows: [][]any{
			{
				"unit-1", strPtr("issue"), strPtr("Ship the new thing"),
				time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC),
				strPtr("repo-1"), strPtr("github"),
				strPtr("churn_loc"), floatPtr(40.0),
				strPtr(`{"velocity": 40.0}`), strPtr(`{"velocity.feature": 40.0}`), strPtr(`{"issues": ["issue-a"], "prs": []}`),
				floatPtr(0.8), strPtr("high"),
				strPtr("complete"), strPtr("v1"), strPtr("run-1"),
				explainFixtureNow,
			},
			{
				"unit-2", strPtr("issue"), strPtr("Fix the bug"),
				time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 4, 0, 0, 0, 0, time.UTC),
				strPtr("repo-1"), strPtr("github"),
				strPtr("churn_loc"), floatPtr(10.0),
				strPtr(`{"quality": 10.0}`), strPtr(`{"quality.bugfix": 10.0}`), strPtr(`{"issues": ["issue-b"], "prs": []}`),
				floatPtr(0.3), strPtr("low"),
				strPtr("complete"), strPtr("v1"), strPtr("run-1"),
				explainFixtureNow,
			},
		}}, nil

	case strings.Contains(statement, "argMax(repo, last_synced)"):
		// FetchRepoIdentities -- empty in this fixture (no PR evidence)
		return &fixtureRowScanner{}, nil

	case strings.Contains(statement, "FROM repos"):
		// FetchRepoScopes
		return &fixtureRowScanner{rows: [][]any{
			{"repo-1", "myorg/myrepo"},
		}}, nil

	case strings.Contains(statement, "work_item_team_attributions"):
		// FetchWorkItemTeamAssignments
		return &fixtureRowScanner{rows: [][]any{
			{"issue-a", "team-1", "Platform"},
			{"issue-b", "team-1", "Platform"},
		}}, nil

	case strings.Contains(statement, "work_unit_investment_quotes"):
		// FetchWorkUnitInvestmentQuotes
		return &fixtureRowScanner{rows: [][]any{
			{"unit-1", "Ship the new thing quote", "issue", "issue-a", "run-1"},
		}}, nil

	case strings.Contains(statement, "investment_explanations"):
		// ReadInvestmentExplanation -- always a miss in this fixture
		// (every case uses ForceRefresh=true, matching the golden
		// generator, so this path isn't exercised, but a defined empty
		// result keeps the fake total rather than silently matching
		// nothing).
		return &fixtureRowScanner{}, nil

	default:
		return &fixtureRowScanner{}, nil
	}
}

type explainGolden struct {
	Case               string          `json:"case"`
	LLMProvider        string          `json:"llm_provider"`
	Result             json.RawMessage `json:"result"`
	WrittenCacheRecord *struct {
		CacheKey        string  `json:"cache_key"`
		ExplanationJSON string  `json:"explanation_json"`
		LLMProvider     string  `json:"llm_provider"`
		LLMModel        *string `json:"llm_model"`
	} `json:"written_cache_record"`
}

func loadExplainGolden(t *testing.T, name string) explainGolden {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "explain_investment_mix__"+name+".json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var golden explainGolden
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	return golden
}

func TestExplainInvestmentMixMockProviderMatchesPythonGolden(t *testing.T) {
	golden := loadExplainGolden(t, "mock_provider_invalid_llm_output")

	reader, err := NewReader(explainFixtureClient{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	got, err := reader.ExplainInvestmentMix(context.Background(), nil, CompleteInvestmentMixExplanation, ExplainInvestmentMixOptions{
		OrgID:        "org-golden-4977",
		StartTS:      time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
		EndTS:        time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		LLMProvider:  "mock",
		ForceRefresh: true,
		Now:          explainFixtureNow,
	})
	if err != nil {
		t.Fatalf("ExplainInvestmentMix: %v", err)
	}

	assertExplanationMatchesGolden(t, golden, got)
}

// recordedFixtureCompletionText is generate_explain_investment_mix_golden.py's
// VALID_COMPLETION_TEXT, byte-for-byte -- the same recorded, schema-valid
// completion both planes feed through their own provider seam
// (Python's get_provider patch; Go's CompleteFunc injection below).
const recordedFixtureCompletionText = `{"summary": "Effort appears to lean toward velocity work this period.", "top_findings": [{"finding": "Velocity work leans toward feature delivery", "evidence": {"theme": "velocity", "subcategory": "velocity.feature", "share_pct": 40.0, "delta_pct_points": null, "evidence_quality_mean": null, "evidence_quality_band": null}}], "confidence": {"level": "moderate", "quality_mean": 0.55, "quality_stddev": 0.25, "band_mix": {"high": 1, "moderate": 0, "low": 1, "very_low": 0, "unknown": 0}, "drivers": []}, "what_to_check_next": [{"action": "Review feature-delivery evidence quotes", "why": "Confirms the dominant velocity subcategory", "where": "Work unit evidence panel"}], "anti_claims": ["This does not indicate declining quality investment."]}`

func TestExplainInvestmentMixRecordedFixtureProviderMatchesPythonGolden(t *testing.T) {
	golden := loadExplainGolden(t, "recorded_fixture_provider_valid")

	reader, err := NewReader(explainFixtureClient{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	// IsLLMAvailable("openai", ...) still runs for real (it is not part
	// of the injected seam), so a real-shaped credential must be present
	// for the request to get past that gate -- matching the Python
	// generator's own OPENAI_API_KEY env patch for this exact reason.
	t.Setenv("OPENAI_API_KEY", "sk-fixture-not-real")

	recordedComplete := func(_ context.Context, _, _, _ string) (categorize.CompletionResult, string, string, error) {
		return categorize.CompletionResult{Text: recordedFixtureCompletionText, Model: "fake-recorded", InputTokens: intPtr(11), OutputTokens: intPtr(22)}, "openai", "gpt-5-mini", nil
	}

	conn := newCapturingWriteConn()
	writer, err := NewCacheWriter(conn)
	if err != nil {
		t.Fatalf("NewCacheWriter: %v", err)
	}

	got, err := reader.ExplainInvestmentMix(context.Background(), writer, recordedComplete, ExplainInvestmentMixOptions{
		OrgID:              "org-golden-4977",
		StartTS:            time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
		EndTS:              time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		LLMProvider:        "openai",
		ForceRefresh:       true,
		Now:                explainFixtureNow,
		FiltersForCacheKey: defaultMetricFilterForCacheKey(),
	})
	if err != nil {
		t.Fatalf("ExplainInvestmentMix: %v", err)
	}

	assertExplanationMatchesGolden(t, golden, got)
	assertWrittenCacheRecordMatchesGolden(t, golden, conn)
}

// assertWrittenCacheRecordMatchesGolden proves BYTE-EXACT parity of the
// written investment_explanations row's explanation_json column against
// what write_investment_explanation actually received (captured via a
// stub sink in generate_explain_investment_mix_golden.py) -- the
// structural comparison assertExplanationMatchesGolden does is not
// enough for this specific field, since it re-decodes and re-encodes
// through Go's own json package on both sides, which would silently
// paper over a field-order or separator divergence in
// EncodeInvestmentMixExplanation itself.
func assertWrittenCacheRecordMatchesGolden(t *testing.T, golden explainGolden, conn *capturingWriteConn) {
	t.Helper()
	if golden.WrittenCacheRecord == nil {
		t.Fatal("golden has no written_cache_record -- generator or fixture regressed")
	}
	var batch *capturingBatch
	for query, b := range conn.batches {
		if strings.Contains(query, "INSERT INTO investment_explanations") {
			batch = b
			break
		}
	}
	if batch == nil || len(batch.appended) == 0 {
		t.Fatal("no row appended to investment_explanations")
	}
	row := batch.appended[0]
	gotExplanationJSON, _ := row[1].(string)
	if gotExplanationJSON != golden.WrittenCacheRecord.ExplanationJSON {
		t.Fatalf("explanation_json byte mismatch:\n--- want (python) ---\n%s\n--- got (go) ---\n%s", golden.WrittenCacheRecord.ExplanationJSON, gotExplanationJSON)
	}
	gotCacheKey, _ := row[0].(string)
	if gotCacheKey != golden.WrittenCacheRecord.CacheKey {
		t.Fatalf("cache_key mismatch: want %s, got %s", golden.WrittenCacheRecord.CacheKey, gotCacheKey)
	}
	gotLLMProvider, _ := row[2].(string)
	if gotLLMProvider != golden.WrittenCacheRecord.LLMProvider {
		t.Fatalf("llm_provider mismatch: want %s, got %s", golden.WrittenCacheRecord.LLMProvider, gotLLMProvider)
	}
	gotLLMModel, _ := row[3].(*string)
	wantLLMModel := golden.WrittenCacheRecord.LLMModel
	if (gotLLMModel == nil) != (wantLLMModel == nil) || (gotLLMModel != nil && wantLLMModel != nil && *gotLLMModel != *wantLLMModel) {
		t.Fatalf("llm_model mismatch: want %v, got %v", wantLLMModel, gotLLMModel)
	}
}

func intPtr(i int) *int { return &i }

// defaultMetricFilterForCacheKey is MetricFilter().model_dump(mode="json")
// for a DEFAULT-CONSTRUCTED filter, matching what
// generate_explain_investment_mix_golden.py passes (filters =
// MetricFilter()). Delegates to the exported DefaultMetricFilterForCacheKey
// (defaultfilters.go) so this test and investment_explain_route.go's own
// nil-filters case share one definition instead of two copies drifting.
func defaultMetricFilterForCacheKey() map[string]any {
	return DefaultMetricFilterForCacheKey()
}

func assertExplanationMatchesGolden(t *testing.T, golden explainGolden, got InvestmentMixExplanation) {
	t.Helper()
	gotJSON, err := EncodeInvestmentMixExplanation(got)
	if err != nil {
		t.Fatalf("EncodeInvestmentMixExplanation: %v", err)
	}
	var gotAny, wantAny any
	if err := json.Unmarshal([]byte(gotJSON), &gotAny); err != nil {
		t.Fatalf("re-decode got JSON: %v", err)
	}
	if err := json.Unmarshal(golden.Result, &wantAny); err != nil {
		t.Fatalf("decode golden result: %v", err)
	}
	gotCanon, _ := json.Marshal(gotAny)
	wantCanon, _ := json.Marshal(wantAny)
	if string(gotCanon) != string(wantCanon) {
		t.Fatalf("case %q mismatch:\n--- want (python) ---\n%s\n--- got (go) ---\n%s", golden.Case, wantCanon, gotCanon)
	}
}

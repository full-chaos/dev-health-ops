package investmentexplain

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

// longHaikuDriver starts with the reason_snippet of the prod rejection
// (2026-10-19 14:42:17Z, claude-haiku-5-5) and is over the 120-rune cap.
const longHaikuDriver = "Average evidence quality falls in the moderate range but does not reach the high band, and most work units carry low or very low evidence bands."

func TestParseInvestmentMixResponseKeepsTheAnswerWhenTheModelConfidenceBreaksARule(t *testing.T) {
	cases := []struct {
		name     string
		mutate   func(confidence map[string]any, doc map[string]any)
		wantRule string
		wantPath string
		wantHas  string
	}{
		{
			name:     "driver over 120 characters",
			mutate:   func(c map[string]any, _ map[string]any) { c["drivers"] = []any{longHaikuDriver} },
			wantRule: "confidence_driver_invalid",
			wantPath: "confidence.drivers[0]",
			wantHas:  "Average evidence quality",
		},
		{
			name:     "driver blank",
			mutate:   func(c map[string]any, _ map[string]any) { c["drivers"] = []any{"ok", "  "} },
			wantRule: "confidence_driver_invalid",
			wantPath: "confidence.drivers[1]",
		},
		{
			name:     "too many drivers",
			mutate:   func(c map[string]any, _ map[string]any) { c["drivers"] = make([]any, 11) },
			wantRule: "confidence_drivers_too_many",
			wantPath: "confidence.drivers",
		},
		{
			name:     "level invalid",
			mutate:   func(c map[string]any, _ map[string]any) { c["level"] = "extreme" },
			wantRule: "confidence_level_invalid",
			wantPath: "confidence.level",
			wantHas:  "extreme",
		},
		{
			name:     "band_mix value invalid",
			mutate:   func(c map[string]any, _ map[string]any) { c["band_mix"].(map[string]any)["high"] = -1 },
			wantRule: "confidence_band_mix_value_invalid",
			wantPath: "confidence.band_mix.high",
		},
		{
			name:     "confidence not an object",
			mutate:   func(_ map[string]any, doc map[string]any) { doc["confidence"] = "moderate" },
			wantRule: "confidence_not_object",
			wantPath: "confidence.$",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc := buildValidExplainDoc()
			tc.mutate(doc["confidence"].(map[string]any), doc)
			opts := reasonDetailTestOptions()
			opts.FallbackDrivers = []string{"weak_cross_links"}

			result := ParseInvestmentMixResponse(docText(t, doc), opts)

			if result.Status != ParseStatusValid || result.Output == nil {
				t.Fatalf("status = %q rule = %q path = %q, want the answer kept", result.Status, result.ReasonRule, result.ReasonPath)
			}
			if result.Output.Summary != doc["summary"] {
				t.Fatalf("summary = %q, want the model's summary", result.Output.Summary)
			}
			confidence := result.Output.Confidence
			if confidence.Level != "moderate" || len(confidence.Drivers) != 1 || confidence.Drivers[0] != "weak_cross_links" {
				t.Fatalf("served confidence = %+v, want the computed fallback", confidence)
			}
			ignored := result.ConfidenceIgnored
			if ignored.Rule != tc.wantRule || ignored.Path != tc.wantPath || !strings.Contains(ignored.Snippet, tc.wantHas) {
				t.Fatalf("ConfidenceIgnored = %+v, want rule %q path %q snippet with %q", ignored, tc.wantRule, tc.wantPath, tc.wantHas)
			}
		})
	}
}

func TestParseInvestmentMixResponseReportsNoIgnoredRuleForAValidModelConfidence(t *testing.T) {
	result := ParseInvestmentMixResponse(docText(t, buildValidExplainDoc()), reasonDetailTestOptions())
	if result.Status != ParseStatusValid || result.ConfidenceIgnored != (validationRejection{}) {
		t.Fatalf("status = %q ConfidenceIgnored = %+v, want valid and none", result.Status, result.ConfidenceIgnored)
	}
}

// Through the real explain path: the paid answer is served and the ignored
// rule is logged with the org, provider and model that produced it.
func TestExplainInvestmentMixServesTheAnswerAndLogsTheIgnoredConfidenceRule(t *testing.T) {
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	reader, err := NewReader(explainFixtureClient{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	t.Setenv("OPENAI_API_KEY", "sk-fixture-not-real")
	completionText := strings.Replace(recordedFixtureCompletionText, `"drivers": []`, `"drivers": ["`+longHaikuDriver+`"]`, 1)
	if completionText == recordedFixtureCompletionText {
		t.Fatal("the fixture completion has no empty drivers list to replace")
	}
	complete := func(_ context.Context, _, _, _ string) (categorize.CompletionResult, string, string, error) {
		return categorize.CompletionResult{Text: completionText, Model: "claude-haiku-5-5", InputTokens: intPtr(4511), OutputTokens: intPtr(2149)}, "anthropic", "claude-haiku-5-5", nil
	}

	got, err := reader.ExplainInvestmentMix(context.Background(), nil, availabilityFromIsLLMAvailable, complete, ExplainInvestmentMixOptions{
		OrgID:              "org-10244",
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
	if got.Status == nil || *got.Status != "valid" {
		t.Fatalf("status = %v, want valid", got.Status)
	}
	if !strings.Contains(got.Summary, "lean toward") {
		t.Fatalf("summary = %q, want the model's summary", got.Summary)
	}
	for _, driver := range got.Confidence.Drivers {
		if driver == longHaikuDriver {
			t.Fatal("the model's driver was served")
		}
	}
	// The level and band_mix rules still hold for the served (computed)
	// confidence: a level from the enum, known bands, non-negative counts.
	if !confidenceLevels[got.Confidence.Level] {
		t.Fatalf("served level = %q, not a schema level", got.Confidence.Level)
	}
	if len(got.Confidence.BandMix) == 0 {
		t.Fatal("served band_mix is empty; the fixture has work units")
	}
	for _, count := range got.Confidence.BandMix {
		if _, known := bands[count.Band]; !known || count.Count < 0 {
			t.Fatalf("served band_mix entry %+v breaks the band_mix rule", count)
		}
	}

	var ignored map[string]any
	for _, raw := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
		var line map[string]any
		if err := json.Unmarshal(raw, &line); err != nil {
			t.Fatalf("decode log line: %v\nraw: %s", err, raw)
		}
		switch line["msg"] {
		case "query_api.investment_mix_explain.llm_output_rejected":
			t.Fatalf("the answer was rejected: %s", raw)
		case "query_api.investment_mix_explain.confidence_ignored":
			ignored = line
		}
	}
	if ignored == nil {
		t.Fatalf("no confidence_ignored line in:\n%s", buf.String())
	}
	want := map[string]any{
		"org_id":      "org-10244",
		"reason_rule": "confidence_driver_invalid",
		"reason_path": "confidence.drivers[0]",
		"provider":    "anthropic",
		"model":       "claude-haiku-5-5",
	}
	for key, value := range want {
		if ignored[key] != value {
			t.Fatalf("%s = %v, want %v (line %v)", key, ignored[key], value, ignored)
		}
	}
	if _, present := ignored["reason_snippet"]; present || strings.Contains(buf.String(), "Average evidence quality") {
		t.Fatalf("the confidence_ignored line carries completion text: %v", ignored)
	}
}

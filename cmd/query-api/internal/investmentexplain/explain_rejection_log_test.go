package investmentexplain

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

// TestParseInvestmentMixResponseReasonForMalformedOutput is the red-first
// test for the parser side of the fix: before ParseResult grew a Reason
// field, every one of these malformed shapes collapsed onto the bare
// ParseStatus with nothing else to log -- a real prod rejection was
// indistinguishable from any other. Each case here feeds a genuinely
// malformed completion through the real parser and asserts Status AND
// that Reason names the specific field that failed, not just "invalid".
func TestParseInvestmentMixResponseReasonForMalformedOutput(t *testing.T) {
	opts := ParseOptions{FallbackLevel: "moderate", FallbackBandMix: BandMix{}}

	cases := []struct {
		name          string
		text          string
		wantStatus    ParseStatus
		wantReasonHas string
	}{
		{
			name:          "not json at all",
			text:          "the model wrote a sentence instead of JSON",
			wantStatus:    ParseStatusInvalidJSON,
			wantReasonHas: "could not extract",
		},
		{
			name:          "missing top level keys",
			text:          `{"summary": "ok"}`,
			wantStatus:    ParseStatusInvalidLLMOutput,
			wantReasonHas: "top-level keys",
		},
		{
			name:          "summary is blank",
			text:          `{"summary": "   ", "top_findings": [], "confidence": {"level": "moderate", "quality_mean": null, "quality_stddev": null, "band_mix": {"high": 0, "moderate": 0, "low": 0, "very_low": 0, "unknown": 0}, "drivers": []}, "what_to_check_next": [], "anti_claims": []}`,
			wantStatus:    ParseStatusInvalidLLMOutput,
			wantReasonHas: "summary is blank",
		},
		{
			name:          "summary too long",
			text:          `{"summary": "` + strings.Repeat("a", 1001) + `", "top_findings": [], "confidence": {"level": "moderate", "quality_mean": null, "quality_stddev": null, "band_mix": {"high": 0, "moderate": 0, "low": 0, "very_low": 0, "unknown": 0}, "drivers": []}, "what_to_check_next": [], "anti_claims": []}`,
			wantStatus:    ParseStatusInvalidLLMOutput,
			wantReasonHas: "summary exceeds",
		},
		{
			name:          "top_findings not a list",
			text:          `{"summary": "A minimal valid summary.", "top_findings": "oops", "confidence": {"level": "moderate", "quality_mean": null, "quality_stddev": null, "band_mix": {"high": 0, "moderate": 0, "low": 0, "very_low": 0, "unknown": 0}, "drivers": []}, "what_to_check_next": [], "anti_claims": []}`,
			wantStatus:    ParseStatusInvalidLLMOutput,
			wantReasonHas: "top_findings is not a list",
		},
		{
			name:          "what_to_check_next not a list",
			text:          `{"summary": "A minimal valid summary.", "top_findings": [], "confidence": {"level": "moderate", "quality_mean": null, "quality_stddev": null, "band_mix": {"high": 0, "moderate": 0, "low": 0, "very_low": 0, "unknown": 0}, "drivers": []}, "what_to_check_next": "oops", "anti_claims": []}`,
			wantStatus:    ParseStatusInvalidLLMOutput,
			wantReasonHas: "what_to_check_next is not a list",
		},
		{
			name:          "anti_claims not a list",
			text:          `{"summary": "A minimal valid summary.", "top_findings": [], "confidence": {"level": "moderate", "quality_mean": null, "quality_stddev": null, "band_mix": {"high": 0, "moderate": 0, "low": 0, "very_low": 0, "unknown": 0}, "drivers": []}, "what_to_check_next": [], "anti_claims": "oops"}`,
			wantStatus:    ParseStatusInvalidLLMOutput,
			wantReasonHas: "anti_claims is not a list",
		},
		{
			name:          "forbidden language",
			text:          `{"summary": "This is definitely the answer.", "top_findings": [], "confidence": {"level": "moderate", "quality_mean": null, "quality_stddev": null, "band_mix": {"high": 0, "moderate": 0, "low": 0, "very_low": 0, "unknown": 0}, "drivers": []}, "what_to_check_next": [], "anti_claims": []}`,
			wantStatus:    ParseStatusForbiddenLanguage,
			wantReasonHas: "forbidden",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := ParseInvestmentMixResponse(tc.text, opts)
			if result.Status != tc.wantStatus {
				t.Fatalf("status = %q, want %q (reason: %q)", result.Status, tc.wantStatus, result.Reason)
			}
			if result.Output != nil {
				t.Fatalf("output = %+v, want nil for a rejected completion", result.Output)
			}
			if !strings.Contains(result.Reason, tc.wantReasonHas) {
				t.Fatalf("reason = %q, want it to contain %q", result.Reason, tc.wantReasonHas)
			}
		})
	}
}

// TestLogRejectedExplainOutputFields is the red-first test for the
// actual telemetry deliverable: before logRejectedExplainOutput existed,
// a strict-parser rejection logged NOTHING -- reason, provider, model,
// token counts and the raw completion were all silently discarded,
// which is exactly what made the real prod incident (ClickHouse
// llm_token_usage showing a genuine LLM call at the invalid_llm_output
// fallback's receipt time) undiagnosable from the logs alone. This
// asserts every field the fix is meant to log actually lands on the
// warn line, with the correct value.
func TestLogRejectedExplainOutputFields(t *testing.T) {
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	inputTokens := 2009
	outputTokens := 924
	rawOutput := "```json\n" + strings.Repeat("x", 400) + "\n```"

	parseResult := ParseResult{Status: ParseStatusInvalidLLMOutput, Reason: "top_findings[0] failed validation"}
	completion := categorize.CompletionResult{
		Text:         rawOutput,
		InputTokens:  &inputTokens,
		OutputTokens: &outputTokens,
		Model:        "gpt-5-nano",
	}

	logRejectedExplainOutput(context.Background(), parseResult, "openai", "gpt-5-nano", completion)

	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("decode log line: %v\nraw: %s", err, buf.String())
	}

	if line["msg"] != "query_api.investment_mix_explain.llm_output_rejected" {
		t.Fatalf("msg = %v", line["msg"])
	}
	if line["reason"] != "top_findings[0] failed validation" {
		t.Fatalf("reason = %v", line["reason"])
	}
	if line["status"] != "invalid_llm_output" {
		t.Fatalf("status = %v", line["status"])
	}
	if line["provider"] != "openai" {
		t.Fatalf("provider = %v", line["provider"])
	}
	if line["model"] != "gpt-5-nano" {
		t.Fatalf("model = %v", line["model"])
	}
	if line["prompt_tokens"] != float64(inputTokens) {
		t.Fatalf("prompt_tokens = %v", line["prompt_tokens"])
	}
	if line["completion_tokens"] != float64(outputTokens) {
		t.Fatalf("completion_tokens = %v", line["completion_tokens"])
	}
	wantLen := float64(len([]rune(rawOutput)))
	if line["output_length"] != wantLen {
		t.Fatalf("output_length = %v, want %v (full length, not truncated)", line["output_length"], wantLen)
	}
	head, ok := line["output_head"].(string)
	if !ok {
		t.Fatalf("output_head missing or not a string: %v", line["output_head"])
	}
	// The head is truncated to llmOutputRejectionLogHeadRunes RAW runes
	// first, THEN each embedded newline is escaped to the two-rune
	// sequence "\n" -- so the escaped result can be slightly longer than
	// the cap (one extra rune per embedded newline), never shorter.
	// Un-escaping back gives exactly the truncation window.
	unescaped := strings.ReplaceAll(head, "\\n", "\n")
	if got, want := len([]rune(unescaped)), llmOutputRejectionLogHeadRunes; got != want {
		t.Fatalf("output_head unescaped length = %d, want %d (truncation window)", got, want)
	}
	if strings.Contains(head, "\n") {
		t.Fatalf("output_head contains a raw newline, want it escaped: %q", head)
	}
	if !strings.Contains(head, "\\n") {
		t.Fatalf("output_head = %q, want the escaped newline from the fenced code block to survive", head)
	}
}

// TestLogRejectedExplainOutputFallsBackToCompletionModel asserts the
// resolved-model-empty edge case: when the caller's resolvedModel is
// empty (e.g. the completion layer didn't resolve one), the log still
// names a model rather than logging an empty string.
func TestLogRejectedExplainOutputFallsBackToCompletionModel(t *testing.T) {
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	completion := categorize.CompletionResult{Text: "not json", Model: "gpt-5-nano-2026-01-01"}
	logRejectedExplainOutput(context.Background(), ParseResult{Status: ParseStatusInvalidJSON, Reason: "could not extract a single JSON object from the raw text"}, "openai", "", completion)

	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("decode log line: %v\nraw: %s", err, buf.String())
	}
	if line["model"] != "gpt-5-nano-2026-01-01" {
		t.Fatalf("model = %v, want completion.Model fallback", line["model"])
	}
}

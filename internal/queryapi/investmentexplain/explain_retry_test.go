package investmentexplain

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

type scriptedStep struct {
	text string
	err  error
}

func scriptedComplete(steps []scriptedStep, prompts *[]string) CompleteFunc {
	return func(_ context.Context, _, _, prompt string) (categorize.CompletionResult, string, string, error) {
		i := len(*prompts)
		*prompts = append(*prompts, prompt)
		if i >= len(steps) {
			return categorize.CompletionResult{}, "", "", errors.New("unexpected extra provider call")
		}
		if steps[i].err != nil {
			return categorize.CompletionResult{}, "", "", steps[i].err
		}
		return categorize.CompletionResult{Text: steps[i].text, Model: "fake"}, "openai", "gpt-5-mini", nil
	}
}

func retryReader(t *testing.T) *Reader {
	t.Helper()
	reader, err := NewReader(mixAgreementClient{breakdown: mixRows()})
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

func unknownThemeCompletion() string {
	return strings.Replace(completionWithKeys(10), `"theme":"feature_delivery"`, `"theme":"made_up_theme"`, 1)
}

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buf
}

func TestExplainRetriesOnceAfterAValidatorRejectionAndServesTheSecondAnswer(t *testing.T) {
	logs := captureLogs(t)
	var prompts []string
	complete := scriptedComplete([]scriptedStep{{text: unknownThemeCompletion()}, {text: completionWithKeys(10)}}, &prompts)
	got, err := retryReader(t).ExplainInvestmentMix(context.Background(), nil, alwaysAvailable, complete, explainOpts())
	if err != nil {
		t.Fatal(err)
	}
	if len(prompts) != 2 {
		t.Fatalf("provider calls = %d, want 2", len(prompts))
	}
	if got.Status == nil || *got.Status != "valid" {
		t.Fatalf("status = %v, want valid", got.Status)
	}
	if !strings.HasPrefix(prompts[1], prompts[0]) || !strings.Contains(prompts[1], "theme_unknown") {
		t.Fatalf("retry prompt must be the first prompt plus a note naming the rejected class")
	}
	if strings.Contains(prompts[0], "theme_unknown") {
		t.Fatalf("first prompt must not carry the corrective note")
	}
	var found bool
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var rec map[string]any
		if json.Unmarshal([]byte(line), &rec) == nil && rec["msg"] == "query_api.investment_mix_explain.retry" {
			found = true
			if rec["level"] != "INFO" || rec["reason_rule"] != "theme_unknown" || rec["outcome"] != "accepted" {
				t.Fatalf("retry log = %v", rec)
			}
		}
	}
	if !found {
		t.Fatalf("no retry log line in %s", logs.String())
	}
}

func TestExplainReturnsTheRejectionAfterTwoRejectionsWithExactlyTwoCalls(t *testing.T) {
	var prompts []string
	complete := scriptedComplete([]scriptedStep{{text: unknownThemeCompletion()}, {text: unknownThemeCompletion()}, {text: completionWithKeys(10)}}, &prompts)
	got, err := retryReader(t).ExplainInvestmentMix(context.Background(), nil, alwaysAvailable, complete, explainOpts())
	if err != nil {
		t.Fatal(err)
	}
	if len(prompts) != 2 {
		t.Fatalf("provider calls = %d, want exactly 2", len(prompts))
	}
	if got.Status == nil || *got.Status != "invalid_llm_output" {
		t.Fatalf("status = %v, want invalid_llm_output", got.Status)
	}
}

func TestExplainDoesNotRetryAProviderError(t *testing.T) {
	var prompts []string
	boom := errors.New("provider timeout")
	complete := scriptedComplete([]scriptedStep{{err: boom}, {text: completionWithKeys(10)}}, &prompts)
	_, err := retryReader(t).ExplainInvestmentMix(context.Background(), nil, alwaysAvailable, complete, explainOpts())
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the provider error", err)
	}
	if len(prompts) != 1 {
		t.Fatalf("provider calls = %d, want 1", len(prompts))
	}
}

func TestExplainDoesNotRetryWhenTheRetryCallFailsAndKeepsTheRejection(t *testing.T) {
	var prompts []string
	complete := scriptedComplete([]scriptedStep{{text: unknownThemeCompletion()}, {err: errors.New("budget refused")}}, &prompts)
	got, err := retryReader(t).ExplainInvestmentMix(context.Background(), nil, alwaysAvailable, complete, explainOpts())
	if err != nil {
		t.Fatal(err)
	}
	if len(prompts) != 2 || got.Status == nil || *got.Status != "invalid_llm_output" {
		t.Fatalf("calls %d status %v, want 2 calls and the rejection path", len(prompts), got.Status)
	}
}

package jobruntime

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func investmentLLMCollector(t *testing.T) *MetricsCollector {
	t.Helper()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	return collector
}

func investmentLLMLines(collector *MetricsCollector) []string {
	var lines []string
	for _, line := range strings.Split(collector.PrometheusText(), "\n") {
		if strings.HasPrefix(line, "devhealth_investment_llm_") {
			lines = append(lines, line)
		}
	}
	return lines
}

const llmCommonLabels = `provider="openai",model="gpt-5-mini",stage="initial",prompt_kind="investment_categorize",prompt_version="investment-categorization-v2"`

// Python's instruments are lazy: with no call there is no series.
func TestInvestmentLLMHasNoSeriesBeforeACall(t *testing.T) {
	if lines := investmentLLMLines(investmentLLMCollector(t)); len(lines) != 0 {
		t.Fatalf("series before any call: %v", lines)
	}
}

// One ok request, then one failed request, as llm_call_metrics counts them.
func TestInvestmentLLMRequestCountsAreThoseOfLlmCallMetrics(t *testing.T) {
	collector := investmentLLMCollector(t)
	collector.ObserveInvestmentLLMRequest(InvestmentLLMRequest{
		Provider: " OpenAI ", Model: "gpt-5-mini-2025", Stage: "initial", PromptKind: "investment_categorize",
		PromptVersion: "investment-categorization-v2", Duration: 3 * time.Second, OK: true,
		InputTokens: 120, OutputTokens: 30, OutputChars: 700,
	})
	collector.ObserveInvestmentLLMRequest(InvestmentLLMRequest{
		Provider: "openai", Model: "gpt-5-mini", Stage: "initial", PromptKind: "investment_categorize",
		PromptVersion: "investment-categorization-v2", Duration: 300 * time.Millisecond, ErrorFamily: "rate_limit",
	})
	text := strings.Join(investmentLLMLines(collector), "\n")
	for _, want := range []string{
		fmt.Sprintf(`devhealth_investment_llm_requests_total{%s,outcome="ok"} 1`, llmCommonLabels),
		fmt.Sprintf(`devhealth_investment_llm_requests_total{%s,outcome="error"} 1`, llmCommonLabels),
		fmt.Sprintf(`devhealth_investment_llm_request_errors_total{%s,error_family="rate_limit"} 1`, llmCommonLabels),
		fmt.Sprintf(`devhealth_investment_llm_tokens_total{%s,direction="input"} 120`, llmCommonLabels),
		fmt.Sprintf(`devhealth_investment_llm_tokens_total{%s,direction="output"} 30`, llmCommonLabels),
		fmt.Sprintf(`devhealth_investment_llm_request_duration_seconds_count{%s} 2`, llmCommonLabels),
		fmt.Sprintf(`devhealth_investment_llm_request_duration_seconds_bucket{%s,le="0.5"} 1`, llmCommonLabels),
		fmt.Sprintf(`devhealth_investment_llm_request_duration_seconds_bucket{%s,le="5"} 2`, llmCommonLabels),
		fmt.Sprintf(`devhealth_investment_llm_output_chars_count{%s} 1`, llmCommonLabels),
		fmt.Sprintf(`devhealth_investment_llm_output_chars_bucket{%s,le="1000"} 1`, llmCommonLabels),
		fmt.Sprintf(`devhealth_investment_llm_output_chars_bucket{%s,le="500"} 0`, llmCommonLabels),
	} {
		if !strings.Contains(text, want+"\n") && !strings.HasSuffix(text, want) {
			t.Errorf("missing %s\n%s", want, text)
		}
	}
	// A failed request has no tokens and no output size.
	if strings.Contains(text, `devhealth_investment_llm_output_chars_count{`+llmCommonLabels+`} 2`) {
		t.Error("a failed request observed output chars")
	}
}

// record_validation: one result series; one failure per DISTINCT family; a
// family outside the closed set is "other".
func TestInvestmentLLMValidationCountsDistinctFamilies(t *testing.T) {
	collector := investmentLLMCollector(t)
	validation := InvestmentLLMValidation{
		Provider: "openai", Model: "gpt-5-mini", Stage: "initial", PromptVersion: "investment-categorization-v2",
		Errors: []string{"invalid_weight: a", "invalid_weight: b", "evidence_quote_empty", "novel: x"},
	}
	collector.ObserveInvestmentLLMValidation(validation)
	validation.Errors = nil
	collector.ObserveInvestmentLLMValidation(validation)
	text := strings.Join(investmentLLMLines(collector), "\n")
	for _, want := range []string{
		fmt.Sprintf(`devhealth_investment_llm_validation_total{%s,result="invalid"} 1`, llmCommonLabels),
		fmt.Sprintf(`devhealth_investment_llm_validation_total{%s,result="valid"} 1`, llmCommonLabels),
		fmt.Sprintf(`devhealth_investment_llm_validation_failures_total{%s,error_family="invalid_weight"} 1`, llmCommonLabels),
		fmt.Sprintf(`devhealth_investment_llm_validation_failures_total{%s,error_family="evidence_quote_empty"} 1`, llmCommonLabels),
		fmt.Sprintf(`devhealth_investment_llm_validation_failures_total{%s,error_family="other"} 1`, llmCommonLabels),
	} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %s", want)
		}
	}
}

// record_batch_completion: initial stage, request counted, tokens clamped at
// zero, a failed line adds the batch_item_error family.
func TestInvestmentLLMBatchItemsCountAsInitialRequests(t *testing.T) {
	collector := investmentLLMCollector(t)
	collector.ObserveInvestmentLLMBatchItem(InvestmentLLMBatchItem{
		Provider: "openai", Model: "gpt-5-mini", PromptVersion: "investment-categorization-v2",
		Duration: time.Second, InputTokens: 10, OutputTokens: 5, OutputChars: 50, Succeeded: true,
	})
	collector.ObserveInvestmentLLMBatchItem(InvestmentLLMBatchItem{
		Provider: "openai", Model: "gpt-5-mini", PromptVersion: "investment-categorization-v2",
		Duration: -time.Second, InputTokens: -4, OutputTokens: -4, OutputChars: -4,
	})
	text := strings.Join(investmentLLMLines(collector), "\n")
	for _, want := range []string{
		fmt.Sprintf(`devhealth_investment_llm_requests_total{%s,outcome="ok"} 1`, llmCommonLabels),
		fmt.Sprintf(`devhealth_investment_llm_requests_total{%s,outcome="error"} 1`, llmCommonLabels),
		fmt.Sprintf(`devhealth_investment_llm_request_errors_total{%s,error_family="batch_item_error"} 1`, llmCommonLabels),
		fmt.Sprintf(`devhealth_investment_llm_tokens_total{%s,direction="input"} 10`, llmCommonLabels),
		fmt.Sprintf(`devhealth_investment_llm_request_duration_seconds_count{%s} 2`, llmCommonLabels),
		fmt.Sprintf(`devhealth_investment_llm_output_chars_count{%s} 2`, llmCommonLabels),
	} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %s", want)
		}
	}
}

func TestInvestmentLLMOutcomeLabelsAreTheDeclaredOnes(t *testing.T) {
	collector := investmentLLMCollector(t)
	for _, status := range []string{"ok", "repaired", "invalid_llm_output", "insufficient_evidence", "no_text_sources", "llm_task_failed", "made_up"} {
		collector.ObserveInvestmentLLMCategorizationOutcome(InvestmentLLMOutcome{
			Provider: "anthropic", Model: "claude-x", PromptVersion: "investment-categorization-v2", Status: status,
		})
	}
	text := strings.Join(investmentLLMLines(collector), "\n")
	for _, status := range []string{"ok", "repaired", "invalid_llm_output", "insufficient_evidence", "no_text_sources", "llm_task_failed", "other"} {
		want := fmt.Sprintf(`devhealth_investment_llm_categorization_outcomes_total{provider="anthropic",model="claude",prompt_kind="investment_categorize",prompt_version="investment-categorization-v2",status=%q} 1`, status)
		if !strings.Contains(text, want) {
			t.Errorf("missing %s", want)
		}
	}
}

// The cardinality bound: whatever a caller sends, the series count stays inside
// the closed product, because every label goes through a closed set.
func TestInvestmentLLMCardinalityIsBounded(t *testing.T) {
	collector := investmentLLMCollector(t)
	for i := 0; i < 2000; i++ {
		collector.ObserveInvestmentLLMRequest(InvestmentLLMRequest{
			Provider: fmt.Sprintf("p%d", i), Model: fmt.Sprintf("m%d", i), Stage: fmt.Sprintf("s%d", i),
			PromptKind: fmt.Sprintf("k%d", i), PromptVersion: fmt.Sprintf("v%d", i),
			OK: i%2 == 0, ErrorFamily: fmt.Sprintf("f%d", i), InputTokens: 1,
		})
		collector.ObserveInvestmentLLMValidation(InvestmentLLMValidation{
			Provider: fmt.Sprintf("p%d", i), Model: fmt.Sprintf("m%d", i), Stage: fmt.Sprintf("s%d", i),
			PromptVersion: fmt.Sprintf("v%d", i), Errors: []string{fmt.Sprintf("e%d: x", i)},
		})
		collector.ObserveInvestmentLLMCategorizationOutcome(InvestmentLLMOutcome{
			Provider: fmt.Sprintf("p%d", i), Model: fmt.Sprintf("m%d", i), PromptVersion: fmt.Sprintf("v%d", i), Status: fmt.Sprintf("t%d", i),
		})
	}
	if n := len(investmentLLMLines(collector)); n > 120 {
		t.Fatalf("%d series lines after 2000 distinct label values: a label is not closed", n)
	}
	text := strings.Join(investmentLLMLines(collector), "\n")
	for _, bad := range []string{`provider="p`, `model="m1`, `stage="s`, `prompt_kind="k`, `prompt_version="v`, `error_family="f`, `status="t`} {
		if strings.Contains(text, bad) {
			t.Errorf("an unbounded label value reached the exposition: %s", bad)
		}
	}
}

// The names are ops-only: none is acr's llm.call counter or an acr_ name.
func TestInvestmentLLMNamesDoNotCollideWithAcr(t *testing.T) {
	collector := investmentLLMCollector(t)
	collector.ObserveInvestmentLLMCategorizationOutcome(InvestmentLLMOutcome{Provider: "openai", Status: "ok"})
	for _, line := range strings.Split(collector.PrometheusText(), "\n") {
		if strings.HasPrefix(line, "# TYPE devhealth_investment_llm_") {
			name := strings.Fields(line)[2]
			if strings.HasPrefix(name, "acr_") || strings.Contains(name, "llm_call") {
				t.Errorf("metric name %s collides with acr", name)
			}
		}
	}
}

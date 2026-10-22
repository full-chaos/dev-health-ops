package investment

import (
	"errors"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

type llmRecorder struct {
	mu         sync.Mutex
	batchItems []categorize.LLMBatchItemObservation
	outcomes   []categorize.LLMOutcomeObservation
}

func (r *llmRecorder) ObserveRequest(categorize.LLMRequestObservation)       {}
func (r *llmRecorder) ObserveValidation(categorize.LLMValidationObservation) {}
func (r *llmRecorder) ObserveBatchItem(o categorize.LLMBatchItemObservation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	o.Duration = 0
	r.batchItems = append(r.batchItems, o)
}
func (r *llmRecorder) ObserveOutcome(o categorize.LLMOutcomeObservation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.outcomes = append(r.outcomes, o)
}

// materialize.py:597: a batch line is counted with its own tokens and size; a
// line that is missing is a failure with nothing.
func TestBatchLinesAreCountedAsPythonCountsThem(t *testing.T) {
	recorder := &llmRecorder{}
	m := &Materializer{llmTelemetry: recorder}
	cfg := Config{ProviderName: "openai", Model: "gpt-5-mini"}
	in, out := 12, 7
	m.observeBatchLine(cfg, "gpt-5-mini", categorize.BatchItemResult{Text: "héllo", InputTokens: &in, OutputTokens: &out}, true, time.Second)
	m.observeBatchLine(cfg, "gpt-5-mini", categorize.BatchItemResult{ErrorCode: "http_500", InputTokens: &in}, true, time.Second)
	m.observeBatchLine(cfg, "gpt-5-mini", categorize.BatchItemResult{}, false, time.Second)
	want := []categorize.LLMBatchItemObservation{
		{Provider: "openai", Model: "gpt-5-mini", InputTokens: 12, OutputTokens: 7, OutputChars: 5, Succeeded: true},
		{Provider: "openai", Model: "gpt-5-mini", InputTokens: 12},
		{Provider: "openai", Model: "gpt-5-mini"},
	}
	if !reflect.DeepEqual(recorder.batchItems, want) {
		t.Fatalf("batch items:\n got %+v\nwant %+v", recorder.batchItems, want)
	}
}

// materialize.py:305: every pending unit of a batch that fails as a whole is
// counted as a failed line, a deterministic failure included.
func TestAWholeBatchFailureCountsEveryPendingUnit(t *testing.T) {
	recorder := &llmRecorder{}
	m := &Materializer{llmTelemetry: recorder}
	account := &categorizeAccount{outcomes: map[int]categorize.CategorizationOutcome{}, stats: &Stats{LLMFailureCounts: map[string]int{}}, cancel: func() {}}
	run := batchRun{m: m, cfg: Config{ProviderName: "openai"}, account: account, model: "gpt-5-mini", started: time.Now()}
	run.failAll([]preprocessed{{index: 0}, {index: 1}, {index: 2}}, errors.New("boom"))
	if len(recorder.batchItems) != 3 {
		t.Fatalf("%d failed lines counted for 3 pending units", len(recorder.batchItems))
	}
	for _, item := range recorder.batchItems {
		if item.Succeeded || item.Provider != "openai" || item.Model != "gpt-5-mini" {
			t.Fatalf("item = %+v", item)
		}
	}
}

// The three fallback sites of the generative path count their reason (Python's
// fallback_outcome records it); a bare FallbackOutcome there would skip it.
func TestMaterializeRecordsItsFallbackOutcomes(t *testing.T) {
	source, err := os.ReadFile("materialize.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, want := range []string{
		"RecordedFallbackOutcome(m.llmTelemetry, categorize.StatusInsufficientChars, cfg.ProviderName, cfg.Model)",
		"RecordedFallbackOutcome(m.llmTelemetry, categorize.StatusNoTextSources, cfg.ProviderName, cfg.Model)",
		"Status: categorize.StatusLLMTaskFailed,",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("materialize.go does not contain %q", want)
		}
	}
}

// The adapter bounds nothing and renames nothing: what the categorize package
// reports is what the collector counts, under the one prompt version and kind.
func TestCollectorLLMTelemetryReachesTheCollector(t *testing.T) {
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	telemetry := CollectorLLMTelemetry{Collector: collector}
	telemetry.ObserveOutcome(categorize.LLMOutcomeObservation{Provider: "openai", Model: "gpt-5-mini", Status: "ok"})
	telemetry.ObserveValidation(categorize.LLMValidationObservation{Provider: "openai", Model: "gpt-5-mini", Stage: "repair", Errors: []string{"invalid_weight: x"}})
	telemetry.ObserveRequest(categorize.LLMRequestObservation{Provider: "openai", Model: "gpt-5-mini", Stage: "initial", OK: true, InputTokens: 3})
	telemetry.ObserveBatchItem(categorize.LLMBatchItemObservation{Provider: "openai", Model: "gpt-5-mini", Succeeded: true})
	text := collector.PrometheusText()
	for _, want := range []string{
		`devhealth_investment_llm_categorization_outcomes_total{provider="openai",model="gpt-5-mini",prompt_kind="investment_categorize",prompt_version="investment-categorization-v2",status="ok"} 1`,
		`devhealth_investment_llm_validation_failures_total{provider="openai",model="gpt-5-mini",stage="repair",prompt_kind="investment_categorize",prompt_version="investment-categorization-v2",error_family="invalid_weight"} 1`,
		`devhealth_investment_llm_requests_total{provider="openai",model="gpt-5-mini",stage="initial",prompt_kind="investment_categorize",prompt_version="investment-categorization-v2",outcome="ok"} 2`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %s", want)
		}
	}
	// A nil collector is a no-op.
	CollectorLLMTelemetry{}.ObserveOutcome(categorize.LLMOutcomeObservation{})
}

// A failed batch unit is labelled with the provider's model, a failed
// synchronous one with the configured model.
func TestAFailedUnitIsLabelledAsPythonLabelsIt(t *testing.T) {
	m := &Materializer{provider: categorize.MockProvider{}}
	cfg := Config{ProviderName: "mock", Model: "configured-model"}
	if got := m.failedUnitModel(cfg); got != "configured-model" {
		t.Fatalf("synchronous: %q", got)
	}
	m.batchAnswered = true
	if got := m.failedUnitModel(cfg); got != "mock" {
		t.Fatalf("batch: %q, want the provider's model mock", got)
	}
}

package categorize

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-go/llm"
)

type telemetryRecorder struct {
	mu          sync.Mutex
	requests    []LLMRequestObservation
	validations []LLMValidationObservation
	batchItems  []LLMBatchItemObservation
	outcomes    []LLMOutcomeObservation
}

func (r *telemetryRecorder) ObserveRequest(o LLMRequestObservation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	o.Duration = 0
	r.requests = append(r.requests, o)
}

func (r *telemetryRecorder) ObserveValidation(o LLMValidationObservation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.validations = append(r.validations, o)
}

func (r *telemetryRecorder) ObserveBatchItem(o LLMBatchItemObservation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batchItems = append(r.batchItems, o)
}

func (r *telemetryRecorder) ObserveOutcome(o LLMOutcomeObservation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.outcomes = append(r.outcomes, o)
}

// The parity table of classify_llm_exception_family: Python tests the exception
// TYPE in this order and never the message. A class Python had no branch for is
// an LLMError subclass there, so "llm_error"; a non-provider error is
// "unexpected_error". One row for each Go error kind and shared class.
func TestExceptionFamilyIsTheTypeTableOfPython(t *testing.T) {
	kinds := map[llmErrorKind]string{
		llmErrorGeneric: "llm_error", llmErrorAuth: "auth", llmErrorModelNotFound: "llm_error",
		llmErrorInvalidRequest: "llm_error", llmErrorRateLimit: "rate_limit", llmErrorContextLength: "context_length",
		llmErrorServer: "server_error", llmErrorTimeout: "llm_error", llmErrorTransport: "llm_error",
		llmErrorOutput: "output_error", llmErrorBatchTimeout: "llm_error",
	}
	for kind, want := range kinds {
		// The message carries another family's words: only the type counts.
		err := &llmError{kind: kind, message: "insufficient_quota invalid api key rate limit"}
		if got := ExceptionFamily(err); got != want {
			t.Errorf("kind %d: family %q, want %q", kind, got, want)
		}
	}
	classes := map[llm.ErrorClass]string{
		llm.ClassGeneric: "llm_error", llm.ClassAuth: "auth", llm.ClassModelNotFound: "llm_error",
		llm.ClassInvalidRequest: "llm_error", llm.ClassRateLimit: "rate_limit", llm.ClassContextLength: "context_length",
		llm.ClassServer: "server_error", llm.ClassTimeout: "llm_error", llm.ClassTransport: "llm_error",
		llm.ClassOutput: "output_error", llm.ClassBatchTimeout: "llm_error",
	}
	for class, want := range classes {
		err := llm.NewError(class, llm.Kind("openai"), "m", "insufficient_quota")
		if got := ExceptionFamily(err); got != want {
			t.Errorf("class %s: family %q, want %q", class, got, want)
		}
	}
	for _, err := range []error{errors.New("boom"), context.Canceled, fmt.Errorf("wrapped: %w", errors.New("x"))} {
		if got := ExceptionFamily(err); got != "unexpected_error" {
			t.Errorf("%v: family %q, want unexpected_error", err, got)
		}
	}
	if got := ExceptionFamily(&ModelAnsweredError{Err: &llmError{kind: llmErrorRateLimit}}); got != "rate_limit" {
		t.Errorf("a wrapped provider error lost its family: %q", got)
	}
}

// Python's signals for one categorization, site by site (categorize.py).
func TestCategorizationEmitsPythonsSignals(t *testing.T) {
	bundle := seamBundle()
	type signals struct {
		requests    []LLMRequestObservation
		validations []LLMValidationObservation
		outcomes    []LLMOutcomeObservation
	}
	cases := []struct {
		name  string
		texts []string
		want  signals
	}{
		{"valid", []string{seamValidText}, signals{
			requests:    []LLMRequestObservation{{Provider: "openai", Model: "model-returned", Stage: "initial", OK: true, InputTokens: 100, OutputTokens: 20, OutputChars: len([]rune(seamValidText))}},
			validations: []LLMValidationObservation{{Provider: "openai", Model: "model-returned", Stage: "initial"}},
			outcomes:    []LLMOutcomeObservation{{Provider: "openai", Model: "model-returned", Status: "ok"}},
		}},
		{"repaired", []string{seamBadQuoteText, seamValidText}, signals{
			requests: []LLMRequestObservation{
				{Provider: "openai", Model: "model-returned", Stage: "initial", OK: true, InputTokens: 100, OutputTokens: 20, OutputChars: len([]rune(seamBadQuoteText))},
				{Provider: "openai", Model: "model-returned", Stage: "repair", OK: true, InputTokens: 101, OutputTokens: 21, OutputChars: len([]rune(seamValidText))},
			},
			validations: nil, // checked apart: the first has errors
			outcomes:    []LLMOutcomeObservation{{Provider: "openai", Model: "model-returned", Status: "repaired"}},
		}},
		{"invalid twice", []string{seamBadQuoteText, seamBadQuoteText}, signals{
			outcomes: []LLMOutcomeObservation{{Provider: "openai", Model: "model-returned", Status: "invalid_llm_output"}},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			recorder := &telemetryRecorder{}
			provider := &scriptedProvider{texts: c.texts}
			_, err := CategorizeTextBundle(context.Background(), bundle, CategorizeOptions{
				Provider: provider, ProviderName: "openai", Model: "model-requested", Telemetry: recorder,
			})
			if err != nil {
				t.Fatal(err)
			}
			if c.want.requests != nil && !reflect.DeepEqual(recorder.requests, c.want.requests) {
				t.Fatalf("requests:\n got %+v\nwant %+v", recorder.requests, c.want.requests)
			}
			if c.want.validations != nil && !reflect.DeepEqual(recorder.validations, c.want.validations) {
				t.Fatalf("validations:\n got %+v\nwant %+v", recorder.validations, c.want.validations)
			}
			if !reflect.DeepEqual(recorder.outcomes, c.want.outcomes) {
				t.Fatalf("outcomes:\n got %+v\nwant %+v", recorder.outcomes, c.want.outcomes)
			}
			if len(recorder.requests) != len(c.texts) || len(recorder.validations) != len(c.texts) {
				t.Fatalf("%d requests and %d validations for %d texts", len(recorder.requests), len(recorder.validations), len(c.texts))
			}
			for index, validation := range recorder.validations {
				wantStage := []string{"initial", "repair"}[index]
				if validation.Stage != wantStage {
					t.Fatalf("validation %d has stage %q, want %q", index, validation.Stage, wantStage)
				}
				invalid := index < len(c.texts)-1 || c.name == "invalid twice"
				if invalid != (len(validation.Errors) > 0) {
					t.Fatalf("validation %d: errors %v, invalid want %v", index, validation.Errors, invalid)
				}
			}
		})
	}
}

// A failed request counts its family under the REQUESTED model, and the unit
// has no outcome (Python raised out of llm_call_metrics before any record call).
func TestAFailedRequestCountsItsFamilyUnderTheRequestedModel(t *testing.T) {
	recorder := &telemetryRecorder{}
	failing := &failingProvider{err: &llmError{kind: llmErrorRateLimit, message: "slow down"}}
	_, err := CategorizeTextBundle(context.Background(), seamBundle(), CategorizeOptions{
		Provider: failing, ProviderName: "openai", Model: "model-requested", Telemetry: recorder,
	})
	if err == nil {
		t.Fatal("want the provider error")
	}
	want := []LLMRequestObservation{{Provider: "openai", Model: "model-requested", Stage: "initial", ErrorFamily: "rate_limit"}}
	if !reflect.DeepEqual(recorder.requests, want) || len(recorder.validations) != 0 || len(recorder.outcomes) != 0 {
		t.Fatalf("requests %+v validations %+v outcomes %+v", recorder.requests, recorder.validations, recorder.outcomes)
	}
}

type failingProvider struct{ err error }

func (p *failingProvider) Complete(context.Context, CompletionRequest) (CompletionResult, error) {
	return CompletionResult{}, p.err
}
func (p *failingProvider) Close() error  { return nil }
func (p *failingProvider) Model() string { return "model-requested" }

// fallback_outcome records its reason as the status under the caller's identity.
func TestRecordedFallbackOutcomeCountsItsReason(t *testing.T) {
	recorder := &telemetryRecorder{}
	outcome := RecordedFallbackOutcome(recorder, StatusNoTextSources, "anthropic", "claude-x")
	if outcome.Status != StatusNoTextSources || !reflect.DeepEqual(outcome, FallbackOutcome(StatusNoTextSources)) {
		t.Fatalf("outcome = %+v", outcome)
	}
	want := []LLMOutcomeObservation{{Provider: "anthropic", Model: "claude-x", Status: StatusNoTextSources}}
	if !reflect.DeepEqual(recorder.outcomes, want) {
		t.Fatalf("outcomes = %+v", recorder.outcomes)
	}
	// No telemetry is not an error.
	_ = RecordedFallbackOutcome(nil, StatusNoTextSources, "x", "y")
}

// No telemetry changes nothing: the outcome of a run with a recorder and one
// without is the same.
func TestTelemetryDoesNotChangeTheOutcome(t *testing.T) {
	run := func(telemetry LLMTelemetry) CategorizationOutcome {
		got, err := CategorizeTextBundle(context.Background(), seamBundle(), CategorizeOptions{
			Provider: &scriptedProvider{texts: []string{seamBadQuoteText, seamValidText}}, Telemetry: telemetry,
		})
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	if !reflect.DeepEqual(run(nil), run(&telemetryRecorder{})) {
		t.Fatal("the outcome depends on telemetry")
	}
}

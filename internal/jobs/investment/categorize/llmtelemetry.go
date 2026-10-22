package categorize

import (
	"context"
	"errors"
	"time"

	"github.com/full-chaos/dev-health-go/llm"
)

// The investment LLM signals (CHAOS-8722), the port of llm_telemetry.py. The
// package reports what happened; labels are bounded where the signals are
// counted, never here, so no caller can widen a label set.

const (
	// PromptKindCategorize is llm_telemetry_labels.PROMPT_KIND_CATEGORIZE.
	PromptKindCategorize = "investment_categorize"
	// StageInitial and StageRepair are the stages of a categorization.
	StageInitial = "initial"
	StageRepair  = "repair"
)

// LLMRequestObservation is one finished request (llm_call_metrics). Model is
// the provider's reported model after a success and the requested one after a
// failure.
type LLMRequestObservation struct {
	Provider, Model, Stage string
	Duration               time.Duration
	OK                     bool
	ErrorFamily            string
	InputTokens            int
	OutputTokens           int
	OutputChars            int
}

// LLMValidationObservation is one validation of a completion
// (record_validation); no Errors means valid.
type LLMValidationObservation struct {
	Provider, Model, Stage string
	Errors                 []string
}

// LLMBatchItemObservation is one finished line of a provider batch
// (record_batch_completion).
type LLMBatchItemObservation struct {
	Provider, Model                        string
	Duration                               time.Duration
	InputTokens, OutputTokens, OutputChars int
	Succeeded                              bool
}

// LLMOutcomeObservation is one terminal categorization outcome
// (record_categorization_outcome).
type LLMOutcomeObservation struct {
	Provider, Model, Status string
}

// LLMTelemetry receives the investment LLM signals. Every method may be called
// from several goroutines. A nil LLMTelemetry is tolerated by every caller in
// this package.
type LLMTelemetry interface {
	ObserveRequest(LLMRequestObservation)
	ObserveValidation(LLMValidationObservation)
	ObserveBatchItem(LLMBatchItemObservation)
	ObserveOutcome(LLMOutcomeObservation)
}

func observeRequest(telemetry LLMTelemetry, observation LLMRequestObservation) {
	if telemetry != nil {
		telemetry.ObserveRequest(observation)
	}
}

func observeValidation(telemetry LLMTelemetry, observation LLMValidationObservation) {
	if telemetry != nil {
		telemetry.ObserveValidation(observation)
	}
}

// ObserveBatchItem reports one batch line to telemetry. Nil telemetry is a no-op.
func ObserveBatchItem(telemetry LLMTelemetry, observation LLMBatchItemObservation) {
	if telemetry != nil {
		telemetry.ObserveBatchItem(observation)
	}
}

// ObserveOutcome reports one terminal outcome to telemetry. Nil telemetry is a no-op.
func ObserveOutcome(telemetry LLMTelemetry, observation LLMOutcomeObservation) {
	if telemetry != nil {
		telemetry.ObserveOutcome(observation)
	}
}

// RecordedFallbackOutcome is FallbackOutcome with fallback_outcome's
// record_categorization_outcome call (categorize.py:75-87): the status is
// counted under the provider and model the caller names.
func RecordedFallbackOutcome(telemetry LLMTelemetry, reason, provider, model string) CategorizationOutcome {
	outcome := FallbackOutcome(reason)
	ObserveOutcome(telemetry, LLMOutcomeObservation{Provider: provider, Model: model, Status: reason})
	return outcome
}

// ExceptionFamily ports classify_llm_exception_family. Python tests the
// exception TYPE in a fixed order and never reads a message; the Go error is
// classified by its class, so a message cannot move a failure to another
// family. Every class Python had no branch for (model not found, invalid
// request, timeout, transport, batch timeout) is an LLMError subclass there
// and so "llm_error"; an error that is not a provider error is
// "unexpected_error".
func ExceptionFamily(err error) string {
	var typed *llmError
	if errors.As(err, &typed) {
		return kindFamily(typed.kind)
	}
	if class, ok := llm.ClassOf(err); ok {
		return classFamily(class)
	}
	return "unexpected_error"
}

func kindFamily(kind llmErrorKind) string {
	switch kind {
	case llmErrorAuth:
		return "auth"
	case llmErrorRateLimit:
		return "rate_limit"
	case llmErrorContextLength:
		return "context_length"
	case llmErrorServer:
		return "server_error"
	case llmErrorOutput:
		return "output_error"
	default:
		return "llm_error"
	}
}

func classFamily(class llm.ErrorClass) string {
	switch class {
	case llm.ClassAuth:
		return "auth"
	case llm.ClassRateLimit:
		return "rate_limit"
	case llm.ClassContextLength:
		return "context_length"
	case llm.ClassServer:
		return "server_error"
	case llm.ClassOutput:
		return "output_error"
	default:
		return "llm_error"
	}
}

// complete is categorize.py's _complete: one provider request, measured. The
// model reported after a success labels the success; the requested model
// labels a failure.
func (opts CategorizeOptions) complete(ctx context.Context, request CompletionRequest, stage string) (CompletionResult, error) {
	started := time.Now()
	result, err := opts.Provider.Complete(ctx, request)
	observation := LLMRequestObservation{
		Provider: opts.ProviderName, Model: opts.Model, Stage: stage, Duration: time.Since(started),
	}
	if err != nil {
		observation.ErrorFamily = ExceptionFamily(err)
	} else {
		observation.OK = true
		observation.Model = result.Model
		observation.InputTokens = tokenCount(result.InputTokens)
		observation.OutputTokens = tokenCount(result.OutputTokens)
		observation.OutputChars = len([]rune(result.Text))
	}
	observeRequest(opts.Telemetry, observation)
	return result, err
}

// resolvedOrRequested is Python's `resolved_model or llm_model`.
func resolvedOrRequested(resolved, requested string) string {
	if resolved != "" {
		return resolved
	}
	return requested
}

func (opts CategorizeOptions) recordValidation(stage, resolvedModel string, validation LLMValidationResult) {
	observeValidation(opts.Telemetry, LLMValidationObservation{
		Provider: opts.ProviderName, Model: resolvedOrRequested(resolvedModel, opts.Model),
		Stage: stage, Errors: validation.Errors,
	})
}

func (opts CategorizeOptions) recordOutcome(resolvedModel, status string) {
	ObserveOutcome(opts.Telemetry, LLMOutcomeObservation{
		Provider: opts.ProviderName, Model: resolvedOrRequested(resolvedModel, opts.Model), Status: status,
	})
}

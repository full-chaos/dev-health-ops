package investment

import (
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

// CollectorLLMTelemetry adapts the metrics collector to categorize.LLMTelemetry
// (CHAOS-8722). The collector bounds every label; the prompt kind is always
// the categorization one, and the prompt version is the one this port sends.
type CollectorLLMTelemetry struct {
	Collector *jobruntime.MetricsCollector
}

func (telemetry CollectorLLMTelemetry) ObserveRequest(observation categorize.LLMRequestObservation) {
	if telemetry.Collector == nil {
		return
	}
	telemetry.Collector.ObserveInvestmentLLMRequest(jobruntime.InvestmentLLMRequest{
		Provider: observation.Provider, Model: observation.Model, Stage: observation.Stage,
		PromptKind: categorize.PromptKindCategorize, PromptVersion: categorize.PromptVersion,
		Duration: observation.Duration, OK: observation.OK, ErrorFamily: observation.ErrorFamily,
		InputTokens: observation.InputTokens, OutputTokens: observation.OutputTokens, OutputChars: observation.OutputChars,
	})
}

func (telemetry CollectorLLMTelemetry) ObserveValidation(observation categorize.LLMValidationObservation) {
	if telemetry.Collector == nil {
		return
	}
	telemetry.Collector.ObserveInvestmentLLMValidation(jobruntime.InvestmentLLMValidation{
		Provider: observation.Provider, Model: observation.Model, Stage: observation.Stage,
		PromptVersion: categorize.PromptVersion, Errors: observation.Errors,
	})
}

func (telemetry CollectorLLMTelemetry) ObserveBatchItem(observation categorize.LLMBatchItemObservation) {
	if telemetry.Collector == nil {
		return
	}
	telemetry.Collector.ObserveInvestmentLLMBatchItem(jobruntime.InvestmentLLMBatchItem{
		Provider: observation.Provider, Model: observation.Model, PromptVersion: categorize.PromptVersion,
		Duration: observation.Duration, InputTokens: observation.InputTokens,
		OutputTokens: observation.OutputTokens, OutputChars: observation.OutputChars, Succeeded: observation.Succeeded,
	})
}

func (telemetry CollectorLLMTelemetry) ObserveOutcome(observation categorize.LLMOutcomeObservation) {
	if telemetry.Collector == nil {
		return
	}
	telemetry.Collector.ObserveInvestmentLLMCategorizationOutcome(jobruntime.InvestmentLLMOutcome{
		Provider: observation.Provider, Model: observation.Model, PromptVersion: categorize.PromptVersion, Status: observation.Status,
	})
}

package jobruntime

import (
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// The investment LLM signals of investment.materialize (CHAOS-8722), the port
// of llm_telemetry.py and llm_telemetry_metrics.py. Every label is a member of
// a closed set (units/telemetrylabels.go); a series exists only once it has
// been observed, as the Python instruments did.

const (
	investmentLLMRequestsMetric       = "devhealth_investment_llm_requests_total"
	investmentLLMDurationMetric       = "devhealth_investment_llm_request_duration_seconds"
	investmentLLMErrorsMetric         = "devhealth_investment_llm_request_errors_total"
	investmentLLMTokensMetric         = "devhealth_investment_llm_tokens_total"
	investmentLLMOutputCharsMetric    = "devhealth_investment_llm_output_chars"
	investmentLLMValidationMetric     = "devhealth_investment_llm_validation_total"
	investmentLLMValidationFailMetric = "devhealth_investment_llm_validation_failures_total"
	investmentLLMOutcomesMetric       = "devhealth_investment_llm_categorization_outcomes_total"

	// InvestmentLLMBatchItemErrorFamily is the family of a failed batch line.
	InvestmentLLMBatchItemErrorFamily = "batch_item_error"
)

var (
	investmentLLMDurationBuckets   = []float64{0.25, 0.5, 1, 2.5, 5, 10, 20, 40, 60, 120}
	investmentLLMOutputCharsBucket = []float64{100, 250, 500, 1000, 2000, 4000, 8000, 16000, 32000}
)

// InvestmentLLMErrorFamilies is the closed set of error_family values of the
// request error counter: classify_llm_exception_family's words and the batch
// line family.
func InvestmentLLMErrorFamilies() []string {
	return []string{
		"auth", "rate_limit", "context_length", "server_error", "output_error", "llm_error",
		"unexpected_error", InvestmentLLMBatchItemErrorFamily,
	}
}

type investmentLLMCommon struct {
	provider, model, stage, promptKind, promptVersion string
}

type investmentLLMPrompt struct {
	provider, model, promptKind, promptVersion, status string
}

type investmentLLMKeyed struct {
	common investmentLLMCommon
	value  string
}

type investmentLLMState struct {
	requests         map[investmentLLMKeyed]uint64
	duration         map[investmentLLMCommon]*histogram
	errors           map[investmentLLMKeyed]uint64
	tokens           map[investmentLLMKeyed]uint64
	outputChars      map[investmentLLMCommon]*histogram
	validation       map[investmentLLMKeyed]uint64
	validationFailed map[investmentLLMKeyed]uint64
	outcomes         map[investmentLLMPrompt]uint64
}

// InvestmentLLMRequest is one finished LLM request of the categorization
// stages (llm_call_metrics). Model is the one the provider reported when the
// request succeeded, the requested one when it failed.
type InvestmentLLMRequest struct {
	Provider, Model, Stage, PromptKind, PromptVersion string
	Duration                                          time.Duration
	OK                                                bool
	ErrorFamily                                       string
	InputTokens, OutputTokens, OutputChars            int
}

// InvestmentLLMValidation is one validation of a completion (record_validation).
type InvestmentLLMValidation struct {
	Provider, Model, Stage, PromptVersion string
	Errors                                []string
}

// InvestmentLLMBatchItem is one finished line of a provider batch
// (record_batch_completion).
type InvestmentLLMBatchItem struct {
	Provider, Model, PromptVersion         string
	Duration                               time.Duration
	InputTokens, OutputTokens, OutputChars int
	Succeeded                              bool
}

// InvestmentLLMOutcome is one terminal categorization outcome
// (record_categorization_outcome).
type InvestmentLLMOutcome struct {
	Provider, Model, PromptVersion, Status string
}

func investmentLLMLabels(provider, model, stage, promptKind, promptVersion string) investmentLLMCommon {
	return investmentLLMCommon{
		provider:      units.ProviderBucket(provider),
		model:         units.ModelBucket(model),
		stage:         units.Bounded(stage, units.Stages, "other"),
		promptKind:    units.Bounded(promptKind, units.PromptKinds, "other"),
		promptVersion: units.Bounded(promptVersion, units.PromptVersions, "other"),
	}
}

func (collector *MetricsCollector) investmentLLM() *investmentLLMState {
	if collector.investmentLLMState == nil {
		collector.investmentLLMState = &investmentLLMState{
			requests:         map[investmentLLMKeyed]uint64{},
			duration:         map[investmentLLMCommon]*histogram{},
			errors:           map[investmentLLMKeyed]uint64{},
			tokens:           map[investmentLLMKeyed]uint64{},
			outputChars:      map[investmentLLMCommon]*histogram{},
			validation:       map[investmentLLMKeyed]uint64{},
			validationFailed: map[investmentLLMKeyed]uint64{},
			outcomes:         map[investmentLLMPrompt]uint64{},
		}
	}
	return collector.investmentLLMState
}

func (state *investmentLLMState) observeDuration(labels investmentLLMCommon, seconds float64) {
	if state.duration[labels] == nil {
		state.duration[labels] = newHistogramWithBounds(investmentLLMDurationBuckets)
	}
	state.duration[labels].observe(seconds)
}

func (state *investmentLLMState) observeOutputChars(labels investmentLLMCommon, chars float64) {
	if state.outputChars[labels] == nil {
		state.outputChars[labels] = newHistogramWithBounds(investmentLLMOutputCharsBucket)
	}
	state.outputChars[labels].observe(chars)
}

func investmentLLMErrorFamily(family string) string {
	for _, known := range InvestmentLLMErrorFamilies() {
		if family == known {
			return family
		}
	}
	return "unexpected_error"
}

// ObserveInvestmentLLMRequest ports llm_call_metrics' two exits: an error
// counts the request, its latency and its family; a success counts the
// request, latency, both token directions and the output size.
func (collector *MetricsCollector) ObserveInvestmentLLMRequest(request InvestmentLLMRequest) {
	labels := investmentLLMLabels(request.Provider, request.Model, request.Stage, request.PromptKind, request.PromptVersion)
	collector.mu.Lock()
	defer collector.mu.Unlock()
	state := collector.investmentLLM()
	state.observeDuration(labels, max(request.Duration.Seconds(), 0))
	if !request.OK {
		state.requests[investmentLLMKeyed{labels, "error"}]++
		state.errors[investmentLLMKeyed{labels, investmentLLMErrorFamily(request.ErrorFamily)}]++
		return
	}
	state.requests[investmentLLMKeyed{labels, "ok"}]++
	state.tokens[investmentLLMKeyed{labels, "input"}] += uint64(max(request.InputTokens, 0))
	state.tokens[investmentLLMKeyed{labels, "output"}] += uint64(max(request.OutputTokens, 0))
	state.observeOutputChars(labels, float64(max(request.OutputChars, 0)))
}

// ObserveInvestmentLLMValidation ports record_validation: one result count, and
// one failure count per distinct error family.
func (collector *MetricsCollector) ObserveInvestmentLLMValidation(validation InvestmentLLMValidation) {
	labels := investmentLLMLabels(validation.Provider, validation.Model, validation.Stage, units.SortedPromptKinds[0], validation.PromptVersion)
	result := "valid"
	if len(validation.Errors) > 0 {
		result = "invalid"
	}
	families := map[string]struct{}{}
	for _, raw := range validation.Errors {
		families[units.ValidationErrorFamily(raw)] = struct{}{}
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	state := collector.investmentLLM()
	state.validation[investmentLLMKeyed{labels, result}]++
	for family := range families {
		state.validationFailed[investmentLLMKeyed{labels, family}]++
	}
}

// ObserveInvestmentLLMBatchItem ports record_batch_completion: a batch line is
// an initial-stage categorization request.
func (collector *MetricsCollector) ObserveInvestmentLLMBatchItem(item InvestmentLLMBatchItem) {
	labels := investmentLLMLabels(item.Provider, item.Model, "initial", units.SortedPromptKinds[0], item.PromptVersion)
	outcome := "ok"
	if !item.Succeeded {
		outcome = "error"
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	state := collector.investmentLLM()
	state.requests[investmentLLMKeyed{labels, outcome}]++
	state.observeDuration(labels, max(item.Duration.Seconds(), 0))
	state.tokens[investmentLLMKeyed{labels, "input"}] += uint64(max(item.InputTokens, 0))
	state.tokens[investmentLLMKeyed{labels, "output"}] += uint64(max(item.OutputTokens, 0))
	state.observeOutputChars(labels, float64(max(item.OutputChars, 0)))
	if !item.Succeeded {
		state.errors[investmentLLMKeyed{labels, InvestmentLLMBatchItemErrorFamily}]++
	}
}

// ObserveInvestmentLLMCategorizationOutcome ports record_categorization_outcome.
func (collector *MetricsCollector) ObserveInvestmentLLMCategorizationOutcome(outcome InvestmentLLMOutcome) {
	labels := investmentLLMPrompt{
		provider:      units.ProviderBucket(outcome.Provider),
		model:         units.ModelBucket(outcome.Model),
		promptKind:    units.SortedPromptKinds[0],
		promptVersion: units.Bounded(outcome.PromptVersion, units.PromptVersions, "other"),
		status:        units.Bounded(outcome.Status, units.CategorizationStatuses, "other"),
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.investmentLLM().outcomes[labels]++
}

func (labels investmentLLMCommon) metricLabels(extra ...metricLabel) []metricLabel {
	return append([]metricLabel{
		{"provider", labels.provider}, {"model", labels.model}, {"stage", labels.stage},
		{"prompt_kind", labels.promptKind}, {"prompt_version", labels.promptVersion},
	}, extra...)
}

func (collector *MetricsCollector) writeInvestmentLLM(output *strings.Builder) {
	state := collector.investmentLLMState
	if state == nil {
		state = &investmentLLMState{}
	}
	writeKeyed := func(name, help, extra string, series map[investmentLLMKeyed]uint64) {
		writeMetadata(output, name, help, "counter")
		keys := make([]investmentLLMKeyed, 0, len(series))
		for key := range series {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			return investmentLLMSortKey(keys[i].common, keys[i].value) < investmentLLMSortKey(keys[j].common, keys[j].value)
		})
		for _, key := range keys {
			writeUintSample(output, name, key.common.metricLabels(metricLabel{extra, key.value}), series[key])
		}
	}
	writeHist := func(name, help string, series map[investmentLLMCommon]*histogram) {
		writeMetadata(output, name, help, "histogram")
		keys := make([]investmentLLMCommon, 0, len(series))
		for key := range series {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool { return investmentLLMSortKey(keys[i], "") < investmentLLMSortKey(keys[j], "") })
		for _, key := range keys {
			writeHistogram(output, name, key.metricLabels(), series[key])
		}
	}
	writeKeyed(investmentLLMRequestsMetric, "Investment LLM requests by bounded provider, model, prompt, stage, and outcome", "outcome", state.requests)
	writeHist(investmentLLMDurationMetric, "Investment LLM request latency", state.duration)
	writeKeyed(investmentLLMErrorsMetric, "Investment LLM request failures by bounded family", "error_family", state.errors)
	writeKeyed(investmentLLMTokensMetric, "Investment LLM tokens", "direction", state.tokens)
	writeHist(investmentLLMOutputCharsMetric, "Investment LLM output characters", state.outputChars)
	writeKeyed(investmentLLMValidationMetric, "Investment categorization validation outcomes", "result", state.validation)
	writeKeyed(investmentLLMValidationFailMetric, "Investment categorization validation failures by bounded family", "error_family", state.validationFailed)
	writeMetadata(output, investmentLLMOutcomesMetric, "Terminal investment categorization outcomes", "counter")
	keys := make([]investmentLLMPrompt, 0, len(state.outcomes))
	for key := range state.outcomes {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return strings.Join([]string{keys[i].provider, keys[i].model, keys[i].promptKind, keys[i].promptVersion, keys[i].status}, "\x00") <
			strings.Join([]string{keys[j].provider, keys[j].model, keys[j].promptKind, keys[j].promptVersion, keys[j].status}, "\x00")
	})
	for _, key := range keys {
		writeUintSample(output, investmentLLMOutcomesMetric, []metricLabel{
			{"provider", key.provider}, {"model", key.model}, {"prompt_kind", key.promptKind},
			{"prompt_version", key.promptVersion}, {"status", key.status},
		}, state.outcomes[key])
	}
}

func investmentLLMSortKey(labels investmentLLMCommon, value string) string {
	return strings.Join([]string{labels.provider, labels.model, labels.stage, labels.promptKind, labels.promptVersion, value}, "\x00")
}

package decision

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// Completer is the decision backend as a categorize.BundleCompleter. Build one
// for each Execute (like the provider) and share it between goroutines: it
// holds no per-bundle state.
type Completer struct {
	rubric    *Rubric
	transport Transport
	model     string

	// corrupt is a test hook. It can change the payload before the adapter's
	// own check (stage "before_own_check": the own check must refuse it) or
	// after it (stage "after_own_check": the shared validation is then the only
	// barrier left).
	corrupt func(stage string, payload []byte) []byte
}

var _ categorize.BundleCompleter = (*Completer)(nil)

// NewCompleter builds the completer over a transport. model "" selects
// DefaultModel. It fails when the embedded rubric does not have the pinned
// digest (LoadRubric) or when there is no transport.
func NewCompleter(transport Transport, model string) (*Completer, error) {
	if transport == nil {
		return nil, errors.New("decision: no transport supplied")
	}
	rubric, err := LoadRubric()
	if err != nil {
		return nil, err
	}
	if model == "" {
		model = DefaultModel
	}
	return &Completer{rubric: rubric, transport: transport, model: model}, nil
}

// Model is the requested (pinned) model id.
func (c *Completer) Model() string { return c.model }

// Identity is the identity of every classification of this completer.
func (c *Completer) Identity() Identity { return IdentityFor(c.model) }

// ModelAccepted is the model id rule: the returned id equals the requested id
// or starts with the requested id + "-" (a dated suffix). Anything else is
// request_failed:model_mismatch. An empty requested id accepts nothing (an
// empty returned id then needs no clause of its own: it equals no requested id
// and has no prefix).
func ModelAccepted(requested, returned string) bool {
	return requested != "" && (returned == requested || strings.HasPrefix(returned, requested+"-"))
}

// Terminal is the typed error that ends a classification with no valid mix. It
// leaves CompleteBundle in place of text, so no refusal, missing answer,
// invalid value or failed request can reach the validator as a payload.
// categorize.CategorizeBundleOnce passes it through unchanged.
type Terminal struct {
	State    string
	Details  []string
	Warnings []string
	// Stop reports a deterministic failure (bad or absent credentials, unknown
	// model, exhausted quota): it recurs on every request, so the caller stops
	// asking for the rest of the run.
	Stop bool
	// FailureClass is categorize.FailureClass of the transport error of a
	// request_failed state ("" for every other state). Read it here, not from
	// categorize.FailureClass(terminal): that function reads the error text
	// when the transport error is not the package's own typed error.
	FailureClass string
	// InputTokens, OutputTokens and ModelReturned are the usage and the model
	// of the response the state was decided from: that response was paid for.
	InputTokens   int
	OutputTokens  int
	ModelReturned string

	cause error
}

func (t *Terminal) Error() string {
	return "decision terminal state " + t.State + ": " + strings.Join(t.Details, ",")
}

// Unwrap returns the transport error of a request_failed state, so errors.Is
// and errors.As reach it through the terminal.
func (t *Terminal) Unwrap() error { return t.cause }

// Status is the production status of the terminal state.
func (t *Terminal) Status() string { return StatusForState(t.State) }

// CompleteBundle implements categorize.BundleCompleter: one request, the typed
// answers interpreted to generative-schema text, or a *Terminal. A cancelled
// context returns the context error, never a terminal.
func (c *Completer) CompleteBundle(ctx context.Context, bundle units.TextBundle) (categorize.CompletionResult, error) {
	return (&call{c: c}).CompleteBundle(ctx, bundle)
}

// call is one classification. It records the interpretation for Classify and
// refuses a second ask.
type call struct {
	c      *Completer
	calls  int
	interp *Interpretation
}

func (k *call) CompleteBundle(ctx context.Context, bundle units.TextBundle) (categorize.CompletionResult, error) {
	k.calls++
	if k.calls > 1 {
		return categorize.CompletionResult{}, &Terminal{State: StateAdapterDefect, Details: []string{"adapter_defect:second_ask"}}
	}
	c := k.c
	built, err := BuildRequest(c.rubric, c.model, bundle)
	if err != nil {
		return categorize.CompletionResult{}, &Terminal{State: StateAdapterDefect, Details: []string{"adapter_defect:build:" + err.Error()}}
	}
	body, _, err := c.transport.PostSystemOne(ctx, built.Body)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return categorize.CompletionResult{}, ctxErr
		}
		class := categorize.FailureClass(err)
		return categorize.CompletionResult{}, &Terminal{State: StateRequestFailed,
			Details: []string{"request_failed:" + class}, FailureClass: class,
			Stop: categorize.IsDeterministicFailure(err), cause: err}
	}
	typed, err := ParseResponse(body, ExpectedQuestions(c.rubric, built.Spans))
	if err != nil {
		_, usage := usageFromBody(body)
		return categorize.CompletionResult{}, &Terminal{State: StateRequestFailed, Details: []string{"request_failed:not_json"},
			InputTokens: int(usage.InputTokens), OutputTokens: int(usage.OutputTokens)}
	}
	in, out := int(typed.Usage.InputTokens), int(typed.Usage.OutputTokens)
	if !ModelAccepted(c.model, typed.ReturnedModel) {
		return categorize.CompletionResult{}, &Terminal{State: StateRequestFailed, Details: []string{"request_failed:model_mismatch:" + typed.ReturnedModel},
			InputTokens: in, OutputTokens: out, ModelReturned: typed.ReturnedModel}
	}
	interp := Interpret(c.rubric, bundle, built.Spans, typed)
	k.interp = &interp
	if interp.State != StateOK {
		return categorize.CompletionResult{}, &Terminal{State: interp.State, Details: interp.Details, Warnings: interp.Warnings,
			InputTokens: in, OutputTokens: out, ModelReturned: typed.ReturnedModel}
	}

	// Own check: the payload must pass the production parser and validator. A
	// failure here is a defect of the adapter, not a model error. It is not the
	// only validation: CategorizeBundleOnce runs the shared one after it.
	text := interp.Payload
	if c.corrupt != nil {
		text = c.corrupt("before_own_check", text)
	}
	payload, parseErrs := categorize.ParseLLMJSON(string(text))
	if len(parseErrs) > 0 {
		return categorize.CompletionResult{}, &Terminal{State: StateAdapterDefect, Details: append([]string{"adapter_defect:self_check_parse"}, parseErrs...),
			InputTokens: in, OutputTokens: out, ModelReturned: typed.ReturnedModel}
	}
	if v := categorize.ValidateLLMPayload(payload, bundle.SourceTexts, bundle.HandleMap); !v.OK {
		return categorize.CompletionResult{}, &Terminal{State: StateAdapterDefect, Details: append([]string{"adapter_defect:self_check"}, v.Errors...),
			InputTokens: in, OutputTokens: out, ModelReturned: typed.ReturnedModel}
	}
	if c.corrupt != nil {
		text = c.corrupt("after_own_check", text)
	}
	return categorize.CompletionResult{Text: string(text), InputTokens: &in, OutputTokens: &out, Model: typed.ReturnedModel}, nil
}

// Classification is the full result of one decision classification: what a
// shadow (or, later, a served) row is written from. The replay oracle compares
// every field of this type, found by reflection; a new field is compared the
// day it is added.
type Classification struct {
	// State is one of the nine states; Status is the production status it maps to.
	State  string `json:"state"`
	Status string `json:"status"`
	// CompleteStrict: state ok, no degraded answer, no evidence warning, no
	// unanswered sufficiency question.
	CompleteStrict bool `json:"complete_strict"`
	// Levels holds the level of every support key with a usable answer (all 15
	// for ok, zero_support and the evidence states).
	Levels map[string]int `json:"levels"`
	// LevelProbabilities is the renormalised level distribution of every valid
	// support answer.
	LevelProbabilities map[string][]float64 `json:"level_probabilities"`
	// SufficiencyLevel is 0..n-1, or -1 when the question had no usable answer.
	SufficiencyLevel int `json:"sufficiency_level"`
	// Subcategories is the validated, normalised 15-key mix of an ok
	// classification. It is EMPTY for every other state: a failure has no mix,
	// and no fallback prior is put here to look like one.
	Subcategories map[string]float64 `json:"subcategories"`
	// EvidenceQuotes, EvidenceSpanID and EvidenceHandle are the cited evidence
	// of an ok classification.
	EvidenceQuotes []categorize.EvidenceQuote `json:"evidence_quotes"`
	EvidenceSpanID string                     `json:"evidence_span_id"`
	EvidenceHandle string                     `json:"evidence_handle"`
	// Uncertainty is the uncertainty sentence of an ok classification.
	Uncertainty string `json:"uncertainty"`
	// Warnings are the adapter warnings followed by the validator warnings.
	Warnings []string `json:"warnings"`
	// Errors is empty for ok; else the status, "decision_<state>" and the
	// detail codes.
	Errors []string `json:"errors"`
	// LLMCalls is 1 when a request was attempted.
	LLMCalls      int    `json:"llm_calls"`
	InputTokens   int    `json:"input_tokens"`
	OutputTokens  int    `json:"output_tokens"`
	ModelReturned string `json:"model_returned"`
	// Stop reports a deterministic failure (see Terminal.Stop).
	Stop bool `json:"stop"`
}

// Classify runs one classification through categorize.CategorizeBundleOnce
// (the shared validation, no repair) and returns it with its state. Every
// state is a returned Classification; the error is non-nil only for a
// cancelled context. The pre-call gate (minimum text, a text source) is the
// caller's: Classify sends whatever bundle it gets.
func (c *Completer) Classify(ctx context.Context, bundle units.TextBundle) (Classification, error) {
	k := &call{c: c}
	outcome, err := categorize.CategorizeBundleOnce(ctx, bundle, k)
	out := Classification{
		Levels: map[string]int{}, LevelProbabilities: map[string][]float64{}, SufficiencyLevel: -1,
		Subcategories: map[string]float64{}, EvidenceQuotes: []categorize.EvidenceQuote{},
		Warnings: []string{}, Errors: []string{}, LLMCalls: k.calls,
	}
	if in := k.interp; in != nil {
		out.Levels, out.LevelProbabilities = in.Levels, in.LevelProbs
		if in.SufficiencyLevel != nil {
			out.SufficiencyLevel = *in.SufficiencyLevel
		}
	}
	fail := func(t *Terminal) Classification {
		out.State, out.Status = t.State, t.Status()
		out.Errors = append([]string{t.Status(), "decision_" + t.State}, t.Details...)
		out.Warnings = append(out.Warnings, t.Warnings...)
		out.InputTokens, out.OutputTokens, out.ModelReturned, out.Stop = t.InputTokens, t.OutputTokens, t.ModelReturned, t.Stop
		return out
	}
	var term *Terminal
	switch {
	case errors.As(err, &term):
		return fail(term), nil
	case err != nil:
		return Classification{}, err
	case outcome.Status != categorize.StatusOK:
		// The adapter's own check passed and the shared validation did not:
		// the two disagree, which is a defect of the adapter. The response was
		// paid for, so its tokens stay.
		return fail(&Terminal{State: StateAdapterDefect,
			Details:     append([]string{fmt.Sprintf("adapter_defect:shared_validation:%s", outcome.Status)}, outcome.Errors...),
			InputTokens: outcome.InputTokens, OutputTokens: outcome.OutputTokens, ModelReturned: outcome.LLMModel}), nil
	}
	in := k.interp
	out.State, out.Status, out.CompleteStrict = StateOK, outcome.Status, in.CompleteStrict
	out.Subcategories, out.EvidenceQuotes, out.Uncertainty = outcome.Subcategories, outcome.EvidenceQuotes, outcome.Uncertainty
	out.EvidenceSpanID, out.EvidenceHandle = in.EvidenceSpanID, SpanHandle(in.EvidenceSpanID)
	out.Warnings = append(append(out.Warnings, in.Warnings...), outcome.Warnings...)
	out.InputTokens, out.OutputTokens, out.ModelReturned = outcome.InputTokens, outcome.OutputTokens, outcome.LLMModel
	return out, nil
}

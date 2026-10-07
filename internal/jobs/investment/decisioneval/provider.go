package decisioneval

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// Backend is one decision backend: how to build its request, parse its typed
// answers, and where to send them.
type Backend struct {
	Arm      string
	Provider string
	APIMode  string
	Endpoint string
	Model    string
	APIKey   secrets.Hidden
	// AcceptedModels are returned model ids accepted besides Model. Design:
	// a returned model that differs from the pinned one is
	// request_failed:model_mismatch.
	AcceptedModels []string

	build func(*Rubric, string, units.TextBundle) (BuiltRequest, error)
	parse func([]byte, []ExpectedQuestion) (Typed, error)
}

// NewJevBackend builds the TypeSafe backend. Model "" selects the pinned
// default.
func NewJevBackend(endpoint string, token secrets.Hidden, model string) Backend {
	if model == "" {
		model = DefaultJevModel
	}
	return Backend{Arm: ArmJev, Provider: ProviderTypeSafe, APIMode: APIModeSystemOne, Endpoint: endpoint, Model: model, APIKey: token,
		build: BuildJevRequest, parse: ParseJevResponse}
}

// NewDecisionsBackend builds the OpenAI Decisions backend.
func NewDecisionsBackend(endpoint string, key secrets.Hidden, model string) Backend {
	if model == "" {
		model = DefaultLunaModel
	}
	return Backend{Arm: ArmDecisions, Provider: ProviderOpenAI, APIMode: APIModeDecisions, Endpoint: endpoint, Model: model, APIKey: key,
		build: BuildDecisionsRequest, parse: ParseDecisionsResponse}
}

// BuildRequest renders the request of a bundle.
func (b Backend) BuildRequest(r *Rubric, bundle units.TextBundle) (BuiltRequest, error) {
	return b.build(r, b.Model, bundle)
}

func (b Backend) modelAccepted(returned string) bool {
	return returned != "" && (returned == b.Model || slices.Contains(b.AcceptedModels, returned))
}

// ModelVersionStamp is the experiment stamp of an arm. It is never written to
// ClickHouse.
func ModelVersionStamp(provider, api, model string, v Versions) string {
	return fmt.Sprintf("provider=%s;api=%s;model=%s;taxonomy=%s;prompt=%s;adapter=%s;map=%s",
		provider, api, model, v.Taxonomy, v.Rubric, v.Adapter, v.Map)
}

// DecisionTerminal is the typed terminal error of a candidate arm. It leaves
// the adapter instead of a payload, so the generative repair path cannot turn a
// refusal, a missing answer or an invalid value into a valid mix.
type DecisionTerminal struct {
	State    string
	Details  []string
	Warnings []string
	// StopArm names a reason the whole arm must stop (auth, budget).
	StopArm string
}

func (t *DecisionTerminal) Error() string {
	return "decision terminal state " + t.State + ": " + strings.Join(t.Details, ",")
}

// Status is the production status of the terminal state.
func (t *DecisionTerminal) Status() string { return StatusForState(t.State) }

// SendFailure describes a request that produced no usable response.
type SendFailure struct {
	Class   string
	Detail  string
	StopArm string
}

// SendResult is the outcome of sending one request, retries included.
type SendResult struct {
	Status int
	Body   []byte
	Header http.Header
	Fail   *SendFailure
}

// Sender sends a built request. HTTPSender is the live one; ReplaySender serves
// stored responses.
type Sender interface {
	Send(ctx context.Context, b Backend, built BuiltRequest) SendResult
}

// HTTPSender sends over HTTP with the retry policy of design.md 4.2.
type HTTPSender struct {
	Client *http.Client
	// Sleep waits for a retry; it reports false when ctx ended first. Tests
	// replace it; nil waits for real.
	Sleep func(ctx context.Context, d time.Duration) bool
	// DefaultRetryWait applies when the response has no retry-after (2 s).
	DefaultRetryWait time.Duration
}

const (
	maxRetryAfter    = 60 * time.Second
	defaultRetryWait = 2 * time.Second
	// MaxRequestAttempts is 1 request + 1 retry (same bound as the incumbent's
	// openAIMaxRetries = 1).
	MaxRequestAttempts = 2
)

func (s *HTTPSender) sleep(ctx context.Context, d time.Duration) bool {
	if s.Sleep != nil {
		return s.Sleep(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func retryWait(h http.Header, fallback time.Duration) time.Duration {
	if v := h.Get("Retry-After"); v != "" {
		if secs, err := strconv.ParseFloat(v, 64); err == nil && secs >= 0 {
			d := time.Duration(secs * float64(time.Second))
			if d > maxRetryAfter {
				d = maxRetryAfter
			}
			return d
		}
	}
	if fallback <= 0 {
		fallback = defaultRetryWait
	}
	return fallback
}

// Send sends the request, with at most one retry of the same body on 429, 529,
// 5xx or a timeout. 401, 402 and 403 stop the arm; 400 and 422 do not retry.
func (s *HTTPSender) Send(ctx context.Context, b Backend, built BuiltRequest) SendResult {
	var last SendResult
	for attempt := 1; attempt <= MaxRequestAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.Endpoint, bytes.NewReader(built.Body))
		if err != nil {
			return SendResult{Fail: &SendFailure{Class: "request_build", Detail: err.Error()}}
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+b.APIKey.Reveal())
		resp, err := s.Client.Do(req)
		retryable := false
		wait := time.Duration(0)
		switch {
		case err != nil && errors.Is(err, ErrBudgetRefused):
			return SendResult{Fail: &SendFailure{Class: "budget_refused", Detail: err.Error(), StopArm: "budget"}}
		case err != nil:
			if ctx.Err() != nil {
				return SendResult{Fail: &SendFailure{Class: "canceled", Detail: ctx.Err().Error()}}
			}
			class := transportClass(err)
			last = SendResult{Fail: &SendFailure{Class: class, Detail: scrubError(err)}}
			retryable = true
			wait = s.DefaultRetryWait
		default:
			body, readErr := readAll(resp)
			res := SendResult{Status: resp.StatusCode, Body: body, Header: resp.Header}
			switch {
			case readErr != nil:
				res.Fail = &SendFailure{Class: "body_read_failed", Detail: readErr.Error()}
				return res
			case resp.StatusCode == http.StatusOK:
				return res
			case resp.StatusCode == 401 || resp.StatusCode == 402 || resp.StatusCode == 403:
				res.Fail = &SendFailure{Class: httpClass(resp.StatusCode), StopArm: "auth_" + strconv.Itoa(resp.StatusCode)}
				return res
			case resp.StatusCode == 429 || resp.StatusCode == 529 || resp.StatusCode >= 500:
				res.Fail = &SendFailure{Class: httpClass(resp.StatusCode)}
				last = res
				retryable = true
				wait = retryWait(resp.Header, s.DefaultRetryWait)
			default:
				res.Fail = &SendFailure{Class: httpClass(resp.StatusCode)}
				return res
			}
		}
		if !retryable || attempt == MaxRequestAttempts {
			return last
		}
		if !s.sleep(ctx, wait) {
			return SendResult{Fail: &SendFailure{Class: "canceled", Detail: ctx.Err().Error()}}
		}
	}
	return last
}

func readAll(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	var buf bytes.Buffer
	_, err := buf.ReadFrom(resp.Body)
	return buf.Bytes(), err
}

// scrubError returns an error text that holds no credential: only the
// error chain's own text, never a request header.
func scrubError(err error) string {
	return err.Error()
}

// DecisionProvider implements categorize.Provider for one bundle. The real
// CategorizeTextBundle and ValidateLLMPayload run unchanged over it.
type DecisionProvider struct {
	Rubric  *Rubric
	Weights []float64
	Backend Backend
	Sender  Sender

	source units.TextBundle
	col    *Collector

	calls  int
	built  *BuiltRequest
	result SendResult
	interp *Interpretation

	// postSelfCheck is a test hook: it can corrupt the payload after the
	// adapter self-check so that the production validator rejects it.
	postSelfCheck func([]byte) []byte
}

var _ categorize.Provider = (*DecisionProvider)(nil)

// Model is the requested (pinned) model id.
func (p *DecisionProvider) Model() string { return p.Backend.Model }

// Close is a no-op: the provider holds no connection.
func (p *DecisionProvider) Close() error { return nil }

func terminal(state string, details ...string) error {
	return &DecisionTerminal{State: state, Details: details}
}

// Complete is called by CategorizeTextBundle. Steps in the order of design.md
// 1.1. The adapter never answers a repair prompt.
func (p *DecisionProvider) Complete(ctx context.Context, request categorize.CompletionRequest) (categorize.CompletionResult, error) {
	p.calls++
	if p.calls > 1 {
		return categorize.CompletionResult{}, terminal(StateAdapterDefect, "adapter_defect:repair_entered")
	}
	if request.Prompt != categorize.BuildPrompt(p.source.SourceBlock) {
		return categorize.CompletionResult{}, terminal(StateAdapterDefect, "adapter_defect:prompt_mismatch")
	}
	built, err := p.Backend.BuildRequest(p.Rubric, p.source)
	if err != nil {
		return categorize.CompletionResult{}, terminal(StateAdapterDefect, "adapter_defect:build:"+err.Error())
	}
	p.built = &built
	if p.col != nil {
		p.col.Meta.QuestionCount = built.QuestionCount()
		p.col.Meta.SpanCount = len(built.Spans)
		p.col.Meta.SpansDropped = built.SpansDropped
		p.col.Meta.DelimiterCollision = built.DelimiterCollision
	}

	res := p.Sender.Send(ctx, p.Backend, built)
	p.result = res
	if res.Fail != nil {
		t := &DecisionTerminal{State: StateRequestFailed, Details: []string{"request_failed:" + res.Fail.Class}, StopArm: res.Fail.StopArm}
		if res.Fail.Class == "canceled" {
			return categorize.CompletionResult{}, ctx.Err()
		}
		return categorize.CompletionResult{}, t
	}
	expected := ExpectedQuestions(p.Rubric, built.Spans)
	typed, err := p.Backend.parse(res.Body, expected)
	if err != nil {
		return categorize.CompletionResult{}, terminal(StateRequestFailed, "request_failed:not_json")
	}
	if !p.Backend.modelAccepted(typed.ReturnedModel) {
		return categorize.CompletionResult{}, terminal(StateRequestFailed, "request_failed:model_mismatch:"+typed.ReturnedModel)
	}
	interp := Interpret(p.Rubric, p.Weights, p.Backend.Provider, p.source, built.Spans, typed)
	p.interp = &interp
	if interp.State != StateOK {
		return categorize.CompletionResult{}, &DecisionTerminal{State: interp.State, Details: interp.Details, Warnings: interp.Warnings}
	}

	// Self-check (step 6): the payload must pass the production parser and
	// validator. A failure here is a defect of the adapter, not a model error.
	payload, parseErrs := categorize.ParseLLMJSON(string(interp.Payload))
	if len(parseErrs) > 0 {
		return categorize.CompletionResult{}, terminal(StateAdapterDefect, append([]string{"adapter_defect:self_check_parse"}, parseErrs...)...)
	}
	if v := categorize.ValidateLLMPayload(payload, p.source.SourceTexts, p.source.HandleMap); !v.OK {
		return categorize.CompletionResult{}, terminal(StateAdapterDefect, append([]string{"adapter_defect:self_check"}, v.Errors...)...)
	}
	text := interp.Payload
	if p.postSelfCheck != nil {
		text = p.postSelfCheck(text)
	}
	in, out := int(typed.Usage.InputTokens), int(typed.Usage.OutputTokens)
	return categorize.CompletionResult{Text: string(text), InputTokens: &in, OutputTokens: &out, Model: typed.ReturnedModel}, nil
}

// Classification is the result of one candidate classification.
type Classification struct {
	Outcome  categorize.CategorizationOutcome
	State    string
	Terminal *DecisionTerminal
	Interp   *Interpretation
	Built    *BuiltRequest
	Send     SendResult
}

// DecisionDeps are the inputs of DecisionCategorize.
type DecisionDeps struct {
	Rubric  *Rubric
	Weights []float64
	Backend Backend
	Sender  Sender
	// Collector is optional (nil in a replay); the provider writes the request
	// shape into its meta before the send.
	Collector *Collector

	postSelfCheck func([]byte) []byte
}

// DecisionCategorize is the only entry point of a candidate arm. It calls the
// real CategorizeTextBundle with the adapter as provider. A *DecisionTerminal
// becomes categorize.FallbackOutcome(status) with the state code in Errors,
// the same move materialize.go makes for a failed task. The wrapper adds no
// other logic. The pre-call gate is not applied here: the caller applies
// GateStatus before any arm.
func DecisionCategorize(ctx context.Context, bundle units.TextBundle, deps DecisionDeps) (Classification, error) {
	p := &DecisionProvider{Rubric: deps.Rubric, Weights: deps.Weights, Backend: deps.Backend, Sender: deps.Sender,
		source: bundle, col: deps.Collector, postSelfCheck: deps.postSelfCheck}
	if deps.Collector != nil {
		ctx = WithCollector(ctx, deps.Collector)
	}
	outcome, err := categorize.CategorizeTextBundle(ctx, bundle, categorize.CategorizeOptions{
		Provider: p, ProviderName: deps.Backend.Provider, Model: deps.Backend.Model,
	})
	c := Classification{Interp: p.interp, Built: p.built, Send: p.result}
	var term *DecisionTerminal
	switch {
	case errors.As(err, &term):
		c.Terminal, c.State = term, term.State
		outcome = categorize.FallbackOutcome(term.Status())
		outcome.Errors = append([]string{term.Status(), "decision_" + term.State}, term.Details...)
		outcome.Warnings = term.Warnings
		outcome.LLMCalls = p.calls
	case err != nil:
		return c, err
	default:
		c.State = StateOK
		if outcome.Status != categorize.StatusOK {
			// repaired is unreachable by design; if it ever appears the run is
			// invalid.
			c.State = StateAdapterDefect
			c.Terminal = &DecisionTerminal{State: StateAdapterDefect, Details: []string{"adapter_defect:unexpected_status:" + outcome.Status}}
			outcome = categorize.FallbackOutcome(c.Terminal.Status())
			outcome.Errors = append([]string{c.Terminal.Status(), "decision_adapter_defect"}, c.Terminal.Details...)
		} else if c.Interp != nil {
			outcome.Warnings = append(append([]string(nil), c.Interp.Warnings...), outcome.Warnings...)
		}
	}
	c.Outcome = outcome
	return c, nil
}

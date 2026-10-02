package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/full-chaos/dev-health-ops/internal/httpguard"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// This file is a SMALL, PURPOSE-BUILT OpenAI Chat Completions client for
// exactly one wire exchange: the readiness preflight's fixed two-round tool
// call (settings.py:352-466 -> readiness.py's AgentReadinessService.certify,
// readiness.py:155-230, over openai_compatible.py's
// OpenAICompatibleAgentProvider.decide/build_completion_request). It is NOT
// a general Ask Dev agent adapter -- it hardcodes the one tool
// ("readiness_echo.v1") and the one response schema readiness.py's probe
// ever sends, because that is the only shape this route ever needs. A
// general-purpose Ask Dev tool-calling client (arbitrary tools/schemas,
// capability negotiation for streaming/JSON modes) is Ask Dev agent
// machinery out of this port's scope (CHAOS-6976 design ruling D2839).
//
// PINNED DIVERGENCE (D2839 item 2): the persisted AgentReadinessRecord's
// `fingerprint` is computed from credential-only inputs (source, provider,
// model, base_url, a credential hash, READINESS_VERSION) -- see
// readinessFingerprint in llmsettingsreadiness.go -- not Python's full
// production_runtime._readiness_fingerprint, which additionally folds
// TOOL_CONTRACT_VERSION/BUDGET_POLICY_VERSION/_canonical_contract_digest()/
// _wire_request_digest() (all Ask Dev tool-registry/prompt-contract inputs,
// production_runtime.py:1045-1097). A record this route writes will never
// equal what Python's own resolve_production_provider would compute as
// "current" for live Ask Dev selection -- irrelevant today since prod Ask
// Dev is off, and the outcome/safe_error_code/checked_at fields this route
// writes and GET /status reads back are unaffected. `readiness_version`
// still carries the real openai_compatible.READINESS_VERSION literal
// ("ask-dev-agent-v3") for field-shape parity, even though it is not folded
// into this fingerprint.

// askDevReadinessVersion mirrors openai_compatible.py's READINESS_VERSION.
// A field-shape constant only here -- not an input to readinessFingerprint
// (see the PINNED DIVERGENCE note above).
const askDevReadinessVersion = "ask-dev-agent-v3"

// readinessEchoWireName is sanitize_tool_name of readiness.py's
// READINESS_ECHO_TOOL_ID ("readiness_echo.v1") (openai_capabilities.py):
// every character outside [a-zA-Z0-9_-] (just the dot here) replaced with
// "_".
const readinessEchoWireName = "readiness_echo_v1"

// readinessMaxOutputTokens is readiness.py's READINESS_MAX_OUTPUT_TOKENS.
// model_family_budget's reasoning_headroom_tokens is pinned at 0 for every
// family (budget_policy.py), so request_max_completion_tokens(cap) == cap
// unconditionally -- this constant IS the wire max_completion_tokens value,
// with no per-family addition to replicate.
const readinessMaxOutputTokens = 512

// readinessNonceValue is the fixed literal both probe rounds exchange.
const readinessNonceValue = "ready-v1"

// readinessToolParametersSchema is TOOL_PARAMS: _structural_schema of
// readiness.py's tool.input_schema ({"type":"object",
// "additionalProperties":false,"required":["nonce"],
// "properties":{"nonce":{"type":"string"}}}), computed by hand-running
// openai_compatible.py's _structural_schema over that literal input (its
// shape never varies -- this probe's tool schema is fixed, not
// caller-supplied) and pinned verbatim rather than re-derived at request
// time.
const readinessToolParametersSchema = `{"additionalProperties":false,"properties":{"nonce":{"type":"string"}},"required":["nonce"],"type":"object"}`

// readinessDecisionResponseSchema is DECISION_SCHEMA: openai_compatible.py's
// _decision_response_schema(_structural_schema(readiness.py's
// response_schema), tools=[], allow_final_answer=True), then
// _structural_schema'd again (decision_response_schema's own return path).
// Fixed for the same reason as readinessToolParametersSchema -- round 2 of
// this probe always sends tools=[] and this exact response_schema, so the
// schema this route needs never varies with caller input either.
const readinessDecisionResponseSchema = `{"additionalProperties":false,"properties":{"arguments":{"type":"null"},"call_id":{"anyOf":[{"type":"string"},{"type":"null"}]},"candidates":{"anyOf":[{"items":{"type":"string"},"type":"array"},{"type":"null"}]},"code":{"type":["string","null"]},"kind":{"enum":["final_answer","disambiguation","refusal"],"type":"string"},"message":{"type":["string","null"]},"prompt":{"type":["string","null"]},"tool_id":{"enum":[null],"type":["string","null"]},"value":{"anyOf":[{"additionalProperties":false,"properties":{"nonce":{"const":"ready-v1","type":"string"}},"required":["nonce"],"type":"object"},{"type":"null"}]}},"required":["arguments","call_id","candidates","code","kind","message","prompt","tool_id","value"],"type":"object"}`

// readinessOutcomeReady is the same "ready" literal llmsettingsstatus.go's
// readinessOutcomeReady constant already names; reused directly (not
// redeclared) since both files are package admin.
const readinessOutcomeFailed = "failed"

// safe_error_code values, matching errors.py's AgentProviderErrorCode wire
// values (the persisted record's safe_error_code and the taxonomy
// llmSettingsStatusResponse.readinessSafeFailureMessage already switches
// over in llmsettingsstatus.go).
const (
	errCodeProviderNotConfigured     = "provider_not_configured"
	errCodeModelNotSupported         = "model_not_supported"
	errCodeProviderUnavailable       = "provider_unavailable"
	errCodeInvalidRequest            = "invalid_request"
	errCodeRateLimited               = "rate_limited"
	errCodeInvalidResponse           = "invalid_response"
	errCodeTimeout                   = "timeout"
	errCodeProviderContractViolation = "provider_contract_violation"
	errCodeOutputExhausted           = "output_exhausted"
)

// readinessProber is the seam production code depends on, so the probe
// mechanics (this file) can be swapped for a plain structured-completion
// check in one place without touching the route (D2839 item 1: chris has
// this open question; the default is the wire-faithful tool-calling probe
// below until he answers).
type readinessProber interface {
	probe(ctx context.Context, provider, model, baseURL, apiKey string) (outcome string, safeErrorCode *string)
}

// openAICompatibleReadinessProber is the default readinessProber: the real
// two-round readiness_echo tool-call exchange over HTTP, matching
// openai_compatible.py's build_completion_request/decide/_normalize_response
// for exactly this probe's fixed inputs.
type openAICompatibleReadinessProber struct {
	client providerfoundation.HTTPDoer
}

// newOpenAICompatibleReadinessProber follows the SAME doer-or-default shape
// as pagerduty_oauth_callback.go's pagerDutyClient: a non-nil doer (a
// route's Deps.HTTPDoer test override, threaded through as
// handlers.upstreamDoer -- see postLLMSettingsReadiness) is honored
// verbatim, so a route-level success-path test can inject a fake transport
// without a real network call; nil builds the real hardened client.
func newOpenAICompatibleReadinessProber(doer providerfoundation.HTTPDoer) *openAICompatibleReadinessProber {
	if doer != nil {
		// the saved API key rides these requests: a supplied *http.Client follows no redirect (D4124)
		return &openAICompatibleReadinessProber{client: doer}
	}
	return &openAICompatibleReadinessProber{client: &http.Client{
		// Codex r3 hardening-table audit (D3016, CHAOS-6976): matches
		// Python's make_hardened_async_httpx2_client's timeout=60.0
		// (providers/_http.py:27) exactly -- this value had never been
		// checked against the Python client it ports before now; 30s was
		// an unremarked, undocumented choice, not a deliberate divergence.
		Timeout: 60 * time.Second,
		// Codex r1 P1 (CHAOS-6976): Python's hardened client
		// (llm/providers/_http.py's make_hardened_*_client) sets
		// follow_redirects=False unconditionally -- this route validated the
		// saved base_url (SSRF-checked) but a redirect response is NOT the
		// validated destination and must never be followed automatically,
		// or a validated public host can 3xx this prober to an internal
		// target. ErrUseLastResponse returns the redirect response itself
		// (its non-2xx status) to the caller instead of following it, the
		// Go equivalent of httpx's follow_redirects=False.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		// Codex r3 P1 (CHAOS-6976): the SAME hardened client sets
		// trust_env=False alongside follow_redirects=False (_http.py:9-11)
		// -- httpx's trust_env controls whether it honors ambient
		// HTTP_PROXY/HTTPS_PROXY/NO_PROXY env vars. Go's zero-value
		// Transport (what an http.Client with no Transport set falls back
		// to, http.DefaultTransport) DOES honor them via
		// http.ProxyFromEnvironment. ValidateBaseURLChecked deliberately
		// treats an unresolvable hostname as NOT an SSRF target (this
		// route's own network call is the only thing that would actually
		// resolve/reach it) -- but a configured operator proxy has its own
		// DNS/network view and can resolve or reach a hostname the
		// application's own resolver could not, silently reopening the
		// exact class of target the SSRF gate exists to keep unreachable.
		// Proxy: nil is Go's trust_env=False: no proxy is ever consulted,
		// env-configured or otherwise.
		Transport: &http.Transport{Proxy: nil},
	}}
}

// chatMessage is one element of the wire "messages" array
// (OpenAICompatibleAgentProvider._message_payload).
type chatMessage struct {
	Role       string         `json:"role"`
	Content    string         `json:"content"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
	ToolCalls  []chatToolCall `json:"tool_calls,omitempty"`
}

type chatToolCall struct {
	ID       string               `json:"id"`
	Type     string               `json:"type"`
	Function chatToolCallFunction `json:"function"`
}

type chatToolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type chatTool struct {
	Type     string           `json:"type"`
	Function chatToolFunction `json:"function"`
}

type chatToolFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
	Strict      bool            `json:"strict"`
}

type responseFormatJSONSchema struct {
	Name   string          `json:"name"`
	Strict bool            `json:"strict"`
	Schema json.RawMessage `json:"schema"`
}

type responseFormat struct {
	Type       string                   `json:"type"`
	JSONSchema responseFormatJSONSchema `json:"json_schema"`
}

// chatCompletionRequest is the complete wire request body, mirroring
// build_completion_request's keys exactly (openai_compatible.py:970-1048).
type chatCompletionRequest struct {
	Model               string          `json:"model"`
	Messages            []chatMessage   `json:"messages"`
	Tools               []chatTool      `json:"tools,omitempty"`
	ToolChoice          string          `json:"tool_choice,omitempty"`
	MaxCompletionTokens int             `json:"max_completion_tokens"`
	ParallelToolCalls   *bool           `json:"parallel_tool_calls,omitempty"`
	Temperature         *float64        `json:"temperature,omitempty"`
	ReasoningEffort     string          `json:"reasoning_effort,omitempty"`
	ResponseFormat      *responseFormat `json:"response_format,omitempty"`
}

type chatResponseMessage struct {
	Content   *string        `json:"content"`
	ToolCalls []chatToolCall `json:"tool_calls"`
}

type chatChoice struct {
	Message      chatResponseMessage `json:"message"`
	FinishReason string              `json:"finish_reason"`
}

type chatCompletionResponse struct {
	Choices []chatChoice `json:"choices"`
}

// supportsTemperature mirrors openai_capabilities.py's supports_temperature.
func supportsTemperature(model string) bool {
	m := strings.ToLower(strings.TrimSpace(model))
	return !(strings.HasPrefix(m, "gpt-5") || strings.HasPrefix(m, "o1") || strings.HasPrefix(m, "o3"))
}

// reasoningEffort mirrors chat_completion_reasoning_effort: "minimal" for
// every gpt-5* model, else no field at all.
func reasoningEffort(model string) string {
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(model)), "gpt-5") {
		return "minimal"
	}
	return ""
}

// supportsParallelToolCalls mirrors openai_capabilities.py's
// supports_parallel_tool_calls: every model except the true o-series
// (o1/o3/o4) accepts the wire control.
func supportsParallelToolCalls(model string) bool {
	m := strings.ToLower(strings.TrimSpace(model))
	return !(strings.HasPrefix(m, "o1") || strings.HasPrefix(m, "o3") || strings.HasPrefix(m, "o4"))
}

// endpoint is the chat/completions URL for baseURL, defaulting to OpenAI's
// own API the way the openai Python/wire client does for an empty base_url.
func chatCompletionsEndpoint(baseURL string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if trimmed == "" {
		trimmed = "https://api.openai.com/v1"
	}
	return trimmed + "/chat/completions"
}

// providerFailure is a classified probe failure: a safe_error_code plus an
// unexported reason kept only for the caller's own logs (never persisted or
// returned to the client -- the taxonomy in llmsettingsstatus.go is what
// the admin surface shows).
type providerFailure struct {
	code string
}

func (f *providerFailure) Error() string { return f.code }

func newProviderFailure(code string) *providerFailure { return &providerFailure{code: code} }

// classifyHTTPFailure maps a non-2xx HTTP response to a safe_error_code,
// mirroring llm/errors.py's classify_provider_error's status-code-driven
// branches (msg_lower keyword branches are a Python message-text fallback
// this port does not need: the OpenAI wire API's status codes already carry
// the same information a Go caller can observe directly).
func classifyHTTPFailure(status int, body []byte) *providerFailure {
	lower := strings.ToLower(string(body))
	switch {
	// Codex r1 P1 (CHAOS-6976): errors.py's classify_provider_error checks
	// quota exhaustion FIRST, before every other branch including auth and
	// rate-limit -- a 429 carrying "insufficient_quota"/"current quota" is
	// LLMAuthError (-> provider_not_configured), never rate_limited, even
	// though 429 is also this function's own rate-limit status code below.
	// This case must stay ordered before the 401/429 cases or a quota 429
	// is misclassified as an ordinary transient rate limit.
	case strings.Contains(lower, "insufficient_quota") || strings.Contains(lower, "current quota"):
		return newProviderFailure(errCodeProviderNotConfigured)
	// errors.py checks model-not-found BEFORE auth: a 401 whose body names
	// model_not_found is a model error (executed against the pinned build,
	// CHAOS-7303: the 401 + model_not_found scenario).
	case strings.Contains(lower, "model_not_found") || strings.Contains(lower, "model not found") || strings.Contains(lower, "model does not exist"):
		return newProviderFailure(errCodeModelNotSupported)
	case status == 401 || strings.Contains(lower, "invalid_api_key") || strings.Contains(lower, "authentication"):
		return newProviderFailure(errCodeProviderNotConfigured)
	case status == 429 || strings.Contains(lower, "rate_limit") || strings.Contains(lower, "too many requests"):
		return newProviderFailure(errCodeRateLimited)
	case status == 400 || status == 422 || strings.Contains(lower, "invalid_request_error") || strings.Contains(lower, "unsupported parameter") || strings.Contains(lower, "unsupported value"):
		return newProviderFailure(errCodeInvalidRequest)
	default:
		// Every other 4xx/5xx (incl. 404 model-not-found without the
		// keyword, and every 5xx) falls to the same catch-all Python's
		// safe_agent_provider_error uses for LLMServerError and any other
		// unclassified LLMError: PROVIDER_UNAVAILABLE.
		return newProviderFailure(errCodeProviderUnavailable)
	}
}

// classifyTransportFailure maps a transport-level error (no HTTP response at
// all) to a safe_error_code: a context deadline is Python's LLMTimeoutError
// path; everything else (connection refused, DNS, TLS) is
// LLMTransportError, both of which land on TIMEOUT / PROVIDER_UNAVAILABLE
// respectively in safe_agent_provider_error.
func classifyTransportFailure(err error) *providerFailure {
	if errors.Is(err, context.DeadlineExceeded) {
		return newProviderFailure(errCodeTimeout)
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return newProviderFailure(errCodeTimeout)
	}
	return newProviderFailure(errCodeProviderUnavailable)
}

// maxProbeAttempts is the openai Python SDK's default retry budget
// (max_retries=2, so 3 total attempts) -- codex r1 P1 (CHAOS-6976): a
// single transient failure (a 500, a rate limit, a timeout) must not
// immediately fail the whole preflight when Python's client would have
// retried and likely succeeded.
const maxProbeAttempts = 3

// isRetryableFailureCode is the SDK's retryable class, narrowed to the
// safe_error_codes this port can produce: a 5xx/timeout/transport failure
// (provider_unavailable, timeout) and a rate limit (429, rate_limited) are
// retried; a permanent/config/contract failure (bad credentials, an
// unsupported model, a malformed request or response, a sequential-tool
// violation, output exhaustion) is not -- retrying those would only waste
// the same outcome three times.
func isRetryableFailureCode(code string) bool {
	switch code {
	case errCodeProviderUnavailable, errCodeRateLimited, errCodeTimeout:
		return true
	default:
		return false
	}
}

// doCompletion retries doCompletionOnce up to maxProbeAttempts times on a
// retryable failure, with a short linear backoff between attempts
// (bounded by the context deadline/cancellation, never Python's exact
// jittered-exponential timing -- the finding this fixes is about retry
// COUNT, not backoff shape).
func (p *openAICompatibleReadinessProber) doCompletion(
	ctx context.Context, baseURL, apiKey string, req chatCompletionRequest,
) (*chatCompletionResponse, *providerFailure) {
	var lastFailure *providerFailure
	for attempt := 0; attempt < maxProbeAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, classifyTransportFailure(ctx.Err())
			case <-time.After(time.Duration(attempt) * 200 * time.Millisecond):
			}
		}
		resp, failure := p.doCompletionOnce(ctx, baseURL, apiKey, req)
		if failure == nil {
			return resp, nil
		}
		lastFailure = failure
		if !isRetryableFailureCode(failure.code) {
			return nil, failure
		}
	}
	return nil, lastFailure
}

// doCompletionOnce posts one chat/completions request and returns the
// parsed response, or a classified providerFailure.
func (p *openAICompatibleReadinessProber) doCompletionOnce(
	ctx context.Context, baseURL, apiKey string, req chatCompletionRequest,
) (*chatCompletionResponse, *providerFailure) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, newProviderFailure(errCodeInvalidResponse)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, chatCompletionsEndpoint(baseURL), bytes.NewReader(body))
	if err != nil {
		return nil, newProviderFailure(errCodeProviderUnavailable)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := httpguard.NoRedirectsDoer(p.client).Do(httpReq) // the saved API key rides this request
	if err != nil {
		return nil, classifyTransportFailure(err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, classifyTransportFailure(err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, classifyHTTPFailure(resp.StatusCode, respBody)
	}
	var parsed chatCompletionResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, newProviderFailure(errCodeInvalidResponse)
	}
	if len(parsed.Choices) == 0 {
		return nil, newProviderFailure(errCodeInvalidResponse)
	}
	return &parsed, nil
}

// probe runs the fixed two-round readiness exchange and returns
// ("ready", nil) or ("failed", &safeErrorCode).
func (p *openAICompatibleReadinessProber) probe(
	ctx context.Context, provider, model, baseURL, apiKey string,
) (string, *string) {
	toolChoice := "required"
	var parallelToolCalls *bool
	if supportsParallelToolCalls(model) {
		disable := false
		parallelToolCalls = &disable
	}
	var temperature *float64
	if supportsTemperature(model) {
		zero := 0.0
		temperature = &zero
	}
	round1 := chatCompletionRequest{
		Model: model,
		Messages: []chatMessage{
			{Role: "user", Content: "Call readiness_echo with nonce ready-v1."},
		},
		Tools: []chatTool{{
			Type: "function",
			Function: chatToolFunction{
				Name:        readinessEchoWireName,
				Description: "Return the supplied nonce.",
				Parameters:  json.RawMessage(readinessToolParametersSchema),
				Strict:      true,
			},
		}},
		ToolChoice:          toolChoice,
		MaxCompletionTokens: readinessMaxOutputTokens,
		ParallelToolCalls:   parallelToolCalls,
		Temperature:         temperature,
		ReasoningEffort:     reasoningEffort(model),
	}

	resp1, failure := p.doCompletion(ctx, baseURL, apiKey, round1)
	if failure != nil {
		return readinessOutcomeFailed, ptrString(failure.code)
	}
	choice1 := resp1.Choices[0]
	if choice1.FinishReason == "length" {
		return readinessOutcomeFailed, ptrString(errCodeOutputExhausted)
	}
	callID, callFailure := normalizeRound1Decision(choice1)
	if callFailure != nil {
		return readinessOutcomeFailed, ptrString(callFailure.code)
	}

	round2 := chatCompletionRequest{
		Model: model,
		Messages: []chatMessage{
			{Role: "user", Content: "Call readiness_echo with nonce ready-v1."},
			{
				Role:    "assistant",
				Content: "",
				ToolCalls: []chatToolCall{{
					ID:   callID,
					Type: "function",
					Function: chatToolCallFunction{
						Name:      readinessEchoWireName,
						Arguments: `{"nonce":"ready-v1"}`,
					},
				}},
			},
			{Role: "tool", Content: `{"nonce":"ready-v1"}`, ToolCallID: callID},
			{
				Role: "user",
				Content: `Return a final_answer now with value exactly ` +
					`{"nonce":"ready-v1"}. Do not request another tool.`,
			},
		},
		MaxCompletionTokens: readinessMaxOutputTokens,
		Temperature:         temperature,
		ReasoningEffort:     reasoningEffort(model),
		ResponseFormat: &responseFormat{
			Type: "json_schema",
			JSONSchema: responseFormatJSONSchema{
				Name:   "ask_dev_decision",
				Strict: true,
				Schema: json.RawMessage(readinessDecisionResponseSchema),
			},
		},
	}

	resp2, failure := p.doCompletion(ctx, baseURL, apiKey, round2)
	if failure != nil {
		return readinessOutcomeFailed, ptrString(failure.code)
	}
	choice2 := resp2.Choices[0]
	if choice2.FinishReason == "length" {
		return readinessOutcomeFailed, ptrString(errCodeOutputExhausted)
	}
	if finalFailure := normalizeRound2Decision(choice2); finalFailure != nil {
		return readinessOutcomeFailed, ptrString(finalFailure.code)
	}
	return readinessOutcomeReady, nil
}

// normalizeRound1Decision mirrors _normalize_response for round 1: a
// sequential-tool-contract violation (>1 native tool call) is a distinct
// safe_error_code from every other malformed shape (openai_compatible.py:
// 549-568, 821-853). The JSON-content fallback (_json_tool_request) is not
// ported: it exists for a model that, despite tool_choice="required" and a
// registered tool, answers in plain content instead of a native tool call --
// covered here by INVALID_RESPONSE, same as any other unrecognized round-1
// shape.
func normalizeRound1Decision(choice chatChoice) (string, *providerFailure) {
	if len(choice.Message.ToolCalls) > 1 {
		return "", newProviderFailure(errCodeProviderContractViolation)
	}
	if len(choice.Message.ToolCalls) != 1 {
		return "", newProviderFailure(errCodeInvalidResponse)
	}
	call := choice.Message.ToolCalls[0]
	if call.Function.Name != readinessEchoWireName {
		return "", newProviderFailure(errCodeInvalidResponse)
	}
	var args map[string]any
	if err := json.Unmarshal([]byte(call.Function.Arguments), &args); err != nil {
		return "", newProviderFailure(errCodeInvalidResponse)
	}
	if len(args) != 1 || args["nonce"] != readinessNonceValue {
		return "", newProviderFailure(errCodeInvalidResponse)
	}
	if call.ID == "" {
		return "", newProviderFailure(errCodeInvalidResponse)
	}
	return call.ID, nil
}

// normalizeRound2Decision mirrors _normalize_response for round 2 plus
// certify()'s own isinstance(second.decision, AgentFinalAnswer) check
// (readiness.py:216-219): any decision kind other than "final_answer"
// (disambiguation, refusal, or malformed) fails certify() the same way, so
// this checks kind == "final_answer" directly rather than reproducing the
// disambiguation/refusal shape validation that only feeds a branch certify()
// rejects regardless (see the file doc comment).
func normalizeRound2Decision(choice chatChoice) *providerFailure {
	if len(choice.Message.ToolCalls) > 0 {
		return newProviderFailure(errCodeInvalidResponse)
	}
	if choice.Message.Content == nil {
		return newProviderFailure(errCodeInvalidResponse)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(*choice.Message.Content), &payload); err != nil {
		return newProviderFailure(errCodeInvalidResponse)
	}
	// Codex r1 P1 (CHAOS-6976): openai_compatible.py's
	// _validate_envelope_fields rejects a payload whose field set is
	// anything OTHER than the compact {"kind","value"} pair or the full
	// 9-field DECISION_FIELDS set -- an extra/unexpected top-level key
	// (e.g. a provider echoing a stray field) must fail here, not be
	// silently ignored by only reading "kind" and "value" off the map.
	if !isDecisionFieldSet(payload) {
		return newProviderFailure(errCodeInvalidResponse)
	}
	if payload["kind"] != "final_answer" {
		return newProviderFailure(errCodeInvalidResponse)
	}
	value, ok := payload["value"].(map[string]any)
	if !ok {
		return newProviderFailure(errCodeInvalidResponse)
	}
	if len(value) != 1 || value["nonce"] != readinessNonceValue {
		return newProviderFailure(errCodeInvalidResponse)
	}
	return nil
}

// decisionCompactFields and decisionFullFields are openai_compatible.py's
// _validate_envelope_fields({"kind","value"}) and _DECISION_FIELDS
// (openai_compatible.py:57-67: kind, tool_id, arguments, call_id, value,
// prompt, candidates, code, message).
var (
	decisionCompactFields = map[string]struct{}{"kind": {}, "value": {}}
	decisionFullFields    = map[string]struct{}{
		"kind": {}, "tool_id": {}, "arguments": {}, "call_id": {}, "value": {},
		"prompt": {}, "candidates": {}, "code": {}, "message": {},
	}
)

// isDecisionFieldSet is _validate_envelope_fields: payload's key set must
// equal EXACTLY one of the two field sets above, nothing else (an extra or
// missing key fails it either way).
func isDecisionFieldSet(payload map[string]any) bool {
	return fieldSetEquals(payload, decisionCompactFields) || fieldSetEquals(payload, decisionFullFields)
}

func fieldSetEquals(payload map[string]any, want map[string]struct{}) bool {
	if len(payload) != len(want) {
		return false
	}
	for key := range payload {
		if _, ok := want[key]; !ok {
			return false
		}
	}
	return true
}

func ptrString(s string) *string { return &s }

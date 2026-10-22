package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/externalurl"
	"github.com/full-chaos/dev-health-ops/internal/httpguard"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// The Anthropic readiness prober: the same fixed two-round readiness_echo exchange as
// openAICompatibleReadinessProber, over the Messages API acr runs. Round 1 forces the one tool and
// expects one tool_use; round 2 answers it with a tool_result and expects a final_answer JSON
// object in the text. The outcome and safe_error_code vocabulary are shared with the OpenAI prober.

const (
	anthropicDefaultBaseURL = "https://api.anthropic.com"
	anthropicAPIVersion     = "2023-06-01"
)

const anthropicRound1Prompt = "Call readiness_echo_v1 with nonce ready-v1."

const anthropicRound2Prompt = `Reply with only this JSON object and nothing else: {"kind":"final_answer","value":{"nonce":"ready-v1"}}. Do not request another tool.`

type anthropicReadinessProber struct {
	client providerfoundation.HTTPDoer
}

// newAnthropicReadinessProber honors a non-nil doer verbatim (a route test injects one) and
// otherwise builds the same SSRF-guarded, no-redirect, no-proxy client the OpenAI prober uses.
func newAnthropicReadinessProber(doer providerfoundation.HTTPDoer) *anthropicReadinessProber {
	if doer != nil {
		return &anthropicReadinessProber{client: doer}
	}
	return &anthropicReadinessProber{client: newReadinessHTTPClient(externalurl.GuardedTransport())}
}

// anthropicMessagesEndpoint is the Messages URL for baseURL: an empty base is Anthropic's public
// API; a base already ending in /v1 is not given a second /v1.
func anthropicMessagesEndpoint(baseURL string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if trimmed == "" {
		trimmed = anthropicDefaultBaseURL
	}
	if strings.HasSuffix(trimmed, "/v1") {
		return trimmed + "/messages"
	}
	return trimmed + "/v1/messages"
}

type anthropicTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}

type anthropicToolChoice struct {
	Type                   string `json:"type"`
	Name                   string `json:"name,omitempty"`
	DisableParallelToolUse bool   `json:"disable_parallel_tool_use"`
}

type anthropicContentBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	Content   string          `json:"content,omitempty"`
}

type anthropicMessage struct {
	Role    string                  `json:"role"`
	Content []anthropicContentBlock `json:"content"`
}

type anthropicRequest struct {
	Model      string               `json:"model"`
	MaxTokens  int                  `json:"max_tokens"`
	Messages   []anthropicMessage   `json:"messages"`
	Tools      []anthropicTool      `json:"tools,omitempty"`
	ToolChoice *anthropicToolChoice `json:"tool_choice,omitempty"`
}

type anthropicMessageResponse struct {
	StopReason string                  `json:"stop_reason"`
	Content    []anthropicContentBlock `json:"content"`
}

// classifyAnthropicFailure adds the Messages API's own status semantics to the shared
// classification: 403 is a key without access, 404 not_found_error names the model, and an
// exhausted credit balance is a 400 that is a credential problem, not a malformed request.
func classifyAnthropicFailure(status int, body []byte) *providerFailure {
	lower := strings.ToLower(string(body))
	switch {
	case strings.Contains(lower, "credit balance"):
		return newProviderFailure(errCodeProviderNotConfigured)
	case status == http.StatusForbidden:
		return newProviderFailure(errCodeProviderNotConfigured)
	case status == http.StatusNotFound && strings.Contains(lower, "not_found_error"):
		return newProviderFailure(errCodeModelNotSupported)
	}
	return classifyHTTPFailure(status, body)
}

func withProbeRetries[T any](ctx context.Context, once func() (*T, *providerFailure)) (*T, *providerFailure) {
	var last *providerFailure
	for attempt := 0; attempt < maxProbeAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, classifyTransportFailure(ctx.Err())
			case <-time.After(time.Duration(attempt) * 200 * time.Millisecond):
			}
		}
		resp, failure := once()
		if failure == nil {
			return resp, nil
		}
		last = failure
		if !isRetryableFailureCode(failure.code) {
			return nil, failure
		}
	}
	return nil, last
}

func (p *anthropicReadinessProber) doMessage(ctx context.Context, baseURL, apiKey string, req anthropicRequest) (*anthropicMessageResponse, *providerFailure) {
	return withProbeRetries(ctx, func() (*anthropicMessageResponse, *providerFailure) {
		return p.doMessageOnce(ctx, baseURL, apiKey, req)
	})
}

func (p *anthropicReadinessProber) doMessageOnce(ctx context.Context, baseURL, apiKey string, req anthropicRequest) (*anthropicMessageResponse, *providerFailure) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, newProviderFailure(errCodeInvalidResponse)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, anthropicMessagesEndpoint(baseURL), bytes.NewReader(body))
	if err != nil {
		return nil, newProviderFailure(errCodeProviderUnavailable)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("anthropic-version", anthropicAPIVersion)
	if apiKey != "" {
		httpReq.Header.Set("x-api-key", apiKey)
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
		return nil, classifyAnthropicFailure(resp.StatusCode, respBody)
	}
	var parsed anthropicMessageResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, newProviderFailure(errCodeInvalidResponse)
	}
	return &parsed, nil
}

// probe runs the fixed two-round readiness exchange and returns ("ready", nil) or
// ("failed", &safeErrorCode).
func (p *anthropicReadinessProber) probe(ctx context.Context, _, model, baseURL, apiKey string) (string, *string) {
	round1 := anthropicRequest{
		Model:     model,
		MaxTokens: readinessMaxOutputTokens,
		Messages: []anthropicMessage{{
			Role:    "user",
			Content: []anthropicContentBlock{{Type: "text", Text: anthropicRound1Prompt}},
		}},
		Tools: []anthropicTool{{
			Name:        readinessEchoWireName,
			Description: "Return the supplied nonce.",
			InputSchema: json.RawMessage(readinessToolParametersSchema),
		}},
		// "auto", not a forced tool: newer model families refuse a forced tool_choice with a 400, and the
		// prompt names the one tool, so a model that answers in text fails as invalid_response.
		ToolChoice: &anthropicToolChoice{Type: "auto", DisableParallelToolUse: true},
	}
	resp1, failure := p.doMessage(ctx, baseURL, apiKey, round1)
	if failure != nil {
		return readinessOutcomeFailed, ptrString(failure.code)
	}
	if resp1.StopReason == "max_tokens" {
		return readinessOutcomeFailed, ptrString(errCodeOutputExhausted)
	}
	toolUseID, failure := normalizeAnthropicRound1(resp1)
	if failure != nil {
		return readinessOutcomeFailed, ptrString(failure.code)
	}

	round2 := anthropicRequest{
		Model:     model,
		MaxTokens: readinessMaxOutputTokens,
		Messages: []anthropicMessage{
			round1.Messages[0],
			{Role: "assistant", Content: []anthropicContentBlock{{
				Type: "tool_use", ID: toolUseID, Name: readinessEchoWireName, Input: json.RawMessage(`{"nonce":"ready-v1"}`),
			}}},
			{Role: "user", Content: []anthropicContentBlock{
				{Type: "tool_result", ToolUseID: toolUseID, Content: `{"nonce":"ready-v1"}`},
				{Type: "text", Text: anthropicRound2Prompt},
			}},
		},
		// The tools stay declared: the Messages API refuses a tool_use turn in the history
		// without them.
		Tools: round1.Tools,
	}
	resp2, failure := p.doMessage(ctx, baseURL, apiKey, round2)
	if failure != nil {
		return readinessOutcomeFailed, ptrString(failure.code)
	}
	if resp2.StopReason == "max_tokens" {
		return readinessOutcomeFailed, ptrString(errCodeOutputExhausted)
	}
	if failure := normalizeAnthropicRound2(resp2); failure != nil {
		return readinessOutcomeFailed, ptrString(failure.code)
	}
	return readinessOutcomeReady, nil
}

// normalizeAnthropicRound1 is normalizeRound1Decision for the Messages API: exactly one tool_use,
// of the readiness tool, whose input is exactly {"nonce":"ready-v1"}; more than one is a
// sequential-tool contract violation.
func normalizeAnthropicRound1(resp *anthropicMessageResponse) (string, *providerFailure) {
	var uses []anthropicContentBlock
	for _, block := range resp.Content {
		if block.Type == "tool_use" {
			uses = append(uses, block)
		}
	}
	if len(uses) > 1 {
		return "", newProviderFailure(errCodeProviderContractViolation)
	}
	if len(uses) != 1 {
		return "", newProviderFailure(errCodeInvalidResponse)
	}
	use := uses[0]
	if use.Name != readinessEchoWireName || use.ID == "" {
		return "", newProviderFailure(errCodeInvalidResponse)
	}
	var args map[string]any
	if err := json.Unmarshal(use.Input, &args); err != nil {
		return "", newProviderFailure(errCodeInvalidResponse)
	}
	if len(args) != 1 || args["nonce"] != readinessNonceValue {
		return "", newProviderFailure(errCodeInvalidResponse)
	}
	return use.ID, nil
}

// normalizeAnthropicRound2: no tool_use block, and the text blocks together are the final_answer
// object (the same envelope check the OpenAI prober applies).
func normalizeAnthropicRound2(resp *anthropicMessageResponse) *providerFailure {
	var text strings.Builder
	for _, block := range resp.Content {
		switch block.Type {
		case "tool_use":
			return newProviderFailure(errCodeInvalidResponse)
		case "text":
			text.WriteString(block.Text)
		}
	}
	content := strings.TrimSpace(text.String())
	if content == "" {
		return newProviderFailure(errCodeInvalidResponse)
	}
	return validateFinalAnswerContent(content)
}

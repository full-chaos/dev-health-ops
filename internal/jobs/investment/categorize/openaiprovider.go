package categorize

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// OpenAIProviderConfig configures OpenAIProvider.
type OpenAIProviderConfig struct {
	APIKey string
	// BaseURL defaults to https://api.openai.com/v1 -- overridable for a
	// self-hosted gateway or for tests.
	BaseURL string
	// Model defaults to "gpt-5-nano". Only the GPT-5 family (Responses API)
	// is supported -- openai.py's OpenAIGPTLegacyProvider (Chat Completions,
	// <= GPT-4) has no Go port, since day-one production config names a
	// GPT-5 model.
	Model string
	// MaxOutputTokens is a floor, not a cap: openai.py's own minimum is
	// 2048 for categorization but 4096 for investment-mix explanation
	// (its narrative payloads run larger -- "Explanation payloads are
	// large; start higher than 4096" is openai.py's own comment). This
	// port applies ONE floor (openAIMinOutputTokens) regardless of
	// CompletionRequest.ResponseFormatName; a future mix-explanation
	// caller should pass a higher MaxOutputTokens explicitly rather than
	// rely on a per-format default this config doesn't provide.
	MaxOutputTokens int
	HTTPClient      *http.Client
}

const (
	defaultOpenAIBaseURL = "https://api.openai.com/v1"
	defaultOpenAIModel   = "gpt-5-nano"

	openAIMinOutputTokens = 2048
	openAIMaxOutputTokens = 8192
	openAIMaxRetries      = 1
)

// OpenAIProvider is llm/providers/openai.py's OpenAIGPT5Provider, narrowed
// to the Responses API path GPT-5-family models require. It never sends a
// temperature parameter: openai_capabilities.py's supports_temperature is
// false for every "gpt-5*" model, and this provider only ever targets that
// family.
type OpenAIProvider struct {
	cfg    OpenAIProviderConfig
	client *http.Client
}

// NewOpenAIProvider constructs an OpenAIProvider, applying the same
// defaults (base URL, model, minimum output tokens) that
// llm/providers/base.py's DEFAULT_MODEL_BY_PROVIDER and openai.py's
// OpenAIProvider facade apply.
func NewOpenAIProvider(cfg OpenAIProviderConfig) *OpenAIProvider {
	if cfg.BaseURL == "" {
		cfg.BaseURL = defaultOpenAIBaseURL
	}
	cfg.BaseURL = trimBaseURL(cfg.BaseURL)
	if cfg.Model == "" {
		cfg.Model = defaultOpenAIModel
	}
	if cfg.MaxOutputTokens < openAIMinOutputTokens {
		cfg.MaxOutputTokens = openAIMinOutputTokens
	}
	client := cfg.HTTPClient
	if client == nil {
		client = newHardenedHTTPClient()
	}
	return &OpenAIProvider{cfg: cfg, client: client}
}

var _ Provider = (*OpenAIProvider)(nil)

type openAIResponsesRequest struct {
	Model           string             `json:"model"`
	Instructions    string             `json:"instructions"`
	Input           string             `json:"input"`
	Text            openAIResponseText `json:"text"`
	Reasoning       openAIReasoning    `json:"reasoning"`
	MaxOutputTokens int                `json:"max_output_tokens"`
}

type openAIResponseText struct {
	Format    openAITextFormat `json:"format"`
	Verbosity string           `json:"verbosity"`
}

type openAITextFormat struct {
	Type   string         `json:"type"`
	Name   string         `json:"name,omitempty"`
	Strict bool           `json:"strict,omitempty"`
	Schema map[string]any `json:"schema,omitempty"`
}

type openAIReasoning struct {
	Effort string `json:"effort"`
}

type openAIResponsesResponse struct {
	OutputText        string                   `json:"output_text"`
	Output            []openAIResponseOutput   `json:"output"`
	Usage             *openAIUsage             `json:"usage"`
	IncompleteDetails *openAIIncompleteDetails `json:"incomplete_details"`
}

type openAIResponseOutput struct {
	Content []openAIResponseContent `json:"content"`
}

type openAIResponseContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type openAIIncompleteDetails struct {
	Reason string `json:"reason"`
}

type openAIUsage struct {
	InputTokens        *int                `json:"input_tokens"`
	OutputTokens       *int                `json:"output_tokens"`
	InputTokensDetails *openAITokenDetails `json:"input_tokens_details"`
}

type openAITokenDetails struct {
	CachedTokens *int `json:"cached_tokens"`
}

// Complete ports openai.py's OpenAIGPT5Provider.complete.
func (p *OpenAIProvider) Complete(ctx context.Context, request CompletionRequest) (CompletionResult, error) {
	maxTokens := p.cfg.MaxOutputTokens

	textFormat := openAITextFormat{Type: "json_object"}
	if request.JSONSchema != nil {
		textFormat = openAITextFormat{
			Type:   "json_schema",
			Name:   request.ResponseFormatName,
			Strict: true,
			Schema: request.JSONSchema,
		}
	}

	var lastErr *llmError
	for attempt := 0; attempt <= openAIMaxRetries; attempt++ {
		body := openAIResponsesRequest{
			Model:        p.cfg.Model,
			Instructions: request.SystemMessage,
			Input:        request.Prompt,
			Text: openAIResponseText{
				Format:    textFormat,
				Verbosity: "low",
			},
			Reasoning:       openAIReasoning{Effort: "low"},
			MaxOutputTokens: maxTokens,
		}

		result, incompleteReason, err := p.executeResponsesRequest(ctx, body)
		if err != nil {
			classified := classifyProviderError(err, statusCodeOf(err), headerOf(err), "openai", p.cfg.Model)
			lastErr = classified
			if isRetryable(classified) && attempt < openAIMaxRetries {
				if !sleepForRetry(ctx, retryDelayFor(classified, attempt)) {
					return CompletionResult{}, ctx.Err()
				}
				continue
			}
			return CompletionResult{}, classified
		}

		cleaned := validateJSONOrEmpty(result.text)
		if cleaned != "" {
			return CompletionResult{
				Text:              cleaned,
				InputTokens:       result.inputTokens,
				OutputTokens:      result.outputTokens,
				Model:             p.cfg.Model,
				CachedInputTokens: result.cachedInputTokens,
			}, nil
		}

		if incompleteReason == "max_output_tokens" && attempt < openAIMaxRetries {
			maxTokens = minInt(openAIMaxOutputTokens, maxTokens*2)
			if !sleepForRetry(ctx, 500*time.Millisecond) {
				return CompletionResult{}, ctx.Err()
			}
			continue
		}

		// Final failure: empty text, no error -- the caller (repair-retry
		// loop) handles an empty completion the same way it handles a
		// validation failure. This matches openai.py's own contract.
		return CompletionResult{Model: p.cfg.Model}, nil
	}

	if lastErr != nil {
		return CompletionResult{}, lastErr
	}
	return CompletionResult{Model: p.cfg.Model}, nil
}

type openAICompletionText struct {
	text              string
	inputTokens       *int
	outputTokens      *int
	cachedInputTokens *int
}

// executeResponsesRequest issues one Responses API call and extracts the
// completion text the same way openai.py does: prefer output_text, and
// fall back to concatenating every output_text/text content chunk in
// output[].content[] when it is empty.
func (p *OpenAIProvider) executeResponsesRequest(ctx context.Context, body openAIResponsesRequest) (openAICompletionText, string, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return openAICompletionText{}, "", fmt.Errorf("encode request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.BaseURL+"/responses", bytes.NewReader(payload))
	if err != nil {
		return openAICompletionText{}, "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.cfg.APIKey)

	resp, err := p.client.Do(req)
	if err != nil {
		return openAICompletionText{}, "", &httpTransportError{cause: err}
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return openAICompletionText{}, "", &httpStatusError{statusCode: resp.StatusCode, header: resp.Header, body: err.Error()}
	}

	if resp.StatusCode != http.StatusOK {
		return openAICompletionText{}, "", &httpStatusError{statusCode: resp.StatusCode, header: resp.Header, body: string(responseBody)}
	}

	var decoded openAIResponsesResponse
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return openAICompletionText{}, "", fmt.Errorf("decode response: %w", err)
	}

	content := decoded.OutputText
	if content == "" {
		var parts []byte
		for _, item := range decoded.Output {
			for _, chunk := range item.Content {
				if chunk.Type == "output_text" || chunk.Type == "text" {
					parts = append(parts, chunk.Text...)
				}
			}
		}
		content = string(parts)
	}

	reason := "completed"
	if decoded.IncompleteDetails != nil && decoded.IncompleteDetails.Reason != "" {
		reason = decoded.IncompleteDetails.Reason
	}

	out := openAICompletionText{text: content}
	if decoded.Usage != nil {
		out.inputTokens = decoded.Usage.InputTokens
		out.outputTokens = decoded.Usage.OutputTokens
		if decoded.Usage.InputTokensDetails != nil {
			out.cachedInputTokens = decoded.Usage.InputTokensDetails.CachedTokens
		}
	}
	return out, reason, nil
}

// Close ports openai.py's aclose. There is no persistent SDK client to tear
// down (this provider issues plain net/http requests), so this releases the
// hardened client's idle connections rather than being a true no-op.
func (p *OpenAIProvider) Close() error {
	p.client.CloseIdleConnections()
	return nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

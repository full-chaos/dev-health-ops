package categorize

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// LMStudioProviderConfig configures LMStudioProvider.
type LMStudioProviderConfig struct {
	// BaseURL defaults to LM Studio's local server root (no /v1 suffix --
	// that path is LocalProvider's OpenAI-compatibility route; this hits LM
	// Studio's own native REST API instead, under /api/v0).
	BaseURL string
	// Model defaults to "local-model" -- mirrors base.py's
	// DEFAULT_MODEL_BY_PROVIDER["lmstudio"] byte-for-byte. That value is a
	// deliberately generic placeholder, not a guess at chris's actual gemma
	// tag: LM Studio serves whatever model is currently loaded in the app,
	// and the ticket (CHAOS-4978) logged the real identifier as an open
	// question to chris rather than inventing one. Set LMSTUDIO_MODEL to
	// override.
	Model string
	// APIKey is sent as a bearer token; LM Studio's local server does not
	// require one by default, but accepts an arbitrary value.
	APIKey          string
	MaxOutputTokens int
	// Temperature is a pointer for the same reason every other provider's
	// is: an explicit 0.0 must be distinguishable from "unset".
	Temperature *float64
	HTTPClient  *http.Client
}

const (
	defaultLMStudioBaseURL     = "http://localhost:1234"
	defaultLMStudioModel       = "local-model"
	defaultLMStudioAPIKey      = "not-needed"
	defaultLMStudioMaxTokens   = 4096
	defaultLMStudioTemperature = 0.3
	lmStudioMaxRetries         = 1
)

// LMStudioProvider speaks LM Studio's own native REST API
// (/api/v0/chat/completions -- https://lmstudio.ai/docs/app/api/endpoints/rest)
// rather than the OpenAI-compatibility endpoint LocalProvider uses when
// pointed at LM Studio via LOCAL_LLM_BASE_URL. The request/response shape
// is deliberately close to OpenAI's Chat Completions (LM Studio designed it
// that way), so this reuses the same json_schema structured-output
// convention LocalProvider does; the distinguishing native behavior is the
// /api/v0 route and LM Studio's own richer response stats. Python has no
// equivalent of this native route: llm/providers/local.py's LMStudioProvider
// is a thin LocalProvider subclass over the OpenAI-compatible endpoint, so
// Python is the oracle for this provider's name/env vars/default model only,
// not its wire protocol.
type LMStudioProvider struct {
	cfg    LMStudioProviderConfig
	client *http.Client
}

func NewLMStudioProvider(cfg LMStudioProviderConfig) *LMStudioProvider {
	if cfg.BaseURL == "" {
		cfg.BaseURL = defaultLMStudioBaseURL
	}
	cfg.BaseURL = trimBaseURL(cfg.BaseURL)
	if cfg.Model == "" {
		cfg.Model = defaultLMStudioModel
	}
	if cfg.APIKey == "" {
		cfg.APIKey = defaultLMStudioAPIKey
	}
	if cfg.MaxOutputTokens <= 0 {
		cfg.MaxOutputTokens = defaultLMStudioMaxTokens
	}
	if cfg.Temperature == nil {
		defaultTemperature := defaultLMStudioTemperature
		cfg.Temperature = &defaultTemperature
	}
	client := cfg.HTTPClient
	if client == nil {
		client = newHardenedHTTPClient()
	}
	return &LMStudioProvider{cfg: cfg, client: client}
}

var _ Provider = (*LMStudioProvider)(nil)

type lmStudioChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type lmStudioResponseFormat struct {
	Type       string                     `json:"type"`
	JSONSchema *lmStudioJSONSchemaWrapper `json:"json_schema,omitempty"`
}

type lmStudioJSONSchemaWrapper struct {
	Name   string         `json:"name"`
	Schema map[string]any `json:"schema"`
	Strict bool           `json:"strict"`
}

type lmStudioChatRequest struct {
	Model          string                  `json:"model"`
	Messages       []lmStudioChatMessage   `json:"messages"`
	MaxTokens      int                     `json:"max_tokens"`
	Temperature    float64                 `json:"temperature"`
	ResponseFormat *lmStudioResponseFormat `json:"response_format,omitempty"`
}

type lmStudioChatResponse struct {
	Choices []lmStudioChatChoice `json:"choices"`
	Usage   *lmStudioChatUsage   `json:"usage"`
}

type lmStudioChatChoice struct {
	Message lmStudioChatMessage `json:"message"`
}

type lmStudioChatUsage struct {
	PromptTokens     *int `json:"prompt_tokens"`
	CompletionTokens *int `json:"completion_tokens"`
}

// Complete ports the same retry/schema-fallback/error-classification shape
// LocalProvider.Complete uses, adapted to LM Studio's native route.
func (p *LMStudioProvider) Complete(ctx context.Context, request CompletionRequest) (CompletionResult, error) {
	var responseFormat *lmStudioResponseFormat
	if request.JSONSchema != nil {
		responseFormat = &lmStudioResponseFormat{
			Type: "json_schema",
			JSONSchema: &lmStudioJSONSchemaWrapper{
				Name:   request.ResponseFormatName,
				Schema: request.JSONSchema,
				Strict: true,
			},
		}
	}

	maxTokens := p.cfg.MaxOutputTokens
	if request.MaxOutputTokens > maxTokens {
		maxTokens = request.MaxOutputTokens
	}

	for attempt := 0; attempt <= lmStudioMaxRetries; attempt++ {
		body := lmStudioChatRequest{
			Model: p.cfg.Model,
			Messages: []lmStudioChatMessage{
				{Role: "system", Content: request.SystemMessage},
				{Role: "user", Content: request.Prompt},
			},
			MaxTokens:      maxTokens,
			Temperature:    *p.cfg.Temperature,
			ResponseFormat: responseFormat,
		}

		content, usage, err := p.executeChatRequest(ctx, body)
		if err != nil {
			if statusCodeOf(err) == http.StatusBadRequest && responseFormat != nil && attempt < lmStudioMaxRetries {
				responseFormat = &lmStudioResponseFormat{Type: "text"}
				continue
			}
			classified := classifyProviderError(err, statusCodeOf(err), headerOf(err), "lmstudio", p.cfg.Model)
			if isRetryable(classified) && attempt < lmStudioMaxRetries {
				if !sleepForRetry(ctx, retryDelayFor(classified, attempt)) {
					return CompletionResult{}, ctx.Err()
				}
				continue
			}
			return CompletionResult{}, classified
		}

		text := content
		if request.JSONSchema != nil {
			text = validateJSONOrEmpty(content)
		}
		result := CompletionResult{Text: text, Model: p.cfg.Model}
		if usage != nil {
			result.InputTokens = usage.PromptTokens
			result.OutputTokens = usage.CompletionTokens
		}
		return result, nil
	}

	return CompletionResult{Model: p.cfg.Model}, nil
}

func (p *LMStudioProvider) executeChatRequest(ctx context.Context, body lmStudioChatRequest) (string, *lmStudioChatUsage, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return "", nil, fmt.Errorf("encode request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.BaseURL+"/api/v0/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return "", nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.cfg.APIKey)

	resp, err := p.client.Do(req)
	if err != nil {
		return "", nil, &httpTransportError{cause: err}
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", nil, &httpStatusError{statusCode: resp.StatusCode, header: resp.Header, body: err.Error()}
	}

	if resp.StatusCode != http.StatusOK {
		return "", nil, &httpStatusError{statusCode: resp.StatusCode, header: resp.Header, body: string(responseBody)}
	}

	var decoded lmStudioChatResponse
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return "", nil, fmt.Errorf("decode response: %w", err)
	}
	if len(decoded.Choices) == 0 {
		return "", nil, fmt.Errorf("lmstudio provider response had no choices")
	}
	return decoded.Choices[0].Message.Content, decoded.Usage, nil
}

// Close ports LocalProvider.Close's CloseIdleConnections cleanup.
func (p *LMStudioProvider) Close() error {
	p.client.CloseIdleConnections()
	return nil
}

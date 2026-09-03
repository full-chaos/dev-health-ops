package categorize

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// LocalProviderConfig configures LocalProvider.
type LocalProviderConfig struct {
	// BaseURL defaults to Ollama's local endpoint. LocalProvider speaks the
	// OpenAI-compatible Chat Completions wire shape, so it also works
	// against LMStudio/vLLM/any compatible server by pointing BaseURL at
	// theirs -- local.py's own DEFAULT_ENDPOINTS table.
	BaseURL string
	// Model defaults to "gemma3" (Ollama's tag for the day-one local model
	// chris named; local.py's own default was "llama3.2" for a generic
	// local endpoint, not a specific gemma tag -- override via config for a
	// different tag/quantization).
	Model string
	// APIKey is sent as a bearer token; most local servers ignore it.
	APIKey          string
	MaxOutputTokens int
	Temperature     float64
	HTTPClient      *http.Client
}

const (
	defaultLocalBaseURL     = "http://localhost:11434/v1"
	defaultLocalModel       = "gemma3"
	defaultLocalAPIKey      = "not-needed"
	defaultLocalMaxTokens   = 4096
	defaultLocalTemperature = 0.3
	localMaxRetries         = 1
)

// LocalProvider is llm/providers/local.py's LocalProvider: an
// OpenAI-compatible Chat Completions client for a self-hosted endpoint
// (Ollama, LMStudio, vLLM). Structured Outputs support varies by server, so
// -- exactly like the Python source -- a 400 on the first attempt is
// retried once with a plain text response format rather than treated as
// fatal.
type LocalProvider struct {
	cfg    LocalProviderConfig
	client *http.Client
}

func NewLocalProvider(cfg LocalProviderConfig) *LocalProvider {
	if cfg.BaseURL == "" {
		cfg.BaseURL = defaultLocalBaseURL
	}
	if cfg.Model == "" {
		cfg.Model = defaultLocalModel
	}
	if cfg.APIKey == "" {
		cfg.APIKey = defaultLocalAPIKey
	}
	if cfg.MaxOutputTokens <= 0 {
		cfg.MaxOutputTokens = defaultLocalMaxTokens
	}
	if cfg.Temperature == 0 {
		cfg.Temperature = defaultLocalTemperature
	}
	client := cfg.HTTPClient
	if client == nil {
		client = newHardenedHTTPClient()
	}
	return &LocalProvider{cfg: cfg, client: client}
}

var _ Provider = (*LocalProvider)(nil)

type localChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type localResponseFormat struct {
	Type       string                  `json:"type"`
	JSONSchema *localJSONSchemaWrapper `json:"json_schema,omitempty"`
}

type localJSONSchemaWrapper struct {
	Name   string         `json:"name"`
	Schema map[string]any `json:"schema"`
	Strict bool           `json:"strict"`
}

type localChatRequest struct {
	Model               string               `json:"model"`
	Messages            []localChatMessage   `json:"messages"`
	MaxCompletionTokens int                  `json:"max_completion_tokens"`
	Temperature         float64              `json:"temperature"`
	ResponseFormat      *localResponseFormat `json:"response_format,omitempty"`
}

type localChatResponse struct {
	Choices []localChatChoice `json:"choices"`
	Usage   *localChatUsage   `json:"usage"`
}

type localChatChoice struct {
	Message localChatMessage `json:"message"`
}

type localChatUsage struct {
	PromptTokens     *int `json:"prompt_tokens"`
	CompletionTokens *int `json:"completion_tokens"`
}

// Complete ports local.py's LocalProvider.complete.
func (p *LocalProvider) Complete(ctx context.Context, prompt string) (CompletionResult, error) {
	responseFormat := &localResponseFormat{
		Type: "json_schema",
		JSONSchema: &localJSONSchemaWrapper{
			Name:   "categorization",
			Schema: categorizationJSONSchema(),
			Strict: true,
		},
	}

	for attempt := 0; attempt <= localMaxRetries; attempt++ {
		body := localChatRequest{
			Model: p.cfg.Model,
			Messages: []localChatMessage{
				{Role: "system", Content: categorizationSystemMessage},
				{Role: "user", Content: prompt},
			},
			MaxCompletionTokens: p.cfg.MaxOutputTokens,
			Temperature:         p.cfg.Temperature,
			ResponseFormat:      responseFormat,
		}

		content, usage, err := p.executeChatCompletionRequest(ctx, body)
		if err != nil {
			if statusCodeOf(err) == http.StatusBadRequest && responseFormat != nil && attempt < localMaxRetries {
				// Common with local OpenAI-compatible servers that do not
				// support Structured Outputs -- fall back to plain text
				// and retry once, exactly as local.py does.
				responseFormat = &localResponseFormat{Type: "text"}
				continue
			}
			classified := classifyProviderError(err, statusCodeOf(err), headerOf(err), "local", p.cfg.Model)
			if isRetryable(classified) && attempt < localMaxRetries {
				if !sleepForRetry(ctx, retryDelayFor(classified, attempt)) {
					return CompletionResult{}, ctx.Err()
				}
				continue
			}
			return CompletionResult{}, classified
		}

		text := validateJSONOrEmpty(content)
		result := CompletionResult{Text: text, Model: p.cfg.Model}
		if usage != nil {
			result.InputTokens = usage.PromptTokens
			result.OutputTokens = usage.CompletionTokens
		}
		return result, nil
	}

	return CompletionResult{Model: p.cfg.Model}, nil
}

func (p *LocalProvider) executeChatCompletionRequest(ctx context.Context, body localChatRequest) (string, *localChatUsage, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return "", nil, fmt.Errorf("encode request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.BaseURL+"/chat/completions", bytes.NewReader(payload))
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

	var decoded localChatResponse
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return "", nil, fmt.Errorf("decode response: %w", err)
	}
	if len(decoded.Choices) == 0 {
		return "", nil, fmt.Errorf("local provider response had no choices")
	}
	return decoded.Choices[0].Message.Content, decoded.Usage, nil
}

// Close ports local.py's aclose.
func (p *LocalProvider) Close() error {
	p.client.CloseIdleConnections()
	return nil
}

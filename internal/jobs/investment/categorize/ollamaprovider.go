package categorize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// OllamaProviderConfig configures OllamaProvider.
type OllamaProviderConfig struct {
	// BaseURL defaults to Ollama's local endpoint WITHOUT the /v1 suffix
	// LocalProvider uses -- the native /api/chat route lives at the server
	// root, not under the OpenAI-compatibility /v1 prefix Ollama also
	// exposes (that path is what LocalProvider speaks when pointed at
	// Ollama via LOCAL_LLM_BASE_URL).
	BaseURL string
	// Model defaults to "llama3.2" -- mirrors base.py's
	// DEFAULT_MODEL_BY_PROVIDER["ollama"] byte-for-byte. Set OLLAMA_MODEL
	// to override.
	Model string
	// APIKey, if set, is sent as a bearer token. Stock Ollama ignores it;
	// this exists for a gateway/proxy sitting in front of Ollama that does
	// enforce one (CHAOS-4978: mirrors credentials.py's
	// _API_KEY_ENV_BY_PROVIDER["ollama"] table existing at all, even though
	// Ollama itself has no native auth concept).
	APIKey          string
	MaxOutputTokens int
	// Temperature is a pointer for the same reason LocalProviderConfig's is:
	// an explicit 0.0 must be distinguishable from "unset".
	Temperature *float64
	HTTPClient  *http.Client
}

const (
	defaultOllamaBaseURL     = "http://localhost:11434"
	defaultOllamaModel       = "llama3.2"
	defaultOllamaMaxTokens   = 4096
	defaultOllamaTemperature = 0.3
	ollamaMaxRetries         = 1
)

// OllamaProvider speaks Ollama's NATIVE /api/chat wire protocol -- as
// opposed to LocalProvider, which speaks the OpenAI-compatible Chat
// Completions shape Ollama also exposes under /v1. Python has no equivalent
// of this: llm/providers/local.py's OllamaProvider is a thin LocalProvider
// subclass over the OpenAI-compatible endpoint, so Python is not the oracle
// for this provider's wire protocol (only for its provider name, env var
// names, and default model, which this mirrors byte-for-byte). This is the
// new capability CHAOS-4978 asks for: native structured output via Ollama's
// `format` field, which accepts a full JSON Schema object (not just the
// OpenAI-style json_schema wrapper), a feature the OpenAI-compatible
// endpoint does not expose the same way. See
// https://ollama.com/blog/structured-outputs.
type OllamaProvider struct {
	cfg    OllamaProviderConfig
	client *http.Client
}

func NewOllamaProvider(cfg OllamaProviderConfig) *OllamaProvider {
	if cfg.BaseURL == "" {
		cfg.BaseURL = defaultOllamaBaseURL
	}
	cfg.BaseURL = trimBaseURL(cfg.BaseURL)
	if cfg.Model == "" {
		cfg.Model = defaultOllamaModel
	}
	if cfg.MaxOutputTokens <= 0 {
		cfg.MaxOutputTokens = defaultOllamaMaxTokens
	}
	if cfg.Temperature == nil {
		defaultTemperature := defaultOllamaTemperature
		cfg.Temperature = &defaultTemperature
	}
	client := cfg.HTTPClient
	if client == nil {
		client = newHardenedHTTPClient()
	}
	return &OllamaProvider{cfg: cfg, client: client}
}

var _ Provider = (*OllamaProvider)(nil)

type ollamaChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ollamaChatOptions struct {
	Temperature float64 `json:"temperature"`
	NumPredict  int     `json:"num_predict"`
}

type ollamaChatRequest struct {
	Model    string              `json:"model"`
	Messages []ollamaChatMessage `json:"messages"`
	Stream   bool                `json:"stream"`
	// Format carries either the literal JSON string "json" or a full JSON
	// Schema object -- Ollama's native structured-output mechanism. nil
	// (omitted) means "no format constraint," matching every other
	// provider's "no schema requested" behavior.
	Format  json.RawMessage    `json:"format,omitempty"`
	Options *ollamaChatOptions `json:"options,omitempty"`
}

type ollamaChatResponse struct {
	Message         ollamaChatMessage `json:"message"`
	Done            bool              `json:"done"`
	PromptEvalCount *int              `json:"prompt_eval_count"`
	EvalCount       *int              `json:"eval_count"`
	// Error is populated by some Ollama versions on a 200 response whose
	// generation nonetheless failed partway through, rather than a non-2xx
	// status -- checked explicitly in executeChatRequest.
	Error string `json:"error"`
}

// Complete ports the same retry/schema-fallback/error-classification shape
// LocalProvider.Complete uses, adapted to Ollama's native request/response
// fields.
func (p *OllamaProvider) Complete(ctx context.Context, request CompletionRequest) (CompletionResult, error) {
	var format json.RawMessage
	if request.JSONSchema != nil {
		schemaBytes, err := json.Marshal(request.JSONSchema)
		if err != nil {
			return CompletionResult{}, fmt.Errorf("encode ollama format schema: %w", err)
		}
		format = schemaBytes
	}

	maxTokens := p.cfg.MaxOutputTokens
	if request.MaxOutputTokens > maxTokens {
		maxTokens = request.MaxOutputTokens
	}

	for attempt := 0; attempt <= ollamaMaxRetries; attempt++ {
		body := ollamaChatRequest{
			Model: p.cfg.Model,
			Messages: []ollamaChatMessage{
				{Role: "system", Content: request.SystemMessage},
				{Role: "user", Content: request.Prompt},
			},
			Stream: false,
			Format: format,
			Options: &ollamaChatOptions{
				Temperature: *p.cfg.Temperature,
				NumPredict:  maxTokens,
			},
		}

		content, promptEvalCount, evalCount, err := p.executeChatRequest(ctx, body)
		if err != nil {
			if statusCodeOf(err) == http.StatusBadRequest && format != nil && attempt < ollamaMaxRetries {
				// Mirrors local.py/LocalProvider's 400-on-structured-output
				// fallback: not every model tag Ollama can serve actually
				// supports the `format` constraint.
				format = nil
				continue
			}
			// executeChatRequest already returns a fully classified
			// *llmError for a done=false response (llmErrorOutput) --
			// re-running it through classifyProviderError would discard
			// that classification (its text doesn't match any of that
			// function's patterns, so it would fall through to a plain
			// llmErrorGeneric, non-retryable) and double-wrap the message.
			classified := new(llmError)
			if !errors.As(err, &classified) {
				classified = classifyProviderError(err, statusCodeOf(err), headerOf(err), "ollama", p.cfg.Model)
			}
			if isRetryable(classified) && attempt < ollamaMaxRetries {
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
		return CompletionResult{
			Text:         text,
			Model:        p.cfg.Model,
			InputTokens:  promptEvalCount,
			OutputTokens: evalCount,
		}, nil
	}

	return CompletionResult{Model: p.cfg.Model}, nil
}

func (p *OllamaProvider) executeChatRequest(ctx context.Context, body ollamaChatRequest) (string, *int, *int, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return "", nil, nil, fmt.Errorf("encode request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.BaseURL+"/api/chat", bytes.NewReader(payload))
	if err != nil {
		return "", nil, nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if p.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+p.cfg.APIKey)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return "", nil, nil, &httpTransportError{cause: err}
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", nil, nil, &httpStatusError{statusCode: resp.StatusCode, header: resp.Header, body: err.Error()}
	}

	if resp.StatusCode != http.StatusOK {
		return "", nil, nil, &httpStatusError{statusCode: resp.StatusCode, header: resp.Header, body: string(responseBody)}
	}

	var decoded ollamaChatResponse
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return "", nil, nil, fmt.Errorf("decode response: %w", err)
	}
	if decoded.Error != "" {
		return "", nil, nil, &httpStatusError{statusCode: resp.StatusCode, header: resp.Header, body: decoded.Error}
	}
	// Ollama's own /api/chat docs define `done` as whether the response has
	// actually finished generating. codex round 1 (#2189) P2: this path
	// ignored `Done` entirely and returned Message.Content as a normal
	// success even when the server reported `done:false` -- a truncated,
	// still-in-progress answer (e.g. cut off by a context/token limit
	// without the server ever finishing) was silently accepted as a
	// complete CompletionResult. `done:false` should never reach here on a
	// non-streaming (`stream:false`) request in practice, but the field
	// exists specifically for this signal, so treat it as authoritative
	// rather than assuming `stream:false` alone guarantees completion.
	if !decoded.Done {
		return "", nil, nil, &llmError{
			kind:     llmErrorOutput,
			message:  "ollama response incomplete (done=false)",
			provider: "ollama",
			model:    p.cfg.Model,
		}
	}
	return decoded.Message.Content, decoded.PromptEvalCount, decoded.EvalCount, nil
}

// Close ports LocalProvider.Close's CloseIdleConnections cleanup.
func (p *OllamaProvider) Close() error {
	p.client.CloseIdleConnections()
	return nil
}

package workgraph

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const maxCompatibilityResponseBytes = 8 * 1024

type HTTPCompatibilityConfig struct {
	Endpoint    string
	BearerToken string
}

type HTTPCompatibilityExecutor struct {
	client *http.Client
	config HTTPCompatibilityConfig
}

func NewHTTPCompatibilityExecutor(client *http.Client, config HTTPCompatibilityConfig) (*HTTPCompatibilityExecutor, error) {
	if client == nil || !validCompatibilityEndpoint(config.Endpoint) || len(config.BearerToken) == 0 || len(config.BearerToken) > 512 {
		return nil, ErrUnavailable
	}
	return &HTTPCompatibilityExecutor{client: client, config: config}, nil
}

func (executor *HTTPCompatibilityExecutor) Execute(ctx context.Context, claim Claim) ([]byte, error) {
	if executor == nil || executor.client == nil || !validRequest(claim.Request) || !validUUID(claim.Token) {
		return nil, ErrUnavailable
	}
	body, err := json.Marshal(struct {
		RequestID  string `json:"request_id"`
		ClaimToken string `json:"claim_token"`
	}{RequestID: claim.Request.ID, ClaimToken: claim.Token})
	if err != nil {
		return nil, ErrUnavailable
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, executor.config.Endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, ErrUnavailable
	}
	request.Header.Set("Authorization", "Bearer "+executor.config.BearerToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := executor.client.Do(request)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, maxCompatibilityResponseBytes+1))
	if err != nil || len(data) > maxCompatibilityResponseBytes || response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, ErrUnavailable
	}
	var decoded struct {
		Status         string          `json:"status"`
		OutputEvidence json.RawMessage `json:"output_evidence"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil || decoded.Status != "success" || !validEvidence(decoded.OutputEvidence) {
		return nil, ErrUnavailable
	}
	return decoded.OutputEvidence, nil
}

func validCompatibilityEndpoint(raw string) bool {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "/internal/worker/workgraph/v1/execute" {
		return false
	}
	if parsed.Scheme == "https" && parsed.Host != "" {
		return true
	}
	host := strings.ToLower(parsed.Hostname())
	return parsed.Scheme == "http" && (host == "127.0.0.1" || host == "::1" || host == "localhost")
}

var _ CompatibilityExecutor = (*HTTPCompatibilityExecutor)(nil)

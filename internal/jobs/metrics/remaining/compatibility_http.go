package remaining

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxCompatibilityResponseBytes = 4 * 1024

// HTTPCompatibilityConfig names the one reviewed remaining-metrics bridge.
// No command, callable, credential, or database URL can cross this boundary.
type HTTPCompatibilityConfig struct {
	Endpoint    string
	BearerToken string
}

type HTTPCompatibilityExecutor struct {
	client   *http.Client
	endpoint string
	token    string
}

func NewHTTPCompatibilityExecutor(client *http.Client, config HTTPCompatibilityConfig) (*HTTPCompatibilityExecutor, error) {
	if client == nil || (client.Timeout != 0 && (client.Timeout < 100*time.Millisecond || client.Timeout > 30*time.Second)) ||
		strings.TrimSpace(config.BearerToken) == "" || !validCompatibilityEndpoint(config.Endpoint) {
		return nil, ErrUnavailable
	}
	return &HTTPCompatibilityExecutor{client: client, endpoint: config.Endpoint, token: config.BearerToken}, nil
}

func (executor *HTTPCompatibilityExecutor) ComputePartition(ctx context.Context, run Run, partition Partition) error {
	if run.ID == "" || partition.ID == "" || partition.RunID != run.ID {
		return ErrInvalidState
	}
	return executor.post(ctx, compatibilityRequest{
		Operation:   "partition",
		RunID:       run.ID,
		PartitionID: partition.ID,
	})
}

type compatibilityRequest struct {
	Operation   string `json:"operation"`
	RunID       string `json:"run_id"`
	PartitionID string `json:"partition_id"`
}

type compatibilityResponse struct {
	Status string `json:"status"`
}

func (executor *HTTPCompatibilityExecutor) post(ctx context.Context, value compatibilityRequest) error {
	if executor == nil || executor.client == nil || executor.endpoint == "" || executor.token == "" {
		return ErrUnavailable
	}
	body, err := json.Marshal(value)
	if err != nil {
		return ErrUnavailable
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, executor.endpoint, bytes.NewReader(body))
	if err != nil {
		return ErrUnavailable
	}
	request.Header.Set("Authorization", "Bearer "+executor.token)
	request.Header.Set("Content-Type", "application/json")
	response, err := executor.client.Do(request)
	if err != nil {
		return ErrUnavailable
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, maxCompatibilityResponseBytes+1))
	if err != nil || len(data) > maxCompatibilityResponseBytes || response.StatusCode < 200 || response.StatusCode >= 300 {
		return ErrUnavailable
	}
	var decoded compatibilityResponse
	if err := json.Unmarshal(data, &decoded); err != nil || (decoded.Status != "success" && decoded.Status != "skipped") {
		return ErrUnavailable
	}
	return nil
}

func validCompatibilityEndpoint(raw string) bool {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		parsed.Path != "/internal/worker/remaining-metrics/v1/execute" {
		return false
	}
	if parsed.Scheme == "https" && parsed.Host != "" {
		return true
	}
	host := strings.ToLower(parsed.Hostname())
	return parsed.Scheme == "http" && (host == "127.0.0.1" || host == "::1" || host == "localhost")
}

package daily

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxCompatibilityResponseBytes = 4 * 1024

// CHAOS-4264: the Python bridge's error responses (both the 503 from a
// failed execution and the 409 from a claim it refused) carry a bounded
// "reason" string alongside the durable "state". These sentinels let the
// daily.go call sites attach the matching jobruntime.Reason without this
// package importing jobruntime itself or the response body's raw text ever
// crossing into an error message.
var (
	ErrCompatibilityProcessSignaled   = errors.New("daily metrics compatibility runner was terminated by a signal")
	ErrCompatibilityResourceExhausted = errors.New("daily metrics compatibility runner exceeded its memory bound")
	ErrCompatibilityAmbiguousRefused  = errors.New("daily metrics compatibility execution claim was refused")
)

// compatibilityErrorBody is the shape of a non-2xx response body from the
// Python bridge. FastAPI's HTTPException(detail={...}) serializes as
// {"detail": {...}} -- NOT a flat top-level object -- so "reason" (and
// everything else worker_metrics._execute/_reserve_execution puts in
// detail) is nested one level down. Only "reason" is read here, and only
// against the fixed switch below -- an unrecognized or missing reason falls
// back to the pre-existing ErrUnavailable, exactly as if this field did not
// exist (codex R2: verified against a real FastAPI TestClient response
// before this shape was checked in -- an earlier version of this struct
// looked for a top-level "reason" and silently never matched anything).
type compatibilityErrorBody struct {
	Detail struct {
		Reason string `json:"reason"`
	} `json:"detail"`
}

func classifyCompatibilityError(data []byte) error {
	var body compatibilityErrorBody
	if json.Unmarshal(data, &body) != nil {
		return ErrUnavailable
	}
	switch body.Detail.Reason {
	case "process_signaled":
		return ErrCompatibilityProcessSignaled
	case "resource_exhausted":
		return ErrCompatibilityResourceExhausted
	case "ambiguous_refused":
		return ErrCompatibilityAmbiguousRefused
	default:
		return ErrUnavailable
	}
}

// HTTPCompatibilityConfig names one fixed internal daily-metrics bridge. It
// intentionally has no executable/command field: the server selects the
// reviewed Python computation from durable run state and the fixed operation.
type HTTPCompatibilityConfig struct {
	Endpoint              string
	BearerToken           string
	AllowInsecureInternal bool
}

type HTTPCompatibilityExecutor struct {
	client   *http.Client
	endpoint string
	token    string
}

func NewHTTPCompatibilityExecutor(client *http.Client, config HTTPCompatibilityConfig) (*HTTPCompatibilityExecutor, error) {
	if client == nil || (client.Timeout != 0 && (client.Timeout < 100*time.Millisecond || client.Timeout > 30*time.Second)) ||
		strings.TrimSpace(config.BearerToken) == "" ||
		!validCompatibilityEndpoint(config.Endpoint, config.AllowInsecureInternal) {
		return nil, ErrUnavailable
	}
	return &HTTPCompatibilityExecutor{client: client, endpoint: config.Endpoint, token: config.BearerToken}, nil
}

func (executor *HTTPCompatibilityExecutor) ComputePartition(ctx context.Context, run Run, partition Partition, skipFamilies []string) error {
	if run.ID == "" || partition.ID == "" || partition.RunID != run.ID {
		return ErrInvalidState
	}
	return executor.post(ctx, compatibilityRequest{
		Operation: "partition", RunID: run.ID, PartitionID: partition.ID,
		SkipFamilies: skipFamilies,
	})
}

func (executor *HTTPCompatibilityExecutor) Finalize(ctx context.Context, run Run) error {
	if run.ID == "" {
		return ErrInvalidState
	}
	return executor.post(ctx, compatibilityRequest{Operation: "finalize", RunID: run.ID})
}

type compatibilityRequest struct {
	Operation   string `json:"operation"`
	RunID       string `json:"run_id"`
	PartitionID string `json:"partition_id,omitempty"`
	// SkipFamilies (CHAOS-4276) names families.json families a
	// NativeFamilyExecutor already computed and wrote for this partition --
	// the Python bridge's run_daily_metrics_job(skip_families=...) must not
	// recompute or rewrite them. omitempty keeps every existing/finalize
	// request byte-identical to before this field existed.
	SkipFamilies []string `json:"skip_families,omitempty"`
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
	if err != nil || len(data) > maxCompatibilityResponseBytes {
		return ErrUnavailable
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return classifyCompatibilityError(data)
	}
	var decoded compatibilityResponse
	if err := json.Unmarshal(data, &decoded); err != nil || (decoded.Status != "success" && decoded.Status != "skipped") {
		return ErrUnavailable
	}
	return nil
}

func validCompatibilityEndpoint(raw string, allowInsecure bool) bool {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "/internal/worker/daily-metrics/v1/execute" {
		return false
	}
	if parsed.Scheme == "https" && parsed.Host != "" {
		return true
	}
	host := strings.ToLower(parsed.Hostname())
	if parsed.Scheme != "http" || parsed.Host == "" {
		return false
	}
	if host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback() || (allowInsecure && ip.IsPrivate())
	}
	return allowInsecure && (!strings.Contains(host, ".") ||
		strings.HasSuffix(host, ".internal") || strings.HasSuffix(host, ".local"))
}

var _ CompatibilityExecutor = (*HTTPCompatibilityExecutor)(nil)

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
	// ErrCompatibilityAmbiguousRefused marks a claim refused because the
	// ledger row's ORIGINAL claim is still live ("state": "executing") -- a
	// genuine concurrent overlap that resolves itself once that claim
	// finishes or its lease expires. Retryable, exactly as before CHAOS-4319.
	ErrCompatibilityAmbiguousRefused = errors.New("daily metrics compatibility execution claim was refused")
	// ErrCompatibilityAmbiguousStuck marks a claim refused because the ledger
	// row is stuck at "ambiguous" (CHAOS-4319). worker_metrics.py's
	// _reserve_execution never auto-heals this state -- only a human
	// /metric-executions/v1/{id}/repair call can move it again, so retrying
	// blindly can only ever reproduce the same 409 until River's attempt
	// budget runs out and silently discards the job. Permanent, not
	// Retryable: see retryCompatibilityError and PartitionHandler.Work's
	// failPartitionPermanently call.
	ErrCompatibilityAmbiguousStuck = errors.New("daily metrics compatibility execution ledger is stuck ambiguous")
	// ErrCompatibilityProgressStalled (CHAOS-4316) marks a runner subprocess
	// the bridge's own liveness watchdog killed for reporting no progress
	// within the derived stall window, or for exceeding the hard-ceiling
	// backstop despite trickling progress -- distinct from
	// ErrCompatibilityProcessSignaled (an external/kernel kill) and
	// ErrCompatibilityResourceExhausted (the runner's own RLIMIT_AS): this
	// one is the bridge choosing to kill its own child because it judged it
	// unresponsive, not reacting to an external signal or memory ceiling.
	ErrCompatibilityProgressStalled = errors.New("daily metrics compatibility runner reported no progress within its liveness bound")
)

// compatibilityErrorBody is the shape of a non-2xx response body from the
// Python bridge. FastAPI's HTTPException(detail={...}) serializes as
// {"detail": {...}} -- NOT a flat top-level object -- so "reason" (and
// everything else worker_metrics._execute/_reserve_execution puts in
// detail) is nested one level down. Only "reason" and "state" are read
// here, and only against the fixed switches below -- an unrecognized or
// missing reason falls back to the pre-existing ErrUnavailable, exactly as
// if this field did not exist (codex R2: verified against a real FastAPI
// TestClient response before this shape was checked in -- an earlier
// version of this struct looked for a top-level "reason" and silently never
// matched anything). "state" (CHAOS-4319) distinguishes a genuinely stuck
// ambiguous_refused claim ("ambiguous") from a merely transient one
// ("executing") -- _reserve_execution has always sent this field; Go simply
// did not read it until this ticket.
type compatibilityErrorBody struct {
	Detail struct {
		Reason string `json:"reason"`
		State  string `json:"state"`
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
		if body.Detail.State == "ambiguous" {
			return ErrCompatibilityAmbiguousStuck
		}
		return ErrCompatibilityAmbiguousRefused
	case "progress_stalled":
		return ErrCompatibilityProgressStalled
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
		// CHAOS-4316: when the caller's ctx carries the Go-side liveness
		// ceiling (PartitionHandler.Work's context.WithTimeout backstop) and
		// it fires, client.Do returns a transport error wrapping this exact
		// ctx's own deadline -- classify it the same as the bridge's own
		// progress_stalled kill instead of collapsing it into the generic
		// ErrUnavailable every other transport failure gets. Without this,
		// the backstop firing (the one case it exists for: the bridge's own
		// watchdog could not run) was the single liveness-kill path with NO
		// bounded Reason, silently indistinguishable from an ordinary
		// network blip (codex review). context.Canceled (e.g. graceful
		// shutdown) is deliberately NOT reclassified here -- only a
		// genuine deadline means this specific backstop.
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return ErrCompatibilityProgressStalled
		}
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

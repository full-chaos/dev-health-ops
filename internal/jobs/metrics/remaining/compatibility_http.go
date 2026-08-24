package remaining

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxCompatibilityResponseBytes = 4 * 1024

// CompatibilityObserver reports what one compatibility-bridge partition
// actually did. Before CHAOS-4243, the bridge could only ever report a status
// code, so a partition that wrote real data and a partition that silently
// wrote nothing were indistinguishable from the outside -- exactly the shape
// of the existing DORAObserver/CapacityObserver rationale for native compute.
// rowsWritten is nil when the family's evidence carries no countable row
// signal (not every family's compute returns one); it is a non-nil zero for
// the "success with nothing written" case this ticket exists to surface.
type CompatibilityObserver interface {
	ObserveCompatibilityPartition(family string, rowsWritten *int) error
}

// CompatibilityOutcome reports the bridge's actual write result for one
// partition, distinct from the plain nil-error "success" ComputePartition
// used to return. A caller that only checks the error can no longer tell a
// real write apart from a reported-zero write; CompletePartition's stored
// result string is built from this so the two are never conflated durably.
type CompatibilityOutcome struct {
	RowsWritten *int
}

// HTTPCompatibilityConfig names the one reviewed remaining-metrics bridge.
// No command, callable, credential, or database URL can cross this boundary.
type HTTPCompatibilityConfig struct {
	Endpoint              string
	BearerToken           string
	AllowInsecureInternal bool
	// Logger and Observer are optional. Logger, when set, emits one warning
	// per zero-row completion (CHAOS-4243 telemetry requirement); Observer,
	// when set, increments the family-labeled rows-written/zero-row counters
	// in internal/jobruntime/telemetry.go.
	Logger   *slog.Logger
	Observer CompatibilityObserver
}

type HTTPCompatibilityExecutor struct {
	client   *http.Client
	endpoint string
	token    string
	logger   *slog.Logger
	observer CompatibilityObserver
}

func NewHTTPCompatibilityExecutor(client *http.Client, config HTTPCompatibilityConfig) (*HTTPCompatibilityExecutor, error) {
	if client == nil || (client.Timeout != 0 && (client.Timeout < 100*time.Millisecond || client.Timeout > 30*time.Second)) ||
		strings.TrimSpace(config.BearerToken) == "" ||
		!validCompatibilityEndpoint(config.Endpoint, config.AllowInsecureInternal) {
		return nil, ErrUnavailable
	}
	return &HTTPCompatibilityExecutor{
		client: client, endpoint: config.Endpoint, token: config.BearerToken,
		logger: config.Logger, observer: config.Observer,
	}, nil
}

func (executor *HTTPCompatibilityExecutor) ComputePartition(
	ctx context.Context, run Run, partition Partition,
) (CompatibilityOutcome, error) {
	if run.ID == "" || partition.ID == "" || partition.RunID != run.ID {
		return CompatibilityOutcome{}, ErrInvalidState
	}
	rowsWritten, err := executor.post(ctx, compatibilityRequest{
		Operation:   "partition",
		RunID:       run.ID,
		PartitionID: partition.ID,
	})
	if err != nil {
		return CompatibilityOutcome{}, err
	}
	if executor.observer != nil {
		_ = executor.observer.ObserveCompatibilityPartition(run.Family, rowsWritten)
	}
	if executor.logger != nil && rowsWritten != nil && *rowsWritten == 0 {
		executor.logger.Warn(
			"remaining-metrics compatibility bridge completed with zero rows written",
			"family", run.Family, "run_id", run.ID, "partition_id", partition.ID,
		)
	}
	return CompatibilityOutcome{RowsWritten: rowsWritten}, nil
}

type compatibilityRequest struct {
	Operation   string `json:"operation"`
	RunID       string `json:"run_id"`
	PartitionID string `json:"partition_id"`
}

type compatibilityResponse struct {
	Status string `json:"status"`
	// RowsWritten is an optional row-count signal the Python bridge emits for
	// families whose evidence carries one (release_impact, extra_metrics,
	// recommendations as of CHAOS-4243). Absent (nil) means "not applicable
	// for this family", never "zero" -- only an explicit 0 in the payload
	// means zero rows were written.
	RowsWritten *int `json:"rows_written"`
}

// post returns the bridge's reported row count (nil when the family/response
// carries none) alongside the usual availability error.
func (executor *HTTPCompatibilityExecutor) post(ctx context.Context, value compatibilityRequest) (*int, error) {
	if executor == nil || executor.client == nil || executor.endpoint == "" || executor.token == "" {
		return nil, ErrUnavailable
	}
	body, err := json.Marshal(value)
	if err != nil {
		return nil, ErrUnavailable
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, executor.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, ErrUnavailable
	}
	request.Header.Set("Authorization", "Bearer "+executor.token)
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
	var decoded compatibilityResponse
	if err := json.Unmarshal(data, &decoded); err != nil || (decoded.Status != "success" && decoded.Status != "skipped") {
		return nil, ErrUnavailable
	}
	return decoded.RowsWritten, nil
}

func validCompatibilityEndpoint(raw string, allowInsecure bool) bool {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		parsed.Path != "/internal/worker/remaining-metrics/v1/execute" {
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

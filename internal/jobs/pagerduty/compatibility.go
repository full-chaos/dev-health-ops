package pagerduty

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

	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

const maxCompatibilityResponseBytes = 4 * 1024

var errBridgeUnavailable = errors.New("pagerduty reconciliation bridge unavailable")

type HTTPCompatibilityConfig struct {
	Endpoint              string
	BearerToken           string
	AllowInsecureInternal bool
}

// HTTPCompatibilityReconciler is a transitional bridge, not a native port. The
// authoritative PagerDuty reconciliation still runs in Python
// (reconcile_pagerduty_webhook_with_locked_graph), which is the only writer of
// the eight operational_* ClickHouse tables under the locked-graph contract.
// This type forwards the durable stream identity and the raw webhook payload to
// that Python worker bridge so Go can own admission, receipts, retry, and ACK
// while the reconciliation effect stays Python.
//
// CUT-20 deletion item: delete this file once the locked-graph reconciliation
// itself is ported to Go; the Reconciler seam is what survives.
//
// The bridge endpoint answers {"status": ...}. "processed" and "skipped" are
// durable successes ("skipped" is the Python receipt-already-completed path);
// every other status is a permanent rejection, matching the Python
// retryable=False dead-letter branch rather than burning the delivery budget.
type HTTPCompatibilityReconciler struct {
	client   *http.Client
	endpoint string
	token    string
}

func NewHTTPCompatibilityReconciler(
	client *http.Client,
	config HTTPCompatibilityConfig,
) (*HTTPCompatibilityReconciler, error) {
	if client == nil || client.Timeout < 100*time.Millisecond || client.Timeout > 30*time.Second ||
		strings.TrimSpace(config.BearerToken) == "" || len(config.BearerToken) > 512 ||
		!validBridgeEndpoint(config.Endpoint, config.AllowInsecureInternal) {
		return nil, errBridgeUnavailable
	}
	return &HTTPCompatibilityReconciler{
		client: client, endpoint: config.Endpoint, token: config.BearerToken,
	}, nil
}

// Reconcile returns nil only after the Python bridge reports a committed
// reconciliation. A permanent rejection is surfaced as streamrunner's
// PermanentError so the runner writes the bounded quarantine row before ACK;
// every other failure stays transient and leaves the entry in the PEL.
func (reconciler *HTTPCompatibilityReconciler) Reconcile(ctx context.Context, event Event) error {
	if reconciler == nil || reconciler.client == nil {
		return errBridgeUnavailable
	}
	if event.BindingID == "" || len(event.Payload) == 0 {
		return &streamrunner.PermanentError{Reason: "pagerduty_schema_invalid"}
	}
	body, err := json.Marshal(struct {
		BindingID  string          `json:"binding_id"`
		EventID    string          `json:"event_id"`
		ReceiptID  string          `json:"receipt_id"`
		ReceivedAt string          `json:"received_at"`
		Payload    json.RawMessage `json:"payload"`
	}{
		BindingID:  event.BindingID,
		EventID:    event.EventID,
		ReceiptID:  event.ReceiptID,
		ReceivedAt: event.Received.UTC().Format(time.RFC3339Nano),
		Payload:    event.Payload,
	})
	if err != nil {
		return errors.New("pagerduty reconciliation request encoding failed")
	}
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, reconciler.endpoint, bytes.NewReader(body),
	)
	if err != nil {
		return errors.New("pagerduty reconciliation request construction failed")
	}
	request.Header.Set("Authorization", "Bearer "+reconciler.token)
	request.Header.Set("Content-Type", "application/json")
	response, err := reconciler.client.Do(request)
	if err != nil {
		return errBridgeUnavailable
	}
	defer response.Body.Close()
	data, readErr := io.ReadAll(io.LimitReader(response.Body, maxCompatibilityResponseBytes+1))
	if readErr != nil || len(data) > maxCompatibilityResponseBytes {
		return errors.New("pagerduty reconciliation response unavailable")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		// 408 and 429 are the only client-class statuses a later delivery can
		// resolve; everything else in the 4xx range is a contract rejection.
		if response.StatusCode >= 400 && response.StatusCode < 500 &&
			response.StatusCode != http.StatusRequestTimeout &&
			response.StatusCode != http.StatusTooManyRequests {
			return &streamrunner.PermanentError{Reason: reasonBridgeRejected}
		}
		return errors.New("pagerduty reconciliation bridge rejected request")
	}
	var result struct {
		Status string `json:"status"`
	}
	if json.Unmarshal(data, &result) != nil {
		return errors.New("pagerduty reconciliation response invalid")
	}
	return classifyBridgeStatus(result.Status)
}

// Terminal outcomes the dead-letter record must keep distinguishable. TRD
// section 10.5 requires feature-disabled, malformed, revoked-binding, and
// retry-exhausted to stay separable: an operator triaging the DLQ needs to know
// whether to re-enable a feature, fix a binding, or chase a producer bug, and a
// single collapsed reason answers none of those.
const (
	reasonBridgeRejected  = "pagerduty_bridge_rejected"
	reasonFeatureDisabled = "pagerduty_feature_disabled"
	reasonBindingRevoked  = "pagerduty_binding_revoked"
	reasonSchemaInvalid   = "pagerduty_schema_invalid"
)

// classifyBridgeStatus maps the bridge's bounded status vocabulary onto stream
// outcomes. An unrecognized status is terminal rather than retryable: the
// bridge answered successfully, so repeating the call cannot change the verdict.
func classifyBridgeStatus(status string) error {
	switch status {
	case "processed", "skipped":
		return nil
	case "feature_disabled":
		return &streamrunner.PermanentError{Reason: reasonFeatureDisabled}
	case "revoked_binding":
		return &streamrunner.PermanentError{Reason: reasonBindingRevoked}
	case "malformed":
		return &streamrunner.PermanentError{Reason: reasonSchemaInvalid}
	default:
		return &streamrunner.PermanentError{Reason: reasonBridgeRejected}
	}
}

// validBridgeEndpoint mirrors the operational dispatcher's rule: TLS anywhere,
// plaintext only for loopback unless the explicit internal opt-in is set, and
// never a credential, query, or fragment in a checked-in endpoint.
func validBridgeEndpoint(raw string, allowInsecure bool) bool {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
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
	if !allowInsecure {
		return false
	}
	// Explicit insecure mode is limited to service-discovery names. Public DNS
	// names remain rejected even when the development opt-in is set.
	return !strings.Contains(host, ".") ||
		strings.HasSuffix(host, ".internal") ||
		strings.HasSuffix(host, ".local")
}

var _ Reconciler = (*HTTPCompatibilityReconciler)(nil)

package syncdispatchruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

var (
	ErrInvalidBridge = errors.New("invalid sync dispatch bridge")
	ErrBridgeRequest = errors.New("sync dispatch bridge request failed")
	// ErrBridgeContractRejected wraps ErrBridgeRequest (so every existing
	// errors.Is(err, ErrBridgeRequest) classification still matches
	// unchanged) and additionally distinguishes the one 4xx class a caller
	// may need to treat differently from a 5xx/transport failure: the
	// endpoint rejected the REQUEST SHAPE itself as invalid (a programming
	// error on this side -- an oversized batch, a malformed field), not an
	// estimate/discovery outage on the far side. CHAOS-4175's BudgetGuard
	// chunking (budget_estimate_bridge.go) is the first caller that needs
	// this distinction: a batch that exceeds the endpoint's own documented
	// size limit must fail the pass loudly, not silently admit every unit
	// with zero budget checked.
	ErrBridgeContractRejected = fmt.Errorf("%w: bridge rejected the request as malformed", ErrBridgeRequest)
)

type HTTPBridgeConfig struct {
	BaseURL       string
	BearerToken   string
	Timeout       time.Duration
	AllowInsecure bool
}

type HTTPBridge struct {
	client      *http.Client
	baseURL     *url.URL
	bearerToken string
}

func NewHTTPBridge(config HTTPBridgeConfig) (*HTTPBridge, error) {
	base, err := url.Parse(strings.TrimRight(strings.TrimSpace(config.BaseURL), "/"))
	if err != nil || base == nil || base.Host == "" || (base.Scheme != "https" && !(config.AllowInsecure && base.Scheme == "http")) ||
		strings.TrimSpace(config.BearerToken) == "" || config.Timeout < 100*time.Millisecond || config.Timeout > 30*time.Second {
		return nil, ErrInvalidBridge
	}
	return &HTTPBridge{
		client:      bridgeHTTPClient(config.Timeout),
		baseURL:     base,
		bearerToken: config.BearerToken,
	}, nil
}

func bridgeHTTPClient(connectTimeout time.Duration) *http.Client {
	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		transport = &http.Transport{}
	} else {
		transport = transport.Clone()
	}
	dialer := &net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}
	transport.DialContext = dialer.DialContext
	transport.TLSHandshakeTimeout = connectTimeout
	// Compatibility endpoints do not return headers until execution completes.
	// Their River context, not this connection budget, owns the operation
	// deadline.
	return &http.Client{Transport: transport}
}

func (bridge *HTTPBridge) do(ctx context.Context, path string, payload any) (*http.Response, error) {
	if bridge == nil || bridge.client == nil || bridge.baseURL == nil || strings.TrimSpace(bridge.bearerToken) == "" || ctx == nil {
		return nil, ErrInvalidBridge
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, ErrInvalidBridge
	}
	target := bridge.baseURL.ResolveReference(&url.URL{Path: path})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(encoded))
	if err != nil {
		return nil, ErrInvalidBridge
	}
	request.Header.Set("Authorization", "Bearer "+bridge.bearerToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := bridge.client.Do(request)
	if err != nil {
		// Preserve the real transport failure (connection refused, DNS
		// failure, TLS handshake timeout, context deadline) instead of
		// discarding it -- callers that classify retryability from the
		// error text (isRetryableDiscoveryError) need it, and a bare
		// sentinel with no detail is useless in an incident.
		return nil, fmt.Errorf("%w: %v", ErrBridgeRequest, logging.TransportFailure(err))
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4097))
		// A 4xx means the endpoint parsed and rejected THIS request's own
		// shape (bounded batch size, malformed field, bad auth) -- distinct
		// from a 5xx or transport failure, where the request itself may
		// have been fine and the far side (or the network) is what's
		// unavailable. Every existing errors.Is(err, ErrBridgeRequest)
		// check still matches either way, since ErrBridgeContractRejected
		// wraps it.
		if response.StatusCode >= http.StatusBadRequest && response.StatusCode < http.StatusInternalServerError {
			return nil, fmt.Errorf("%w: status=%d", ErrBridgeContractRejected, response.StatusCode)
		}
		return nil, fmt.Errorf("%w: status=%d", ErrBridgeRequest, response.StatusCode)
	}
	return response, nil
}

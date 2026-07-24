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
)

var (
	ErrInvalidBridge = errors.New("invalid sync dispatch bridge")
	ErrBridgeRequest = errors.New("sync dispatch bridge request failed")
)

// CoordinatorBridge is the reference-only execution seam used by River
// workers. It deliberately does not expose an arbitrary command or payload.
type CoordinatorBridge interface {
	Dispatch(context.Context, DispatchSyncRunArgs) error
	Finalize(context.Context, FinalizeSyncRunArgs) error
	Discover(context.Context, ReferenceDiscoveryArgs) error
	TeamAutoImport(context.Context, DomainReference) error
}

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

type bridgeReference struct {
	OrganizationID  string `json:"organization_id"`
	SyncRunID       string `json:"sync_run_id"`
	OutboxID        string `json:"outbox_id"`
	RouteGeneration int64  `json:"route_generation"`
}

type teamAutoImportReference struct {
	OrganizationID string `json:"organization_id"`
	SyncRunID      string `json:"sync_run_id"`
}

func (bridge *HTTPBridge) Dispatch(ctx context.Context, args DispatchSyncRunArgs) error {
	return bridge.call(ctx, "/api/internal/worker-sync/dispatch", bridgeReferenceFor(args))
}

func (bridge *HTTPBridge) Finalize(ctx context.Context, args FinalizeSyncRunArgs) error {
	return bridge.call(ctx, "/api/internal/worker-sync/finalize", bridgeReferenceFor(args))
}

func (bridge *HTTPBridge) Discover(ctx context.Context, args ReferenceDiscoveryArgs) error {
	return bridge.call(ctx, "/api/internal/worker-sync/reference-discovery", bridgeReferenceFor(args))
}

func (bridge *HTTPBridge) TeamAutoImport(ctx context.Context, reference DomainReference) error {
	if bridge == nil || !uuidPattern.MatchString(reference.OrganizationID) || !uuidPattern.MatchString(reference.SyncRunID) {
		return ErrInvalidBridge
	}
	return bridge.call(ctx, "/api/internal/worker-sync/team-autoimport", teamAutoImportReference{
		OrganizationID: reference.OrganizationID,
		SyncRunID:      reference.SyncRunID,
	})
}

func bridgeReferenceFor(args Args) bridgeReference {
	return bridgeReference{
		OrganizationID:  args.OrganizationID(),
		SyncRunID:       args.SyncRunID(),
		OutboxID:        args.OutboxID(),
		RouteGeneration: args.RouteGeneration(),
	}
}

func (bridge *HTTPBridge) call(ctx context.Context, path string, payload any) error {
	if bridge == nil || bridge.client == nil || bridge.baseURL == nil || strings.TrimSpace(bridge.bearerToken) == "" || ctx == nil {
		return ErrInvalidBridge
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return ErrInvalidBridge
	}
	target := bridge.baseURL.ResolveReference(&url.URL{Path: path})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(encoded))
	if err != nil {
		return ErrInvalidBridge
	}
	request.Header.Set("Authorization", "Bearer "+bridge.bearerToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := bridge.client.Do(request)
	if err != nil {
		return ErrBridgeRequest
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4097))
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("%w: status=%d", ErrBridgeRequest, response.StatusCode)
	}
	return nil
}

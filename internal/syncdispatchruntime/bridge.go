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

// PopulateReferenceDiscovery calls the narrow, identifiers-only
// /reference-discovery-populate endpoint (CHAOS-4175, ruling widened
// 2026-08-24): it wraps run_reference_discovery_populate_for_sync_run
// EXACTLY, wrapping _load_discovery_context's credential/scope resolution
// together with run_team_autoimport_strict as one Python-side step.
//
// This is NOT part of the CoordinatorBridge interface -- it is not a
// fire-and-forget dispatch acknowledgment like Dispatch/Finalize/Discover/
// TeamAutoImport, it is a synchronous call whose RESULT (the populator's
// summary dict, used later for ClickHouse readback verification) the
// caller needs back. teamAutoImportReference is reused verbatim: the
// request carries organization_id/sync_run_id only, by construction of
// that type, not by convention at this call site -- there is no field on
// it a caller could accidentally widen to carry credential material.
func (bridge *HTTPBridge) PopulateReferenceDiscovery(ctx context.Context, orgID, runID string) (map[string]any, error) {
	if bridge == nil || !uuidPattern.MatchString(orgID) || !uuidPattern.MatchString(runID) {
		return nil, ErrInvalidBridge
	}
	return bridge.callWithResult(ctx, "/api/internal/worker-sync/reference-discovery-populate", teamAutoImportReference{
		OrganizationID: orgID,
		SyncRunID:      runID,
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
	response, err := bridge.do(ctx, path, payload)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4097))
	return nil
}

// callWithResult is call's sibling for an endpoint whose response body the
// caller actually needs (unlike every existing CoordinatorBridge method,
// which is a fire-and-forget dispatch acknowledgment). 1 MiB is a generous
// cap for a team/sprint-key summary dict -- large enough that no real
// populator response is at risk of truncation, small enough that a
// misbehaving endpoint cannot make this call buffer an unbounded body.
func (bridge *HTTPBridge) callWithResult(ctx context.Context, path string, payload any) (map[string]any, error) {
	response, err := bridge.do(ctx, path, payload)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	var result map[string]any
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result); err != nil {
		return nil, fmt.Errorf("%w: decode response body: %v", ErrBridgeRequest, err)
	}
	return result, nil
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
		return nil, ErrBridgeRequest
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4097))
		return nil, fmt.Errorf("%w: status=%d", ErrBridgeRequest, response.StatusCode)
	}
	return response, nil
}

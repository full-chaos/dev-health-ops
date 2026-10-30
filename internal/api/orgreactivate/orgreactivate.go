// Package orgreactivate is go-api's client of auth-service's organization
// reactivate route (POST /internal/v1/organizations/reactivate). A suspension
// in the auth schema ends only by this route (D6456), so go-api's PATCH
// is_active=true calls it before it writes its own row, behind
// AUTH_PROXY_ORG_PROVISIONING. It presents the go-api service credential
// (AUTH_SESSION_ISSUE_TOKEN) holding memberships:write, or the go-api
// workload token.
package orgreactivate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-go/workloadtoken"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

const (
	// Path is the reactivate route on auth-service's API listener.
	Path = "/internal/v1/organizations/reactivate"
	// SuspendPath is the suspend route on auth-service's API listener.
	SuspendPath = "/internal/v1/organizations/suspend"
	// StatePath is the organization state read route on auth-service's API listener.
	StatePath = "/internal/v1/organizations/state"
)

var (
	// ErrRefused: auth-service answered with a refusal (any 4xx). The call
	// changed nothing.
	ErrRefused = errors.New("orgreactivate: auth-service refused the reactivation")
	// ErrUnavailable: the route was unreachable or slow, answered 5xx or
	// anything else that is not a reactivated organization.
	ErrUnavailable = errors.New("orgreactivate: auth-service organization reactivate route unavailable")
	// ErrStateChanged: auth-service answered 409 state_changed: the if_revision sent is stale. The call changed nothing.
	ErrStateChanged = errors.New("orgreactivate: the organization changed since the revision read")
)

// Reactivator ends an organization's suspension in the auth schema. A nil
// error means the organization is active there (a no-op included).
type Reactivator interface {
	Reactivate(ctx context.Context, orgID, actor uuid.UUID, ifRevision *int64) error
}

// Suspender suspends an organization in the auth schema. A nil error means the
// organization is suspended there (a no-op included).
type Suspender interface {
	Suspend(ctx context.Context, orgID, actor uuid.UUID, ifRevision *int64) error
}

// State is an organization's status and aggregate revision in the auth schema, read together.
type State struct {
	Status   string
	Revision int64
}

// StateReader reads an organization's State in auth-service.
type StateReader interface {
	State(ctx context.Context, orgID uuid.UUID) (State, error)
}

// Client calls the reactivate and suspend routes of one auth-service over the
// shared provisioning transport (authprovision.Core).
type Client struct {
	core *authprovision.Core
	http *http.Client
}

// New builds a Client for base (AUTH_PROXY_URL's origin) and token. It refuses
// a token that is not an auth-session-issuer credential, and never prints it.
func New(base *url.URL, token string, timeout time.Duration) (*Client, error) {
	if base == nil {
		return nil, errors.New("orgreactivate: no auth-service URL")
	}
	if !sessionissue.ValidToken(token) {
		return nil, fmt.Errorf("%s must be an auth-session-issuer service credential (svc_authsess_...)", sessionissue.EnvToken)
	}
	core, err := authprovision.NewCore(base, token, timeout)
	if err != nil {
		return nil, err
	}
	return &Client{core: core, http: core.HTTP()}, nil
}

// NewWorkload builds a Client for base that presents the workload token of
// workload (see authprovision.NewWorkloadCore).
func NewWorkload(base *url.URL, workload *workloadtoken.Client, timeout time.Duration) (*Client, error) {
	core, err := authprovision.NewWorkloadCore(base, workload, timeout)
	if err != nil {
		return nil, err
	}
	return &Client{core: core, http: core.HTTP()}, nil
}

type request struct {
	OrganizationID   string `json:"organization_id"`
	ActorPrincipalID string `json:"actor_principal_id"`
	IfRevision       *int64 `json:"if_revision,omitempty"`
}

type stateRequest struct {
	OrganizationID string `json:"organization_id"`
}

type answer struct {
	OrganizationID string `json:"organization_id"`
	Status         string `json:"status"`
}

type stateAnswer struct {
	OrganizationID string          `json:"organization_id"`
	Status         string          `json:"status"`
	Revision       json.RawMessage `json:"revision"`
}

// Reactivate asks auth-service to end the suspension of orgID, acting as the
// superuser actor, only if the organization is still at ifRevision when given
// (a stale one is ErrStateChanged and changes nothing). Only an answer that
// names orgID as active is success; the error carries a class and a status,
// never the credential or the answer text.
func (c *Client) Reactivate(ctx context.Context, orgID, actor uuid.UUID, ifRevision *int64) error {
	return c.post(ctx, Path, orgID, actor, "active", ifRevision)
}

// Suspend asks auth-service to suspend orgID, acting as the superuser actor,
// guarded by ifRevision as Reactivate is. Only an answer that names orgID as
// suspended is success, with the same error classes as Reactivate.
func (c *Client) Suspend(ctx context.Context, orgID, actor uuid.UUID, ifRevision *int64) error {
	return c.post(ctx, SuspendPath, orgID, actor, "suspended", ifRevision)
}

// State reads orgID's status and aggregate revision from auth-service in one call.
func (c *Client) State(ctx context.Context, orgID uuid.UUID) (State, error) {
	var got stateAnswer
	err := c.core.Post(ctx, StatePath, stateRequest{OrganizationID: orgID.String()}, &got)
	var refused *authprovision.Refused
	switch {
	case errors.As(err, &refused):
		return State{}, fmt.Errorf("%w: status %d", ErrRefused, refused.Status)
	case err != nil:
		return State{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	revision, ok := parseRevision(got.Revision)
	if got.OrganizationID != orgID.String() || (got.Status != "active" && got.Status != "suspended") || !ok {
		return State{}, fmt.Errorf("%w: answer is not an organization state", ErrUnavailable)
	}
	return State{Status: got.Status, Revision: revision}, nil
}

func parseRevision(raw json.RawMessage) (int64, bool) {
	if len(raw) == 0 {
		return 0, false
	}
	for _, c := range raw {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(string(raw), 10, 64)
	return n, err == nil
}

func (c *Client) post(ctx context.Context, path string, orgID, actor uuid.UUID, wantStatus string, ifRevision *int64) error {
	var got answer
	err := c.core.Post(ctx, path, request{OrganizationID: orgID.String(), ActorPrincipalID: actor.String(), IfRevision: ifRevision}, &got)
	var refused *authprovision.Refused
	switch {
	case errors.As(err, &refused):
		if refused.Status == http.StatusConflict && ifRevision != nil {
			return fmt.Errorf("%w: status %d", ErrStateChanged, refused.Status)
		}
		return fmt.Errorf("%w: status %d", ErrRefused, refused.Status)
	case err != nil:
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	case got.OrganizationID != orgID.String() || got.Status != wantStatus:
		return fmt.Errorf("%w: answer is not the organization %s", ErrUnavailable, wantStatus)
	}
	return nil
}

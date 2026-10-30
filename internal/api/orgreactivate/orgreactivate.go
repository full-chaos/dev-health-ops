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
	"errors"
	"fmt"
	"net/http"
	"net/url"
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
)

var (
	// ErrRefused: auth-service answered with a refusal (any 4xx). The call
	// changed nothing.
	ErrRefused = errors.New("orgreactivate: auth-service refused the reactivation")
	// ErrUnavailable: the route was unreachable or slow, answered 5xx or
	// anything else that is not a reactivated organization.
	ErrUnavailable = errors.New("orgreactivate: auth-service organization reactivate route unavailable")
)

// Reactivator ends an organization's suspension in the auth schema. A nil
// error means the organization is active there (a no-op included).
type Reactivator interface {
	Reactivate(ctx context.Context, orgID, actor uuid.UUID) error
}

// Suspender suspends an organization in the auth schema. A nil error means the
// organization is suspended there (a no-op included).
type Suspender interface {
	Suspend(ctx context.Context, orgID, actor uuid.UUID) error
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
}

type answer struct {
	OrganizationID string `json:"organization_id"`
	Status         string `json:"status"`
}

// Reactivate asks auth-service to end the suspension of orgID, acting as the
// superuser actor. Only an answer that names orgID as active is success; the
// error carries a class and a status, never the credential or the answer text.
func (c *Client) Reactivate(ctx context.Context, orgID, actor uuid.UUID) error {
	return c.post(ctx, Path, orgID, actor, "active")
}

// Suspend asks auth-service to suspend orgID, acting as the superuser actor.
// Only an answer that names orgID as suspended is success, with the same error
// classes as Reactivate.
func (c *Client) Suspend(ctx context.Context, orgID, actor uuid.UUID) error {
	return c.post(ctx, SuspendPath, orgID, actor, "suspended")
}

func (c *Client) post(ctx context.Context, path string, orgID, actor uuid.UUID, wantStatus string) error {
	var got answer
	err := c.core.Post(ctx, path, request{OrganizationID: orgID.String(), ActorPrincipalID: actor.String()}, &got)
	var refused *authprovision.Refused
	switch {
	case errors.As(err, &refused):
		return fmt.Errorf("%w: status %d", ErrRefused, refused.Status)
	case err != nil:
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	case got.OrganizationID != orgID.String() || got.Status != wantStatus:
		return fmt.Errorf("%w: answer is not the organization %s", ErrUnavailable, wantStatus)
	}
	return nil
}

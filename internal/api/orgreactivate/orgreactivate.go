// Package orgreactivate is go-api's client of auth-service's organization
// reactivate route (POST /internal/v1/organizations/reactivate). A suspension
// in the auth schema ends only by this route (D6456), so go-api's PATCH
// is_active=true calls it before it writes its own row, behind
// AUTH_PROXY_ORG_PROVISIONING. It presents the go-api service credential
// (AUTH_SESSION_ISSUE_TOKEN) holding memberships:write.
package orgreactivate

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
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

const (
	// Path is the reactivate route on auth-service's API listener.
	Path = "/internal/v1/organizations/reactivate"
	// SuspendPath is the suspend route on auth-service's API listener.
	SuspendPath = "/internal/v1/organizations/suspend"
)

const (
	maxAnswerBytes = 64 << 10
	defaultTimeout = 10 * time.Second
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

// Client calls the reactivate and suspend routes of one auth-service.
type Client struct {
	origin string
	token  string
	http   *http.Client
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
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	origin := url.URL{Scheme: base.Scheme, Host: base.Host}
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          4,
		MaxIdleConnsPerHost:   4,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: time.Second,
		ForceAttemptHTTP2:     true,
	}
	return &Client{origin: origin.String(), token: token, http: &http.Client{
		Transport:     transport,
		Timeout:       timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
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
	raw, err := json.Marshal(request{OrganizationID: orgID.String(), ActorPrincipalID: actor.String()})
	if err != nil {
		return fmt.Errorf("%w: encoding the request", ErrUnavailable)
	}
	out, err := http.NewRequestWithContext(ctx, http.MethodPost, c.origin+path, bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("%w: building the request", ErrUnavailable)
	}
	out.Header.Set("Authorization", "Bearer "+c.token)
	out.Header.Set("Content-Type", "application/json")
	out.Header.Set("Accept", "application/json")
	response, err := c.http.Do(out)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, errors.Unwrap(err))
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxAnswerBytes+1))
	switch {
	case response.StatusCode >= 400 && response.StatusCode < 500:
		return fmt.Errorf("%w: status %d", ErrRefused, response.StatusCode)
	case response.StatusCode != http.StatusOK:
		return fmt.Errorf("%w: status %d", ErrUnavailable, response.StatusCode)
	case err != nil:
		return fmt.Errorf("%w: reading the answer", ErrUnavailable)
	case len(body) > maxAnswerBytes:
		return fmt.Errorf("%w: answer over %d bytes", ErrUnavailable, maxAnswerBytes)
	}
	var got answer
	if err := json.Unmarshal(body, &got); err != nil {
		return fmt.Errorf("%w: answer is not JSON", ErrUnavailable)
	}
	if got.OrganizationID != orgID.String() || got.Status != wantStatus {
		return fmt.Errorf("%w: answer is not the organization %s", ErrUnavailable, wantStatus)
	}
	return nil
}

// Package sessionissue is go-api's client of auth-service's session issue
// route (POST /internal/v1/sessions). With AUTH_PROXY_SESSIONS on, auth-service
// owns every session: the go-api flows that still sign a user in (login,
// switch-org, accept-invite, change-password, social and SSO login) take their
// token pair from this route instead of minting one and writing
// public.refresh_tokens.
package sessionissue

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"
)

// EnvToken is the go-api service credential auth-service checks on the route.
const EnvToken = "AUTH_SESSION_ISSUE_TOKEN"

// Path is the issue route on auth-service's API listener.
const Path = "/internal/v1/sessions"

// UnavailableText is the 503 detail a route answers when no pair could be
// issued, the text the auth proxy answers for an unreachable auth-service.
const UnavailableText = "Authentication service unavailable"

// ErrUnavailable is every failure to obtain a pair: the route unreachable or
// slow, a refusal, or an answer that is not a pair. A route answers it with
// 503 and mints nothing.
var ErrUnavailable = errors.New("sessionissue: session issue route unavailable")

// Request names the session to issue.
type Request struct {
	PrincipalID    uuid.UUID
	OrganizationID *uuid.UUID
	AMR            []string
}

// Pair is an issued access token and its opaque refresh credential.
type Pair struct {
	Access  string
	Refresh string
}

// Issuer issues sessions.
type Issuer interface {
	Issue(ctx context.Context, r *http.Request, req Request) (Pair, error)
}

// maxAnswerBytes bounds the issue route's answer.
const maxAnswerBytes = 64 << 10

// Client calls the issue route of one auth-service.
type Client struct{}

// New builds a Client.
func New(*url.URL, string, time.Duration) (*Client, error) { return &Client{}, nil }

// Issue asks auth-service for a session of req.
func (c *Client) Issue(context.Context, *http.Request, Request) (Pair, error) {
	return Pair{}, ErrUnavailable
}

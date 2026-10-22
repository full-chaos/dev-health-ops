// Package sessionissue is go-api's client of auth-service's session issue
// route (POST /internal/v1/sessions). With AUTH_PROXY_SESSIONS on, auth-service
// owns every session: the go-api flows that still sign a user in (login,
// switch-org, accept-invite, change-password, social and SSO login) take their
// token pair from this route instead of minting one and writing
// public.refresh_tokens.
package sessionissue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/clientip"
)

// EnvToken is the go-api service credential auth-service checks on the route.
const EnvToken = "AUTH_SESSION_ISSUE_TOKEN"

// Path is the issue route on auth-service's API listener.
const Path = "/internal/v1/sessions"

// tokenPattern is the svc_ credential shape of the auth-session-issuer
// service: its prefix and 32 random bytes as unpadded base64url.
var tokenPattern = regexp.MustCompile(`^svc_authsess_[A-Za-z0-9_-]{43}$`)

// maxAnswerBytes bounds the issue route's answer.
const maxAnswerBytes = 64 << 10

// defaultTimeout bounds one issue call.
const defaultTimeout = 10 * time.Second

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

// Client calls the issue route of one auth-service.
type Client struct {
	endpoint string
	token    string
	http     *http.Client
}

// New builds a Client for base (AUTH_PROXY_URL's origin) and token. It
// refuses a token that is not an auth-session-issuer credential.
func New(base *url.URL, token string, timeout time.Duration) (*Client, error) {
	if base == nil {
		return nil, errors.New("sessionissue: no auth-service URL")
	}
	if !tokenPattern.MatchString(token) {
		return nil, fmt.Errorf("%s must be an auth-session-issuer service credential (svc_authsess_...)", EnvToken)
	}
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	endpoint := url.URL{Scheme: base.Scheme, Host: base.Host, Path: Path}
	// The bearer is a service credential: no proxy from the environment, a
	// connection pool of its own, and no redirect following, as the auth
	// proxy dials auth-service.
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          16,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: time.Second,
		ForceAttemptHTTP2:     true,
	}
	return &Client{endpoint: endpoint.String(), token: token, http: &http.Client{
		Transport:     transport,
		Timeout:       timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

type issueBody struct {
	PrincipalID    string   `json:"principal_id"`
	OrganizationID *string  `json:"organization_id"`
	AMR            []string `json:"amr"`
}

type issueAnswer struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type"`
}

// Issue asks auth-service for a session of req. The end user's User-Agent
// and client address travel with the call, so auth-service records them on
// the session and its audit row. Every failure wraps ErrUnavailable.
func (c *Client) Issue(ctx context.Context, r *http.Request, req Request) (Pair, error) {
	body := issueBody{PrincipalID: req.PrincipalID.String(), AMR: req.AMR}
	if req.OrganizationID != nil {
		org := req.OrganizationID.String()
		body.OrganizationID = &org
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return Pair{}, fmt.Errorf("%w: encoding the request: %w", ErrUnavailable, err)
	}
	out, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(raw))
	if err != nil {
		return Pair{}, fmt.Errorf("%w: building the request: %w", ErrUnavailable, err)
	}
	out.Header.Set("Authorization", "Bearer "+c.token)
	out.Header.Set("Content-Type", "application/json")
	out.Header.Set("Accept", "application/json")
	out.Header.Set("User-Agent", r.UserAgent())
	if addr, err := netip.ParseAddr(clientip.FromRequest(r)); err == nil {
		out.Header.Set("X-Forwarded-For", addr.String())
	}
	response, err := c.http.Do(out)
	if err != nil {
		return Pair{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	defer response.Body.Close()
	answer, err := io.ReadAll(io.LimitReader(response.Body, maxAnswerBytes+1))
	if err != nil {
		return Pair{}, fmt.Errorf("%w: reading the answer: %w", ErrUnavailable, err)
	}
	if response.StatusCode != http.StatusOK {
		return Pair{}, fmt.Errorf("%w: status %d", ErrUnavailable, response.StatusCode)
	}
	if len(answer) > maxAnswerBytes {
		return Pair{}, fmt.Errorf("%w: answer over %d bytes", ErrUnavailable, maxAnswerBytes)
	}
	var pair issueAnswer
	if err := json.Unmarshal(answer, &pair); err != nil {
		return Pair{}, fmt.Errorf("%w: answer is not JSON", ErrUnavailable)
	}
	if pair.AccessToken == "" || pair.RefreshToken == "" || !strings.EqualFold(pair.TokenType, "bearer") {
		return Pair{}, fmt.Errorf("%w: answer is not a token pair", ErrUnavailable)
	}
	return Pair{Access: pair.AccessToken, Refresh: pair.RefreshToken}, nil
}

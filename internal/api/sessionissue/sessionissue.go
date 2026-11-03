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

	"github.com/full-chaos/dev-health-go/workloadtoken"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/clientip"
	"github.com/full-chaos/dev-health-ops/internal/httpguard"
)

// EnvToken is the go-api service credential auth-service checks on the route.
const EnvToken = "AUTH_SESSION_ISSUE_TOKEN"

// Path is the issue route on auth-service's API listener.
const Path = "/internal/v1/sessions"

// tokenPattern is the svc_ credential shape of the auth-session-issuer
// service: its prefix and 32 random bytes as unpadded base64url.
var tokenPattern = regexp.MustCompile(`^svc_authsess_[A-Za-z0-9_-]{43}$`)

// ValidToken reports whether token has the auth-session-issuer credential
// shape, for the other go-api clients of auth-service that present it.
func ValidToken(token string) bool { return tokenPattern.MatchString(token) }

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

// ErrSessionEnded is a superseding issue auth-service refused because the
// predecessor session is no longer live (401 session_revoked): the caller's
// own session ended first, for example under a concurrent organization
// switch from it. A route answers it with 401 and mints nothing.
var ErrSessionEnded = errors.New("sessionissue: the predecessor session has ended")

// RefusedError is a refusal the issue route answered with its error.v1
// document. It is ErrUnavailable too, so a caller that answers 503 for every
// failure still does.
type RefusedError struct {
	Status int
	Reason string
}

func (e *RefusedError) Error() string {
	return fmt.Sprintf("%v: status %d, reason %s", ErrUnavailable, e.Status, e.Reason)
}

func (e *RefusedError) Unwrap() error { return ErrUnavailable }

// reasonCodePattern is an error.v1 reason code.
var reasonCodePattern = regexp.MustCompile(`^[a-z][a-z_]{0,63}$`)

// refusalIn is the refusal of a non-200 answer when its body is the error.v1
// document for that status, else nil. A body that names a member twice is not
// that document: the decision never rests on whichever value came last.
func refusalIn(status int, answer []byte) *RefusedError {
	if len(answer) > maxAnswerBytes {
		return nil
	}
	members, ok := objectMembers(answer)
	if !ok {
		return nil
	}
	var schemaVersion, reasonCode string
	var docStatus int
	if json.Unmarshal(members["schema_version"], &schemaVersion) != nil || json.Unmarshal(members["status"], &docStatus) != nil ||
		json.Unmarshal(members["reason_code"], &reasonCode) != nil {
		return nil
	}
	if schemaVersion != "error.v1" || docStatus != status || !reasonCodePattern.MatchString(reasonCode) {
		return nil
	}
	return &RefusedError{Status: status, Reason: reasonCode}
}

// objectMembers reads raw as exactly one JSON object and returns its members.
// It refuses anything else: a repeated member, trailing data, or not an object.
func objectMembers(raw []byte) (map[string]json.RawMessage, bool) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, false
	}
	members := map[string]json.RawMessage{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, false
		}
		name, _ := tok.(string)
		if _, seen := members[name]; seen {
			return nil, false
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return nil, false
		}
		members[name] = value
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return nil, false
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, false
	}
	return members, true
}

// Request names the session to issue.
type Request struct {
	PrincipalID    uuid.UUID
	OrganizationID *uuid.UUID
	AMR            []string
	// PredecessorSessionID is the caller's own session, which this issue
	// supersedes: auth-service revokes it in the transaction that creates
	// the new session (an organization switch, CHAOS-12085). Nil issues
	// without touching any other session.
	PredecessorSessionID *uuid.UUID
}

// Pair is an issued access token and its opaque refresh credential. Under
// auth-service's eddsa access token class SessionAccess is the token of
// audience auth-service its session routes accept; it is empty otherwise.
type Pair struct {
	Access        string
	Refresh       string
	SessionAccess string
	// ExpiresIn is the access token's lifetime in seconds as auth-service answered it (1..3600).
	ExpiresIn int64
}

// Issuer issues sessions.
type Issuer interface {
	Issue(ctx context.Context, r *http.Request, req Request) (Pair, error)
}

// WorkloadAudience is the audience of the workload token auth-service accepts on its session issue and session state routes.
const WorkloadAudience = "auth-service"

// Client calls the issue route of one auth-service, and its session state
// route (State). It presents either the session credential (token, set by New)
// or the go-api workload token (workload set, see NewWorkload); bearer is the
// one place that chooses.
type Client struct {
	endpoint      string
	stateEndpoint string
	token         string
	workload      *workloadtoken.Client
	http          *http.Client
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
	stateEndpoint := url.URL{Scheme: base.Scheme, Host: base.Host, Path: PathState}
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
	return &Client{endpoint: endpoint.String(), stateEndpoint: stateEndpoint.String(), token: token, http: &http.Client{
		Transport:     transport,
		Timeout:       timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

// NewWorkload builds a Client for base (AUTH_PROXY_URL's origin) that presents
// the workload token of workload (audience WorkloadAudience). No token is held:
// Issue and State ask the workload client for one on every call (it is cached,
// and one exchange serves every call of its lifetime), so a call with no usable token
// fails as ErrUnavailable with nothing sent. There is no session credential to
// fall back to. The token is set here, not by the workload client's own
// transport, because that transport drops every header but a fixed few and
// the end user's X-Forwarded-For must reach auth-service. The client uses the
// shared outbound transport (no proxy from the environment) and follows no
// redirect.
func NewWorkload(base *url.URL, workload *workloadtoken.Client, timeout time.Duration) (*Client, error) {
	if base == nil {
		return nil, errors.New("sessionissue: no auth-service URL")
	}
	if workload == nil {
		return nil, errors.New("sessionissue: no workload token client")
	}
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	endpoint := url.URL{Scheme: base.Scheme, Host: base.Host, Path: Path}
	stateEndpoint := url.URL{Scheme: base.Scheme, Host: base.Host, Path: PathState}
	client := httpguard.NoRedirects(&http.Client{Transport: httpguard.SharedTransport(), Timeout: timeout})
	return &Client{endpoint: endpoint.String(), stateEndpoint: stateEndpoint.String(), workload: workload, http: client}, nil
}

// bearer is the credential of one call: the session credential, or the workload token asked of the workload client
// (cached there; one exchange serves every call of its lifetime). A workload token that cannot be had fails the call as
// ErrUnavailable before anything is sent; there is no fallback to a session credential.
func (c *Client) bearer(ctx context.Context) (string, error) {
	if c.workload == nil {
		return c.token, nil
	}
	token, err := c.workload.Token(ctx, WorkloadAudience)
	if err != nil {
		return "", fmt.Errorf("%w: workload token: %w", ErrUnavailable, err)
	}
	return token, nil
}

type issueBody struct {
	PrincipalID          string   `json:"principal_id"`
	OrganizationID       *string  `json:"organization_id"`
	AMR                  []string `json:"amr"`
	PredecessorSessionID *string  `json:"predecessor_session_id,omitempty"`
}

// maxExpiresIn is the access-token.v1 lifetime ceiling in seconds: an issue answer above it is not a token pair.
const maxExpiresIn = 3600

type issueAnswer struct {
	AccessToken        string `json:"access_token"`
	RefreshToken       string `json:"refresh_token"`
	TokenType          string `json:"token_type"`
	SessionAccessToken string `json:"session_access_token"`
	ExpiresIn          *int64 `json:"expires_in"`
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
	if req.PredecessorSessionID != nil {
		predecessor := req.PredecessorSessionID.String()
		body.PredecessorSessionID = &predecessor
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return Pair{}, fmt.Errorf("%w: encoding the request: %w", ErrUnavailable, err)
	}
	out, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(raw))
	if err != nil {
		return Pair{}, fmt.Errorf("%w: building the request: %w", ErrUnavailable, err)
	}
	bearer, err := c.bearer(ctx)
	if err != nil {
		return Pair{}, err
	}
	out.Header.Set("Authorization", "Bearer "+bearer)
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
		if refused := refusalIn(response.StatusCode, answer); refused != nil {
			// A superseding issue refused because the predecessor already
			// ended is the caller's own session ending, not an outage.
			if req.PredecessorSessionID != nil && refused.Status == http.StatusUnauthorized && refused.Reason == "session_revoked" {
				return Pair{}, ErrSessionEnded
			}
			return Pair{}, refused
		}
		return Pair{}, fmt.Errorf("%w: status %d", ErrUnavailable, response.StatusCode)
	}
	if len(answer) > maxAnswerBytes {
		return Pair{}, fmt.Errorf("%w: answer over %d bytes", ErrUnavailable, maxAnswerBytes)
	}
	var pair issueAnswer
	if err := json.Unmarshal(answer, &pair); err != nil {
		return Pair{}, fmt.Errorf("%w: answer is not JSON", ErrUnavailable)
	}
	// An answer without expires_in is go-api's 60 minutes (the issue route has always sent it; older fakes did not).
	expiresIn := int64(maxExpiresIn)
	if pair.ExpiresIn != nil {
		expiresIn = *pair.ExpiresIn
	}
	if pair.AccessToken == "" || pair.RefreshToken == "" || !strings.EqualFold(pair.TokenType, "bearer") ||
		expiresIn < 1 || expiresIn > maxExpiresIn {
		return Pair{}, fmt.Errorf("%w: answer is not a token pair", ErrUnavailable)
	}
	return Pair{Access: pair.AccessToken, Refresh: pair.RefreshToken, SessionAccess: pair.SessionAccessToken, ExpiresIn: expiresIn}, nil
}

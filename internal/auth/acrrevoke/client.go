// Package acrrevoke is the client of acr's on-behalf credential revoke (POST
// /internal/v1/credentials/{credential_id}/revoke, body credential_revoke_on_behalf_request.v1): the Auth Control
// Plane revokes an MCP-bound credential for its approving user or an organization admin it authorized (CHAOS-12959).
//
// One Revoke sends exactly two requests: the RFC 8693 token exchange of acrlisting (the workload's subject token for
// an access token of the credential's organization, which carries the binding role "revoke"), then the revoke with
// that access token. It follows no redirect, retries nothing and caches no token, and its errors name no token, no
// body and no credential id.
package acrrevoke

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/auth/acrlisting"
	"github.com/full-chaos/dev-health-ops/internal/httpguard"
)

const (
	revokeSchema   = "credential_revoke_on_behalf_request.v1"
	errorSchema    = "error.v1"
	defaultTimeout = 20 * time.Second
	maxAnswerBytes = 16 << 10
	maxActorID     = 200

	// ActorUser is the user who approved the credential; ActorAdmin an admin of the credential's organization.
	ActorUser  = "user"
	ActorAdmin = "admin"
)

var credentialIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// ValidCredentialID reports whether id has the form of an acr credential id.
func ValidCredentialID(id string) bool { return credentialIDPattern.MatchString(id) }

var (
	// ErrNotFound is acr's one answer to every refusal: an unknown id, a credential that is not MCP-bound, one of
	// another organization, or a user who did not approve it.
	ErrNotFound = errors.New("acrrevoke: credential not found")
	// ErrConflict is acr's answer to a credential that is already revoked, for an actor the credential admits.
	ErrConflict = errors.New("acrrevoke: credential lifecycle conflict")
	// ErrInvalidRequest is a Revoke input this client refuses before it sends anything.
	ErrInvalidRequest = errors.New("acrrevoke: invalid request")
	// ErrInvalidConfig reports an unusable Config.
	ErrInvalidConfig = errors.New("acrrevoke: invalid configuration")
)

// Error is a failed revoke that is neither ErrNotFound nor ErrConflict. Kind is acrlisting's closed failure class;
// Status is acr's HTTP status, or zero.
type Error struct {
	Kind   acrlisting.Kind
	Status int
	cause  error
}

func (e *Error) Error() string {
	if e.Status != 0 {
		return "acrrevoke: " + string(e.Kind) + " (status " + strconv.Itoa(e.Status) + ")"
	}
	return "acrrevoke: " + string(e.Kind)
}

// Unwrap exposes only a context error.
func (e *Error) Unwrap() error { return e.cause }

func contextCause(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	}
	return nil
}

// Config builds a Client.
type Config struct {
	// BaseURL is the origin of acr's internal API, TokenEndpoint acr's token endpoint; both as acrlisting.Config.
	BaseURL       *url.URL
	TokenEndpoint *url.URL
	// SubjectTokenFile is the projected ServiceAccount token of the workload bound in acr with the role "revoke".
	SubjectTokenFile string
	// HTTPClient is optional; a nil one is httpguard.NewClient. Whatever is given follows no redirect.
	HTTPClient *http.Client
	Timeout    time.Duration
}

// Client revokes credentials of any organization the workload is bound in. It is safe for concurrent use.
type Client struct {
	base, endpoint *url.URL
	subjectFile    string
	http           *http.Client
}

// New returns a Client.
func New(cfg Config) (*Client, error) {
	switch {
	case !acrlisting.ValidOrigin(cfg.BaseURL) || cfg.BaseURL.RawQuery != "":
		return nil, fmt.Errorf("%w: base url", ErrInvalidConfig)
	case !acrlisting.ValidOrigin(cfg.TokenEndpoint):
		return nil, fmt.Errorf("%w: token endpoint", ErrInvalidConfig)
	case strings.TrimSpace(cfg.SubjectTokenFile) == "":
		return nil, fmt.Errorf("%w: subject token file", ErrInvalidConfig)
	}
	c := &Client{base: cfg.BaseURL, endpoint: cfg.TokenEndpoint, subjectFile: cfg.SubjectTokenFile}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	if cfg.HTTPClient != nil {
		c.http = httpguard.NoRedirects(cfg.HTTPClient)
	} else {
		c.http = httpguard.NewClient(timeout)
	}
	return c, nil
}

func validActorID(id string) bool {
	return id != "" && strings.TrimSpace(id) == id && utf8.ValidString(id) && utf8.RuneCountInString(id) <= maxActorID
}

// Revoke revokes credentialID of orgID on behalf of the actor. orgID is a lowercase UUID, actorType ActorUser or
// ActorAdmin; the caller has authorized the actor. nil is acr's 204.
func (c *Client) Revoke(ctx context.Context, orgID, credentialID, actorType, actorID string) error {
	if !acrlisting.ValidOrgID(orgID) || !ValidCredentialID(credentialID) || (actorType != ActorUser && actorType != ActorAdmin) || !validActorID(actorID) {
		return ErrInvalidRequest
	}
	token, _, err := acrlisting.Exchange(ctx, c.http, c.endpoint, c.subjectFile, orgID)
	if err != nil {
		var refusal *acrlisting.Error
		if errors.As(err, &refusal) {
			return &Error{Kind: refusal.Kind, Status: refusal.Status, cause: contextCause(err)}
		}
		return &Error{Kind: acrlisting.KindExchange, cause: contextCause(err)}
	}
	body, err := json.Marshal(struct {
		SchemaVersion string `json:"schema_version"`
		ActorType     string `json:"actor_type"`
		ActorID       string `json:"actor_id"`
	}{revokeSchema, actorType, actorID})
	if err != nil {
		return &Error{Kind: acrlisting.KindTransport}
	}
	target := c.base.JoinPath("internal", "v1", "credentials", credentialID, "revoke")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(body))
	if err != nil {
		return &Error{Kind: acrlisting.KindTransport}
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return &Error{Kind: acrlisting.KindTransport, cause: contextCause(err)}
	}
	defer resp.Body.Close()
	answer, err := io.ReadAll(io.LimitReader(resp.Body, maxAnswerBytes+1))
	if err != nil {
		return &Error{Kind: acrlisting.KindTransport, cause: contextCause(err)}
	}
	return classify(resp.StatusCode, answer)
}

// classify maps acr's answer. A 404 or 409 is acr's refusal only when its body is acr's error.v1 envelope with the
// route's own code and status; any other 404 or 409 (a route acr does not serve, a proxy page) is a rejected call,
// so a missing route never reads as "no such credential".
func classify(status int, answer []byte) error {
	switch status {
	case http.StatusNoContent:
		return nil
	case http.StatusNotFound:
		if errorCode(answer, status) == "not_found" {
			return ErrNotFound
		}
	case http.StatusConflict:
		if errorCode(answer, status) == "credential_lifecycle_conflict" {
			return ErrConflict
		}
	case http.StatusUnauthorized:
		return &Error{Kind: acrlisting.KindUnauthorized, Status: status}
	case http.StatusForbidden:
		return &Error{Kind: acrlisting.KindForbidden, Status: status}
	case http.StatusTooManyRequests:
		return &Error{Kind: acrlisting.KindRateLimited, Status: status}
	}
	if status >= 500 {
		return &Error{Kind: acrlisting.KindUnavailable, Status: status}
	}
	return &Error{Kind: acrlisting.KindRejected, Status: status}
}

func errorCode(answer []byte, status int) string {
	if len(answer) > maxAnswerBytes {
		return ""
	}
	var envelope struct {
		SchemaVersion string `json:"schema_version"`
		Error         struct {
			Code       string `json:"code"`
			HTTPStatus int    `json:"http_status"`
		} `json:"error"`
	}
	if json.Unmarshal(answer, &envelope) != nil || envelope.SchemaVersion != errorSchema || envelope.Error.HTTPStatus != status {
		return ""
	}
	return envelope.Error.Code
}

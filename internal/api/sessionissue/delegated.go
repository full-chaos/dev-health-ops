package sessionissue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/clientip"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

// ReasonAdminImpersonation is the reason go-api sends for a delegated session
// started by POST /api/v1/admin/impersonate.
const ReasonAdminImpersonation = "admin_impersonation"

// DelegatedRequest names the delegated session to start. The actor is the
// principal of the base session; the body names none.
type DelegatedRequest struct {
	BaseSessionID     uuid.UUID
	TargetPrincipalID uuid.UUID
	OrganizationID    uuid.UUID
	Reason            string
}

// DelegatedSession is a started delegated session and its tokens.
type DelegatedSession struct {
	SessionID uuid.UUID
	ExpiresAt time.Time
	Access    string
	// SessionAccess is the token of audience auth-service, under the eddsa class only.
	SessionAccess string
	// ExpiresIn is the access token's lifetime in seconds (1..3600).
	ExpiresIn int64
}

// Delegator starts and ends delegated sessions at auth-service. Every failure
// wraps ErrUnavailable.
type Delegator interface {
	StartDelegated(ctx context.Context, r *http.Request, req DelegatedRequest) (DelegatedSession, error)
	EndDelegated(ctx context.Context, r *http.Request, sessionID uuid.UUID) error
}

type delegatedStartBody struct {
	BaseSessionID     string `json:"base_session_id"`
	TargetPrincipalID string `json:"target_principal_id"`
	OrganizationID    string `json:"organization_id"`
	Reason            string `json:"reason"`
}

type delegatedAnswer struct {
	Session *struct {
		SessionID string `json:"session_id"`
		ExpiresAt string `json:"expires_at"`
	} `json:"session"`
	AccessToken        string `json:"access_token"`
	SessionAccessToken string `json:"session_access_token"`
	TokenType          string `json:"token_type"`
	ExpiresIn          *int64 `json:"expires_in"`
}

// post sends body to endpoint with the credential of the call and the end
// user's User-Agent and address, and returns the answer of status want. Any
// other status is a *RefusedError when the answer is an error.v1 document, else
// ErrUnavailable.
func (c *Client) post(ctx context.Context, endpoint string, r *http.Request, body any, want int) ([]byte, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("%w: encoding the request: %w", ErrUnavailable, err)
	}
	out, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("%w: building the request: %w", ErrUnavailable, err)
	}
	bearer, err := c.bearer(ctx)
	if err != nil {
		return nil, err
	}
	out.Header.Set("Authorization", "Bearer "+bearer)
	out.Header.Set("Content-Type", "application/json")
	out.Header.Set("Accept", "application/json")
	if r != nil {
		out.Header.Set("User-Agent", r.UserAgent())
		if addr, err := netip.ParseAddr(clientip.FromRequest(r)); err == nil {
			out.Header.Set("X-Forwarded-For", addr.String())
		}
	}
	response, err := c.http.Do(out)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	defer response.Body.Close()
	answer, err := io.ReadAll(io.LimitReader(response.Body, maxAnswerBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%w: reading the answer: %w", ErrUnavailable, err)
	}
	if response.StatusCode != want {
		if refused := refusalIn(response.StatusCode, answer); refused != nil {
			return nil, refused
		}
		return nil, fmt.Errorf("%w: status %d", ErrUnavailable, response.StatusCode)
	}
	if len(answer) > maxAnswerBytes {
		return nil, fmt.Errorf("%w: answer over %d bytes", ErrUnavailable, maxAnswerBytes)
	}
	return answer, nil
}

// StartDelegated starts a delegated session of req at auth-service.
func (c *Client) StartDelegated(ctx context.Context, r *http.Request, req DelegatedRequest) (DelegatedSession, error) {
	answer, err := c.post(ctx, c.endpoint+"/delegated", r, delegatedStartBody{
		BaseSessionID: req.BaseSessionID.String(), TargetPrincipalID: req.TargetPrincipalID.String(),
		OrganizationID: req.OrganizationID.String(), Reason: req.Reason,
	}, http.StatusCreated)
	if err != nil {
		return DelegatedSession{}, err
	}
	var parsed delegatedAnswer
	if err := json.Unmarshal(answer, &parsed); err != nil || parsed.Session == nil {
		return DelegatedSession{}, fmt.Errorf("%w: answer is not a delegated session", ErrUnavailable)
	}
	id, idErr := sessionid.Parse(parsed.Session.SessionID)
	expires, expiresErr := time.Parse(time.RFC3339Nano, parsed.Session.ExpiresAt)
	if idErr != nil || expiresErr != nil || parsed.ExpiresIn == nil || parsed.AccessToken == "" ||
		!strings.EqualFold(parsed.TokenType, "bearer") || *parsed.ExpiresIn < 1 || *parsed.ExpiresIn > maxExpiresIn {
		return DelegatedSession{}, fmt.Errorf("%w: answer is not a delegated session", ErrUnavailable)
	}
	return DelegatedSession{
		SessionID: id, ExpiresAt: expires.UTC(), Access: parsed.AccessToken, SessionAccess: parsed.SessionAccessToken, ExpiresIn: *parsed.ExpiresIn,
	}, nil
}

// EndDelegated ends the delegated session sessionID at auth-service. A session
// that is no longer there (404 session_not_found) is ended.
func (c *Client) EndDelegated(ctx context.Context, r *http.Request, sessionID uuid.UUID) error {
	_, err := c.post(ctx, c.endpoint+"/delegated/end", r, struct {
		SessionID string `json:"session_id"`
	}{sessionID.String()}, http.StatusNoContent)
	var refused *RefusedError
	if errors.As(err, &refused) && refused.Status == http.StatusNotFound && refused.Reason == "session_not_found" {
		return nil
	}
	return err
}

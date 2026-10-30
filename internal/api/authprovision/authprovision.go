// Package authprovision is go-api's client of auth-service's provisioning
// routes (memberships, invites, organization deprovision), used behind
// AUTH_PROXY_ORG_PROVISIONING. It presents the go-api service credential
// (AUTH_SESSION_ISSUE_TOKEN) holding memberships:write. Core is the one
// transport; orgreactivate's client is built on it too.
package authprovision

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
	"regexp"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

// The provisioning routes on auth-service's API listener.
const (
	PathMembershipAdd      = "/internal/v1/memberships/add"
	PathMembershipRole     = "/internal/v1/memberships/role"
	PathMembershipRemove   = "/internal/v1/memberships/remove"
	PathMembershipTransfer = "/internal/v1/memberships/transfer"
	PathInviteCreate       = "/internal/v1/invites/create"
	PathInviteAccept       = "/internal/v1/invites/accept"
	PathOrgDeprovision     = "/internal/v1/organizations/deprovision"
)

const (
	maxAnswerBytes = 64 << 10
	defaultTimeout = 10 * time.Second
	// maxRequestMetadataBytes is the routes' bound on request_metadata.
	maxRequestMetadataBytes = 2048
)

var (
	// ErrRefused: auth-service answered with a refusal (any 4xx). The call
	// changed nothing. The error is a *Refused.
	ErrRefused = errors.New("authprovision: auth-service refused the request")
	// ErrUnavailable: the route was unreachable or slow, answered 5xx or
	// anything else that is not the answer the request asked for.
	ErrUnavailable = errors.New("authprovision: auth-service provisioning route unavailable")
)

// Refused is a 4xx answer: its status and its error.v1 reason_code ("" when
// the answer named none, or named something that is not a plain code).
type Refused struct {
	Status int
	Reason string
}

func (r *Refused) Error() string {
	return fmt.Sprintf("%s: status %d reason %q", ErrRefused, r.Status, r.Reason)
}

func (r *Refused) Is(target error) bool { return target == ErrRefused }

// ReasonOf is the reason code of a refusal, "" for any other error.
func ReasonOf(err error) string {
	var refused *Refused
	if errors.As(err, &refused) {
		return refused.Reason
	}
	return ""
}

var reasonPattern = regexp.MustCompile(`^[a-z][a-z_]{0,63}$`)

// Core posts JSON to one auth-service with the service credential: no
// environment proxy, no redirect followed, a bounded answer, and no
// credential or answer text in any error.
type Core struct {
	base  url.URL
	token string
	http  *http.Client
}

// NewCore builds a Core for base (AUTH_PROXY_URL's origin) and token. It
// refuses a token that is not an auth-session-issuer credential, and never
// prints it.
func NewCore(base *url.URL, token string, timeout time.Duration) (*Core, error) {
	if base == nil {
		return nil, errors.New("authprovision: no auth-service URL")
	}
	if !sessionissue.ValidToken(token) {
		return nil, fmt.Errorf("%s must be an auth-session-issuer service credential (svc_authsess_...)", sessionissue.EnvToken)
	}
	if timeout <= 0 {
		timeout = defaultTimeout
	}
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
	return &Core{base: url.URL{Scheme: base.Scheme, Host: base.Host}, token: token, http: &http.Client{
		Transport:     transport,
		Timeout:       timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

// HTTP is the Core's client (its redirect policy is checked by tests).
func (c *Core) HTTP() *http.Client { return c.http }

// Post sends request to path and decodes a 200 answer into answer. A 4xx is a
// *Refused; anything else that is not a 200 with a JSON body is
// ErrUnavailable.
func (c *Core) Post(ctx context.Context, path string, request, answer any) error {
	raw, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("%w: encoding the request", ErrUnavailable)
	}
	endpoint := c.base
	endpoint.Path = path
	out, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(raw))
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
		return &Refused{Status: response.StatusCode, Reason: reasonIn(body)}
	case response.StatusCode != http.StatusOK:
		return fmt.Errorf("%w: status %d", ErrUnavailable, response.StatusCode)
	case err != nil:
		return fmt.Errorf("%w: reading the answer", ErrUnavailable)
	case len(body) > maxAnswerBytes:
		return fmt.Errorf("%w: answer over %d bytes", ErrUnavailable, maxAnswerBytes)
	}
	if err := json.Unmarshal(body, answer); err != nil {
		return fmt.Errorf("%w: answer is not JSON", ErrUnavailable)
	}
	return nil
}

func reasonIn(body []byte) string {
	var doc struct {
		ReasonCode string `json:"reason_code"`
	}
	if len(body) > maxAnswerBytes || json.Unmarshal(body, &doc) != nil || !reasonPattern.MatchString(doc.ReasonCode) {
		return ""
	}
	return doc.ReasonCode
}

// Membership is a membership answer.
type Membership struct {
	ID, OrgID, PrincipalID uuid.UUID
	Role, Status           string
}

// InviteRequest is an invite create.
type InviteRequest struct {
	OrgID           uuid.UUID
	Email, Role     string
	TTL             time.Duration
	Actor           uuid.UUID
	Description     string
	RequestMetadata json.RawMessage
}

// Invite is a created invite: its token is answered once.
type Invite struct {
	ID        uuid.UUID
	Token     string
	ExpiresAt time.Time
}

// AcceptRequest is an invite accept by PrincipalID.
type AcceptRequest struct {
	Token           string
	PrincipalID     uuid.UUID
	Description     string
	RequestMetadata json.RawMessage
}

// Accepted is an accepted invite.
type Accepted struct {
	InviteID, OrgID, MembershipID uuid.UUID
	Role                          string
}

// Provisioner is the provisioning routes as go-api calls them.
type Provisioner interface {
	AddMember(ctx context.Context, org, principal, actor uuid.UUID, role string, invitedBy *uuid.UUID) (Membership, error)
	ChangeRole(ctx context.Context, org, principal, actor uuid.UUID, role string) (Membership, error)
	RemoveMember(ctx context.Context, org, principal, actor uuid.UUID) error
	TransferOwnership(ctx context.Context, org, from, to, actor uuid.UUID) error
	CreateInvite(ctx context.Context, in InviteRequest) (Invite, error)
	AcceptInvite(ctx context.Context, in AcceptRequest) (Accepted, error)
	Deprovision(ctx context.Context, org, actor uuid.UUID) error
}

// Client calls the provisioning routes of one auth-service.
type Client struct{ core *Core }

var _ Provisioner = (*Client)(nil)

// New builds a Client (see NewCore).
func New(base *url.URL, token string, timeout time.Duration) (*Client, error) {
	core, err := NewCore(base, token, timeout)
	if err != nil {
		return nil, err
	}
	return &Client{core: core}, nil
}

type routeMembership struct {
	MembershipID   string  `json:"membership_id"`
	OrganizationID string  `json:"organization_id"`
	PrincipalID    string  `json:"principal_id"`
	Role           *string `json:"role"`
	Status         string  `json:"status"`
}

// membership checks that the answer names org and principal in status (and,
// when role is set, with that role).
func (a routeMembership) membership(org, principal uuid.UUID, status, role string) (Membership, error) {
	id, err := uuid.Parse(a.MembershipID)
	if err != nil || a.OrganizationID != org.String() || a.PrincipalID != principal.String() || a.Status != status ||
		(role != "" && (a.Role == nil || *a.Role != role)) {
		return Membership{}, fmt.Errorf("%w: answer is not the requested membership", ErrUnavailable)
	}
	out := Membership{ID: id, OrgID: org, PrincipalID: principal, Status: a.Status}
	if a.Role != nil {
		out.Role = *a.Role
	}
	return out, nil
}

// AddMember adds principal to org with role, acting as actor.
func (c *Client) AddMember(ctx context.Context, org, principal, actor uuid.UUID, role string, invitedBy *uuid.UUID) (Membership, error) {
	request := map[string]any{"organization_id": org.String(), "principal_id": principal.String(), "actor_principal_id": actor.String(), "role": role}
	if invitedBy != nil {
		request["invited_by_principal_id"] = invitedBy.String()
	}
	var answer routeMembership
	if err := c.core.Post(ctx, PathMembershipAdd, request, &answer); err != nil {
		return Membership{}, err
	}
	return answer.membership(org, principal, "active", role)
}

// ChangeRole sets principal's role in org, acting as actor.
func (c *Client) ChangeRole(ctx context.Context, org, principal, actor uuid.UUID, role string) (Membership, error) {
	request := map[string]any{"organization_id": org.String(), "principal_id": principal.String(), "actor_principal_id": actor.String(), "role": role}
	var answer routeMembership
	if err := c.core.Post(ctx, PathMembershipRole, request, &answer); err != nil {
		return Membership{}, err
	}
	return answer.membership(org, principal, "active", role)
}

// RemoveMember removes principal from org, acting as actor.
func (c *Client) RemoveMember(ctx context.Context, org, principal, actor uuid.UUID) error {
	request := map[string]any{"organization_id": org.String(), "principal_id": principal.String(), "actor_principal_id": actor.String()}
	var answer routeMembership
	if err := c.core.Post(ctx, PathMembershipRemove, request, &answer); err != nil {
		return err
	}
	_, err := answer.membership(org, principal, "removed", "")
	return err
}

// TransferOwnership moves org's ownership from one member to another, acting
// as actor.
func (c *Client) TransferOwnership(ctx context.Context, org, from, to, actor uuid.UUID) error {
	request := map[string]any{"organization_id": org.String(), "from_principal_id": from.String(), "to_principal_id": to.String(),
		"actor_principal_id": actor.String()}
	var answer struct {
		OrganizationID   string `json:"organization_id"`
		FromPrincipalID  string `json:"from_principal_id"`
		OwnerPrincipalID string `json:"owner_principal_id"`
	}
	if err := c.core.Post(ctx, PathMembershipTransfer, request, &answer); err != nil {
		return err
	}
	if answer.OrganizationID != org.String() || answer.FromPrincipalID != from.String() || answer.OwnerPrincipalID != to.String() {
		return fmt.Errorf("%w: answer is not the requested transfer", ErrUnavailable)
	}
	return nil
}

// CreateInvite mints an invite. An empty description and request metadata
// over the route's bound are left out (the route refuses an empty text as
// malformed; auth-service then records its default).
func (c *Client) CreateInvite(ctx context.Context, in InviteRequest) (Invite, error) {
	request := map[string]any{"organization_id": in.OrgID.String(), "email": in.Email, "role": in.Role,
		"ttl_seconds": int64(in.TTL / time.Second), "actor_principal_id": in.Actor.String()}
	if in.Description != "" {
		request["description"] = in.Description
	}
	if len(in.RequestMetadata) > 0 && len(in.RequestMetadata) <= maxRequestMetadataBytes {
		request["request_metadata"] = in.RequestMetadata
	}
	var answer struct {
		InviteID  string `json:"invite_id"`
		Token     string `json:"token"`
		ExpiresAt string `json:"expires_at"`
	}
	if err := c.core.Post(ctx, PathInviteCreate, request, &answer); err != nil {
		return Invite{}, err
	}
	id, err1 := uuid.Parse(answer.InviteID)
	expires, err2 := time.Parse(time.RFC3339Nano, answer.ExpiresAt)
	if err1 != nil || err2 != nil || answer.Token == "" {
		return Invite{}, fmt.Errorf("%w: answer is not a created invite", ErrUnavailable)
	}
	return Invite{ID: id, Token: answer.Token, ExpiresAt: expires.UTC()}, nil
}

// AcceptInvite accepts the invite of a token for in.PrincipalID.
func (c *Client) AcceptInvite(ctx context.Context, in AcceptRequest) (Accepted, error) {
	request := map[string]any{"token": in.Token, "principal_id": in.PrincipalID.String()}
	if in.Description != "" {
		request["description"] = in.Description
	}
	if len(in.RequestMetadata) > 0 && len(in.RequestMetadata) <= maxRequestMetadataBytes {
		request["request_metadata"] = in.RequestMetadata
	}
	var answer struct {
		InviteID       string `json:"invite_id"`
		OrganizationID string `json:"organization_id"`
		MembershipID   string `json:"membership_id"`
		Role           string `json:"role"`
	}
	if err := c.core.Post(ctx, PathInviteAccept, request, &answer); err != nil {
		return Accepted{}, err
	}
	invite, err1 := uuid.Parse(answer.InviteID)
	org, err2 := uuid.Parse(answer.OrganizationID)
	membership, err3 := uuid.Parse(answer.MembershipID)
	if errors.Join(err1, err2, err3) != nil || answer.Role == "" {
		return Accepted{}, fmt.Errorf("%w: answer is not an accepted invite", ErrUnavailable)
	}
	return Accepted{InviteID: invite, OrgID: org, MembershipID: membership, Role: answer.Role}, nil
}

// Deprovision ends org's memberships and invites and suspends it, acting as
// the superuser actor. A repeat is a 200 that changes nothing.
func (c *Client) Deprovision(ctx context.Context, org, actor uuid.UUID) error {
	request := map[string]any{"organization_id": org.String(), "actor_principal_id": actor.String()}
	var answer struct {
		OrganizationID string `json:"organization_id"`
	}
	if err := c.core.Post(ctx, PathOrgDeprovision, request, &answer); err != nil {
		return err
	}
	if answer.OrganizationID != org.String() {
		return fmt.Errorf("%w: answer is not the organization", ErrUnavailable)
	}
	return nil
}

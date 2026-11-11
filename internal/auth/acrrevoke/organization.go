package acrrevoke

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/auth/acrlisting"
)

// acr's organization revoke (POST /internal/v1/organization/credentials/revoke): every live credential of the
// organization the exchanged access token is bound in. acr revokes the caller binding's own credentials last, the
// calling token included, so every call is made with a new exchange.
const (
	organizationRequestSchema  = "credential_revoke_organization_request.v1"
	organizationResponseSchema = "credential_revoke_organization_response.v1"

	// ReasonOrganizationDisabled and ReasonOrganizationDeleted are acr's closed set of organization revoke reasons.
	ReasonOrganizationDisabled = "organization_disabled"
	ReasonOrganizationDeleted  = "organization_deleted"

	// MaxOrganizationCalls bounds one RevokeOrganization: acr revokes at most 500 credentials per call, so the bound
	// is 32 000 revoke operations. Past it the revoke is incomplete and a repeat continues it.
	MaxOrganizationCalls = 64
)

var (
	// ErrOrganizationLive is acr's refusal to revoke the credentials of an organization Dev Health answers as live
	// (409 organization_live); nothing was revoked.
	ErrOrganizationLive = errors.New("acrrevoke: organization is live")
	// ErrOrganizationIncomplete is a revoke that reached MaxOrganizationCalls with credentials left; a repeat
	// continues it.
	ErrOrganizationIncomplete = errors.New("acrrevoke: organization revoke incomplete, more remains")
)

// OrganizationRevocation sums the answers of one RevokeOrganization.
type OrganizationRevocation struct {
	Revoked       int
	CallerRevoked int
	Calls         int
}

// RevokeOrganization revokes every live acr credential of orgID for the admin actorID, with reason
// ReasonOrganizationDisabled or ReasonOrganizationDeleted. It repeats acr's call, each time with a new token
// exchange, until acr answers that none is left, at most MaxOrganizationCalls times.
func (c *Client) RevokeOrganization(ctx context.Context, orgID, actorID, reason string) (OrganizationRevocation, error) {
	var total OrganizationRevocation
	if !acrlisting.ValidOrgID(orgID) || !validActorID(actorID) || (reason != ReasonOrganizationDisabled && reason != ReasonOrganizationDeleted) {
		return total, ErrInvalidRequest
	}
	body, err := json.Marshal(struct {
		SchemaVersion string `json:"schema_version"`
		ActorID       string `json:"actor_id"`
		Reason        string `json:"reason"`
	}{organizationRequestSchema, actorID, reason})
	if err != nil {
		return total, &Error{Kind: acrlisting.KindTransport}
	}
	for total.Calls < MaxOrganizationCalls {
		answer, err := c.revokeOrganizationOnce(ctx, orgID, body)
		if err != nil {
			return total, err
		}
		total.Calls++
		total.Revoked += answer.Revoked
		total.CallerRevoked += answer.CallerRevoked
		if !answer.More {
			return total, nil
		}
	}
	return total, ErrOrganizationIncomplete
}

type organizationAnswer struct {
	SchemaVersion string `json:"schema_version"`
	Revoked       int    `json:"revoked"`
	CallerRevoked int    `json:"caller_revoked"`
	More          bool   `json:"more"`
}

func (c *Client) revokeOrganizationOnce(ctx context.Context, orgID string, body []byte) (organizationAnswer, error) {
	exchange := acrlisting.Exchange
	if c.allowClusterHTTP {
		exchange = acrlisting.ExchangeAllowingClusterHTTP
	}
	token, _, err := exchange(ctx, c.http, c.endpoint, c.subjectFile, orgID)
	if err != nil {
		var refusal *acrlisting.Error
		if errors.As(err, &refusal) {
			return organizationAnswer{}, &Error{Kind: refusal.Kind, Status: refusal.Status, cause: contextCause(err)}
		}
		return organizationAnswer{}, &Error{Kind: acrlisting.KindExchange, cause: contextCause(err)}
	}
	target := c.base.JoinPath("internal", "v1", "organization", "credentials", "revoke")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(body))
	if err != nil {
		return organizationAnswer{}, &Error{Kind: acrlisting.KindTransport}
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return organizationAnswer{}, &Error{Kind: acrlisting.KindTransport, cause: contextCause(err)}
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxAnswerBytes+1))
	if err != nil {
		return organizationAnswer{}, &Error{Kind: acrlisting.KindTransport, cause: contextCause(err)}
	}
	return classifyOrganization(resp.StatusCode, raw)
}

// classifyOrganization maps acr's answer. A 409 is acr's live-organization refusal only with acr's error.v1
// envelope and its code; any other non-200 answer is a failure, so an acr without the route never reads as
// "nothing to revoke".
func classifyOrganization(status int, raw []byte) (organizationAnswer, error) {
	switch status {
	case http.StatusOK:
		var wire struct {
			SchemaVersion string `json:"schema_version"`
			Revoked       *int   `json:"revoked"`
			CallerRevoked *int   `json:"caller_revoked"`
			More          *bool  `json:"more"`
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if len(raw) > maxAnswerBytes || decoder.Decode(&wire) != nil || decoder.More() || wire.SchemaVersion != organizationResponseSchema ||
			wire.Revoked == nil || wire.CallerRevoked == nil || wire.More == nil || *wire.Revoked < 0 || *wire.CallerRevoked < 0 {
			return organizationAnswer{}, &Error{Kind: acrlisting.KindRejected, Status: status}
		}
		return organizationAnswer{SchemaVersion: wire.SchemaVersion, Revoked: *wire.Revoked, CallerRevoked: *wire.CallerRevoked, More: *wire.More}, nil
	case http.StatusConflict:
		if errorCode(raw, status) == "organization_live" {
			return organizationAnswer{}, ErrOrganizationLive
		}
		return organizationAnswer{}, &Error{Kind: acrlisting.KindRejected, Status: status}
	case http.StatusNotFound:
		return organizationAnswer{}, &Error{Kind: acrlisting.KindRejected, Status: status}
	}
	if err := classify(status, raw); err != nil {
		return organizationAnswer{}, err
	}
	// classify reads a 204 as the on-behalf revoke's success; this route answers only 200.
	return organizationAnswer{}, &Error{Kind: acrlisting.KindRejected, Status: status}
}

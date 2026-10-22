package authapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/servicecred"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// PatternIssueSession is the session issue route go-api calls, with its
// auth-session-issuer service credential, for the sign-in flows it still
// serves while auth-service owns sessions (AUTH_PROXY_SESSIONS). The go-api
// proxy never forwards it: it forwards only /api/v1/auth routes.
const PatternIssueSession = "/internal/v1/sessions"

// opSessionIssue is the outcome-metric operation of the issue route.
const opSessionIssue = "session_issue"

// EventSessionIssued is the audit event of a session go-api asked for.
const EventSessionIssued = "auth.session.issued"

// sourceGoAPIDelegated is the issued session's audit source: go-api signed
// the user in and asked auth-service for the session.
const sourceGoAPIDelegated = "go-api-delegated"

// maxIssueBody bounds the issue request.
const maxIssueBody = 4 << 10

// issue-route refusal codes beyond the shared ones.
const (
	reasonInvalidCredentials = "invalid_credentials"
	reasonNotEntitled        = "not_entitled"
)

// ServiceCredentials checks the bearer of the issue route
// (*servicecred.PG).
type ServiceCredentials interface {
	Check(ctx context.Context, token contracts.SecretString) (servicecred.Credential, servicecred.Refusal, error)
}

var errIssueRequest = errors.New("authapi: not a session issue request")

// issueRequest is the issue route's body: exactly principal_id,
// organization_id (a UUID or null) and amr.
type issueRequest struct {
	principal    uuid.UUID
	organization *uuid.UUID
	amr          []string
}

func parseIssueRequest(body io.Reader) (issueRequest, error) {
	dec := json.NewDecoder(body)
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return issueRequest{}, errIssueRequest
	}
	members := map[string]json.RawMessage{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return issueRequest{}, errIssueRequest
		}
		name, _ := tok.(string)
		if _, seen := members[name]; seen {
			return issueRequest{}, errIssueRequest
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return issueRequest{}, errIssueRequest
		}
		members[name] = value
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return issueRequest{}, errIssueRequest
	}
	if _, err := dec.Token(); err != io.EOF {
		return issueRequest{}, errIssueRequest
	}
	principalRaw, okPrincipal := members["principal_id"]
	orgRaw, okOrg := members["organization_id"]
	amrRaw, okAMR := members["amr"]
	if len(members) != 3 || !okPrincipal || !okOrg || !okAMR {
		return issueRequest{}, errIssueRequest
	}
	var out issueRequest
	var principal string
	if err := json.Unmarshal(principalRaw, &principal); err != nil {
		return issueRequest{}, errIssueRequest
	}
	id, err := uuid.Parse(principal)
	if err != nil || principal != id.String() {
		return issueRequest{}, errIssueRequest
	}
	out.principal = id
	if string(orgRaw) != "null" {
		var org string
		if err := json.Unmarshal(orgRaw, &org); err != nil {
			return issueRequest{}, errIssueRequest
		}
		orgID, err := uuid.Parse(org)
		if err != nil || org != orgID.String() {
			return issueRequest{}, errIssueRequest
		}
		out.organization = &orgID
	}
	if err := json.Unmarshal(amrRaw, &out.amr); err != nil || !session.ValidAMR(out.amr) {
		return issueRequest{}, errIssueRequest
	}
	return out, nil
}

type issueAnswer struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
}

// issueSession is POST /internal/v1/sessions. It refuses anything but a live
// auth-session-issuer credential, an active user and, with an organization,
// a member of it. It creates the session (pinned to the user's
// token_version) with its auth.session.issued audit row in one transaction
// and answers the go-api compatible pair.
func (h compatHandlers) issueSession(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	token, ok := bearerToken(r)
	if !ok {
		h.rec.refuse(w, r, opSessionIssue, unauthorized(reasonInvalidCredentials), nil)
		return
	}
	credential, refused, err := h.ServiceCredentials.Check(ctx, contracts.NewSecretString(token))
	if err != nil {
		h.rec.refuse(w, r, opSessionIssue, unavailable(reasonStoreUnavailable), err)
		return
	}
	if refused != "" {
		h.rec.logger.WarnContext(ctx, "session issue refused: service credential", "refusal", string(refused))
		h.rec.refuse(w, r, opSessionIssue, unauthorized(reasonInvalidCredentials), nil)
		return
	}
	req, err := parseIssueRequest(http.MaxBytesReader(w, r.Body, maxIssueBody))
	if err != nil {
		h.rec.refuse(w, r, opSessionIssue, unauthorized(reasonMalformed), nil)
		return
	}
	user, found, err := h.Users.State(ctx, req.principal)
	if err != nil {
		h.rec.refuse(w, r, opSessionIssue, unavailable(reasonStoreUnavailable), err)
		return
	}
	if !found {
		h.rec.refuse(w, r, opSessionIssue, refusal{http.StatusForbidden, reasonPrincipalNotFound}, nil)
		return
	}
	if !user.IsActive {
		h.rec.refuse(w, r, opSessionIssue, refusal{http.StatusForbidden, reasonDeactivated}, nil)
		return
	}
	role := roleText(nil, false)
	if req.organization != nil {
		value, member, err := h.Users.Role(ctx, req.principal, *req.organization)
		if err != nil {
			h.rec.refuse(w, r, opSessionIssue, unavailable(reasonStoreUnavailable), err)
			return
		}
		if !member {
			h.rec.refuse(w, r, opSessionIssue, refusal{http.StatusForbidden, reasonNotEntitled}, nil)
			return
		}
		role = roleText(value, member)
	}
	err = h.Projector.Project(ctx, req.principal)
	if err == nil && req.organization != nil {
		err = h.Projector.ProjectOrganization(ctx, *req.organization)
	}
	switch {
	case errors.Is(err, identity.ErrUserNotFound):
		h.rec.refuse(w, r, opSessionIssue, refusal{http.StatusForbidden, reasonPrincipalNotFound}, nil)
		return
	case errors.Is(err, identity.ErrOrganizationNotFound):
		h.rec.refuse(w, r, opSessionIssue, refusal{http.StatusForbidden, reasonNotEntitled}, nil)
		return
	case err != nil:
		h.rec.refuse(w, r, opSessionIssue, unavailable(reasonDependency), err)
		return
	}
	issued, err := h.Sessions.CreateAudited(ctx, req.principal, req.organization, req.amr,
		issuedEvent(ctx, req, credential))
	switch {
	case errors.Is(err, session.ErrInvalidAMR):
		h.rec.refuse(w, r, opSessionIssue, unauthorized(reasonMalformed), nil)
		return
	case errors.Is(err, session.ErrPrincipalUnavailable):
		h.rec.refuse(w, r, opSessionIssue, refusal{http.StatusForbidden, reasonDeactivated}, nil)
		return
	case err != nil:
		h.rec.refuse(w, r, opSessionIssue, unavailable(reasonStoreUnavailable), err)
		return
	}
	access, err := h.Issuer.Access(ctx, Grant{
		PrincipalID: req.principal, OrganizationID: req.organization, Role: role, SessionID: issued.Session.ID, User: user,
	})
	if err != nil {
		h.rec.refuse(w, r, opSessionIssue, unavailable(reasonDependency), err)
		return
	}
	h.rec.record(ctx, opSessionIssue, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, issueAnswer{
		AccessToken: access, RefreshToken: issued.Credential.Reveal(), TokenType: "bearer", ExpiresIn: AccessExpiresIn,
	})
}

// issuedEvent is the auth.session.issued audit row: the user as subject, the
// organization, the amr, the service credential that asked, and its source.
func issuedEvent(ctx context.Context, req issueRequest, credential servicecred.Credential) audit.AuditEvent {
	principal := req.principal.String()
	event := audit.AuditEvent{
		EventType: EventSessionIssued, Outcome: audit.OutcomeAllowed,
		SubjectPrincipalID: &principal, RequestID: requestid.Audit(ctx),
		Attributes: map[string]any{
			"reason_code": reasonAccepted, "target_kind": "session", "source": sourceGoAPIDelegated,
			"amr": append([]string(nil), req.amr...), "credential_id": credential.ID,
		},
	}
	if req.organization != nil {
		org := req.organization.String()
		event.OrganizationID = &org
	}
	return event
}

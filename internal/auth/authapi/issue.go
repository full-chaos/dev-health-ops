package authapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/servicecred"
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
	var raw map[string]json.RawMessage
	dec := json.NewDecoder(body)
	if err := dec.Decode(&raw); err != nil || dec.More() {
		return issueRequest{}, errIssueRequest
	}
	if len(raw) != 3 {
		return issueRequest{}, errIssueRequest
	}
	var out issueRequest
	var principal string
	if err := json.Unmarshal(raw["principal_id"], &principal); err != nil {
		return issueRequest{}, errIssueRequest
	}
	id, err := uuid.Parse(principal)
	if err != nil || principal != id.String() {
		return issueRequest{}, errIssueRequest
	}
	out.principal = id
	orgRaw, ok := raw["organization_id"]
	if !ok {
		return issueRequest{}, errIssueRequest
	}
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
	if err := json.Unmarshal(raw["amr"], &out.amr); err != nil || len(out.amr) == 0 {
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

// issueSession is POST /internal/v1/sessions.
func (h compatHandlers) issueSession(w http.ResponseWriter, r *http.Request) {
	h.rec.refuse(w, r, opSessionIssue, unavailable(reasonDependency), nil)
}

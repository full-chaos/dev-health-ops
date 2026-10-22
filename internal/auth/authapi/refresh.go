package authapi

import (
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"regexp"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// refreshCredentialPattern is refresh-request.v1 refresh_credential.
var refreshCredentialPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{43,256}$`)

// maxRefreshExpiresIn is refresh-response.v1 refresh_expires_in's maximum.
const maxRefreshExpiresIn = int(edgetoken.RefreshLifetime / time.Second)

var errRefreshRequest = errors.New("authapi: body is not a refresh-request.v1 document")

// parseRefreshRequest reads a refresh-request.v1 document the way the schema
// and the shared duplicate-member rule read it: one object, exactly
// schema_version and refresh_credential, each once, both strings, the
// credential in its alphabet, nothing after the object. The error never
// carries the body: it holds a credential.
func parseRefreshRequest(body io.Reader) (contracts.SecretString, error) {
	dec := json.NewDecoder(body)
	dec.UseNumber()
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return contracts.SecretString{}, errRefreshRequest
	}
	var version, credential string
	seen := map[string]bool{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return contracts.SecretString{}, errRefreshRequest
		}
		name, _ := tok.(string)
		if seen[name] || (name != "schema_version" && name != "refresh_credential") {
			return contracts.SecretString{}, errRefreshRequest
		}
		seen[name] = true
		value, err := dec.Token()
		if err != nil {
			return contracts.SecretString{}, errRefreshRequest
		}
		text, ok := value.(string)
		if !ok {
			return contracts.SecretString{}, errRefreshRequest
		}
		if name == "schema_version" {
			version = text
		} else {
			credential = text
		}
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return contracts.SecretString{}, errRefreshRequest
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return contracts.SecretString{}, errRefreshRequest
	}
	if version != contracts.RefreshRequestSurface || !refreshCredentialPattern.MatchString(credential) {
		return contracts.SecretString{}, errRefreshRequest
	}
	return contracts.NewSecretString(credential), nil
}

// refreshResponse is refresh-response.v1, written from revealed values (the
// contracts types marshal secrets as "[redacted]").
type refreshResponse struct {
	SchemaVersion     string `json:"schema_version"`
	AccessToken       string `json:"access_token"`
	TokenType         string `json:"token_type"`
	ExpiresIn         int    `json:"expires_in"`
	RefreshCredential string `json:"refresh_credential"`
	RefreshExpiresIn  int    `json:"refresh_expires_in"`
	SessionID         string `json:"session_id"`
}

// refreshExpiresIn is the whole seconds left on the rotated credential,
// rounded up and held inside the schema's 1..604800.
func refreshExpiresIn(expiresAt, now time.Time) int {
	seconds := int(math.Ceil(expiresAt.Sub(now).Seconds()))
	return min(max(seconds, 1), maxRefreshExpiresIn)
}

// refresh is POST /v1/token/refresh: exchange the opaque refresh credential
// for a rotated one and a new access token (TRD §17 Refresh). The access token
// is minted from the user's state read after the exchange, as go-api's
// refresh route reads it (internal/api/session/refresh.go). A user that is
// gone or deactivated gets no token and the session is revoked, as go-api
// revokes the family of a deactivated user.
func (h handlers) refresh(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	credential, err := parseRefreshRequest(r.Body)
	if err != nil {
		h.rec.refuse(w, r, opRefresh, unauthorized(reasonMalformed), nil)
		return
	}
	result, err := h.Sessions.Exchange(ctx, credential)
	if err != nil || !result.Reason.Success() {
		h.rec.refuse(w, r, opRefresh, exchangeRefusal(result.Reason), err)
		return
	}
	record := result.Session
	user, found, err := h.Users.State(ctx, record.PrincipalID)
	if err != nil {
		h.rec.refuse(w, r, opRefresh, unavailable(reasonStoreUnavailable), err)
		return
	}
	if !found || !user.IsActive {
		reason := reasonDeactivated
		if !found {
			reason = reasonPrincipalNotFound
		}
		if _, revokeErr := h.Sessions.Revoke(ctx, record.ID, session.Actor{RequestID: requestid.Audit(ctx)}); revokeErr != nil {
			h.rec.refuse(w, r, opRefresh, unavailable(reasonStoreUnavailable), revokeErr)
			return
		}
		h.rec.refuse(w, r, opRefresh, unauthorized(reason), nil)
		return
	}
	switch checkPin(ctx, h.rec.logger, record, user) {
	case pinMissing:
		h.rec.refuse(w, r, opRefresh, unauthorized(reasonRevisionStale), nil)
		return
	case pinStale:
		if _, revokeErr := h.Sessions.Revoke(ctx, record.ID, session.Actor{RequestID: requestid.Audit(ctx)}); revokeErr != nil {
			h.rec.refuse(w, r, opRefresh, unavailable(reasonStoreUnavailable), revokeErr)
			return
		}
		h.rec.refuse(w, r, opRefresh, unauthorized(reasonRevisionStale), nil)
		return
	}
	role := roleText(nil, false)
	if record.OrganizationID != nil {
		value, member, roleErr := h.Users.Role(ctx, record.PrincipalID, *record.OrganizationID)
		if roleErr != nil {
			h.rec.refuse(w, r, opRefresh, unavailable(reasonStoreUnavailable), roleErr)
			return
		}
		role = roleText(value, member)
	}
	access, err := h.Issuer.Access(Grant{
		PrincipalID: record.PrincipalID, OrganizationID: record.OrganizationID, Role: role,
		SessionID: record.ID, User: user,
	})
	if err != nil {
		h.rec.refuse(w, r, opRefresh, unavailable(reasonDependency), err)
		return
	}
	h.rec.record(ctx, opRefresh, "success", string(result.Reason), nil)
	writeJSON(w, http.StatusOK, refreshResponse{
		SchemaVersion:     contracts.RefreshResponseSurface,
		AccessToken:       access,
		TokenType:         "bearer",
		ExpiresIn:         AccessExpiresIn,
		RefreshCredential: result.Credential.Reveal(),
		RefreshExpiresIn:  refreshExpiresIn(result.ExpiresAt, h.Issuer.now()),
		SessionID:         session.WireSessionID(record.ID),
	})
}

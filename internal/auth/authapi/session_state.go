package authapi

import (
	"errors"
	"net/http"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// PatternSessionState is the session state read go-api's Ask Dev principal
// access read calls when its query names a session (acp ruling D6559 on
// CHAOS-12122). It is mounted only when the embedded endpoint-profile document
// holds its row: without the row it answers 404 and the service starts.
const PatternSessionState = "/internal/v1/sessions/state"

const opSessionState = "session_state"

// reasonSessionNotFound is the 404 of an unknown session. go-api reads this
// reason, and only this reason, as "no such session"; any other 404 (an
// auth-service with no state route) is a fault there.
const reasonSessionNotFound = "session_not_found"

// sessionStateAnswer is the session's state as auth-service itself decides it:
// revoked is the effective revoke (an explicit revoke, or a password change
// after the session was created), and expired is the rule of authenticate
// (the effective expiry, the absolute cap or the idle expiry, whichever is
// first, is not after now). Ids are canonical UUIDs.
type sessionStateAnswer struct {
	PrincipalID        string  `json:"principal_id"`
	OrganizationID     *string `json:"organization_id"`
	Revoked            bool    `json:"revoked"`
	Expired            bool    `json:"expired"`
	LegacyTokenVersion *int64  `json:"legacy_token_version"`
}

// withSessionStateRoute mounts the session state route when the embedded
// endpoint-profile document holds its row.
func (h compatHandlers) withSessionStateRoute(routes []httpapi.Route) []httpapi.Route {
	return h.withSessionStateRouteRows(routes, authv1.AuthServiceEndpointProfiles)
}

// withSessionStateRouteRows is withSessionStateRoute over a given document;
// without a row that lists every class the handler admits (the session issuer
// credential when service credentials are configured, the go-api workload
// token when its binding is) it logs one line naming the route and mounts
// nothing.
func (h compatHandlers) withSessionStateRouteRows(routes []httpapi.Route, rows []byte) []httpapi.Route {
	var admitted []string
	if h.ServiceCredentials != nil {
		admitted = append(admitted, serviceSessionIssuerClass)
	}
	if h.IssueWorkload != nil {
		admitted = append(admitted, workloadTokenClass)
	}
	if rowsAcceptFor(rows, []string{PatternSessionState}, admitted) != nil {
		h.rec.logger.Info("auth session route not mounted: no endpoint-profile row", "route", PatternSessionState)
		return routes
	}
	return append(routes, httpapi.Route{Method: http.MethodPost, Pattern: PatternSessionState, Handler: http.HandlerFunc(h.sessionState)})
}

// sessionState is POST /internal/v1/sessions/state: one strict object
// {"session_id"} in, the session's state out. It takes the bearer the session
// issue route takes (the session issuer service credential, or the go-api
// workload token), checked the same way. It is read-only: no audit row, and no
// last_seen_at stamp, so a state read never extends a session's idle expiry.
func (h compatHandlers) sessionState(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	token, ok := bearerToken(r)
	if !ok {
		h.rec.refuse(w, r, opSessionState, unauthorized(reasonInvalidCredentials), nil)
		return
	}
	if _, ok := h.issueCallerFor(w, r, token, opSessionState); !ok {
		return
	}
	body, err := readStrictObject(http.MaxBytesReader(w, r.Body, maxIssueBody), "session_id")
	if err != nil {
		h.rec.refuse(w, r, opSessionState, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	id, err := body.uuid("session_id")
	if err != nil {
		h.rec.refuse(w, r, opSessionState, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	record, err := h.Sessions.Current(ctx, id)
	switch {
	case errors.Is(err, session.ErrSessionNotFound):
		h.rec.refuse(w, r, opSessionState, refusal{http.StatusNotFound, reasonSessionNotFound}, nil)
		return
	case err != nil:
		h.rec.refuse(w, r, opSessionState, unavailable(reasonStoreUnavailable), err)
		return
	}
	answer := sessionStateAnswer{
		PrincipalID:        record.PrincipalID.String(),
		Revoked:            record.RevokedAt != nil,
		Expired:            !record.ExpiresAt.After(h.Issuer.now()),
		LegacyTokenVersion: record.LegacyTokenVersion,
	}
	if record.OrganizationID != nil {
		org := record.OrganizationID.String()
		answer.OrganizationID = &org
	}
	h.rec.record(ctx, opSessionState, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, answer)
}

package authapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// The delegated session routes go-api calls. They are mounted only when the
// embedded endpoint-profile document holds a row for each: without the rows
// they answer 404 and the service starts.
const (
	PatternDelegatedSession    = "/internal/v1/sessions/delegated"
	PatternDelegatedSessionEnd = "/internal/v1/sessions/delegated/end"
)

const (
	opDelegatedStart = "delegated_session_start"
	opDelegatedEnd   = "delegated_session_end"
)

// DelegatedSessions is the session domain the delegated routes drive
// (*session.Store).
type DelegatedSessions interface {
	StartDelegation(ctx context.Context, in session.StartInput) (session.Record, error)
	EndDelegation(ctx context.Context, sessionID uuid.UUID, actor session.Actor) (bool, error)
}

// delegatedAnswer is the 201 of a delegated session. It has no refresh member:
// the session has no refresh credential and ends at its expires_at.
type delegatedAnswer struct {
	Session     contracts.Session `json:"session"`
	AccessToken string            `json:"access_token"`
	// SessionToken is the session routes' token under the eddsa class; absent
	// under hs256_compat.
	SessionToken string `json:"session_token,omitempty"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
}

// withDelegatedRoutes mounts the delegated routes when the embedded
// endpoint-profile document holds their rows.
func (h compatHandlers) withDelegatedRoutes(routes []httpapi.Route) []httpapi.Route {
	return h.withDelegatedRouteRows(routes, authv1.AuthServiceEndpointProfiles)
}

// withDelegatedRouteRows is withDelegatedRoutes over a given document: without
// a row for each route that lists every class the handlers admit it logs one
// line and mounts neither.
func (h compatHandlers) withDelegatedRouteRows(routes []httpapi.Route, rows []byte) []httpapi.Route {
	if rowsAcceptFor(rows, []string{PatternDelegatedSession, PatternDelegatedSessionEnd}, h.issueCallerClasses()) != nil {
		h.rec.logger.Info("auth delegated routes not mounted: no endpoint-profile row", "route", PatternDelegatedSession)
		return routes
	}
	return append(routes,
		httpapi.Route{Method: http.MethodPost, Pattern: PatternDelegatedSession, Handler: http.HandlerFunc(h.startDelegated)},
		httpapi.Route{Method: http.MethodPost, Pattern: PatternDelegatedSessionEnd, Handler: http.HandlerFunc(h.endDelegated)},
	)
}

// delegatedCaller checks the bearer of a delegated route, the bearer the
// session issue route takes.
func (h compatHandlers) delegatedCaller(w http.ResponseWriter, r *http.Request, op string) (issueCaller, bool) {
	token, ok := bearerToken(r)
	if !ok {
		h.rec.refuse(w, r, op, unauthorized(reasonInvalidCredentials), nil)
		return issueCaller{}, false
	}
	return h.issueCallerFor(w, r, token, op)
}

func parseDelegatedStart(body strictObject) (session.StartInput, bool) {
	base, baseErr := body.uuid("base_session_id")
	target, targetErr := body.uuid("target_principal_id")
	org, orgErr := body.uuid("organization_id")
	reason, reasonErr := body.anyText("reason", maxIssueBody)
	if baseErr != nil || targetErr != nil || orgErr != nil || reasonErr != nil {
		return session.StartInput{}, false
	}
	return session.StartInput{BaseSessionID: base, TargetPrincipalID: target, OrganizationID: org, Reason: reason}, true
}

// startRefusal maps a StartDelegation error to its answer. The exact cause goes
// to the log only (G-32).
func startRefusal(err error) refusal {
	switch {
	case errors.Is(err, session.ErrInvalidReason):
		return refusal{http.StatusBadRequest, reasonMalformed}
	case errors.Is(err, session.ErrBaseNotLive):
		return unauthorized(reasonSessionRevoked)
	case errors.Is(err, session.ErrNotOwned), errors.Is(err, session.ErrActorNotAuthorized),
		errors.Is(err, session.ErrActorUnavailable), errors.Is(err, session.ErrNestedDelegation),
		errors.Is(err, session.ErrTargetIsActor), errors.Is(err, session.ErrTargetUnavailable),
		errors.Is(err, session.ErrTargetPlatformRole), errors.Is(err, session.ErrTargetNotMember),
		errors.Is(err, session.ErrTargetRoleUnknown), errors.Is(err, session.ErrOrganizationUnavailable):
		return refusal{http.StatusForbidden, reasonNotEntitled}
	}
	return unavailable(reasonStoreUnavailable)
}

// startDelegated is POST /internal/v1/sessions/delegated. The body names no
// actor: the actor is the principal of the base session. Tokens are minted
// after the row commits, and a mint failure ends the row, so no delegated row
// outlives a request that answered no token.
func (h compatHandlers) startDelegated(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if _, ok := h.delegatedCaller(w, r, opDelegatedStart); !ok {
		return
	}
	body, err := readStrictObject(http.MaxBytesReader(w, r.Body, maxIssueBody), "base_session_id", "target_principal_id", "organization_id", "reason")
	if err != nil {
		h.rec.refuse(w, r, opDelegatedStart, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	in, ok := parseDelegatedStart(body)
	if !ok {
		h.rec.refuse(w, r, opDelegatedStart, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	if err := h.Issuer.Preflight(ctx); err != nil {
		h.rec.refuse(w, r, opDelegatedStart, unavailable(reasonDependency), err)
		return
	}
	err = h.Projector.Project(ctx, in.TargetPrincipalID)
	if err == nil {
		err = h.Projector.ProjectOrganization(ctx, in.OrganizationID)
	}
	switch {
	case errors.Is(err, identity.ErrUserNotFound), errors.Is(err, identity.ErrOrganizationNotFound):
		h.rec.refuse(w, r, opDelegatedStart, refusal{http.StatusForbidden, reasonNotEntitled}, nil)
		return
	case err != nil:
		h.rec.refuse(w, r, opDelegatedStart, unavailable(reasonDependency), err)
		return
	}
	record, err := h.Sessions.StartDelegation(ctx, in)
	if err != nil {
		h.rec.refuse(w, r, opDelegatedStart, startRefusal(err), err)
		return
	}
	tokens, err := h.delegatedTokens(ctx, record)
	if err != nil {
		if _, endErr := h.Sessions.EndDelegation(ctx, record.ID, session.Actor{RequestID: requestid.Audit(ctx)}); endErr != nil {
			err = errors.Join(err, endErr)
		}
		h.rec.refuse(w, r, opDelegatedStart, unavailable(reasonDependency), err)
		return
	}
	h.rec.record(ctx, opDelegatedStart, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusCreated, delegatedAnswer{
		Session: record.Wire(), AccessToken: tokens.Access, SessionToken: tokens.Session, TokenType: "bearer", ExpiresIn: tokens.ExpiresIn,
	})
}

var errDelegatedUser = errors.New("authapi: the delegated session's user is gone")

func (h compatHandlers) delegatedTokens(ctx context.Context, record session.Record) (Tokens, error) {
	user, found, err := h.Users.State(ctx, record.PrincipalID)
	if err != nil {
		return Tokens{}, err
	}
	if !found || !user.IsActive {
		return Tokens{}, errDelegatedUser
	}
	role, member, err := h.Users.Role(ctx, record.PrincipalID, *record.OrganizationID)
	if err != nil {
		return Tokens{}, err
	}
	if !member {
		return Tokens{}, errDelegatedUser
	}
	return h.Issuer.Tokens(ctx, Grant{
		PrincipalID: record.PrincipalID, OrganizationID: record.OrganizationID, Role: roleText(role, member),
		SessionID: record.ID, User: user, ExpiresAt: &record.ExpiresAt, Actor: &record.Delegation.Actor,
	})
}

// endDelegated is POST /internal/v1/sessions/delegated/end: it ends a delegated
// session and answers 204, also when the session had already ended. A plain
// session is not a delegated session here: 404, and nothing changes.
func (h compatHandlers) endDelegated(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	caller, ok := h.delegatedCaller(w, r, opDelegatedEnd)
	if !ok {
		return
	}
	body, err := readStrictObject(http.MaxBytesReader(w, r.Body, maxIssueBody), "session_id")
	if err != nil {
		h.rec.refuse(w, r, opDelegatedEnd, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	id, err := body.uuid("session_id")
	if err != nil {
		h.rec.refuse(w, r, opDelegatedEnd, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	_, err = h.Sessions.EndDelegation(ctx, id, session.Actor{PrincipalID: caller.workload, RequestID: requestid.Audit(ctx)})
	switch {
	case errors.Is(err, session.ErrSessionNotFound), errors.Is(err, session.ErrNotDelegated):
		h.rec.refuse(w, r, opDelegatedEnd, refusal{http.StatusNotFound, reasonSessionNotFound}, nil)
		return
	case err != nil:
		h.rec.refuse(w, r, opDelegatedEnd, unavailable(reasonStoreUnavailable), err)
		return
	}
	h.rec.record(ctx, opDelegatedEnd, "success", reasonAccepted, nil)
	writeNoContent(w)
}

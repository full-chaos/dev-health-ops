package authapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// Route patterns. The endpoint-profile rows name these exact strings.
const (
	PatternRefresh   = "/v1/token/refresh"
	PatternCurrent   = "/v1/sessions/current"
	PatternRevoke    = "/v1/sessions/revoke"
	PatternRevokeAll = "/v1/sessions/revoke-all"
)

// Sessions is the session domain these routes drive (*session.Store).
type Sessions interface {
	Exchange(ctx context.Context, credential contracts.SecretString) (session.Result, error)
	Current(ctx context.Context, sessionID uuid.UUID) (session.Record, error)
	Revoke(ctx context.Context, sessionID uuid.UUID, actor session.Actor) (bool, error)
	RevokeAll(ctx context.Context, principalID uuid.UUID, actor session.Actor) (int, error)
}

// Deps are the routes' collaborators.
type Deps struct {
	Sessions Sessions
	Users    Users
	Issuer   *Issuer
	Logger   *slog.Logger
	// Meter registers the outcome counter; nil uses the process meter.
	Meter metric.Meter
}

type handlers struct {
	Deps
	rec recorder
}

// Routes returns the session routes. Each registration is on a line of its
// own: the endpoint-profile rows anchor to these lines.
func Routes(deps Deps) ([]httpapi.Route, error) {
	if deps.Sessions == nil || deps.Users == nil || deps.Issuer == nil {
		return nil, errors.New("authapi: sessions, users and issuer are required")
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	meter := deps.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/authapi")
	}
	rec, err := newRecorder(deps.Logger, meter)
	if err != nil {
		return nil, err
	}
	h := handlers{Deps: deps, rec: rec}
	return []httpapi.Route{
		{Method: http.MethodPost, Pattern: PatternRefresh, Handler: http.HandlerFunc(h.refresh)},
		{Method: http.MethodGet, Pattern: PatternCurrent, Handler: http.HandlerFunc(h.current)},
		{Method: http.MethodPost, Pattern: PatternRevoke, Handler: http.HandlerFunc(h.revoke)},
		{Method: http.MethodPost, Pattern: PatternRevokeAll, Handler: http.HandlerFunc(h.revokeAll)},
	}, nil
}

// bearerToken is the credential of an "Authorization: Bearer <token>" header.
// The scheme is case-insensitive (RFC 9110 §11.1); anything else is no token.
func bearerToken(r *http.Request) (string, bool) {
	values := r.Header.Values("Authorization")
	if len(values) != 1 {
		return "", false
	}
	scheme, token, ok := strings.Cut(values[0], " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	token = strings.TrimSpace(token)
	return token, token != ""
}

// authenticate resolves the bearer of a session route: a valid access token
// with sid, whose session is live and belongs to sub, whose user is active
// and whose tv is the user's current token_version (go-api's own check, so a
// token go-api refuses is refused here too). It answers the refusal itself and
// returns false.
func (h handlers) authenticate(w http.ResponseWriter, r *http.Request, operation string) (Bearer, session.Record, bool) {
	token, ok := bearerToken(r)
	if !ok {
		h.rec.refuse(w, r, operation, unauthorized(reasonMalformed), nil)
		return Bearer{}, session.Record{}, false
	}
	bearer, reason, err := h.Issuer.Verify(token)
	if err != nil {
		h.rec.refuse(w, r, operation, unauthorized(reason), nil)
		return Bearer{}, session.Record{}, false
	}
	record, err := h.Sessions.Current(r.Context(), bearer.SessionID)
	switch {
	case errors.Is(err, session.ErrSessionNotFound):
		h.rec.refuse(w, r, operation, unauthorized(reasonSessionRevoked), nil)
		return Bearer{}, session.Record{}, false
	case err != nil:
		h.rec.refuse(w, r, operation, unavailable(reasonStoreUnavailable), err)
		return Bearer{}, session.Record{}, false
	case record.PrincipalID != bearer.PrincipalID:
		h.rec.refuse(w, r, operation, unauthorized(reasonMalformed), nil)
		return Bearer{}, session.Record{}, false
	case record.RevokedAt != nil:
		h.rec.refuse(w, r, operation, unauthorized(reasonSessionRevoked), nil)
		return Bearer{}, session.Record{}, false
	case !record.ExpiresAt.After(h.Issuer.now()):
		h.rec.refuse(w, r, operation, unauthorized(reasonSessionExpired), nil)
		return Bearer{}, session.Record{}, false
	}
	user, found, err := h.Users.State(r.Context(), bearer.PrincipalID)
	switch {
	case err != nil:
		h.rec.refuse(w, r, operation, unavailable(reasonStoreUnavailable), err)
		return Bearer{}, session.Record{}, false
	case !found:
		h.rec.refuse(w, r, operation, unauthorized(reasonPrincipalNotFound), nil)
		return Bearer{}, session.Record{}, false
	case !user.IsActive:
		h.rec.refuse(w, r, operation, unauthorized(reasonDeactivated), nil)
		return Bearer{}, session.Record{}, false
	case user.TokenVersion != bearer.TokenVersion:
		h.rec.refuse(w, r, operation, unauthorized(reasonRevisionStale), nil)
		return Bearer{}, session.Record{}, false
	}
	return bearer, record, true
}

// Package authapi is the auth-service HTTP layer over the session domain
// (internal/auth/session). Not implemented yet: every route answers 501.
package authapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

const (
	PatternRefresh   = "/v1/token/refresh"
	PatternCurrent   = "/v1/sessions/current"
	PatternRevoke    = "/v1/sessions/revoke"
	PatternRevokeAll = "/v1/sessions/revoke-all"
)

const AccessExpiresIn = int(edgetoken.AccessLifetime / time.Second)

var errNotImplemented = errors.New("authapi: not implemented")

var errRefreshRequest = errors.New("authapi: body is not a refresh-request.v1 document")

type Issuer struct {
	verifier *edgetoken.Verifier
	now      func() time.Time
}

func NewIssuer(secret, issuer, audience string) (*Issuer, error) {
	verifier, err := edgetoken.New(secret, issuer, audience)
	if err != nil {
		return nil, err
	}
	return &Issuer{verifier: verifier, now: time.Now}, nil
}

type UserState struct {
	Email        string
	Username     *string
	FullName     *string
	IsActive     bool
	IsSuperuser  bool
	TokenVersion int64
}

type Grant struct {
	PrincipalID    uuid.UUID
	OrganizationID *uuid.UUID
	Role           string
	SessionID      uuid.UUID
	User           UserState
}

func (i *Issuer) Access(Grant) (string, error) { return "", errNotImplemented }

type Bearer struct {
	PrincipalID  uuid.UUID
	SessionID    uuid.UUID
	TokenVersion int64
}

func (i *Issuer) Verify(string) (Bearer, string, error) {
	return Bearer{}, "malformed", errNotImplemented
}

type Users interface {
	State(ctx context.Context, userID uuid.UUID) (UserState, bool, error)
	Role(ctx context.Context, userID, orgID uuid.UUID) (*string, bool, error)
}

type PGUsers struct{ Pool *pgxpool.Pool }

func (PGUsers) State(context.Context, uuid.UUID) (UserState, bool, error) {
	return UserState{}, false, errNotImplemented
}

func (PGUsers) Role(context.Context, uuid.UUID, uuid.UUID) (*string, bool, error) {
	return nil, false, errNotImplemented
}

type Sessions interface {
	Exchange(ctx context.Context, credential contracts.SecretString) (session.Result, error)
	Current(ctx context.Context, sessionID uuid.UUID) (session.Record, error)
	Revoke(ctx context.Context, sessionID uuid.UUID, actor session.Actor) (bool, error)
	RevokeAll(ctx context.Context, principalID uuid.UUID, actor session.Actor) (int, error)
}

type Deps struct {
	Sessions Sessions
	Users    Users
	Issuer   *Issuer
	Logger   *slog.Logger
	Meter    metric.Meter
}

func Routes(deps Deps) ([]httpapi.Route, error) {
	if deps.Sessions == nil || deps.Users == nil || deps.Issuer == nil {
		return nil, errors.New("authapi: sessions, users and issuer are required")
	}
	notImplemented := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotImplemented)
	})
	return []httpapi.Route{
		{Method: http.MethodPost, Pattern: PatternRefresh, Handler: notImplemented},
		{Method: http.MethodGet, Pattern: PatternCurrent, Handler: notImplemented},
		{Method: http.MethodPost, Pattern: PatternRevoke, Handler: notImplemented},
		{Method: http.MethodPost, Pattern: PatternRevokeAll, Handler: notImplemented},
	}, nil
}

func parseRefreshRequest(any) (contracts.SecretString, error) {
	return contracts.SecretString{}, errRefreshRequest
}

func refreshExpiresIn(time.Time, time.Time) int { return 0 }

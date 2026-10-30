package policy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/platform/pgident"
)

// SessionRow is the auth-service session an access token's sid names.
type SessionRow struct {
	PrincipalID uuid.UUID
	Revoked     bool
	// LegacyTokenVersion is the users.token_version the session began at; nil
	// is refused.
	LegacyTokenVersion *int64
}

// SessionStore is the Store half that reads auth-service sessions: a token
// auth-service issued carries sid and is good only while that session lives.
// A Store without it refuses every token that carries sid.
type SessionStore interface {
	SessionState(ctx context.Context, id uuid.UUID) (row SessionRow, found bool, err error)
}

// defaultAuthSchema is AUTH_DATABASE_SCHEMA's default.
const defaultAuthSchema = "auth"

// EnvAuthSchema names the auth-service schema the sessions live in; the same
// variable auth-service and auth-migrate read.
const EnvAuthSchema = "AUTH_DATABASE_SCHEMA"

// AuthSchemaFromEnv is AUTH_DATABASE_SCHEMA, or "auth" when unset or blank,
// refused when it is not a usable schema name.
func AuthSchemaFromEnv(getenv func(string) string) (string, error) {
	name := strings.TrimSpace(getenv(EnvAuthSchema))
	if name == "" {
		return defaultAuthSchema, nil
	}
	if err := checkSchemaName(name); err != nil {
		return "", fmt.Errorf("%s: %w", EnvAuthSchema, err)
	}
	return name, nil
}

// checkSession decides the sid claim: absent, nothing to check (a go-api
// token) and no session; present, it must name a live session of subject,
// and that session's id is returned (User.SessionID). A lookup that fails is
// that error, never a pass.
func (a *Authenticator) checkSession(ctx context.Context, claims map[string]any, subject uuid.UUID, userVersion int64) (*uuid.UUID, error) {
	raw, present := claims["sid"]
	if !present {
		return nil, nil
	}
	text, _ := raw.(string)
	id, err := sessionid.Parse(text)
	if err != nil {
		a.logger.WarnContext(ctx, "api access denied: unusable session claim", slog.String("user_id", subject.String()))
		return nil, errRejected
	}
	store, ok := a.store.(SessionStore)
	if !ok {
		a.logger.WarnContext(ctx, "api access denied: session claim with no session store", slog.String("user_id", subject.String()))
		return nil, errRejected
	}
	row, found, err := store.SessionState(ctx, id)
	if err != nil {
		a.logger.WarnContext(ctx, "api access token session lookup failed",
			slog.String("user_id", subject.String()), slog.Bool("unavailable", errors.Is(err, ErrUnavailable)))
		return nil, err
	}
	switch {
	case !found:
		a.logger.WarnContext(ctx, "api access denied: session not found", slog.String("user_id", subject.String()))
		return nil, errRejected
	case row.PrincipalID != subject:
		a.logger.WarnContext(ctx, "api access denied: session of another principal", slog.String("user_id", subject.String()))
		return nil, errRejected
	case row.Revoked:
		a.logger.WarnContext(ctx, "api access denied: session revoked", slog.String("user_id", subject.String()))
		return nil, errRejected
	case row.LegacyTokenVersion == nil:
		a.logger.WarnContext(ctx, "api access denied: session has no token_version pin",
			slog.String("user_id", subject.String()), slog.String("reason_code", "session_pin_missing"))
		return nil, errRejected
	case *row.LegacyTokenVersion != userVersion:
		a.logger.WarnContext(ctx, "api access denied: session predates the token_version", slog.String("user_id", subject.String()))
		return nil, errRejected
	}
	return &id, nil
}

// sessionStateQuery reads one auth-service session in PostgreSQL by its
// primary key; {auth_schema} is the validated, quoted AUTH_DATABASE_SCHEMA.
const sessionStateQuery = `SELECT principal_id, revoked_at IS NOT NULL, legacy_token_version FROM {auth_schema}.sessions WHERE id = $1`

// SessionState reads the session id names, in the auth schema AuthSchema
// names: its principal, whether it is revoked and its token_version pin.
func (s PGStore) SessionState(ctx context.Context, id uuid.UUID) (SessionRow, bool, error) {
	name := s.AuthSchema
	if name == "" {
		name = defaultAuthSchema
	}
	if err := checkSchemaName(name); err != nil {
		return SessionRow{}, false, fmt.Errorf("policy: auth schema: %w", err)
	}
	var principal uuid.UUID
	var revoked bool
	var pin *int64
	query := strings.ReplaceAll(sessionStateQuery, "{auth_schema}", pgx.Identifier{name}.Sanitize())
	err := s.Pool.QueryRow(ctx, query, id).Scan(&principal, &revoked, &pin)
	if errors.Is(err, pgx.ErrNoRows) {
		return SessionRow{}, false, nil
	}
	if err != nil {
		return SessionRow{}, false, storeError(err, "SELECT on <auth schema>.sessions (id, principal_id, revoked_at, legacy_token_version)")
	}
	return SessionRow{PrincipalID: principal, Revoked: revoked, LegacyTokenVersion: pin}, true, nil
}

// errSchemaName is a schema name that cannot be a bare PostgreSQL identifier.
var errSchemaName = errors.New("policy: not a usable schema name")

// checkSchemaName is pgident's rule, the one auth-migrate and auth-service
// apply to the same AUTH_DATABASE_SCHEMA.
func checkSchemaName(name string) error {
	if pgident.Check(name) != pgident.OK {
		return fmt.Errorf("%w: %q", errSchemaName, name)
	}
	return nil
}

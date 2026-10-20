// Package identity is the auth-service identity store: the principal
// projection of public.users, the login lookup by email, and external
// identities keyed by (issuer, provider, subject), on the auth schema.
//
// Until the users cutover, public.users is the authority (go-api still writes
// it) and this package reads it and never writes it. auth.principals.id is
// public.users.id. ByEmail reads public.users live, so a password or state
// change in go-api is seen at once; auth.users.password_hash stays NULL.
//
// An external identity is found only by its full key (IdentityBySubject) and
// is created only by an explicit action committed through audit.Commit with its
// audit row and outbox event: LinkIdentity, ProvisionFederated, or the legacy
// projection of a public.users provider subject. No function here takes an
// email to find or link an external identity.
package identity

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

var (
	// ErrMultipleUsers reports more than one public.users row for one login
	// key (go-api answers that case with an unhandled error, a 500).
	ErrMultipleUsers = errors.New("identity: more than one user matches the email")
	// ErrUserNotFound reports a Project of an id public.users does not hold.
	ErrUserNotFound = errors.New("identity: user not found")
	// ErrEmailCollision reports a projection whose login key another
	// principal already holds. Nothing is merged and nothing is written.
	ErrEmailCollision = errors.New("identity: the email belongs to another principal")
	// ErrPrincipalKind reports a projection onto a principal id that is not a
	// user.
	ErrPrincipalKind = errors.New("identity: the principal is not a user")
	// ErrStore wraps a database failure.
	ErrStore = errors.New("identity: store unavailable")
)

const usersEmailLowerKey = "users_email_lower_key"

// Config builds a Store.
type Config struct {
	Pool   *pgxpool.Pool
	Schema authschema.ValidatedIdentifier
	Logger *slog.Logger
	// UsersOwnedByAuth is the users-cutover (D3) switch. Off (the default),
	// public.users is the authority: ProvisionFederated refuses with
	// ErrSignupNotAvailable, IdentityBySubject may fall back to the legacy
	// public.users columns, and HasPassword reads public.users. On, auth.users
	// is the authority and no public table is read.
	UsersOwnedByAuth bool
	// Meter registers the projection and link counters; nil uses the process
	// meter.
	Meter metric.Meter
}

// Store is the identity store over one auth schema.
type Store struct {
	pool        *pgxpool.Pool
	schema      authschema.ValidatedIdentifier
	q           string
	ownsUsers   bool
	logger      *slog.Logger
	projections metric.Int64Counter
	links       metric.Int64Counter
	slugOmitted metric.Int64Counter
}

// New validates the configuration and builds a Store.
func New(cfg Config) (*Store, error) {
	if cfg.Pool == nil {
		return nil, errors.New("identity: no connection pool")
	}
	if cfg.Schema.String() == "" {
		return nil, errors.New("identity: no auth schema")
	}
	s := &Store{pool: cfg.Pool, schema: cfg.Schema, q: authschema.Quote(cfg.Schema), ownsUsers: cfg.UsersOwnedByAuth, logger: cfg.Logger}
	if s.logger == nil {
		s.logger = slog.Default()
	}
	meter := cfg.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/identity")
	}
	var err error
	if s.projections, err = meter.Int64Counter("auth.identity.projections",
		metric.WithDescription("Principal projections of public.users, by outcome.")); err != nil {
		return nil, fmt.Errorf("identity: registering the projection counter: %w", err)
	}
	if s.links, err = meter.Int64Counter("auth.identity.external_links",
		metric.WithDescription("External identity link and unlink attempts, by action and outcome.")); err != nil {
		return nil, fmt.Errorf("identity: registering the link counter: %w", err)
	}
	if s.slugOmitted, err = meter.Int64Counter("auth.identity.wire_slug_omitted",
		metric.WithDescription("Organization events whose slug the provisioning wire cannot carry, so it was left out.")); err != nil {
		return nil, fmt.Errorf("identity: registering the slug counter: %w", err)
	}
	return s, nil
}

// Principal is one user principal. ByEmail (local login) fills ID, IsActive,
// PasswordHash, IsVerified and AuthProvider, read live from public.users.
// IdentityBySubject fills ID, IsActive and DisabledAt from auth.principals.
type Principal struct {
	ID           uuid.UUID
	IsActive     bool
	PasswordHash *string
	IsVerified   bool
	AuthProvider *string
	DisabledAt   *time.Time
}

// ByEmail returns the user whose login key is normalizedEmail (the output of
// NormalizeEmail), or (nil, nil) when there is none. It is for local login
// only.
//
// Before the users cutover it is the go-api userByEmail query, lower(email) =
// $1 over public.users, read live; more than one row is ErrMultipleUsers.
// After it (Config.UsersOwnedByAuth) it reads auth.users by email_lower, which
// is unique. auth.users has no auth_provider column: AuthProvider is "local"
// for a user with a password and nil without one, so the local-login rule for
// an unverified "local" user covers every user who can present a password.
func (s *Store) ByEmail(ctx context.Context, normalizedEmail string) (*Principal, error) {
	if s.ownsUsers {
		return s.readPrincipal(ctx, "read user by email", s.ownedPrincipalSelect()+`WHERE u.email_lower = $1::text`, normalizedEmail)
	}
	return s.readPrincipal(ctx, "read user by email", legacyPrincipalSelect+`WHERE lower(email) = $1::text`, normalizedEmail)
}

// ByID returns the user with id, or (nil, nil) when there is none, with the
// same fields and the same live-read rule as ByEmail: public.users before the
// users cutover, auth.users after it.
func (s *Store) ByID(ctx context.Context, id uuid.UUID) (*Principal, error) {
	if s.ownsUsers {
		return s.readPrincipal(ctx, "read user by id", s.ownedPrincipalSelect()+`WHERE u.principal_id = $1`, id)
	}
	return s.readPrincipal(ctx, "read user by id", legacyPrincipalSelect+`WHERE id = $1`, id)
}

const legacyPrincipalSelect = `SELECT id, password_hash, is_active, is_verified, auth_provider
FROM public.users `

func (s *Store) ownedPrincipalSelect() string {
	return `SELECT u.principal_id, u.password_hash, p.disabled_at IS NULL, u.email_verified_at IS NOT NULL,
       CASE WHEN u.password_hash IS NULL THEN NULL ELSE 'local' END
FROM ` + s.q + `.users u JOIN ` + s.q + `.principals p ON p.id = u.principal_id
`
}

func (s *Store) readPrincipal(ctx context.Context, op, sql string, arg any) (*Principal, error) {
	rows, err := s.pool.Query(ctx, sql, arg)
	if err != nil {
		return nil, s.storeErr(ctx, op, err)
	}
	defer rows.Close()
	var found *Principal
	for rows.Next() {
		if found != nil {
			return nil, ErrMultipleUsers
		}
		var p Principal
		if err := rows.Scan(&p.ID, &p.PasswordHash, &p.IsActive, &p.IsVerified, &p.AuthProvider); err != nil {
			return nil, s.storeErr(ctx, op, err)
		}
		found = &p
	}
	if err := rows.Err(); err != nil {
		return nil, s.storeErr(ctx, op, err)
	}
	return found, nil
}

// source is the projected state of one public.users row.
type source struct {
	email      string
	emailLower string
	active     bool
	verified   bool
}

// projected is the state auth.principals and auth.users hold for one id.
type projected struct {
	kind       string
	email      string
	emailLower string
	disabled   bool
	verified   bool
}

// Project makes auth.principals and auth.users agree with public.users for one
// user. A first projection inserts both rows, and a later one changes only
// what differs; each commits through audit.Commit with one audit row and one
// provisioning event per created aggregate or changed field. An unchanged user writes
// nothing. The password verifier is never copied.
func (s *Store) Project(ctx context.Context, userID uuid.UUID) (err error) {
	outcome := "unchanged"
	defer func() {
		if err != nil {
			outcome = projectionOutcome(err)
		}
		s.projections.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
	}()
	src, err := s.readSource(ctx, userID)
	if err != nil {
		return err
	}
	inserted, err := s.insertProjection(ctx, userID, src)
	if err != nil {
		return err
	}
	if inserted {
		outcome = "inserted"
		s.logger.InfoContext(ctx, "identity: principal projected", "principal_id", userID.String())
		return nil
	}
	cur, err := s.readProjection(ctx, userID)
	if err != nil {
		return err
	}
	if cur.kind != "user" {
		return ErrPrincipalKind
	}
	changed := changedFields(src, cur)
	if len(changed) == 0 {
		return nil
	}
	if err := s.applyProjectionChange(ctx, userID, src, cur, changed); err != nil {
		return err
	}
	outcome = "updated"
	s.logger.InfoContext(ctx, "identity: principal projection updated",
		"principal_id", userID.String(), "changed_fields", changed)
	return nil
}

func (s *Store) readSource(ctx context.Context, userID uuid.UUID) (source, error) {
	var email string
	var src source
	err := s.pool.QueryRow(ctx, `SELECT email, is_active, is_verified FROM public.users WHERE id = $1`, userID).
		Scan(&email, &src.active, &src.verified)
	if errors.Is(err, pgx.ErrNoRows) {
		return source{}, ErrUserNotFound
	}
	if err != nil {
		return source{}, s.storeErr(ctx, "read public.users", err)
	}
	src.email, src.emailLower = email, storedLoginKey(email)
	return src, nil
}

// storedLoginKey is the auth.users.email_lower of a public.users email.
// go-api finds a user by lower(email), and public.users holds addresses that
// pydantic EmailStr refuses (SSO and social provisioning store the provider's
// claim, and EmailStr refuses special-use domains), so a refused address keeps
// Python strip(lower()) as its key instead of failing the projection.
func storedLoginKey(email string) string {
	if key, err := NormalizeEmail(email); err == nil {
		return key
	}
	return pythonparity.Strip(pythonparity.Lower(email))
}

// errPrincipalExists ends a first projection whose principal row already
// exists (an earlier or a concurrent projection); Project then compares and
// updates instead.
var errPrincipalExists = errors.New("identity: principal already projected")

// insertProjection inserts the principal and user rows when the principal is
// new, in one audit.Commit with one audit row (reason principal_projected)
// and the principal.created and user.created events, plus user.email_verified
// when the source is verified. The rows are written as the source holds them;
// the 0013 create guard counts them as the state change. It reports whether
// it inserted.
func (s *Store) insertProjection(ctx context.Context, id uuid.UUID, src source) (bool, error) {
	// Most projections are of a principal that exists: answer those with a
	// read, not a refused commit. The insert below still decides a race.
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+s.q+`.principals WHERE id = $1)`, id).Scan(&exists); err != nil {
		return false, s.storeErr(ctx, "read principal", err)
	}
	if exists {
		return false, nil
	}
	principal := id.String()
	events := []audit.OutboxEvent{principalEvent(principal, "principal.created", src.active), userEvent(principal, "user.created")}
	if src.verified {
		events = append(events, userEvent(principal, "user.email_verified"))
	}
	err := audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			var inserted uuid.UUID
			err := tx.QueryRow(ctx, `INSERT INTO `+s.q+`.principals (id, kind, disabled_at)
VALUES ($1, 'user', CASE WHEN $2::boolean THEN NULL ELSE clock_timestamp() END)
ON CONFLICT (id) DO NOTHING
RETURNING id`, id, src.active).Scan(&inserted)
			if errors.Is(err, pgx.ErrNoRows) {
				return errPrincipalExists
			}
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `INSERT INTO `+s.q+`.users (principal_id, email, email_lower, email_verified_at)
VALUES ($1, $2, $3, CASE WHEN $4::boolean THEN clock_timestamp() ELSE NULL END)`,
				id, src.email, src.emailLower, src.verified)
			return err
		},
		Audit: audit.AuditEvent{
			EventType: "auth.principal.projected", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &principal,
			Attributes: map[string]any{"reason_code": "principal_projected", "target_kind": "principal", "target_id": principal},
		},
		Events: events,
	})
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, errPrincipalExists):
		return false, nil
	case isEmailCollision(err):
		return false, ErrEmailCollision
	}
	return false, s.storeErr(ctx, "insert projection", err)
}

func (s *Store) readProjection(ctx context.Context, id uuid.UUID) (projected, error) {
	var cur projected
	var email, emailLower *string
	var verified *bool
	err := s.pool.QueryRow(ctx, `SELECT p.kind, p.disabled_at IS NOT NULL, u.email, u.email_lower, u.email_verified_at IS NOT NULL
FROM `+s.q+`.principals p LEFT JOIN `+s.q+`.users u ON u.principal_id = p.id
WHERE p.id = $1`, id).Scan(&cur.kind, &cur.disabled, &email, &emailLower, &verified)
	if err != nil {
		return projected{}, s.storeErr(ctx, "read projection", err)
	}
	if email == nil || emailLower == nil || verified == nil {
		return projected{}, ErrPrincipalKind
	}
	cur.email, cur.emailLower, cur.verified = *email, *emailLower, *verified
	return cur, nil
}

func changedFields(src source, cur projected) []string {
	var changed []string
	if cur.disabled == src.active {
		changed = append(changed, "disabled_at")
	}
	if cur.email != src.email || cur.emailLower != src.emailLower {
		changed = append(changed, "email")
	}
	if cur.verified != src.verified {
		changed = append(changed, "email_verified_at")
	}
	return changed
}

func (s *Store) applyProjectionChange(ctx context.Context, id uuid.UUID, src source, cur projected, changed []string) error {
	reason := "principal_projected"
	switch {
	case cur.disabled && src.active:
		reason = "principal_enabled"
	case !cur.disabled && !src.active:
		reason = "principal_disabled"
	}
	principal := id.String()
	err := audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if _, err := tx.Exec(ctx, `UPDATE `+s.q+`.principals
SET disabled_at = CASE WHEN $2::boolean THEN NULL ELSE clock_timestamp() END, updated_at = clock_timestamp()
WHERE id = $1 AND (disabled_at IS NULL) IS DISTINCT FROM $2::boolean`, id, src.active); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE `+s.q+`.users
SET email = $2, email_lower = $3, updated_at = clock_timestamp()
WHERE principal_id = $1 AND (email IS DISTINCT FROM $2 OR email_lower IS DISTINCT FROM $3)`,
				id, src.email, src.emailLower); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `UPDATE `+s.q+`.users
SET email_verified_at = CASE WHEN $2::boolean THEN clock_timestamp() ELSE NULL END, updated_at = clock_timestamp()
WHERE principal_id = $1 AND (email_verified_at IS NOT NULL) IS DISTINCT FROM $2::boolean`, id, src.verified)
			return err
		},
		Audit: audit.AuditEvent{
			EventType: "auth.principal.projection_updated", Outcome: audit.OutcomeAllowed,
			SubjectPrincipalID: &principal,
			Attributes: map[string]any{
				"reason_code": reason, "target_kind": "principal", "target_id": principal, "changed_fields": changed,
			},
		},
		Events: projectionEvents(principal, src, changed),
	})
	switch {
	case err == nil:
		return nil
	case errors.Is(err, audit.ErrNoStateChange):
		// A concurrent projection applied the same change first.
		return nil
	case isEmailCollision(err):
		return ErrEmailCollision
	}
	return s.storeErr(ctx, "update projection", err)
}

// projectionEvents is one provisioning event per changed field.
func projectionEvents(principal string, src source, changed []string) []audit.OutboxEvent {
	var events []audit.OutboxEvent
	for _, field := range changed {
		switch field {
		case "disabled_at":
			verb := "principal.disabled"
			if src.active {
				verb = "principal.enabled"
			}
			events = append(events, principalEvent(principal, verb, src.active))
		case "email":
			events = append(events, userEvent(principal, "user.email_changed"))
		case "email_verified_at":
			verb := "user.email_unverified"
			if src.verified {
				verb = "user.email_verified"
			}
			events = append(events, userEvent(principal, verb))
		}
	}
	return events
}

// principalEvent is a principal event carrying the status after it.
func principalEvent(principal, verb string, active bool) audit.OutboxEvent {
	status := "disabled"
	if active {
		status = "active"
	}
	return audit.OutboxEvent{AggregateType: "principal", AggregateID: provisioning.WireID("principal", principal),
		EventType: verb, Change: map[string]any{"status": status}}
}

// userEvent is a user event; a user event carries no change member.
func userEvent(principal, verb string) audit.OutboxEvent {
	return audit.OutboxEvent{AggregateType: "user", AggregateID: provisioning.WireID("user", principal), EventType: verb}
}

func projectionOutcome(err error) string {
	switch {
	case errors.Is(err, ErrEmailCollision):
		return "email_collision"
	case errors.Is(err, ErrUserNotFound):
		return "user_not_found"
	case errors.Is(err, ErrInvalidEmail):
		return "invalid_email"
	case errors.Is(err, ErrPrincipalKind):
		return "principal_kind"
	}
	return "error"
}

func (s *Store) storeErr(ctx context.Context, op string, err error) error {
	s.logger.ErrorContext(ctx, "identity: store failure", "op", op, logging.ErrorAttr(err))
	return fmt.Errorf("%w: %s: %w", ErrStore, op, err)
}

func isEmailCollision(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == usersEmailLowerKey
}

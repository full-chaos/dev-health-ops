// Package session is the auth-service session domain: sessions, their opaque
// refresh credentials, rotation, reuse detection and revocation, on the auth
// schema.
//
// One writer per aggregate. The aggregate is the session row, and every
// writer (rotation, reuse revoke, revoke, revoke-all, revision bump) takes
// that row FOR UPDATE before it reads or writes a credential of the session.
// Writers of one session therefore run one at a time, under READ COMMITTED,
// and each re-reads the credential after it holds the lock.
//
// Rotation writes only columns migration 0006/0010 leave unguarded (an insert,
// consumed_at, superseded_by), so it commits in a plain transaction. Every
// write of a guarded column (revoked_at, revision) goes through audit.Commit,
// so the revocation, its audit row and its outbox event commit together or not
// at all. Time is the database clock (clock_timestamp()) throughout.
package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// GraceWindow is how long a just-rotated credential presented again is a
// concurrent refresh answered with the same successor, not reuse (go-api's
// rotationGraceWindow).
const GraceWindow = 30 * time.Second

// defaultRefreshTTL is go-api's rolling refresh lifetime, which ACP-ADR-03
// keeps (7 days rolling, single-use, family-linked).
const defaultRefreshTTL = 7 * 24 * time.Hour

var (
	errNotImplemented = errors.New("session: not implemented")
	// ErrSessionNotFound reports an unknown session id.
	ErrSessionNotFound = errors.New("session: not found")
	// ErrSessionNotLive reports a write to a revoked or expired session.
	ErrSessionNotLive = errors.New("session: revoked or expired")
	// ErrPrincipalUnavailable reports a Create for a principal that does not
	// exist or is disabled.
	ErrPrincipalUnavailable = errors.New("session: principal missing or disabled")
	// ErrInvalidAMR reports an amr list session.v1 would refuse.
	ErrInvalidAMR = errors.New("session: amr must be a non-empty set of pwd, otp, mfa, hwk, swk, fed")
	// ErrGraceUnavailable reports a grace replay whose recomputed successor
	// does not match the stored one (the successor key changed). It is
	// retryable and revokes nothing: the client is not the cause.
	ErrGraceUnavailable = errors.New("session: grace successor cannot be recomputed; retry")
	// ErrStore wraps a database failure.
	ErrStore = errors.New("session: store unavailable")
)

// Config builds a Store.
type Config struct {
	Pool   *pgxpool.Pool
	Schema authschema.ValidatedIdentifier
	// SuccessorKey is a 32-byte server secret, base64url without padding. It
	// derives rotated credentials; see deriveSuccessor.
	SuccessorKey contracts.SecretString
	// RefreshTTL is a credential's idle lifetime; 0 means 7 days (go-api's).
	RefreshTTL time.Duration
	// SessionLifetime is an optional absolute session lifetime. 0 means no
	// absolute cap (go-api parity: a session lives while it keeps refreshing).
	// With a cap, a credential never outlives its session.
	SessionLifetime time.Duration
	Logger          *slog.Logger
	// Meter registers the exchange and revocation counters; nil uses the
	// process meter.
	Meter metric.Meter
}

// Store is the session domain over one auth schema.
type Store struct {
	pool            *pgxpool.Pool
	schema          authschema.ValidatedIdentifier
	q               string
	key             *[successorKeyBytes]byte
	refreshTTL      time.Duration
	sessionLifetime time.Duration
	logger          *slog.Logger
	exchanges       metric.Int64Counter
	revocations     metric.Int64Counter
}

// Format and LogValue keep the successor key out of every printed form: fmt
// reaches unexported fields through reflection, past any method on them.
func (Store) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, "session.Store{[redacted]}") }

func (Store) LogValue() slog.Value { return slog.StringValue("session.Store{[redacted]}") }

// New validates the configuration and builds a Store.
func New(cfg Config) (*Store, error) {
	if cfg.Pool == nil {
		return nil, errors.New("session: no connection pool")
	}
	if cfg.Schema.String() == "" {
		return nil, errors.New("session: no auth schema")
	}
	key, err := decodeSuccessorKey(cfg.SuccessorKey)
	if err != nil {
		return nil, err
	}
	s := &Store{
		pool: cfg.Pool, schema: cfg.Schema, q: authschema.Quote(cfg.Schema), key: key,
		refreshTTL: cfg.RefreshTTL, sessionLifetime: cfg.SessionLifetime, logger: cfg.Logger,
	}
	if s.refreshTTL <= 0 {
		s.refreshTTL = defaultRefreshTTL
	}
	if s.sessionLifetime < 0 {
		return nil, errors.New("session: a negative session lifetime")
	}
	if s.logger == nil {
		s.logger = slog.Default()
	}
	meter := cfg.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/session")
	}
	if s.exchanges, err = meter.Int64Counter("auth.session.refresh_exchanges",
		metric.WithDescription("Refresh exchanges by auth-outcome reason code.")); err != nil {
		return nil, fmt.Errorf("session: registering the exchange counter: %w", err)
	}
	if s.revocations, err = meter.Int64Counter("auth.session.revocations",
		metric.WithDescription("Sessions revoked, by revoked_reason.")); err != nil {
		return nil, fmt.Errorf("session: registering the revocation counter: %w", err)
	}
	return s, nil
}

// Record is one session row. It carries no secret.
type Record struct {
	ID             uuid.UUID
	PrincipalID    uuid.UUID
	OrganizationID *uuid.UUID
	Revision       int64
	AuthTime       time.Time
	AMR            []string
	CreatedAt      time.Time
	ExpiresAt      time.Time
	RevokedAt      *time.Time
}

// Wire is the session.v1 document of the record.
func (r Record) Wire() contracts.Session {
	out := contracts.Session{
		SchemaVersion: contracts.SessionSurface,
		SessionID:     WireSessionID(r.ID),
		PrincipalID:   WirePrincipalID(r.PrincipalID),
		Revision:      contracts.Revision(r.Revision),
		AuthTime:      contracts.Timestamp{Time: r.AuthTime.UTC()},
		AMR:           append([]string(nil), r.AMR...),
		CreatedAt:     contracts.Timestamp{Time: r.CreatedAt.UTC()},
		ExpiresAt:     contracts.Timestamp{Time: r.ExpiresAt.UTC()},
	}
	if r.RevokedAt != nil {
		out.RevokedAt = &contracts.Timestamp{Time: r.RevokedAt.UTC()}
	}
	return out
}

// Issued is a session's live refresh credential, returned once.
type Issued struct {
	Session    Record
	Credential contracts.SecretString
	ExpiresAt  time.Time
}

// Result is the outcome of an exchange. Credential is set only when
// Reason.Success().
type Result struct {
	Reason Reason
	Issued
}

// Actor names who asked for a revocation or revision change, for the audit row.
type Actor struct {
	PrincipalID *uuid.UUID
	RequestID   *string
}

// amrValues is session.v1's closed amr enum.
var amrValues = map[string]bool{"pwd": true, "otp": true, "mfa": true, "hwk": true, "swk": true, "fed": true}

func validAMR(amr []string) bool {
	if len(amr) == 0 {
		return false
	}
	seen := map[string]bool{}
	for _, v := range amr {
		if !amrValues[v] || seen[v] {
			return false
		}
		seen[v] = true
	}
	return true
}

// sessionColumns selects a Record from alias s. expires_at on the record is the
// EFFECTIVE expiry: the absolute cap (sessions.expires_at, 'infinity' when
// uncapped) or the newest credential's expiry, whichever is first. A session
// whose credentials were all swept falls back to its last rotation plus the
// refresh lifetime, which is when its last credential expired.
func (s *Store) sessionColumns() string {
	return fmt.Sprintf(`s.id::text, s.principal_id::text, s.organization_id::text, s.revision,
	s.auth_time, s.amr, s.created_at,
	LEAST(s.expires_at, COALESCE(
		(SELECT max(r.expires_at) FROM %s.refresh_credentials r WHERE r.session_id = s.id),
		COALESCE(s.last_seen_at, s.created_at) + make_interval(secs => %d))),
	s.revoked_at`, s.q, int64(s.refreshTTL/time.Second))
}

func scanRecord(row pgx.Row, extra ...any) (Record, error) {
	var (
		r                 Record
		id, principal     string
		organization      *string
		authTime          *time.Time
		dest              = []any{&id, &principal, &organization, &r.Revision, &authTime, &r.AMR, &r.CreatedAt, &r.ExpiresAt, &r.RevokedAt}
		parseErr, scanErr error
	)
	if scanErr = row.Scan(append(dest, extra...)...); scanErr != nil {
		return Record{}, scanErr
	}
	if r.ID, parseErr = uuid.Parse(id); parseErr != nil {
		return Record{}, parseErr
	}
	if r.PrincipalID, parseErr = uuid.Parse(principal); parseErr != nil {
		return Record{}, parseErr
	}
	if organization != nil {
		org, err := uuid.Parse(*organization)
		if err != nil {
			return Record{}, err
		}
		r.OrganizationID = &org
	}
	if authTime != nil {
		r.AuthTime = *authTime
	} else {
		r.AuthTime = r.CreatedAt
	}
	return r, nil
}

// Create starts a session for an active principal and issues its first
// refresh credential, which starts a new family. auth_time is now.
func (s *Store) Create(ctx context.Context, principalID uuid.UUID, organizationID *uuid.UUID, amr []string) (Issued, error) {
	return Issued{}, errNotImplemented
}

// presented is a credential row and its successor, read under the session lock.
type presented struct {
	id, familyID      string
	successor         *string
	successorHash     []byte
	consumed, revoked bool
	expired           bool
	inGrace           bool
	successorLive     bool
}

const presentedQuery = `
	SELECT r.id::text, r.family_id::text, r.superseded_by::text, c.token_hash,
	       r.consumed_at IS NOT NULL, r.revoked_at IS NOT NULL, r.expires_at <= clock_timestamp(),
	       r.consumed_at IS NOT NULL AND clock_timestamp() - r.consumed_at <= make_interval(secs => $3::double precision),
	       c.id IS NOT NULL AND c.consumed_at IS NULL AND c.revoked_at IS NULL AND c.expires_at > clock_timestamp()
	FROM %[1]s.refresh_credentials r
	LEFT JOIN %[1]s.refresh_credentials c ON c.id = r.superseded_by
	WHERE r.token_hash = $1 AND r.session_id = $2`

func (s *Store) readPresented(ctx context.Context, db queryRower, hash []byte, sessionID string) (presented, error) {
	var p presented
	err := db.QueryRow(ctx, fmt.Sprintf(presentedQuery, s.q), hash, sessionID, GraceWindow.Seconds()).Scan(
		&p.id, &p.familyID, &p.successor, &p.successorHash,
		&p.consumed, &p.revoked, &p.expired, &p.inGrace, &p.successorLive)
	return p, err
}

// queryRower is a pgx.Tx or an audit.TxOps.
type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// lockedSession is the session row read FOR UPDATE, with what an exchange
// decides on.
type lockedSession struct {
	Record
	expired, principalDisabled bool
}

func (s *Store) lockSession(ctx context.Context, db queryRower, sessionID string) (lockedSession, error) {
	var l lockedSession
	var err error
	l.Record, err = scanRecord(db.QueryRow(ctx, `
		SELECT `+s.sessionColumns()+`, s.expires_at <= clock_timestamp(), p.disabled_at IS NOT NULL
		FROM `+s.q+`.sessions s JOIN `+s.q+`.principals p ON p.id = s.principal_id
		WHERE s.id = $1
		FOR UPDATE OF s`, sessionID), &l.expired, &l.principalDisabled)
	return l, err
}

// Exchange presents a refresh credential and returns its successor.
//
// A live credential is rotated: its successor is inserted and it is marked
// consumed, in one transaction under the session lock. The same credential
// presented again within GraceWindow, while its successor is still unused,
// returns that same successor (refresh_grace_replay). Any other presentation
// of a consumed credential is reuse: the family and the session are revoked,
// audited and announced in one audit.Commit transaction (refresh_reused).
//
// A refusal is a Result with a non-success Reason and a nil error. The error
// is set only when the store failed or the grace successor could not be
// recomputed; Result.Reason then says which.
func (s *Store) Exchange(ctx context.Context, credential contracts.SecretString) (result Result, err error) {
	return Result{Reason: ReasonStoreUnavailable}, errNotImplemented
}

func equalHash(a, b []byte) bool {
	if len(a) != len(b) || len(a) == 0 {
		return false
	}
	var diff byte
	for i := range a {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}

// rotate inserts the successor and consumes the parent. The caller holds the
// session lock.
func (s *Store) rotate(ctx context.Context, tx pgx.Tx, session Record, p presented, parent contracts.SecretString) (Issued, error) {
	childID, err := uuid.NewRandom()
	if err != nil {
		return Issued{}, err
	}
	successor := deriveSuccessor(s.key, childID, parent)
	var expires time.Time
	if err := tx.QueryRow(ctx, `
		INSERT INTO `+s.q+`.refresh_credentials (id, session_id, principal_id, token_hash, family_id, parent_id, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, LEAST(clock_timestamp() + make_interval(secs => $7::double precision),
			(SELECT expires_at FROM `+s.q+`.sessions WHERE id = $2)))
		RETURNING expires_at`,
		childID.String(), session.ID.String(), session.PrincipalID.String(), hashCredential(successor),
		p.familyID, p.id, s.refreshTTL.Seconds()).Scan(&expires); err != nil {
		return Issued{}, err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE `+s.q+`.refresh_credentials
		SET consumed_at = clock_timestamp(), superseded_by = $2, updated_at = clock_timestamp()
		WHERE id = $1 AND consumed_at IS NULL`, p.id, childID.String())
	if err != nil {
		return Issued{}, err
	}
	if tag.RowsAffected() != 1 {
		return Issued{}, fmt.Errorf("consuming the parent changed %d rows under the session lock", tag.RowsAffected())
	}
	if _, err := tx.Exec(ctx, `UPDATE `+s.q+`.sessions SET last_seen_at = clock_timestamp() WHERE id = $1`, session.ID.String()); err != nil {
		return Issued{}, err
	}
	// Re-read: the session's effective expiry moved with the new credential.
	session, err = scanRecord(tx.QueryRow(ctx, `SELECT `+s.sessionColumns()+` FROM `+s.q+`.sessions s WHERE s.id = $1`, session.ID.String()))
	if err != nil {
		return Issued{}, err
	}
	return Issued{Session: session, Credential: successor, ExpiresAt: expires}, nil
}

var errNotReuse = errors.New("session: the credential is no longer a reuse under the lock")

// revokeForReuse revokes the session and every credential of it, with the
// audit row and outbox event, in one audit.Commit transaction. It re-reads the
// credential under the session lock: the decision is taken on the rows it
// writes, not on the earlier read.
func (s *Store) revokeForReuse(ctx context.Context, session Record, familyID string, hash []byte) (Result, error) {
	sessionID := session.ID.String()
	principal := session.PrincipalID.String()
	err := audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			locked, err := s.lockSession(ctx, tx, sessionID)
			if err != nil {
				return err
			}
			if locked.RevokedAt != nil {
				return nil
			}
			p, err := s.readPresented(ctx, tx, hash, sessionID)
			if err != nil {
				return err
			}
			if !p.consumed || p.expired || (p.inGrace && p.successorLive) {
				return errNotReuse
			}
			return s.revokeSessions(ctx, tx, []string{sessionID}, revokedByReuse)
		},
		Audit: audit.AuditEvent{
			EventType:          "auth.session.refresh_reuse_revoked",
			Outcome:            audit.OutcomeDenied,
			SubjectPrincipalID: &principal,
			Attributes: map[string]any{
				"reason_code": "session_revoked", "target_kind": "session", "target_id": sessionID,
				"credential_class": "refresh_credential",
			},
		},
		Event: audit.OutboxEvent{
			AggregateType: "session", AggregateID: sessionID, EventType: "session.revoked",
			Payload:        map[string]any{"session_id": sessionID, "principal_id": principal, "reason_code": revokedByReuse},
			IdempotencyKey: "session.revoked:" + sessionID,
		},
	})
	switch {
	case err == nil:
		s.revocations.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", revokedByReuse)))
		s.logger.WarnContext(ctx, "refresh credential reuse: session and family revoked",
			"session_id", sessionID, "family_id", familyID, "reason_code", string(ReasonRefreshReused))
		return Result{Reason: ReasonRefreshReused}, nil
	case errors.Is(err, audit.ErrNoStateChange):
		return Result{Reason: ReasonSessionRevoked}, nil
	case errors.Is(err, errNotReuse):
		// Unreachable under the lock (consumed never clears and the grace only
		// shrinks); refused rather than guessed.
		return Result{Reason: ReasonStoreUnavailable}, fmt.Errorf("%w: %w", ErrStore, err)
	default:
		return Result{Reason: ReasonStoreUnavailable}, fmt.Errorf("%w: revoking a reused family: %w", ErrStore, err)
	}
}

// revokeSessions revokes the given sessions and every credential of them. The
// caller holds their locks inside an audit.Commit transaction.
func (s *Store) revokeSessions(ctx context.Context, tx audit.TxOps, ids []string, reason string) error {
	if _, err := tx.Exec(ctx, `
		UPDATE `+s.q+`.refresh_credentials
		SET revoked_at = clock_timestamp(), revoked_reason = $2, updated_at = clock_timestamp()
		WHERE session_id = ANY($1::uuid[]) AND revoked_at IS NULL`, ids, reason); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		UPDATE `+s.q+`.sessions
		SET revoked_at = clock_timestamp(), revoked_reason = $2, updated_at = clock_timestamp()
		WHERE id = ANY($1::uuid[]) AND revoked_at IS NULL`, ids, reason)
	return err
}

// Revoke ends one session and every credential of it. It reports false when
// the session was already revoked.
func (s *Store) Revoke(ctx context.Context, sessionID uuid.UUID, actor Actor) (bool, error) {
	return false, errNotImplemented
}

// RevokeAll ends every live session of a principal, in one transaction with
// one audit row and one outbox event. It returns how many it revoked.
func (s *Store) RevokeAll(ctx context.Context, principalID uuid.UUID, actor Actor) (int, error) {
	return 0, errNotImplemented
}

// BumpRevision increments a live session's revision (the successor of the
// legacy token_version): an access token minted at an older revision is stale.
func (s *Store) BumpRevision(ctx context.Context, sessionID uuid.UUID, actor Actor) (int64, error) {
	return 0, errNotImplemented
}

// Current returns the session record, live or not. It carries no secret.
func (s *Store) Current(ctx context.Context, sessionID uuid.UUID) (Record, error) {
	return Record{}, errNotImplemented
}

func actorID(a Actor) *string {
	if a.PrincipalID == nil {
		return nil
	}
	id := a.PrincipalID.String()
	return &id
}

func (s *Store) recordExchange(ctx context.Context, result Result, err error) {
	s.exchanges.Add(ctx, 1, metric.WithAttributes(attribute.String("reason_code", string(result.Reason))))
	switch {
	case err != nil:
		s.logger.ErrorContext(ctx, "refresh exchange failed", "reason_code", string(result.Reason), "error", err)
	case result.Reason.Success():
		s.logger.DebugContext(ctx, "refresh exchange", "reason_code", string(result.Reason), "session_id", result.Session.ID.String())
	default:
		s.logger.InfoContext(ctx, "refresh exchange refused", "reason_code", string(result.Reason))
	}
}

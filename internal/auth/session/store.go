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
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
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
	return `s.id::text, s.principal_id::text, s.organization_id::text, s.revision,
	s.auth_time, s.amr, s.created_at, ` + s.effectiveExpiry() + `, ` + s.effectiveRevokedAt()
}

// effectiveRevokedAt is the expression for alias s's revoked_at as the record
// reports it: the explicit revoke, else the principal's password stamp
// (users.password_updated_at) when that is strictly after the session was
// created. A password change thus ends every older session in the same
// transaction that wrote the new hash, whether or not RevokeAll ran. Both
// times are database-clock values; a principal with no stamp voids nothing.
func (s *Store) effectiveRevokedAt() string {
	return fmt.Sprintf(`COALESCE(s.revoked_at,
		(SELECT u.password_updated_at FROM %s.users u
		 WHERE u.principal_id = s.principal_id AND u.password_updated_at > s.created_at))`, s.q)
}

// effectiveExpiry is the expression for alias s's effective expiry.
func (s *Store) effectiveExpiry() string {
	return fmt.Sprintf(`LEAST(s.expires_at, COALESCE(
		(SELECT r.expires_at FROM %s.refresh_credentials r WHERE r.session_id = s.id
		 ORDER BY r.issued_at DESC, r.created_at DESC LIMIT 1),
		COALESCE(s.last_seen_at, s.created_at) + %d * interval '1 microsecond'))`, s.q, s.refreshTTL.Microseconds())
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
	if !validAMR(amr) {
		return Issued{}, ErrInvalidAMR
	}
	credential, err := newCredential()
	if err != nil {
		return Issued{}, err
	}
	handle, err := newCredential()
	if err != nil {
		return Issued{}, err
	}
	var org *string
	if organizationID != nil {
		text := organizationID.String()
		org = &text
	}
	var out Issued
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// The session token_hash is a digest of a value never returned: there is
		// no session bearer in wave 2, and the column is NOT NULL and unique.
		// An uncapped session stores 'infinity'; the record reports the
		// effective expiry (see sessionColumns).
		var sessionID string
		err := tx.QueryRow(ctx, `
			INSERT INTO `+s.q+`.sessions (principal_id, organization_id, token_hash, issued_at, created_at, updated_at, expires_at, auth_time, amr)
			SELECT p.id, $2::uuid, $3, t.at, t.at, t.at,
			       CASE WHEN $4::double precision > 0 THEN t.at + make_interval(secs => $4::double precision) ELSE 'infinity' END,
			       t.at, $5
			FROM `+s.q+`.principals p, (SELECT clock_timestamp() AS at) t
			WHERE p.id = $1 AND p.disabled_at IS NULL
			RETURNING id::text`,
			principalID.String(), org, hashCredential(handle), s.sessionLifetime.Seconds(), amr).Scan(&sessionID)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrPrincipalUnavailable
		}
		if err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO `+s.q+`.refresh_credentials (session_id, principal_id, token_hash, expires_at)
			SELECT s.id, s.principal_id, $2, LEAST(clock_timestamp() + make_interval(secs => $3::double precision), s.expires_at)
			FROM `+s.q+`.sessions s WHERE s.id = $1
			RETURNING expires_at`,
			sessionID, hashCredential(credential), s.refreshTTL.Seconds(),
		).Scan(&out.ExpiresAt); err != nil {
			return err
		}
		out.Session, err = scanRecord(tx.QueryRow(ctx, `SELECT `+s.sessionColumns()+` FROM `+s.q+`.sessions s WHERE s.id = $1`, sessionID))
		return err
	})
	if err != nil {
		if errors.Is(err, ErrPrincipalUnavailable) {
			return Issued{}, err
		}
		return Issued{}, fmt.Errorf("%w: creating a session: %w", ErrStore, err)
	}
	out.Credential = credential
	s.logger.DebugContext(ctx, "session created", "session_id", out.Session.ID.String())
	return out, nil
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
// decides on. expired is the EFFECTIVE expiry (Record.ExpiresAt) against the
// database clock, the same liveness Current reports; capExpired is the
// absolute cap alone, which an exchange checks before the credential's own
// expiry so an expired credential reports refresh_expired.
type lockedSession struct {
	Record
	expired, capExpired, principalDisabled bool
}

func (s *Store) lockSession(ctx context.Context, db queryRower, sessionID string) (lockedSession, error) {
	var l lockedSession
	var now time.Time
	var err error
	l.Record, err = scanRecord(db.QueryRow(ctx, `
		SELECT `+s.sessionColumns()+`, clock_timestamp(), s.expires_at <= clock_timestamp(), p.disabled_at IS NOT NULL
		FROM `+s.q+`.sessions s JOIN `+s.q+`.principals p ON p.id = s.principal_id
		WHERE s.id = $1
		FOR UPDATE OF s`, sessionID), &now, &l.capExpired, &l.principalDisabled)
	l.expired = err == nil && !now.Before(l.ExpiresAt)
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
	defer func() { s.recordExchange(ctx, result, err) }()
	if !wellFormed(credential) {
		return Result{Reason: ReasonMalformed}, nil
	}
	hash := hashCredential(credential)

	var reuse *lockedSession
	var reusedFamily string
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var sessionID string
		if err := tx.QueryRow(ctx, `SELECT session_id::text FROM `+s.q+`.refresh_credentials WHERE token_hash = $1`, hash).Scan(&sessionID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				result.Reason = ReasonRefreshUnknown
				return nil
			}
			return err
		}
		locked, err := s.lockSession(ctx, tx, sessionID)
		if errors.Is(err, pgx.ErrNoRows) {
			result.Reason = ReasonRefreshUnknown
			return nil
		}
		if err != nil {
			return err
		}
		p, err := s.readPresented(ctx, tx, hash, sessionID)
		if errors.Is(err, pgx.ErrNoRows) {
			result.Reason = ReasonRefreshUnknown
			return nil
		}
		if err != nil {
			return err
		}
		switch {
		case locked.RevokedAt != nil || p.revoked:
			result.Reason = ReasonSessionRevoked
		case locked.capExpired:
			result.Reason = ReasonSessionExpired
		case locked.principalDisabled:
			result.Reason = ReasonPrincipalDeactivated
		case !p.consumed && p.expired:
			result.Reason = ReasonRefreshExpired
		case !p.consumed:
			issued, err := s.rotate(ctx, tx, locked.Record, p, credential)
			if err != nil {
				return err
			}
			result = Result{Reason: ReasonAccepted, Issued: issued}
		case p.inGrace && p.successorLive:
			childID, err := uuid.Parse(*p.successor)
			if err != nil {
				return err
			}
			successor := deriveSuccessor(s.key, childID, credential)
			if !equalHash(hashCredential(successor), p.successorHash) {
				result.Reason = ReasonDependency
				return ErrGraceUnavailable
			}
			var expires time.Time
			if err := tx.QueryRow(ctx, `SELECT expires_at FROM `+s.q+`.refresh_credentials WHERE id = $1`, childID.String()).Scan(&expires); err != nil {
				return err
			}
			result = Result{Reason: ReasonGraceReplay, Issued: Issued{Session: locked.Record, Credential: successor, ExpiresAt: expires}}
		default:
			reuse, reusedFamily = &locked, p.familyID
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrGraceUnavailable) {
			return Result{Reason: ReasonDependency}, err
		}
		return Result{Reason: ReasonStoreUnavailable}, fmt.Errorf("%w: refresh exchange: %w", ErrStore, err)
	}
	if reuse == nil {
		return result, nil
	}
	return s.revokeForReuse(ctx, reuse.Record, reusedFamily, hash)
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
			if !p.consumed || (p.inGrace && p.successorLive) {
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

// Revoke ends one live session and every credential of it. It reports false
// when the session was already revoked or its effective expiry has passed.
func (s *Store) Revoke(ctx context.Context, sessionID uuid.UUID, actor Actor) (bool, error) {
	id := sessionID.String()
	var principal string
	if err := s.pool.QueryRow(ctx, `SELECT principal_id::text FROM `+s.q+`.sessions WHERE id = $1`, id).Scan(&principal); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, ErrSessionNotFound
		}
		return false, fmt.Errorf("%w: reading the session: %w", ErrStore, err)
	}
	err := audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			locked, err := s.lockSession(ctx, tx, id)
			if err != nil {
				return err
			}
			if locked.expired {
				// Already dead: nothing to revoke, so no audit row (ErrNoStateChange).
				return nil
			}
			return s.revokeSessions(ctx, tx, []string{id}, revokedByCaller)
		},
		Audit: audit.AuditEvent{
			EventType: "auth.session.revoked", Outcome: audit.OutcomeAllowed,
			ActorPrincipalID: actorID(actor), SubjectPrincipalID: &principal, RequestID: actor.RequestID,
			Attributes: map[string]any{"reason_code": "session_revoked", "target_kind": "session", "target_id": id},
		},
		Event: audit.OutboxEvent{
			AggregateType: "session", AggregateID: id, EventType: "session.revoked",
			Payload:        map[string]any{"session_id": id, "principal_id": principal, "reason_code": revokedByCaller},
			IdempotencyKey: "session.revoked:" + id,
		},
	})
	switch {
	case err == nil:
		s.revocations.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", revokedByCaller)))
		s.logger.InfoContext(ctx, "session revoked", "session_id", id)
		return true, nil
	case errors.Is(err, audit.ErrNoStateChange):
		return false, nil
	default:
		return false, fmt.Errorf("%w: revoking a session: %w", ErrStore, err)
	}
}

// RevokeAll ends every effectively live session of a principal, in one transaction with
// one audit row and one outbox event. It returns how many it revoked.
func (s *Store) RevokeAll(ctx context.Context, principalID uuid.UUID, actor Actor) (int, error) {
	principal := principalID.String()
	eventID, err := uuid.NewRandom()
	if err != nil {
		return 0, err
	}
	var revoked int
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			// Locks in id order, so two revoke-alls of one principal cannot
			// deadlock; a single-session writer holds one lock and cannot either.
			// Every unrevoked session is locked, then only the effectively
			// live ones are revoked (the same liveness every writer uses).
			rows, err := tx.Query(ctx, `
				SELECT s.id::text, `+s.effectiveExpiry()+` > clock_timestamp()
				FROM `+s.q+`.sessions s
				WHERE s.principal_id = $1 AND s.revoked_at IS NULL
				ORDER BY s.id FOR UPDATE OF s`, principal)
			if err != nil {
				return err
			}
			var ids []string
			for rows.Next() {
				var id string
				var live bool
				if err := rows.Scan(&id, &live); err != nil {
					rows.Close()
					return err
				}
				if live {
					ids = append(ids, id)
				}
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return err
			}
			revoked = len(ids)
			if revoked == 0 {
				return nil
			}
			return s.revokeSessions(ctx, tx, ids, revokedByCaller)
		},
		Audit: audit.AuditEvent{
			EventType: "auth.session.revoked_all", Outcome: audit.OutcomeAllowed,
			ActorPrincipalID: actorID(actor), SubjectPrincipalID: &principal, RequestID: actor.RequestID,
			Attributes: map[string]any{"reason_code": "session_revoked", "target_kind": "principal", "target_id": principal},
		},
		Event: audit.OutboxEvent{
			AggregateType: "principal", AggregateID: principal, EventType: "principal.sessions_revoked",
			Payload:        map[string]any{"principal_id": principal, "reason_code": revokedByCaller},
			IdempotencyKey: "principal.sessions_revoked:" + principal + ":" + eventID.String(),
		},
	})
	switch {
	case err == nil:
		s.revocations.Add(ctx, int64(revoked), metric.WithAttributes(attribute.String("reason", revokedByCaller)))
		s.logger.InfoContext(ctx, "all sessions of a principal revoked", "principal_id", principal, "sessions", revoked)
		return revoked, nil
	case errors.Is(err, audit.ErrNoStateChange):
		return 0, nil
	default:
		return 0, fmt.Errorf("%w: revoking all sessions: %w", ErrStore, err)
	}
}

// BumpRevision increments a live session's revision (the successor of the
// legacy token_version): an access token minted at an older revision is stale.
func (s *Store) BumpRevision(ctx context.Context, sessionID uuid.UUID, actor Actor) (int64, error) {
	id := sessionID.String()
	var principal string
	if err := s.pool.QueryRow(ctx, `SELECT principal_id::text FROM `+s.q+`.sessions WHERE id = $1`, id).Scan(&principal); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrSessionNotFound
		}
		return 0, fmt.Errorf("%w: reading the session: %w", ErrStore, err)
	}
	eventID, err := uuid.NewRandom()
	if err != nil {
		return 0, err
	}
	var revision int64
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			locked, err := s.lockSession(ctx, tx, id)
			if err != nil {
				return err
			}
			if locked.RevokedAt != nil || locked.expired {
				return nil
			}
			return tx.QueryRow(ctx, `
				UPDATE `+s.q+`.sessions SET revision = revision + 1, updated_at = clock_timestamp()
				WHERE id = $1 RETURNING revision`, id).Scan(&revision)
		},
		Audit: audit.AuditEvent{
			EventType: "auth.session.revision_bumped", Outcome: audit.OutcomeAllowed,
			ActorPrincipalID: actorID(actor), SubjectPrincipalID: &principal, RequestID: actor.RequestID,
			Attributes: map[string]any{"target_kind": "session", "target_id": id},
		},
		Event: audit.OutboxEvent{
			AggregateType: "session", AggregateID: id, EventType: "session.revision_changed",
			Payload:        map[string]any{"session_id": id, "principal_id": principal},
			IdempotencyKey: "session.revision:" + id + ":" + eventID.String(),
		},
	})
	switch {
	case err == nil:
		s.logger.InfoContext(ctx, "session revision bumped", "session_id", id, "revision", revision)
		return revision, nil
	case errors.Is(err, audit.ErrNoStateChange):
		return 0, ErrSessionNotLive
	default:
		return 0, fmt.Errorf("%w: bumping the revision: %w", ErrStore, err)
	}
}

// Current returns the session record, live or not. It carries no secret.
func (s *Store) Current(ctx context.Context, sessionID uuid.UUID) (Record, error) {
	record, err := scanRecord(s.pool.QueryRow(ctx, `SELECT `+s.sessionColumns()+` FROM `+s.q+`.sessions s WHERE s.id = $1`, sessionID.String()))
	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, ErrSessionNotFound
	}
	if err != nil {
		return Record{}, fmt.Errorf("%w: reading the session: %w", ErrStore, err)
	}
	return record, nil
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
		s.logger.ErrorContext(ctx, "refresh exchange failed", "reason_code", string(result.Reason), logging.ErrorAttr(err))
	case result.Reason.Success():
		s.logger.DebugContext(ctx, "refresh exchange", "reason_code", string(result.Reason), "session_id", result.Session.ID.String())
	default:
		s.logger.InfoContext(ctx, "refresh exchange refused", "reason_code", string(result.Reason))
	}
}

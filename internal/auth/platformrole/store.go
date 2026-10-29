package platformrole

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// activeSQL is the store's current-state rule (rule.ActiveRule) over its own
// aliases: r the assignment, p its principal. It is read on the database clock
// of the statement that reads it, and a guard statement runs after the write
// lock, so the wait for the lock is not counted as earlier time.
var activeSQL = rule.ActiveRule("r", "p")

// HoldsSQL is the one SQL condition that principalExpr holds the platform role
// roleExpr now, under the store's rule; it is rule.HoldsSQL. schema is a quoted
// auth schema; principalExpr and roleExpr are SQL expressions of the caller's
// own statement.
func HoldsSQL(schema, principalExpr, roleExpr string) string {
	return rule.HoldsSQL(schema, principalExpr, roleExpr)
}

// writeLock is the advisory transaction lock every grant and revoke takes
// first, so the store's writes run one at a time.
const writeLock = "auth.platform_role_assignments:write"

// Config builds a Store.
type Config struct {
	Pool   *pgxpool.Pool
	Schema authschema.ValidatedIdentifier
	Logger *slog.Logger
}

// Store reads and writes auth.platform_role_assignments.
type Store struct {
	pool   *pgxpool.Pool
	schema authschema.ValidatedIdentifier
	q      string
	logger *slog.Logger
}

// New validates the configuration and builds a Store.
func New(cfg Config) (*Store, error) {
	if cfg.Pool == nil {
		return nil, errors.New("platformrole: no connection pool")
	}
	if cfg.Schema.String() == "" {
		return nil, errors.New("platformrole: no auth schema")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Store{pool: cfg.Pool, schema: cfg.Schema, q: authschema.Quote(cfg.Schema), logger: logger}, nil
}

// Assignment is one stored platform role assignment and whether it is active
// now.
type Assignment struct {
	ID          uuid.UUID
	PrincipalID uuid.UUID
	Role        string
	GrantedBy   *uuid.UUID
	ExpiresAt   *time.Time
	Active      bool
}

// GrantInput is one grant. Actor must hold GrantAction now.
type GrantInput struct {
	Actor     uuid.UUID
	Principal uuid.UUID
	Role      string
	// ExpiresAt is nil for a permanent assignment.
	ExpiresAt *time.Time
}

// RevokeInput is one revoke. Actor must hold GrantAction now.
type RevokeInput struct {
	Actor     uuid.UUID
	Principal uuid.UUID
	Role      string
}

type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Resolve is the platform authority principal holds now. Any read failure is
// the zero Authority and an error: no authority is ever assumed.
func (s *Store) Resolve(ctx context.Context, principal uuid.UUID) (Authority, error) {
	a, err := s.resolveOn(ctx, s.pool, principal, false)
	if err != nil {
		return Authority{}, s.storeErr(ctx, "resolve", err)
	}
	return a, nil
}

// resolveOn reads the active role keys of principal through q. With lock set
// the principal row is share-locked until the transaction ends, so a disable
// of it waits; the assignment rows are covered by the write lock.
func (s *Store) resolveOn(ctx context.Context, q rowQuerier, principal uuid.UUID, lock bool) (Authority, error) {
	locking := ""
	if lock {
		locking = " FOR SHARE OF p"
	}
	var stored []string
	err := q.QueryRow(ctx, `SELECT coalesce(array_agg(role_key ORDER BY role_key), '{}') FROM (
	SELECT r.role_key FROM `+s.q+`.platform_role_assignments r JOIN `+s.q+`.principals p ON p.id = r.principal_id
	WHERE r.principal_id = $1 AND `+activeSQL+locking+`) AS held`, principal).Scan(&stored)
	if err != nil {
		return Authority{}, err
	}
	a, ignored := authorityOf(stored)
	if ignored > 0 {
		s.logger.WarnContext(ctx, "platformrole: stored role keys outside the catalog grant nothing", "ignored", ignored)
	}
	return a, nil
}

// Grant gives in.Principal the platform role in.Role in one audit.Commit. It
// takes the write lock first; the actor's authority, the target principal and
// the expiry are checked in the same transaction as the insert, on the
// database clock.
func (s *Store) Grant(ctx context.Context, in GrantInput) (uuid.UUID, error) {
	if !IsPlatformRole(in.Role) {
		return uuid.Nil, ErrNotPlatformRole
	}
	id := uuid.New()
	actor, subject := in.Actor.String(), in.Principal.String()
	err := audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if err := lockWrites(ctx, tx); err != nil {
				return err
			}
			held, err := s.resolveOn(ctx, tx, in.Actor, true)
			if err != nil {
				return err
			}
			if !held.Allows(GrantAction) {
				return ErrGranterNotHeld
			}
			var kind string
			var enabled bool
			err = tx.QueryRow(ctx, `SELECT kind, disabled_at IS NULL FROM `+s.q+`.principals WHERE id = $1 FOR SHARE OF principals`, in.Principal).Scan(&kind, &enabled)
			switch {
			case errors.Is(err, pgx.ErrNoRows):
				return ErrPrincipalMissing
			case err != nil:
				return err
			case kind != "user":
				return ErrNotHumanUser
			case !enabled:
				return ErrPrincipalOff
			}
			if in.ExpiresAt != nil {
				var future bool
				if err := tx.QueryRow(ctx, `SELECT $1::timestamptz > statement_timestamp()`, *in.ExpiresAt).Scan(&future); err != nil {
					return err
				}
				if !future {
					return ErrExpiryNotFuture
				}
			}
			var inserted uuid.UUID
			err = tx.QueryRow(ctx, `INSERT INTO `+s.q+`.platform_role_assignments (id, principal_id, role_key, granted_by, expires_at)
VALUES ($1, $2, $3, $4, $5) ON CONFLICT (principal_id, role_key) DO NOTHING RETURNING id`,
				id, in.Principal, in.Role, in.Actor, in.ExpiresAt).Scan(&inserted)
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrAlreadyHeld
			}
			return err
		},
		Audit:  roleAudit("auth.platform_role.granted", "role_granted", actor, subject),
		Events: []audit.OutboxEvent{roleEvent(id, "platform_role_assignment.granted", "active")},
	})
	if err != nil {
		return uuid.Nil, s.writeErr(ctx, "grant", err)
	}
	return id, nil
}

// Revoke removes in.Role from in.Principal in one audit.Commit. The write
// lock is taken first, so concurrent grants and revokes serialize; a revoke
// that would leave no active platform administrator is refused.
func (s *Store) Revoke(ctx context.Context, in RevokeInput) error {
	if !IsPlatformRole(in.Role) {
		return ErrNotPlatformRole
	}
	var target uuid.UUID
	err := s.pool.QueryRow(ctx, `SELECT id FROM `+s.q+`.platform_role_assignments WHERE principal_id = $1 AND role_key = $2`,
		in.Principal, in.Role).Scan(&target)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotHeld
	}
	if err != nil {
		return s.storeErr(ctx, "read revoke target", err)
	}
	actor, subject := in.Actor.String(), in.Principal.String()
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if err := lockWrites(ctx, tx); err != nil {
				return err
			}
			rows, err := tx.Query(ctx, `SELECT r.id, r.principal_id, r.role_key, `+activeSQL+`
FROM `+s.q+`.platform_role_assignments r JOIN `+s.q+`.principals p ON p.id = r.principal_id
ORDER BY r.id FOR SHARE OF p`)
			if err != nil {
				return err
			}
			var actorRoles []string
			found, admins := false, 0
			for rows.Next() {
				var id, principal uuid.UUID
				var role string
				var active bool
				if err := rows.Scan(&id, &principal, &role, &active); err != nil {
					rows.Close()
					return err
				}
				if id == target {
					found = principal == in.Principal && role == in.Role
				}
				if !active {
					continue
				}
				if principal == in.Actor {
					actorRoles = append(actorRoles, role)
				}
				if role == PlatformAdmin && id != target {
					admins++
				}
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return err
			}
			if held, _ := authorityOf(actorRoles); !held.Allows(GrantAction) {
				return ErrGranterNotHeld
			}
			if !found {
				return ErrNotHeld
			}
			if admins == 0 {
				return ErrLastPlatformAdmin
			}
			_, err = tx.Exec(ctx, `DELETE FROM `+s.q+`.platform_role_assignments WHERE id = $1`, target)
			return err
		},
		Audit:  roleAudit("auth.platform_role.revoked", "role_revoked", actor, subject),
		Events: []audit.OutboxEvent{roleEvent(target, "platform_role_assignment.revoked", "revoked")},
	})
	if err != nil {
		return s.writeErr(ctx, "revoke", err)
	}
	return nil
}

// lockWrites takes the store's advisory write lock for the rest of the
// transaction. The runtime role holds no UPDATE on the assignments, so it
// cannot row-lock them; this lock serializes the store's writers instead.
func lockWrites(ctx context.Context, tx audit.TxOps) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, writeLock)
	return err
}

// List is every stored assignment, with Active on the database clock.
func (s *Store) List(ctx context.Context) ([]Assignment, error) {
	rows, err := s.pool.Query(ctx, `SELECT r.id, r.principal_id, r.role_key, r.granted_by, r.expires_at, `+activeSQL+`
FROM `+s.q+`.platform_role_assignments r JOIN `+s.q+`.principals p ON p.id = r.principal_id
ORDER BY r.principal_id, r.role_key`)
	if err != nil {
		return nil, s.storeErr(ctx, "list", err)
	}
	defer rows.Close()
	var out []Assignment
	for rows.Next() {
		var a Assignment
		if err := rows.Scan(&a.ID, &a.PrincipalID, &a.Role, &a.GrantedBy, &a.ExpiresAt, &a.Active); err != nil {
			return nil, s.storeErr(ctx, "list", err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, s.storeErr(ctx, "list", err)
	}
	return out, nil
}

var refusals = []error{ErrGranterNotHeld, ErrPrincipalMissing, ErrNotHumanUser, ErrPrincipalOff,
	ErrExpiryNotFuture, ErrAlreadyHeld, ErrNotHeld, ErrLastPlatformAdmin}

// writeErr returns a refusal as itself and any other failure as ErrStore.
func (s *Store) writeErr(ctx context.Context, op string, err error) error {
	for _, refusal := range refusals {
		if errors.Is(err, refusal) {
			return refusal
		}
	}
	return s.storeErr(ctx, op, err)
}

func (s *Store) storeErr(ctx context.Context, op string, err error) error {
	s.logger.ErrorContext(ctx, "platformrole: store failure", "op", op, logging.ErrorAttr(err))
	return fmt.Errorf("%w: %s: %w", ErrStore, op, err)
}

func roleEvent(assignment uuid.UUID, verb, status string) audit.OutboxEvent {
	return audit.OutboxEvent{AggregateType: "platform_role_assignment",
		AggregateID: provisioning.WireID("platform_role_assignment", assignment.String()),
		EventType:   verb, Change: map[string]any{"status": status}}
}

func roleAudit(eventType, reason, actor, subject string) audit.AuditEvent {
	return audit.AuditEvent{
		EventType: eventType, Outcome: audit.OutcomeAllowed, ActorPrincipalID: &actor, SubjectPrincipalID: &subject,
		Attributes: map[string]any{"reason_code": reason, "target_kind": "principal", "target_id": subject},
	}
}

package identity

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// ErrInvalidPasswordHash reports a password verifier that is not a bcrypt
// hash. Only a verifier is ever stored, never a password.
var ErrInvalidPasswordHash = errors.New("identity: the password hash is not a bcrypt hash")

const passwordAlgorithm = "bcrypt"

// LocalProvisionRequest is a new local (email and password) user. A
// self-service registration passes Actor: SelfSignUp(requestID).
type LocalProvisionRequest struct {
	Email        string
	FullName     string
	PasswordHash *string
	Actor        Actor
}

// ProvisionLocal creates a user principal and its auth.users row in one
// audit.Commit (reason principal_provisioned). It writes the auth schema only
// and is available only after the users cutover (ErrSignupNotAvailable
// before). An email another principal holds is ErrEmailTaken. The email is
// stored as its login key (NormalizeEmail), as the go-api register route
// stores it.
func (s *Store) ProvisionLocal(ctx context.Context, req LocalProvisionRequest) (_ uuid.UUID, err error) {
	defer func() { s.countLink(ctx, "provision", err) }()
	if !s.ownsUsers {
		return uuid.Nil, ErrSignupNotAvailable
	}
	email, err := NormalizeEmail(req.Email)
	if err != nil {
		return uuid.Nil, err
	}
	var algorithm *string
	if req.PasswordHash != nil {
		if !validBcrypt(*req.PasswordHash) {
			return uuid.Nil, ErrInvalidPasswordHash
		}
		a := passwordAlgorithm
		algorithm = &a
	}
	principal := uuid.New()
	subject := principal.String()
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			// The 0013 create guard counts these two rows as the state change.
			if _, err := tx.Exec(ctx, `INSERT INTO `+s.q+`.principals (id, kind, display_name) VALUES ($1, 'user', $2)`,
				principal, req.FullName); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO `+s.q+`.users
	(principal_id, email, email_lower, password_hash, password_algorithm, password_updated_at)
VALUES ($1, $2, $2, $3, $4, CASE WHEN $3::text IS NULL THEN NULL ELSE clock_timestamp() END)`,
				principal, email, req.PasswordHash, algorithm)
			return err
		},
		Audit:  localAudit("auth.principal.provisioned", "principal_provisioned", subject, req.Actor),
		Events: []audit.OutboxEvent{principalEvent(subject, "principal.created", true), userEvent(subject, "user.created")},
	})
	switch {
	case err == nil:
	case isEmailCollision(err):
		return uuid.Nil, ErrEmailTaken
	default:
		return uuid.Nil, s.storeErr(ctx, "provision local user", err)
	}
	s.logger.InfoContext(ctx, "identity: local principal provisioned")
	return principal, nil
}

// SetEmailVerified sets auth.users.email_verified_at (a guarded column) in one
// audit.Commit (reason email_verified). Setting the value it already holds
// writes nothing. Available only after the users cutover.
func (s *Store) SetEmailVerified(ctx context.Context, principal uuid.UUID, at time.Time, actor Actor) (err error) {
	defer func() { s.countLink(ctx, "verify_email", err) }()
	if !s.ownsUsers {
		return ErrSignupNotAvailable
	}
	subject := principal.String()
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if err := s.lockUser(ctx, tx, principal); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `UPDATE `+s.q+`.users SET email_verified_at = $2, updated_at = clock_timestamp()
WHERE principal_id = $1 AND email_verified_at IS DISTINCT FROM $2`, principal, at.UTC())
			return err
		},
		Audit:  withChangedFields(localAudit("auth.user.email_verified", "email_verified", subject, actor), "email_verified_at"),
		Events: []audit.OutboxEvent{userEvent(subject, "user.email_verified")},
	})
	return s.localErr(ctx, "set email verified", err)
}

// SetPasswordHash sets auth.users.password_hash (a guarded column) to a bcrypt
// verifier, with password_algorithm bcrypt, in one audit.Commit (reason
// password_changed). Available only after the users cutover.
func (s *Store) SetPasswordHash(ctx context.Context, principal uuid.UUID, hash string, actor Actor) error {
	return s.SetPasswordHashWith(ctx, principal, hash, actor, nil)
}

// SetPasswordHashWith is SetPasswordHash with end, when not nil, run in the
// same transaction after the password is written: an error from end rolls the
// password back with everything else.
func (s *Store) SetPasswordHashWith(ctx context.Context, principal uuid.UUID, hash string, actor Actor, end func(context.Context, audit.TxOps) error) (err error) {
	defer func() { s.countLink(ctx, "set_password", err) }()
	if !s.ownsUsers {
		return ErrSignupNotAvailable
	}
	if !validBcrypt(hash) {
		return ErrInvalidPasswordHash
	}
	subject := principal.String()
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if err := s.lockUser(ctx, tx, principal); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE `+s.q+`.users
SET password_hash = $2, password_algorithm = $3, password_updated_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE principal_id = $1`, principal, hash, passwordAlgorithm); err != nil {
				return err
			}
			if end != nil {
				return end(ctx, tx)
			}
			return nil
		},
		Audit:  localAudit("auth.user.password_changed", "password_changed", subject, actor),
		Events: []audit.OutboxEvent{userEvent(subject, "user.credentials_changed")},
	})
	return s.localErr(ctx, "set password hash", err)
}

func (s *Store) lockUser(ctx context.Context, tx audit.TxOps, principal uuid.UUID) error {
	var locked uuid.UUID
	err := tx.QueryRow(ctx, `SELECT principal_id FROM `+s.q+`.users WHERE principal_id = $1 FOR UPDATE`, principal).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrUserNotFound
	}
	return err
}

func (s *Store) localErr(ctx context.Context, op string, err error) error {
	switch {
	case err == nil, errors.Is(err, audit.ErrNoStateChange):
		return nil
	case errors.Is(err, ErrUserNotFound):
		return ErrUserNotFound
	}
	return s.storeErr(ctx, op, err)
}

func validBcrypt(hash string) bool {
	_, err := bcrypt.Cost([]byte(hash))
	return err == nil
}

func localAudit(eventType, reason, subject string, actor Actor) audit.AuditEvent {
	event := audit.AuditEvent{
		EventType: eventType, Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &subject, RequestID: actor.RequestID,
		Attributes: map[string]any{"reason_code": reason, "target_kind": "principal", "target_id": subject},
	}
	switch {
	case actor.self:
		a := subject
		event.ActorPrincipalID = &a
	case actor.PrincipalID != nil:
		a := actor.PrincipalID.String()
		event.ActorPrincipalID = &a
	}
	return event
}

func withChangedFields(event audit.AuditEvent, fields ...string) audit.AuditEvent {
	event.Attributes["changed_fields"] = fields
	return event
}

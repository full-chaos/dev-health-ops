package identity

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/publicaudit"
)

// ErrInvalidPasswordHash reports a password verifier that is not a bcrypt
// hash. Only a verifier is ever stored, never a password.
var ErrInvalidPasswordHash = errors.New("identity: the password hash is not a bcrypt hash")

const passwordAlgorithm = "bcrypt"

// LocalProvisionRequest is a new local (email and password) user. A
// self-service registration passes Actor: SelfSignUp(requestID).
type LocalProvisionRequest struct {
	Email string
	// FullName is stored as given in public.users (nil is NULL, "" is '');
	// the principal display name is "" for nil.
	FullName     *string
	PasswordHash *string
	Actor        Actor
	// Organization, when set, adds a new public organization named so and the
	// user's owner membership in the same transaction (go-api's register does
	// the same). Only new rows are written.
	Organization *NewOrganization
}

// NewOrganization is the organization a sign-up creates.
type NewOrganization struct{ Name string }

// ProvisionLocal creates a user principal and its auth.users row in one
// audit.Commit (reason principal_provisioned). It writes the auth schema only
// and is available only after the users cutover (ErrSignupNotAvailable
// before). An email another principal holds is ErrEmailTaken. The email is
// stored as its login key (NormalizeEmail), as the go-api register route
// stores it. With req.Organization the public organization and the owner
// membership are written in the same transaction, so an email collision leaves
// neither.
func (s *Store) ProvisionLocal(ctx context.Context, req LocalProvisionRequest) (_ uuid.UUID, err error) {
	defer func() { s.countLink(ctx, "provision", err) }()
	if !s.ownsUsers {
		return uuid.Nil, ErrSignupNotAvailable
	}
	email, err := NormalizeLocalRegisterEmail(req.Email)
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
				principal, derefOrEmpty(req.FullName)); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO `+s.q+`.users
	(principal_id, email, email_lower, password_hash, password_algorithm, password_updated_at)
VALUES ($1, $2, $2, $3, $4, CASE WHEN $3::text IS NULL THEN NULL ELSE clock_timestamp() END)`,
				principal, email, req.PasswordHash, algorithm); err != nil {
				return err
			}
			provider := "local"
			if err := insertPublicUser(ctx, tx, publicUser{ID: principal, Email: email, PasswordHash: req.PasswordHash, FullName: req.FullName, AuthProvider: provider}); err != nil {
				return err
			}
			if req.Organization == nil {
				return nil
			}
			org, err := insertPublicOwnedOrganization(ctx, tx, principal, req.Organization.Name)
			if err != nil {
				return err
			}
			// go-api's register row (internal/api/session register.go:167), after the organization it
			// references (audit_logs_org_id_fkey is not deferrable), in the same transaction (CHAOS-11400).
			changes := pyjson.NewObject()
			changes.Set("email", email)
			changes.Set("organization_id", org.String())
			encoded, err := publicaudit.Changes(changes)
			if err != nil {
				return err
			}
			return publicaudit.UserRow(ctx, tx, org, principal, "create", "User registered", encoded)
		},
		Audit:  provisionAudit(subject, req.Actor, req.Organization != nil),
		Events: []audit.OutboxEvent{principalEvent(subject, "principal.created", true), userEvent(subject, "user.created")},
	})
	switch {
	case err == nil:
	case isEmailCollision(err), isPublicEmailCollision(err):
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
func (s *Store) SetEmailVerified(ctx context.Context, principal uuid.UUID, at time.Time, actor Actor) error {
	return s.SetEmailVerifiedWith(ctx, principal, at, actor, nil)
}

// SetEmailVerifiedWith is SetEmailVerified with end, when not nil, run in the
// same transaction after the address is marked verified: an error from end
// rolls the verification back with everything else, and the transaction always
// holds a guarded write, so its one audit row is written.
func (s *Store) SetEmailVerifiedWith(ctx context.Context, principal uuid.UUID, at time.Time, actor Actor, end func(context.Context, audit.TxOps) error) (err error) {
	defer func() { s.countLink(ctx, "verify_email", err) }()
	if !s.ownsUsers {
		return ErrSignupNotAvailable
	}
	org, err := AuditOrganization(ctx, s.pool, s.q, principal)
	if err != nil {
		return s.storeErr(ctx, "read the membership of a verified user", err)
	}
	subject := principal.String()
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if err := s.lockUser(ctx, tx, principal); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `UPDATE `+s.q+`.users SET email_verified_at = $2, updated_at = clock_timestamp()
WHERE principal_id = $1 AND email_verified_at IS DISTINCT FROM $2`, principal, at.UTC())
			if err != nil {
				return err
			}
			if err := updatePublicVerified(ctx, tx, principal); err != nil {
				return err
			}
			if end != nil {
				return end(ctx, tx)
			}
			return nil
		},
		Audit:    verifiedAudit(subject, actor, org),
		Events:   []audit.OutboxEvent{userEvent(subject, "user.email_verified")},
		Refusals: []error{ErrLinkUnusable},
	})
	return s.localErr(ctx, "set email verified", err)
}

// SetPasswordHash sets auth.users.password_hash (a guarded column) to a bcrypt
// verifier, with password_algorithm bcrypt, in one audit.Commit (reason
// password_changed) and adds 1 to legacy_token_version, as the public.users
// token_version gets (a go-api token minted before it ends and the users
// reconciliation stays at zero diff). Available only after the users cutover.
func (s *Store) SetPasswordHash(ctx context.Context, principal uuid.UUID, hash string, actor Actor) error {
	return s.SetPasswordHashWith(ctx, principal, hash, actor, nil)
}

// SetPasswordHashWith is SetPasswordHash with end, when not nil, run in the
// same transaction after the password is written: an error from end rolls the
// password back with everything else.
func (s *Store) SetPasswordHashWith(ctx context.Context, principal uuid.UUID, hash string, actor Actor, end func(context.Context, audit.TxOps) error) error {
	_, err := s.SetPasswordHashFor(ctx, principal, hash, actor, PasswordChanged, end)
	return err
}

// ErrLinkUnusable is what an end callback returns when the one-time link it
// meant to consume is no longer usable: the transaction rolls back, the
// password is unchanged, and the store reports it without logging a failure.
var ErrLinkUnusable = errors.New("identity: the one-time link is unusable")

// PasswordCause names why a password was written; it picks the audit event
// type and reason.
type PasswordCause struct{ eventType, reason string }

var (
	// PasswordChanged is a signed-in user's own change.
	PasswordChanged = PasswordCause{"auth.user.password_changed", "password_changed"}
	// PasswordReset is a reset by a one-time link.
	PasswordReset = PasswordCause{"auth.password.reset", "password_reset"}
)

// SetPasswordHashFor is SetPasswordHashWith with the audit event type and
// reason of cause. It returns the new public.users.token_version.
func (s *Store) SetPasswordHashFor(ctx context.Context, principal uuid.UUID, hash string, actor Actor, cause PasswordCause, end func(context.Context, audit.TxOps) error) (version int64, err error) {
	defer func() { s.countLink(ctx, "set_password", err) }()
	if !s.ownsUsers {
		return 0, ErrSignupNotAvailable
	}
	if !validBcrypt(hash) {
		return 0, ErrInvalidPasswordHash
	}
	org, err := AuditOrganization(ctx, s.pool, s.q, principal)
	if err != nil {
		return 0, s.storeErr(ctx, "read the membership of a password writer", err)
	}
	subject := principal.String()
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if err := s.lockUser(ctx, tx, principal); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE `+s.q+`.users
SET password_hash = $2, password_algorithm = $3, password_updated_at = clock_timestamp(),
    legacy_token_version = legacy_token_version + 1, updated_at = clock_timestamp()
WHERE principal_id = $1`, principal, hash, passwordAlgorithm); err != nil {
				return err
			}
			var err error
			if version, err = updatePublicPassword(ctx, tx, principal, hash); err != nil {
				return err
			}
			if end != nil {
				return end(ctx, tx)
			}
			return nil
		},
		Audit:    passwordAudit(cause, subject, actor, org),
		Events:   []audit.OutboxEvent{userEvent(subject, "user.credentials_changed")},
		Refusals: []error{ErrLinkUnusable},
	})
	return version, s.localErr(ctx, "set password hash", err)
}

func (s *Store) lockUser(ctx context.Context, tx audit.TxOps, principal uuid.UUID) error {
	// Lock order step 1 (principals) before any public.users row: a membership writer holds principals and then waits for public.users.
	var key uuid.UUID
	err := tx.QueryRow(ctx, `SELECT id FROM `+s.q+`.principals WHERE id = $1 FOR KEY SHARE`, principal).Scan(&key)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrUserNotFound
	}
	if err != nil {
		return err
	}
	var locked uuid.UUID
	err = tx.QueryRow(ctx, `SELECT principal_id FROM `+s.q+`.users WHERE principal_id = $1 FOR UPDATE`, principal).Scan(&locked)
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
	case errors.Is(err, ErrLinkUnusable):
		return ErrLinkUnusable
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

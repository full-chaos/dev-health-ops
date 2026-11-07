package identity

import (
	"context"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// CLIUserRequest is a verified local user an operator creates with
// `dho auth users-create`. It names no organization and no membership.
type CLIUserRequest struct {
	Email    string
	FullName *string
	// PasswordHash is a bcrypt verifier; the caller has applied the password policy.
	PasswordHash string
}

// ProvisionCLIUser creates a verified local user in one audit.Commit: the
// principal, its auth.users row (email verified), and the public.users row
// (is_verified). The audit row is auth.principal.provisioned, reason
// principal_provisioned, source operator-cli. It writes no organization, no
// membership and no platform role. Available only while auth owns users
// (ErrSignupNotAvailable otherwise). An email another principal holds is
// ErrEmailTaken.
func (s *Store) ProvisionCLIUser(ctx context.Context, req CLIUserRequest) (_ uuid.UUID, err error) {
	defer func() { s.countLink(ctx, "provision", err) }()
	if !s.ownsUsers {
		return uuid.Nil, ErrSignupNotAvailable
	}
	email, err := provisionEmail(ctx, req.Email)
	if err != nil {
		return uuid.Nil, err
	}
	if !validBcrypt(req.PasswordHash) {
		return uuid.Nil, ErrInvalidPasswordHash
	}
	hash := req.PasswordHash
	algorithm := passwordAlgorithm
	principal := uuid.New()
	subject := principal.String()
	event := withChangedFields(localAudit("auth.principal.provisioned", "principal_provisioned", subject, Actor{}), "email", "email_verified_at")
	event.Attributes["source"] = audit.SourceOperatorCLI
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if _, err := tx.Exec(ctx, `INSERT INTO `+s.q+`.principals (id, kind, display_name) VALUES ($1, 'user', $2)`,
				principal, derefOrEmpty(req.FullName)); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO `+s.q+`.users
	(principal_id, email, email_lower, email_verified_at, password_hash, password_algorithm, password_updated_at)
VALUES ($1, $2, $2, clock_timestamp(), $3, $4, clock_timestamp())`,
				principal, email, hash, algorithm); err != nil {
				return err
			}
			return insertPublicUser(ctx, tx, publicUser{ID: principal, Email: email, PasswordHash: &hash, FullName: req.FullName, AuthProvider: "local", Verified: true})
		},
		Audit: event,
		Events: []audit.OutboxEvent{
			principalEvent(subject, "principal.created", true), userEvent(subject, "user.created"), userEvent(subject, "user.email_verified"),
		},
	})
	switch {
	case err == nil:
	case isEmailCollision(err), isPublicEmailCollision(err):
		return uuid.Nil, ErrEmailTaken
	default:
		return uuid.Nil, s.storeErr(ctx, "provision cli user", err)
	}
	s.logger.InfoContext(ctx, "identity: cli user provisioned")
	return principal, nil
}

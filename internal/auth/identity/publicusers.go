package identity

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// ErrLegacyProjection reports a user creation that cannot be written to
// public.users with the columns go-api's own create paths write, so the
// auth.users rows are not written either.
var ErrLegacyProjection = errors.New("identity: the user cannot be projected to public.users")

// publicUser is the row go-api's create paths (register, social login, SSO
// account creation) write to public.users. is_active is true, is_superuser
// false and token_version 0 in all of them; created_at and updated_at are one
// instant. username and full_name are NULL when empty, as the providers'
// absent values are.
type publicUser struct {
	ID             uuid.UUID
	Email          string
	Username       string
	PasswordHash   *string
	FullName       string
	AuthProvider   string
	AuthProviderID *string
	Verified       bool
}

// insertPublicUser writes the public.users row inside the audit.Commit that
// writes the auth rows, so either both exist or neither does. The row carries
// exactly authschema.UsersWriteGrant's insert columns.
func insertPublicUser(ctx context.Context, tx audit.TxOps, u publicUser) error {
	_, err := tx.Exec(ctx, `INSERT INTO public.users
	(id, email, username, password_hash, full_name, auth_provider, auth_provider_id,
	 is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULLIF($3, ''), $4, NULLIF($5, ''), $6, $7, true, $8, false, 0, clock_timestamp(), clock_timestamp())`,
		u.ID, u.Email, u.Username, u.PasswordHash, u.FullName, u.AuthProvider, u.AuthProviderID, u.Verified)
	return err
}

// updatePublicPassword copies a new password verifier to public.users. It
// never writes token_version: the flow that ends the user's sessions owns that
// bump. A user with no public row fails the transaction.
func updatePublicPassword(ctx context.Context, tx audit.TxOps, id uuid.UUID, hash string) error {
	tag, err := tx.Exec(ctx, `UPDATE public.users SET password_hash = $2, updated_at = clock_timestamp() WHERE id = $1`, id, hash)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLegacyProjection
	}
	return nil
}

// updatePublicVerified marks the public.users row verified.
func updatePublicVerified(ctx context.Context, tx audit.TxOps, id uuid.UUID) error {
	tag, err := tx.Exec(ctx, `UPDATE public.users SET is_verified = true, updated_at = clock_timestamp() WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLegacyProjection
	}
	return nil
}

// isPublicEmailCollision is a unique violation on a public.users index: the
// address is held by a go-api user ("Email already registered").
func isPublicEmailCollision(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.SchemaName == "public" && pgErr.TableName == "users"
}

// federatedPublicUser maps a federated provisioning to its public.users row.
// A social provider's row carries the provider name and the provider's subject;
// an SSO provider's key is a provider row id, so the caller names the
// users.auth_provider value and the stored subject, and a request without them
// is refused.
func federatedPublicUser(id uuid.UUID, emailLower string, req ProvisionRequest) (publicUser, error) {
	u := publicUser{ID: id, Email: emailLower, Username: req.Username, FullName: req.LegacyFullName, Verified: true}
	switch {
	case req.Key.Provider.IsSocial():
		u.AuthProvider = string(req.Key.Provider)
		subject := req.Key.Subject
		u.AuthProviderID = &subject
	case req.LegacyProvider != "" && req.LegacySubject != "":
		u.AuthProvider = req.LegacyProvider
		subject := req.LegacySubject
		u.AuthProviderID = &subject
	default:
		return publicUser{}, ErrLegacyProjection
	}
	return u, nil
}

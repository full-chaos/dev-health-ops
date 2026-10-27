package identity

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/registerorg"
	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// ErrLegacyProjection reports a user creation that cannot be written to
// public.users with the columns go-api's own create paths write, so the
// auth.users rows are not written either.
var ErrLegacyProjection = errors.New("identity: the user cannot be projected to public.users")

// publicUser is the row go-api's create paths (register, social login, SSO
// account creation) write to public.users. is_active is true, is_superuser
// false and token_version 0 in all of them; created_at and updated_at are one
// instant. username is NULL when empty, as the providers'
// absent values are. full_name is stored as given: nil is NULL, "" is ”.
type publicUser struct {
	ID             uuid.UUID
	Email          string
	Username       string
	PasswordHash   *string
	FullName       *string
	AuthProvider   string
	AuthProviderID *string
	Verified       bool
	// SSOProviderID is the provider row an OIDC or SAML account belongs to
	// (go-api writes it since ops revision 0155); nil for every other account.
	SSOProviderID *uuid.UUID
}

// insertPublicUser writes the public.users row inside the audit.Commit that
// writes the auth rows, so either both exist or neither does. The row carries
// exactly authschema.UsersWriteGrant's insert columns. An OIDC or SAML row is
// bound only to a provider row of its own protocol: any other provider row, or
// none, is ErrLegacyProjection and nothing is written.
func insertPublicUser(ctx context.Context, tx audit.TxOps, u publicUser) error {
	if u.SSOProviderID != nil {
		var protocol string
		err := tx.QueryRow(ctx, `SELECT protocol FROM public.sso_providers WHERE id = $1`, *u.SSOProviderID).Scan(&protocol)
		if errors.Is(err, pgx.ErrNoRows) || err == nil && protocol != u.AuthProvider {
			return ErrLegacyProjection
		}
		if err != nil {
			return err
		}
	}
	_, err := tx.Exec(ctx, `INSERT INTO public.users
	(id, email, username, password_hash, full_name, auth_provider, auth_provider_id,
	 is_active, is_verified, is_superuser, token_version, created_at, updated_at, sso_provider_id)
VALUES ($1, $2, NULLIF($3, ''), $4, $5, $6, $7, true, $8, false, 0, now(), now(), $9)`,
		u.ID, u.Email, u.Username, u.PasswordHash, u.FullName, u.AuthProvider, u.AuthProviderID, u.Verified, u.SSOProviderID)
	return err
}

// insertPublicOwnedOrganization writes the organization and the owner
// membership go-api's register route writes: new rows only, with the columns
// authschema.RegisterOrganizationGrants lists and the model defaults go-api's
// insert gives the others.
func insertPublicOwnedOrganization(ctx context.Context, tx audit.TxOps, userID uuid.UUID, name string) error {
	orgID := uuid.New()
	if _, err := tx.Exec(ctx, `INSERT INTO public.organizations
	(id, slug, name, settings, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $3, '{}', 'community', 'stripe', true, now(), now())`,
		orgID, registerorg.Slug(name, userID), name); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO public.memberships
	(id, user_id, org_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'owner', now(), now(), now())`, uuid.New(), userID, orgID)
	return err
}

// updatePublicPassword copies a new password verifier to public.users and adds
// 1 to token_version in the same statement, as go-api's own password change
// does, so a go-api token minted before it is refused. A user with no public
// row fails the transaction.
func updatePublicPassword(ctx context.Context, tx audit.TxOps, id uuid.UUID, hash string) (int64, error) {
	var version int64
	err := tx.QueryRow(ctx, `UPDATE public.users SET password_hash = $2, token_version = token_version + 1, updated_at = now() WHERE id = $1 RETURNING token_version`, id, hash).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrLegacyProjection
	}
	return version, err
}

// updatePublicVerified marks the public.users row verified.
func updatePublicVerified(ctx context.Context, tx audit.TxOps, id uuid.UUID) error {
	tag, err := tx.Exec(ctx, `UPDATE public.users SET is_verified = true, updated_at = now() WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLegacyProjection
	}
	return nil
}

// publicSSOIdentityIndex is the ops index of an OIDC or SAML account's key
// (sso_provider_id, auth_provider_id), revision 0155.
const publicSSOIdentityIndex = "ix_users_sso_identity"

// isPublicEmailCollision is a unique violation on a public.users index other
// than publicSSOIdentityIndex: the address is held by a go-api user ("Email
// already registered").
func isPublicEmailCollision(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.SchemaName == "public" && pgErr.TableName == "users" &&
		pgErr.ConstraintName != publicSSOIdentityIndex
}

// isPublicSSOIdentityCollision is a unique violation on publicSSOIdentityIndex:
// a go-api account already holds the subject at the provider row.
func isPublicSSOIdentityCollision(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.SchemaName == "public" && pgErr.TableName == "users" &&
		pgErr.ConstraintName == publicSSOIdentityIndex
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
		if isSSORelinkProtocol(req.LegacyProvider) {
			provider, err := uuid.Parse(string(req.Key.Provider))
			if err != nil {
				return publicUser{}, ErrLegacyProjection
			}
			u.SSOProviderID = &provider
		}
	default:
		return publicUser{}, ErrLegacyProjection
	}
	if RefuseInvalidProviderID(u.AuthProvider, u.AuthProviderID) != nil {
		return publicUser{}, ErrLegacyProjection
	}
	return u, nil
}

func derefOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

package sso

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// provisionedUser is the fields oidcCallback's SSOLoginResponse and access
// token need from users.
type provisionedUser struct {
	id                 uuid.UUID
	email              string
	username, fullName *string
	isSuperuser        bool
	tokenVersion       int64
}

// provisionedMembership is the fields oidcCallback's response and access
// token need from memberships.
type provisionedMembership struct {
	orgID uuid.UUID
	role  string
}

func stringOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// provisionOrGetUser is SSOService.provision_or_get_user (sso.py:674-774).
//
// Python's own function re-fetches the provider and re-runs the
// allowed_domains check the caller (oidcCallback) already ran with the
// SAME provider row and the SAME allowed_domains list; both reads are
// necessarily identical within one request (no write to sso_providers
// happens in between), so this port runs that check once, in oidcCallback,
// and does not repeat it here -- an accepted simplification, not a
// behavioral difference (the Python duplicate can never actually
// disagree).
//
// Existing accounts are selected by (auth_provider, sso provider, auth_provider_id)
// and only full_name, last_login_at and updated_at are written, plus the SAML NameID
// format and SPNameQualifier of a SAML sign-in (saml_nameid_format, saml_sp_name_qualifier);
// the identity columns are never rewritten (resolveProviderIdentity).
func (h handlers) provisionOrGetUser(ctx context.Context, row *providerRow, email, name string, providerID uuid.UUID, externalID string, saml *samlNameIDParts) (*provisionedUser, *provisionedMembership, error) {
	authProviderValue := "oidc"
	if row.Protocol == "saml" {
		authProviderValue = "saml"
	}

	var (
		id                         uuid.UUID
		existingEmail              string
		username, existingFullName *string
		isSuperuser                bool
		tokenVersion               int64
	)
	userID, found, err := h.resolveProviderIdentity(ctx, authProviderValue, providerID, externalID, email)
	if err != nil {
		return nil, nil, err
	}
	err = pgx.ErrNoRows
	if found {
		err = h.Pool.QueryRow(ctx, `SELECT id, email, username, full_name, is_superuser, token_version
FROM users WHERE id = $1::uuid`, userID).Scan(&id, &existingEmail, &username, &existingFullName,
			&isSuperuser, &tokenVersion)
	}

	now := h.Now().UTC()
	if errors.Is(err, pgx.ErrNoRows) {
		if !row.AutoProvision {
			return nil, nil, ssoErr("User not found and auto-provisioning disabled")
		}
		var fullNamePtr, externalIDPtr *string
		if name != "" {
			fullNamePtr = &name
		}
		if externalID != "" {
			externalIDPtr = &externalID
		}
		return h.createAccountAndMembership(ctx, row, ssoNewAccount{
			email: email, authProvider: authProviderValue, externalID: externalIDPtr, fullName: fullNamePtr,
			ssoProviderID: &providerID, saml: saml,
		})
	}
	if err != nil {
		return nil, nil, err
	}

	newFullName := existingFullName
	if name != "" && stringOrEmpty(existingFullName) == "" {
		newFullName = &name
	}
	// While auth-service owns users the sign-in goes on and writes no public.users row. The two SAML NameID
	// columns ride in this one UPDATE: its own row lock covers them, and no other column is touched. An OIDC
	// sign-in passes NULL and keeps both as they are. The UPDATE runs inside membershipOrJoin, in the join's
	// transaction when the account joins.
	var update func(ctx context.Context, db dbExecer) error
	if h.refuseWhileUsersOwned() == nil {
		update = func(ctx context.Context, db dbExecer) error {
			_, err := db.Exec(ctx, `UPDATE users SET full_name = $2, last_login_at = $3, updated_at = $3,
	saml_nameid_format = COALESCE($4::text, saml_nameid_format),
	saml_sp_name_qualifier = COALESCE($5::text, saml_sp_name_qualifier)
WHERE id = $1::uuid`,
				id, newFullName, now, saml.formatArg(), saml.qualifierArg())
			return err
		}
	}
	membership, err := h.membershipOrJoin(ctx, row, id, update)
	if err != nil {
		return nil, nil, err
	}

	return &provisionedUser{id: id, email: existingEmail, username: username, fullName: newFullName,
		isSuperuser: isSuperuser, tokenVersion: tokenVersion}, membership, nil
}

// errIdentityConflict: the email belongs to an account with another or no SSO
// identity, or the identity key matches more than one account.
var errIdentityConflict = errors.New("identity conflict")

// resolveProviderIdentity selects an OIDC or SAML account by its (auth_provider,
// sso_provider_id, auth_provider_id) key: the subject is an identity of the provider
// that issued it, so the same subject at another provider selects nothing. Without a
// key hit, an existing account with the same email is a conflict; it is never
// adopted or rewritten. An account that holds a subject but no provider is no
// provider's identity.
func (h handlers) resolveProviderIdentity(ctx context.Context, authProvider string, providerID uuid.UUID, externalID, email string) (uuid.UUID, bool, error) {
	return h.resolveIdentityKey(ctx, `SELECT id FROM users WHERE auth_provider = $1::text AND sso_provider_id = $3::uuid AND auth_provider_id = $2::text LIMIT 2`,
		[]any{authProvider, externalID, providerID}, externalID, email)
}

// resolveSSOIdentity selects an OAuth account by its (auth_provider,
// auth_provider_id) key. OAuth providers are the public identity providers
// (github, gitlab, google): their subject is global per type, so the key carries no
// SSO provider. Without a key hit, an existing account with the same email is a
// conflict; it is never adopted or rewritten.
func (h handlers) resolveSSOIdentity(ctx context.Context, authProvider, externalID, email string) (uuid.UUID, bool, error) {
	return h.resolveIdentityKey(ctx, `SELECT id FROM users WHERE auth_provider = $1::text AND auth_provider_id = $2::text LIMIT 2`,
		[]any{authProvider, externalID}, externalID, email)
}

func (h handlers) resolveIdentityKey(ctx context.Context, keyQuery string, keyArgs []any, externalID, email string) (uuid.UUID, bool, error) {
	if externalID != "" {
		rows, err := h.Pool.Query(ctx, keyQuery, keyArgs...)
		if err != nil {
			return uuid.Nil, false, err
		}
		defer rows.Close()
		var ids []uuid.UUID
		for rows.Next() {
			var id uuid.UUID
			if err := rows.Scan(&id); err != nil {
				return uuid.Nil, false, err
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			return uuid.Nil, false, err
		}
		switch len(ids) {
		case 1:
			return ids[0], true, nil
		case 0:
		default:
			return uuid.Nil, false, errIdentityConflict
		}
	}
	var taken bool
	if err := h.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM users WHERE lower(email) = lower($1::text))`, email).Scan(&taken); err != nil {
		return uuid.Nil, false, err
	}
	if taken {
		return uuid.Nil, false, errIdentityConflict
	}
	return uuid.Nil, false, nil
}

// invalidProviderIDDetail is the 400 body of a sign-in whose identity provider returned no usable user id; it names
// no value.
func invalidProviderIDDetail() *pyjson.Object {
	detail := pyjson.NewObject()
	detail.Set("message", "The identity provider returned an unusable user id")
	detail.Set("reason", "invalid_provider_id")
	return detail
}

func identityConflictDetail() *pyjson.Object {
	detail := pyjson.NewObject()
	detail.Set("message", "This account is bound to another sign-in identity")
	detail.Set("reason", "identity_conflict")
	return detail
}

// errMembershipRequired: the account has no membership in the provider's
// organization; an organization's SSO never issues a session without one.
var errMembershipRequired = errors.New("membership required")

func (h handlers) requireMembership(ctx context.Context, userID uuid.UUID, orgID string) (*provisionedMembership, error) {
	providerOrgID, err := pythonparity.ParseUUID(orgID)
	if err != nil {
		return nil, err
	}
	var membershipOrgID uuid.UUID
	var role *string
	err = h.Pool.QueryRow(ctx, `SELECT org_id, role FROM memberships WHERE user_id = $1::uuid AND org_id = $2::uuid`,
		userID, providerOrgID).Scan(&membershipOrgID, &role)
	switch {
	case err == nil:
		roleText := "member"
		if role != nil {
			roleText = *role
		}
		return &provisionedMembership{orgID: membershipOrgID, role: roleText}, nil
	case errors.Is(err, pgx.ErrNoRows):
		return nil, errMembershipRequired
	default:
		return nil, err
	}
}

func membershipRequiredDetail() *pyjson.Object {
	detail := pyjson.NewObject()
	detail.Set("message", "A membership in this organization is required")
	detail.Set("reason", "membership_required")
	return detail
}

type ssoNewAccount struct {
	email, authProvider                       string
	externalID, fullName, username, avatarURL *string
	// ssoProviderID is the OIDC or SAML provider whose identity the account holds; nil for OAuth.
	ssoProviderID *uuid.UUID
	// saml carries the NameID Format and SPNameQualifier of a SAML sign-up; nil for OIDC and OAuth.
	saml *samlNameIDParts
}

// samlNameIDParts are the NameID attributes the identity key needs besides the NameID value. An empty string
// is stored as an empty string: the assertion carried none, which is not the same as never captured (NULL).
// The values are never logged.
type samlNameIDParts struct{ format, spNameQualifier string }

func (p *samlNameIDParts) formatArg() *string {
	if p == nil {
		return nil
	}
	return &p.format
}

func (p *samlNameIDParts) qualifierArg() *string {
	if p == nil {
		return nil
	}
	return &p.spNameQualifier
}

func jitRole(row *providerRow) string {
	if row.DefaultRole == "" {
		return "member"
	}
	return row.DefaultRole
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// createAccountAndMembership writes the account, its identity and its
// membership in the provider's organization in one transaction.
func (h handlers) createAccountAndMembership(ctx context.Context, row *providerRow, account ssoNewAccount) (*provisionedUser, *provisionedMembership, error) {
	if err := h.refuseWhileUsersOwned(); err != nil {
		return nil, nil, err
	}
	if err := identity.RefuseInvalidProviderID(account.authProvider, account.externalID); err != nil {
		return nil, nil, err
	}
	orgID, err := pythonparity.ParseUUID(row.OrgID)
	if err != nil {
		return nil, nil, err
	}
	now := h.Now().UTC()
	newID := uuid.New()
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `INSERT INTO users
	(id, email, username, full_name, avatar_url, auth_provider, auth_provider_id, sso_provider_id, is_active, is_verified, is_superuser,
	 token_version, last_login_at, created_at, updated_at, saml_nameid_format, saml_sp_name_qualifier)
VALUES ($1, $2, $3, $4, $5, $6, $7, $9, true, true, false, 0, $8, $8, $8, $10, $11)`,
		newID, account.email, account.username, account.fullName, account.avatarURL, account.authProvider, account.externalID, now, account.ssoProviderID,
		account.saml.formatArg(), account.saml.qualifierArg()); err != nil {
		if isUniqueViolation(err) {
			return nil, nil, errIdentityConflict
		}
		return nil, nil, err
	}
	role := jitRole(row)
	if _, err := tx.Exec(ctx, `INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $5, $5)`, uuid.New(), orgID, newID, role, now); err != nil {
		return nil, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, err
	}
	return &provisionedUser{id: newID, email: account.email, username: account.username, fullName: account.fullName, tokenVersion: 0},
		&provisionedMembership{orgID: orgID, role: role}, nil
}

// dbExecer is the Exec of a pool or a transaction.
type dbExecer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// membershipOrJoin returns the account's membership in the provider's
// organization. With auto-provision on, an account that has no membership in
// any organization joins in one transaction; a member of another organization
// only is refused. update is the sign-in's write to the user row, run once the
// membership is settled: when the account joins it runs in the join's
// transaction, so a failure of the update leaves no membership behind (a
// committed join followed by a failing update made every later sign-in fail
// the same way); otherwise it runs on the pool. nil means no write.
func (h handlers) membershipOrJoin(ctx context.Context, row *providerRow, userID uuid.UUID, update func(ctx context.Context, db dbExecer) error) (*provisionedMembership, error) {
	membership, err := h.requireMembership(ctx, userID, row.OrgID)
	if !errors.Is(err, errMembershipRequired) || !row.AutoProvision {
		if err != nil {
			return nil, err
		}
		if update != nil {
			if err := update(ctx, h.Pool); err != nil {
				return nil, err
			}
		}
		return membership, nil
	}
	orgID, err := pythonparity.ParseUUID(row.OrgID)
	if err != nil {
		return nil, err
	}
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var locked uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM users WHERE id = $1::uuid FOR UPDATE`, userID).Scan(&locked); err != nil {
		return nil, err
	}
	var memberships int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM memberships WHERE user_id = $1::uuid`, userID).Scan(&memberships); err != nil {
		return nil, err
	}
	if memberships > 0 {
		return nil, errMembershipRequired
	}
	role := jitRole(row)
	now := h.Now().UTC()
	if _, err := tx.Exec(ctx, `INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $5, $5)`, uuid.New(), orgID, userID, role, now); err != nil {
		return nil, err
	}
	if update != nil {
		if err := update(ctx, tx); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &provisionedMembership{orgID: orgID, role: role}, nil
}

package sso

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
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
// Existing accounts are selected by (auth_provider, auth_provider_id) and only
// full_name and last_login_at are written; the identity columns are never
// rewritten (resolveSSOIdentity).
func (h handlers) provisionOrGetUser(ctx context.Context, row *providerRow, email, name string, providerID uuid.UUID, externalID string) (*provisionedUser, *provisionedMembership, error) {
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
	userID, found, err := h.resolveSSOIdentity(ctx, authProviderValue, externalID, email)
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
		})
	}
	if err != nil {
		return nil, nil, err
	}

	membership, err := h.membershipOrJoin(ctx, row, id)
	if err != nil {
		return nil, nil, err
	}

	newFullName := existingFullName
	if name != "" && stringOrEmpty(existingFullName) == "" {
		newFullName = &name
	}
	if _, err := h.Pool.Exec(ctx, `UPDATE users SET full_name = $2, last_login_at = $3, updated_at = $3 WHERE id = $1::uuid`,
		id, newFullName, now); err != nil {
		return nil, nil, err
	}

	return &provisionedUser{id: id, email: existingEmail, username: username, fullName: newFullName,
		isSuperuser: isSuperuser, tokenVersion: tokenVersion}, membership, nil
}

// errIdentityConflict: the email belongs to an account with another or no SSO
// identity, or the identity key matches more than one account.
var errIdentityConflict = errors.New("identity conflict")

// resolveSSOIdentity selects the account by its (auth_provider,
// auth_provider_id) key. Without a key hit, an existing account with the same
// email is a conflict; it is never adopted or rewritten.
func (h handlers) resolveSSOIdentity(ctx context.Context, authProvider, externalID, email string) (uuid.UUID, bool, error) {
	if externalID != "" {
		rows, err := h.Pool.Query(ctx, `SELECT id FROM users WHERE auth_provider = $1::text AND auth_provider_id = $2::text LIMIT 2`,
			authProvider, externalID)
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
	(id, email, username, full_name, avatar_url, auth_provider, auth_provider_id, is_active, is_verified, is_superuser,
	 token_version, last_login_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, true, true, false, 0, $8, $8, $8)`,
		newID, account.email, account.username, account.fullName, account.avatarURL, account.authProvider, account.externalID, now); err != nil {
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

// membershipOrJoin returns the account's membership in the provider's
// organization. With auto-provision on, an account that has no membership in
// any organization joins in one transaction; a member of another organization
// only is refused.
func (h handlers) membershipOrJoin(ctx context.Context, row *providerRow, userID uuid.UUID) (*provisionedMembership, error) {
	membership, err := h.requireMembership(ctx, userID, row.OrgID)
	if !errors.Is(err, errMembershipRequired) || !row.AutoProvision {
		return membership, err
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
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &provisionedMembership{orgID: orgID, role: role}, nil
}

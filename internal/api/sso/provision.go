package sso

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

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
// Every attribute Python updates via SQLAlchemy setattr + a single flush
// (auth_provider, auth_provider_id, full_name, last_login_at) is written
// here as one UPDATE with the same precedence, whether or not a given
// field's value actually changed -- Python's own flush touches
// updated_at either way (last_login_at is set unconditionally on every
// existing-user call), so the two are never observably different.
func (h handlers) provisionOrGetUser(ctx context.Context, row *providerRow, email, name string, providerID uuid.UUID, externalID string) (*provisionedUser, *provisionedMembership, error) {
	authProviderValue := "oidc"
	if row.Protocol == "saml" {
		authProviderValue = "saml"
	}

	var (
		id                                                                     uuid.UUID
		existingEmail                                                          string
		username, existingFullName, currentAuthProvider, currentAuthProviderID *string
		isSuperuser                                                            bool
		tokenVersion                                                           int64
	)
	err := h.Pool.QueryRow(ctx, `SELECT id, email, username, full_name, auth_provider, auth_provider_id, is_superuser, token_version
FROM users WHERE email = $1::text`, email).Scan(&id, &existingEmail, &username, &existingFullName,
		&currentAuthProvider, &currentAuthProviderID, &isSuperuser, &tokenVersion)

	now := h.Now().UTC()
	if errors.Is(err, pgx.ErrNoRows) {
		if !row.AutoProvision {
			return nil, nil, oidcErr("User not found and auto-provisioning disabled")
		}
		newID := uuid.New()
		var fullNamePtr, externalIDPtr *string
		if name != "" {
			fullNamePtr = &name
		}
		if externalID != "" {
			externalIDPtr = &externalID
		}
		if _, err := h.Pool.Exec(ctx, `INSERT INTO users
	(id, email, full_name, auth_provider, auth_provider_id, is_active, is_verified, is_superuser, token_version,
	 last_login_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, true, true, false, 0, $6, $6, $6)`,
			newID, email, fullNamePtr, authProviderValue, externalIDPtr, now); err != nil {
			return nil, nil, err
		}
		return &provisionedUser{id: newID, email: email, fullName: fullNamePtr, tokenVersion: 0}, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}

	newAuthProvider := authProviderValue
	newAuthProviderID := currentAuthProviderID
	if externalID != "" && stringOrEmpty(currentAuthProviderID) != externalID {
		newAuthProviderID = &externalID
	}
	newFullName := existingFullName
	if name != "" && stringOrEmpty(existingFullName) == "" {
		newFullName = &name
	}
	if _, err := h.Pool.Exec(ctx, `UPDATE users SET auth_provider = $2, auth_provider_id = $3, full_name = $4,
	last_login_at = $5, updated_at = $5 WHERE id = $1::uuid`,
		id, newAuthProvider, newAuthProviderID, newFullName, now); err != nil {
		return nil, nil, err
	}

	providerOrgID, err := pythonparity.ParseUUID(row.OrgID)
	if err != nil {
		return nil, nil, err
	}
	var membership *provisionedMembership
	var membershipOrgID uuid.UUID
	var role *string
	err = h.Pool.QueryRow(ctx, `SELECT org_id, role FROM memberships WHERE user_id = $1::uuid AND org_id = $2::uuid`,
		id, providerOrgID).Scan(&membershipOrgID, &role)
	switch {
	case err == nil:
		roleText := "member"
		if role != nil {
			roleText = *role
		}
		membership = &provisionedMembership{orgID: membershipOrgID, role: roleText}
	case errors.Is(err, pgx.ErrNoRows):
		// No membership: onboarding required, matching Python's own log
		// line and nil membership.
	default:
		return nil, nil, err
	}

	return &provisionedUser{id: id, email: email, username: username, fullName: newFullName,
		isSuperuser: isSuperuser, tokenVersion: tokenVersion}, membership, nil
}

package admin

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// organization is users.py's Organization row (organizations.py's
// _organization_response's source).
type organization struct {
	ID          uuid.UUID
	Slug        string
	Name        string
	Description *string
	Settings    []byte // raw json column bytes, "{}" when empty
	Tier        string
	ManagedBy   string
	IsActive    bool
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

const orgColumns = `id, slug, name, description, settings, tier, managed_by, is_active, created_at, updated_at`

func scanOrganization(row pgx.Row) (*organization, error) {
	var org organization
	err := row.Scan(&org.ID, &org.Slug, &org.Name, &org.Description, &org.Settings,
		&org.Tier, &org.ManagedBy, &org.IsActive, &org.CreatedAt, &org.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &org, nil
}

// orgByID is OrganizationService.get_by_id.
func (s pgStore) orgByID(ctx context.Context, id uuid.UUID) (*organization, error) {
	return scanOrganization(s.Pool.QueryRow(ctx, `SELECT `+orgColumns+` FROM organizations WHERE id = $1`, id))
}

// orgBySlug is OrganizationService.get_by_slug (case-insensitive).
func (s pgStore) orgBySlug(ctx context.Context, slug string) (*organization, error) {
	return scanOrganization(s.Pool.QueryRow(ctx,
		`SELECT `+orgColumns+` FROM organizations WHERE lower(slug) = lower($1)`, slug))
}

// listOrganizations is OrganizationService.list_all.
func (s pgStore) listOrganizations(ctx context.Context, limit, offset int, activeOnly bool) ([]*organization, error) {
	query := `SELECT ` + orgColumns + ` FROM organizations`
	if activeOnly {
		query += ` WHERE is_active = true`
	}
	query += ` ORDER BY created_at DESC LIMIT $1 OFFSET $2`
	rows, err := s.Pool.Query(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*organization
	for rows.Next() {
		org, err := scanOrganization(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, org)
	}
	return out, rows.Err()
}

// insertOrganization is OrganizationService.create (slug uniqueness resolved
// by the caller before calling this, matching the Python service's own
// get_by_slug-then-suffix dance).
func (s pgStore) insertOrganization(ctx context.Context, org *organization) error {
	now := s.now().UTC()
	org.CreatedAt, org.UpdatedAt = now, now
	settings := org.Settings
	if len(settings) == 0 {
		settings = []byte("{}")
	}
	_, err := s.Pool.Exec(ctx, `
INSERT INTO organizations (id, slug, name, description, settings, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		org.ID, org.Slug, org.Name, org.Description, settings, org.Tier, org.ManagedBy, org.IsActive, org.CreatedAt, org.UpdatedAt)
	return err
}

// orgUpdate is OrganizationUpdate: a nil field leaves the column unchanged.
type orgUpdate struct {
	Name        *string
	Description *string
	Settings    []byte
	Tier        *string
	IsActive    *bool
}

// updateOrganization is OrganizationService.update, minus the license-tier
// sync side effect (org_licenses is out of this route's scope; a tier
// change here does not touch OrgLicense -- see RISK-NOTES).
func (s pgStore) updateOrganization(ctx context.Context, id uuid.UUID, patch orgUpdate) (*organization, error) {
	existing, err := s.orgByID(ctx, id)
	if err != nil || existing == nil {
		return existing, err
	}
	name, description, tier, isActive := existing.Name, existing.Description, existing.Tier, existing.IsActive
	settings := existing.Settings
	if patch.Name != nil {
		name = *patch.Name
	}
	if patch.Description != nil {
		description = patch.Description
	}
	if patch.Settings != nil {
		settings = patch.Settings
	}
	managedBy := existing.ManagedBy
	if patch.Tier != nil {
		tier = *patch.Tier
		if tier != "community" {
			managedBy = "manual"
		} else {
			managedBy = "stripe"
		}
	}
	if patch.IsActive != nil {
		isActive = *patch.IsActive
	}
	now := s.now().UTC()
	_, err = s.Pool.Exec(ctx, `
UPDATE organizations SET name = $2, description = $3, settings = $4, tier = $5, managed_by = $6, is_active = $7, updated_at = $8
WHERE id = $1`, id, name, description, settings, tier, managedBy, isActive, now)
	if err != nil {
		return nil, err
	}
	return s.orgByID(ctx, id)
}

// slugify is users.py's _slugify: lowercase, strip, drop everything but
// word/space/hyphen, collapse runs of hyphen/space to one hyphen, cap 50.
func slugify(name string) string {
	lowered := strings.ToLower(strings.TrimSpace(name))
	var kept strings.Builder
	for _, r := range lowered {
		if r == '_' || r == '-' || r == ' ' || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			kept.WriteRune(r)
		}
	}
	fields := strings.FieldsFunc(kept.String(), func(r rune) bool { return r == '-' || r == ' ' })
	slug := strings.Join(fields, "-")
	if len(slug) > 50 {
		slug = slug[:50]
	}
	return slug
}

// membership is users.py's Membership row.
type membership struct {
	ID          uuid.UUID
	OrgID       uuid.UUID
	UserID      uuid.UUID
	Role        string
	InvitedByID *uuid.UUID
	JoinedAt    *time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

const membershipColumns = `id, org_id, user_id, role, invited_by_id, joined_at, created_at, updated_at`

func scanMembership(row pgx.Row) (*membership, error) {
	var m membership
	err := row.Scan(&m.ID, &m.OrgID, &m.UserID, &m.Role, &m.InvitedByID, &m.JoinedAt, &m.CreatedAt, &m.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// membershipByOrgUser is MembershipService.get_membership.
func (s pgStore) membershipByOrgUser(ctx context.Context, orgID, userID uuid.UUID) (*membership, error) {
	return scanMembership(s.Pool.QueryRow(ctx,
		`SELECT `+membershipColumns+` FROM memberships WHERE org_id = $1 AND user_id = $2`, orgID, userID))
}

// listMemberships is MembershipService.list_members.
func (s pgStore) listMemberships(ctx context.Context, orgID uuid.UUID, role *string) ([]*membership, error) {
	query := `SELECT ` + membershipColumns + ` FROM memberships WHERE org_id = $1`
	args := []any{orgID}
	if role != nil {
		query += ` AND role = $2`
		args = append(args, *role)
	}
	query += ` ORDER BY created_at`
	rows, err := s.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*membership
	for rows.Next() {
		m, err := scanMembership(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// insertMembership is MembershipService.add_member. Returns a distinct
// sentinel error when the pair already has a membership row.
var errMembershipExists = errors.New("user is already a member of this organization")

func (s pgStore) insertMembership(ctx context.Context, orgID, userID uuid.UUID, role string, invitedByID *uuid.UUID) (*membership, error) {
	existing, err := s.membershipByOrgUser(ctx, orgID, userID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return nil, errMembershipExists
	}
	now := s.now().UTC()
	m := &membership{ID: uuid.New(), OrgID: orgID, UserID: userID, Role: role, InvitedByID: invitedByID,
		JoinedAt: &now, CreatedAt: now, UpdatedAt: now}
	_, err = s.Pool.Exec(ctx, `
INSERT INTO memberships (id, org_id, user_id, role, invited_by_id, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		m.ID, m.OrgID, m.UserID, m.Role, m.InvitedByID, m.JoinedAt, m.CreatedAt, m.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// validMemberRoles is MemberRole's values.
var validMemberRoles = map[string]bool{"owner": true, "admin": true, "member": true, "viewer": true}

var errInvalidRole = errors.New("invalid role")

// updateMembershipRole is MembershipService.update_role.
func (s pgStore) updateMembershipRole(ctx context.Context, orgID, userID uuid.UUID, role string) (*membership, error) {
	existing, err := s.membershipByOrgUser(ctx, orgID, userID)
	if err != nil || existing == nil {
		return existing, err
	}
	if !validMemberRoles[role] {
		return nil, errInvalidRole
	}
	now := s.now().UTC()
	_, err = s.Pool.Exec(ctx, `UPDATE memberships SET role = $3, updated_at = $4 WHERE org_id = $1 AND user_id = $2`,
		orgID, userID, role, now)
	if err != nil {
		return nil, err
	}
	existing.Role, existing.UpdatedAt = role, now
	return existing, nil
}

var errLastOwner = errors.New("cannot remove the last owner of an organization")

// removeMembership is MembershipService.remove_member: refuses to remove an
// organization's last remaining owner.
func (s pgStore) removeMembership(ctx context.Context, orgID, userID uuid.UUID) (bool, error) {
	existing, err := s.membershipByOrgUser(ctx, orgID, userID)
	if err != nil || existing == nil {
		return false, err
	}
	if existing.Role == "owner" {
		owner := "owner"
		owners, err := s.listMemberships(ctx, orgID, &owner)
		if err != nil {
			return false, err
		}
		if len(owners) <= 1 {
			return false, errLastOwner
		}
	}
	_, err = s.Pool.Exec(ctx, `DELETE FROM memberships WHERE org_id = $1 AND user_id = $2`, orgID, userID)
	if err != nil {
		return false, err
	}
	return true, nil
}

var errNotAnOwner = errors.New("source user is not an owner")
var errTargetNotMember = errors.New("target user is not a member")

// transferOwnership is MembershipService.transfer_ownership: swaps the
// source owner to admin and the target member to owner. Go's fromUserID is
// resolved server-side (the org's current owner) per team-lead's ruling --
// see the transfer-ownership route's own doc comment for why this diverges
// from the Python path's own {from_user_id} URL segment.
func (s pgStore) transferOwnership(ctx context.Context, tx pgx.Tx, orgID, fromUserID, toUserID uuid.UUID) error {
	var fromRole string
	err := tx.QueryRow(ctx, `SELECT role FROM memberships WHERE org_id = $1 AND user_id = $2`, orgID, fromUserID).Scan(&fromRole)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && fromRole != "owner") {
		return errNotAnOwner
	}
	if err != nil {
		return err
	}
	var toExists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM memberships WHERE org_id = $1 AND user_id = $2)`,
		orgID, toUserID).Scan(&toExists); err != nil {
		return err
	}
	if !toExists {
		return errTargetNotMember
	}
	now := s.now().UTC()
	if _, err := tx.Exec(ctx, `UPDATE memberships SET role = 'admin', updated_at = $3 WHERE org_id = $1 AND user_id = $2`,
		orgID, fromUserID, now); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE memberships SET role = 'owner', updated_at = $3 WHERE org_id = $1 AND user_id = $2`,
		orgID, toUserID, now); err != nil {
		return err
	}
	return nil
}

// currentOwner is the Go-only lookup transfer-ownership needs to resolve
// "the org's current owner" server-side. 0 or >1 owner rows both fail with
// errNotAnOwner (an org must have exactly one owner for the divergence to
// apply cleanly; see the route's doc comment).
func (s pgStore) currentOwner(ctx context.Context, tx pgx.Tx, orgID uuid.UUID) (uuid.UUID, error) {
	rows, err := tx.Query(ctx, `SELECT user_id FROM memberships WHERE org_id = $1 AND role = 'owner'`, orgID)
	if err != nil {
		return uuid.UUID{}, err
	}
	defer rows.Close()
	var owners []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return uuid.UUID{}, err
		}
		owners = append(owners, id)
	}
	if err := rows.Err(); err != nil {
		return uuid.UUID{}, err
	}
	if len(owners) != 1 {
		return uuid.UUID{}, errNotAnOwner
	}
	return owners[0], nil
}

// orgInvite is org_invite.py's OrgInvite row.
type orgInvite struct {
	ID          uuid.UUID
	OrgID       uuid.UUID
	Email       string
	Role        string
	InvitedByID *uuid.UUID
	Status      string
	ExpiresAt   time.Time
	AcceptedAt  *time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

var errPendingInviteExists = errors.New("a pending invite already exists for this email")

// insertInvite is invites.py's create_invite (token minting is the
// caller's -- see requestcrypto.go).
// insertInvite takes tx, not the pool: Python's create_invite and the
// router's own emit_audit_log share the request's one SQLAlchemy session
// and its one implicit commit, so the invite INSERT and the member_invited
// audit row share one Go transaction too.
func (s pgStore) insertInvite(ctx context.Context, tx pgx.Tx, orgID uuid.UUID, email, role string, invitedByID uuid.UUID, tokenHash string, ttl time.Duration) (*orgInvite, error) {
	now := s.now().UTC()
	normalizedEmail := strings.ToLower(strings.TrimSpace(email))
	var existsID uuid.UUID
	err := tx.QueryRow(ctx, `
SELECT id FROM org_invites
WHERE org_id = $1 AND lower(email) = $2 AND status = 'pending' AND expires_at >= $3
LIMIT 1`, orgID, normalizedEmail, now).Scan(&existsID)
	if err == nil {
		return nil, errPendingInviteExists
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	invite := &orgInvite{
		ID: uuid.New(), OrgID: orgID, Email: normalizedEmail, Role: role, InvitedByID: &invitedByID,
		Status: "pending", ExpiresAt: now.Add(ttl), CreatedAt: now, UpdatedAt: now,
	}
	_, err = tx.Exec(ctx, `
INSERT INTO org_invites (id, org_id, email, role, token_hash, invited_by_id, status, expires_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		invite.ID, invite.OrgID, invite.Email, invite.Role, tokenHash, invite.InvitedByID,
		invite.Status, invite.ExpiresAt, invite.CreatedAt, invite.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return invite, nil
}

// orgNameByID is create_org_invite's own organizations lookup (id, name).
func (s pgStore) orgNameByID(ctx context.Context, orgID uuid.UUID) (string, bool, error) {
	var name string
	err := s.Pool.QueryRow(ctx, `SELECT name FROM organizations WHERE id = $1`, orgID).Scan(&name)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return name, true, nil
}

// inviterDisplay is create_org_invite's inviter_name resolution: full_name,
// else email, else fall back to the caller's own JWT email (done by the
// handler, not here).
func (s pgStore) inviterDisplay(ctx context.Context, userID uuid.UUID) (fullName, email *string, err error) {
	var name, mail *string
	dbErr := s.Pool.QueryRow(ctx, `SELECT full_name, email FROM users WHERE id = $1`, userID).Scan(&name, &mail)
	if errors.Is(dbErr, pgx.ErrNoRows) {
		return nil, nil, nil
	}
	if dbErr != nil {
		return nil, nil, dbErr
	}
	return name, mail, nil
}

// settingsObject decodes an organizations.settings json column into a
// pyjson.Object (empty object when the column is empty/null).
func settingsObject(raw []byte) (pyjson.Value, error) {
	if len(raw) == 0 {
		return pyjson.NewObject(), nil
	}
	value, err := pyjson.DecodeString(string(raw))
	if err != nil {
		return nil, fmt.Errorf("decode settings: %w", err)
	}
	if value == nil {
		return pyjson.NewObject(), nil
	}
	return value, nil
}

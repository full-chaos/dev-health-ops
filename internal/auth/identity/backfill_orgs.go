package identity

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
)

var (
	// ErrBackfillOrgMissing reports a membership whose organization is not in the auth schema.
	ErrBackfillOrgMissing = errors.New("identity: the organization is not in the auth schema")
	// ErrBackfillPrincipalMissing reports a membership whose user principal is not in the auth schema.
	ErrBackfillPrincipalMissing = errors.New("identity: the user principal is not in the auth schema")
	// ErrBackfillRole reports a role outside the organization roles of the closed catalog.
	ErrBackfillRole = errors.New("identity: the role is not an organization role")
)

var errMembershipExists = errors.New("identity: membership exists")

// OrganizationRoles are the organization roles of contracts/auth/v1/vocabulary/roles.v1.json.
var OrganizationRoles = []string{"viewer", "member", "admin", "owner"}

// IsOrganizationRole reports whether role is one of OrganizationRoles.
func IsOrganizationRole(role string) bool {
	for _, r := range OrganizationRoles {
		if r == role {
			return true
		}
	}
	return false
}

// OrgBackfillTarget is the state of one public.organizations row.
type OrgBackfillTarget struct {
	ID     uuid.UUID
	Slug   string
	Name   string
	Active bool
}

// OrgBackfillResult says what BackfillOrganization wrote (or, in a dry run, would write).
type OrgBackfillResult struct {
	Inserted    bool
	Updated     bool
	Reactivated bool
	Suspended   bool
}

// BackfillOrganization makes auth.organizations agree with t. A missing
// organization is inserted; a differing one is updated (disabled_at is set
// only when the organization turns inactive and cleared when it turns active,
// so an equal organization keeps its timestamp); an equal one writes nothing.
func (s *Store) BackfillOrganization(ctx context.Context, t OrgBackfillTarget, dryRun bool) (OrgBackfillResult, error) {
	var res OrgBackfillResult
	src := orgState{slug: t.Slug, name: t.Name, active: t.Active}
	var cur orgState
	err := s.pool.QueryRow(ctx, `SELECT slug, name, disabled_at IS NULL FROM `+s.q+`.organizations WHERE id = $1`, t.ID).
		Scan(&cur.slug, &cur.name, &cur.active)
	found := true
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		found = false
	case err != nil:
		return res, s.storeErr(ctx, "read auth.organizations", err)
	case cur == src:
		return res, nil
	}
	if !found || cur.slug != src.slug {
		var held bool
		if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+s.q+`.organizations WHERE slug = $1 AND id <> $2)`,
			t.Slug, t.ID).Scan(&held); err != nil {
			return res, s.storeErr(ctx, "read auth.organizations slug", err)
		}
		if held {
			return res, ErrOrganizationSlugCollision
		}
	}
	if !found {
		res.Inserted = true
		if dryRun {
			return res, nil
		}
		if err := s.insertOrganization(ctx, t.ID, src); err != nil {
			return OrgBackfillResult{}, err
		}
		return res, nil
	}
	res.Updated = true
	res.Reactivated = src.active && !cur.active
	res.Suspended = !src.active && cur.active
	if dryRun {
		return res, nil
	}
	if err := s.updateOrganization(ctx, t.ID, src, cur); err != nil {
		return OrgBackfillResult{}, err
	}
	return res, nil
}

// MembershipBackfillTarget is one public.memberships row.
type MembershipBackfillTarget struct {
	OrgID    uuid.UUID
	UserID   uuid.UUID
	Role     string
	JoinedAt *time.Time
}

// MembershipBackfillResult says what BackfillMembership wrote (or would write).
type MembershipBackfillResult struct {
	Inserted    bool
	RoleUpdated bool
	// Diverged is set when the auth membership exists and is not active. It is
	// reported and never written: a status in the auth schema is auth's own.
	Diverged bool
}

// BackfillMembership makes the auth membership of (OrgID, UserID) agree with
// t. A missing one is inserted active with t's role; an active one with
// another role gets t's role; a membership that is not active is left alone
// and reported as Diverged. The organization and the user principal must
// already be in the auth schema.
func (s *Store) BackfillMembership(ctx context.Context, t MembershipBackfillTarget, dryRun bool) (MembershipBackfillResult, error) {
	var res MembershipBackfillResult
	if !IsOrganizationRole(t.Role) {
		return res, ErrBackfillRole
	}
	var orgExists, principalExists bool
	var id *uuid.UUID
	var status, role *string
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+s.q+`.organizations WHERE id = $1),
       EXISTS (SELECT 1 FROM `+s.q+`.principals WHERE id = $2 AND kind = 'user'),
       m.id, m.status, m.role
FROM (SELECT 1) x LEFT JOIN `+s.q+`.memberships m ON m.organization_id = $1 AND m.principal_id = $2`,
		t.OrgID, t.UserID).Scan(&orgExists, &principalExists, &id, &status, &role)
	if err != nil {
		return res, s.storeErr(ctx, "read auth.memberships", err)
	}
	switch {
	case !orgExists:
		return res, ErrBackfillOrgMissing
	case !principalExists:
		return res, ErrBackfillPrincipalMissing
	case id == nil:
		res.Inserted = true
		if dryRun {
			return res, nil
		}
		if err := s.insertMembership(ctx, t); err != nil {
			return MembershipBackfillResult{}, err
		}
		return res, nil
	case status == nil || *status != "active":
		res.Diverged = true
		return res, nil
	case role != nil && *role == t.Role:
		return res, nil
	}
	res.RoleUpdated = true
	if dryRun {
		return res, nil
	}
	if err := s.updateMembershipRole(ctx, *id, t); err != nil {
		return MembershipBackfillResult{}, err
	}
	return res, nil
}

func membershipEvent(id uuid.UUID, verb string, change map[string]any) audit.OutboxEvent {
	return audit.OutboxEvent{AggregateType: "membership", AggregateID: provisioning.WireID("membership", id.String()),
		EventType: verb, Change: change}
}

func membershipAudit(eventType, reason string, t MembershipBackfillTarget, id uuid.UUID) audit.AuditEvent {
	org, user := t.OrgID.String(), t.UserID.String()
	return audit.AuditEvent{
		EventType: eventType, Outcome: audit.OutcomeAllowed, OrganizationID: &org, SubjectPrincipalID: &user,
		Attributes: map[string]any{"reason_code": reason, "target_kind": "membership", "target_id": id.String()},
	}
}

func (s *Store) insertMembership(ctx context.Context, t MembershipBackfillTarget) error {
	id := uuid.New()
	err := audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			var inserted uuid.UUID
			err := tx.QueryRow(ctx, `INSERT INTO `+s.q+`.memberships (id, organization_id, principal_id, status, role, joined_at)
VALUES ($1, $2, $3, 'active', $4, $5)
ON CONFLICT (organization_id, principal_id) DO NOTHING
RETURNING id`, id, t.OrgID, t.UserID, t.Role, t.JoinedAt).Scan(&inserted)
			if errors.Is(err, pgx.ErrNoRows) {
				return errMembershipExists
			}
			return err
		},
		Audit: membershipAudit("auth.membership.projected", "membership_added", t, id),
		Events: []audit.OutboxEvent{membershipEvent(id, "membership.activated",
			map[string]any{"status": "active", "role_id": "rol_" + t.Role})},
	})
	switch {
	case err == nil, errors.Is(err, errMembershipExists):
		return nil
	}
	return s.storeErr(ctx, "insert membership", err)
}

func (s *Store) updateMembershipRole(ctx context.Context, id uuid.UUID, t MembershipBackfillTarget) error {
	err := audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `UPDATE `+s.q+`.memberships SET role = $2, updated_at = clock_timestamp()
WHERE id = $1 AND status = 'active' AND role IS DISTINCT FROM $2`, id, t.Role)
			return err
		},
		Audit:  membershipAudit("auth.membership.projection_updated", "membership_status_changed", t, id),
		Events: []audit.OutboxEvent{membershipEvent(id, "membership.role_changed", map[string]any{"role_id": "rol_" + t.Role})},
	})
	if err == nil || errors.Is(err, audit.ErrNoStateChange) {
		return nil
	}
	return s.storeErr(ctx, "update membership role", err)
}

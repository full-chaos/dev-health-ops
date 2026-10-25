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
	}
	d := decideOrg(found, cur, src)
	if d.action == orgNone {
		return res, nil
	}
	if d.slugCheck {
		var held bool
		if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+s.q+`.organizations WHERE slug = $1 AND id <> $2)`,
			t.Slug, t.ID).Scan(&held); err != nil {
			return res, s.storeErr(ctx, "read auth.organizations slug", err)
		}
		if held {
			return res, ErrOrganizationSlugCollision
		}
	}
	res = OrgBackfillResult{Inserted: d.action == orgInsert, Updated: d.action == orgUpdate, Reactivated: d.reactivated, Suspended: d.suspended}
	if dryRun {
		return res, nil
	}
	if d.action == orgInsert {
		err = s.insertOrganization(ctx, t.ID, src)
	} else {
		err = s.updateOrganization(ctx, t.ID, src, cur)
	}
	if err != nil {
		return OrgBackfillResult{}, err
	}
	return res, nil
}

type orgAction int

const (
	orgNone orgAction = iota
	orgInsert
	orgUpdate
)

type orgDecision struct {
	action      orgAction
	slugCheck   bool
	reactivated bool
	suspended   bool
}

// decideOrg is the whole organization rule: a missing organization is
// inserted, an equal one is left alone (nothing is written, so disabled_at is
// never touched), a differing one is updated and a flip of the active state is
// named. A slug is checked against other organizations when it is new or changed.
func decideOrg(found bool, cur, src orgState) orgDecision {
	switch {
	case !found:
		return orgDecision{action: orgInsert, slugCheck: true}
	case cur == src:
		return orgDecision{}
	}
	return orgDecision{action: orgUpdate, slugCheck: cur.slug != src.slug,
		reactivated: src.active && !cur.active, suspended: !src.active && cur.active}
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
	action, err := decideMembership(orgExists, principalExists, id != nil, status, role, t.Role)
	if err != nil {
		return res, err
	}
	res = MembershipBackfillResult{Inserted: action == memberInsert, RoleUpdated: action == memberSetRole, Diverged: action == memberDiverged}
	if dryRun {
		return res, nil
	}
	switch action {
	case memberInsert:
		err = s.insertMembership(ctx, t)
	case memberSetRole:
		err = s.updateMembershipRole(ctx, *id, t)
	}
	if err != nil {
		return MembershipBackfillResult{}, err
	}
	return res, nil
}

type memberAction int

const (
	memberNone memberAction = iota
	memberInsert
	memberSetRole
	memberDiverged
)

// decideMembership is the whole membership rule. The organization and the user
// principal must exist; a missing membership is inserted; one that is not
// active is only reported (nothing is written); an active one with another role
// gets the role; an equal one is left alone.
func decideMembership(orgExists, principalExists, found bool, status, role *string, want string) (memberAction, error) {
	switch {
	case !orgExists:
		return memberNone, ErrBackfillOrgMissing
	case !principalExists:
		return memberNone, ErrBackfillPrincipalMissing
	case !found:
		return memberInsert, nil
	case status == nil || *status != "active":
		return memberDiverged, nil
	case role != nil && *role == want:
		return memberNone, nil
	}
	return memberSetRole, nil
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

// roleChangeAudit names the changed field; the audit vocabulary has no slot for a role value.
func roleChangeAudit(t MembershipBackfillTarget, id uuid.UUID) audit.AuditEvent {
	ev := membershipAudit("auth.membership.role_changed", "membership_status_changed", t, id)
	ev.Attributes["changed_fields"] = []string{"role_key"}
	return ev
}

func (s *Store) updateMembershipRole(ctx context.Context, id uuid.UUID, t MembershipBackfillTarget) error {
	err := audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `UPDATE `+s.q+`.memberships SET role = $2, updated_at = clock_timestamp()
WHERE id = $1 AND status = 'active' AND role IS DISTINCT FROM $2`, id, t.Role)
			return err
		},
		Audit:  roleChangeAudit(t, id),
		Events: []audit.OutboxEvent{membershipEvent(id, "membership.role_changed", map[string]any{"role_id": "rol_" + t.Role})},
	})
	if err == nil || errors.Is(err, audit.ErrNoStateChange) {
		return nil
	}
	return s.storeErr(ctx, "update membership role", err)
}

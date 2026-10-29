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
// only when the organization turns inactive, so an equal organization keeps
// its timestamp; a suspended one stays suspended, keepSuspended); an equal one
// writes nothing.
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
	if found {
		src = keepSuspended(cur, src)
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
	// InvitedBy is public.memberships.invited_by_id. It is the same id space as UserID: an auth user
	// principal has the id of its public.users row (BackfillMembership checks UserID the same way).
	InvitedBy *uuid.UUID
}

// MembershipBackfillResult says what BackfillMembership wrote (or would write).
type MembershipBackfillResult struct {
	Inserted    bool
	RoleUpdated bool
	// Diverged is set when the auth membership exists and is not active. It is
	// reported and never written: a status in the auth schema is auth's own.
	Diverged bool
	// InviterSet and JoinedSet say a missing invited_by / joined_at was (or would be) filled from the
	// public row. InviterUnresolved says the public inviter has no auth user principal, so invited_by
	// stays NULL; it is a count, never a failure.
	InviterSet        bool
	JoinedSet         bool
	InviterUnresolved bool
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
	var orgExists, principalExists, inviterResolved bool
	var id *uuid.UUID
	var status, role *string
	var curInviter *uuid.UUID
	var curJoined *time.Time
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+s.q+`.organizations WHERE id = $1),
       EXISTS (SELECT 1 FROM `+s.q+`.principals WHERE id = $2 AND kind = 'user'),
       EXISTS (SELECT 1 FROM `+s.q+`.principals WHERE id = $3::uuid AND kind = 'user'),
       m.id, m.status, m.role, m.invited_by, m.joined_at
FROM (SELECT 1) x LEFT JOIN `+s.q+`.memberships m ON m.organization_id = $1 AND m.principal_id = $2`,
		t.OrgID, t.UserID, t.InvitedBy).Scan(&orgExists, &principalExists, &inviterResolved, &id, &status, &role, &curInviter, &curJoined)
	if err != nil {
		return res, s.storeErr(ctx, "read auth.memberships", err)
	}
	action, err := decideMembership(orgExists, principalExists, id != nil, status, role, t.Role)
	if err != nil {
		return res, err
	}
	res = MembershipBackfillResult{Inserted: action == memberInsert, RoleUpdated: action == memberSetRole, Diverged: action == memberDiverged}
	var fill fillDecision
	switch action {
	case memberInsert:
		fill = decideFill(true, nil, nil, t.InvitedBy, inviterResolved, t.JoinedAt)
		fill.Joined = false
	case memberSetRole, memberNone:
		fill = decideFill(true, curInviter, curJoined, t.InvitedBy, inviterResolved, t.JoinedAt)
	}
	res.InviterSet, res.JoinedSet, res.InviterUnresolved = fill.Inviter, fill.Joined, fill.Unresolved
	if dryRun {
		return res, nil
	}
	switch action {
	case memberInsert:
		var inviter *uuid.UUID
		if fill.Inviter {
			inviter = t.InvitedBy
		}
		err = s.insertMembership(ctx, t, inviter)
	case memberSetRole:
		err = s.updateMembershipRole(ctx, *id, t)
	}
	if err == nil && action != memberInsert && (fill.Inviter || fill.Joined) {
		err = s.fillMembership(ctx, *id, t, fill)
	}
	if err != nil {
		return MembershipBackfillResult{}, err
	}
	return res, nil
}

// fillDecision is what BackfillMembership fills on an existing membership. Unresolved counts a public
// inviter with no auth user principal; nothing is written for it.
type fillDecision struct{ Inviter, Joined, Unresolved bool }

// decideFill is the whole fill rule: fill missing values only, and only on an active membership. A value
// the auth row holds is never overwritten, whichever one is older. An inviter is written only when it is
// an auth user principal.
func decideFill(active bool, curInviter *uuid.UUID, curJoined *time.Time, want *uuid.UUID, resolved bool, wantJoined *time.Time) fillDecision {
	if !active {
		return fillDecision{}
	}
	var d fillDecision
	if curInviter == nil && want != nil {
		d.Inviter, d.Unresolved = resolved, !resolved
	}
	d.Joined = curJoined == nil && wantJoined != nil
	return d
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

func (s *Store) insertMembership(ctx context.Context, t MembershipBackfillTarget, inviter *uuid.UUID) error {
	id := uuid.New()
	err := audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			var inserted uuid.UUID
			err := tx.QueryRow(ctx, `INSERT INTO `+s.q+`.memberships (id, organization_id, principal_id, status, role, joined_at, invited_by)
VALUES ($1, $2, $3, 'active', $4, $5, $6)
ON CONFLICT (organization_id, principal_id) DO NOTHING
RETURNING id`, id, t.OrgID, t.UserID, t.Role, t.JoinedAt, inviter).Scan(&inserted)
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

// fillAudit reuses the projected-membership event the INSERT emits and names the field it filled; the
// audit vocabulary has no slot for the value.
func fillAudit(t MembershipBackfillTarget, id uuid.UUID, d fillDecision) audit.AuditEvent {
	ev := membershipAudit("auth.membership.projected", "membership_added", t, id)
	var fields []string
	if d.Inviter {
		fields = append(fields, "invited_by")
	}
	if d.Joined {
		fields = append(fields, "joined_at")
	}
	ev.Attributes["changed_fields"] = fields
	return ev
}

func (s *Store) fillMembership(ctx context.Context, id uuid.UUID, t MembershipBackfillTarget, d fillDecision) error {
	var inviter *uuid.UUID
	if d.Inviter {
		inviter = t.InvitedBy
	}
	var joined *time.Time
	if d.Joined {
		joined = t.JoinedAt
	}
	err := audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `UPDATE `+s.q+`.memberships
SET invited_by = COALESCE(invited_by, $2::uuid), joined_at = COALESCE(joined_at, $3::timestamptz), updated_at = clock_timestamp()
WHERE id = $1 AND status = 'active'
  AND ((invited_by IS NULL AND $2::uuid IS NOT NULL AND EXISTS (SELECT 1 FROM `+s.q+`.principals WHERE id = $2::uuid AND kind = 'user'))
    OR (joined_at IS NULL AND $3::timestamptz IS NOT NULL))`, id, inviter, joined)
			return err
		},
		Audit: fillAudit(t, id, d),
		Events: []audit.OutboxEvent{membershipEvent(id, "membership.activated",
			map[string]any{"status": "active", "role_id": "rol_" + t.Role})},
	})
	if err == nil || errors.Is(err, audit.ErrNoStateChange) {
		return nil
	}
	return s.storeErr(ctx, "fill membership", err)
}

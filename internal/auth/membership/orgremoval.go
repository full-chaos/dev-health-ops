package membership

import (
	"bytes"
	"context"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// OrgRemoval is the removal of every membership of one organization, planned
// from a lock-free read for an organization deprovision's own audit.Commit:
// each active, suspended or invited membership becomes removed in both tables
// (no public row, the auth row removed). The last-owner rule does not apply:
// the organization itself goes.
type OrgRemoval struct {
	orgID uuid.UUID
	plans []principalPlan
}

// Len is the number of memberships the removal ends.
func (r *OrgRemoval) Len() int { return len(r.plans) }

// Events are the planned membership.removed events, one per membership.
func (r *OrgRemoval) Events() []audit.OutboxEvent {
	out := make([]audit.OutboxEvent, 0, len(r.plans))
	for _, pl := range r.plans {
		out = append(out, eventFor(verbRemove, pl.plan.id, pl.plan.next))
	}
	return out
}

// Referenced are the principals whose auth membership row the removal
// inserts (a membership go-api wrote, with no auth row): the insert's foreign
// key takes a KEY SHARE lock on each, so the caller locks them first
// (LockPrincipals), before the organization row.
func (r *OrgRemoval) Referenced() []uuid.UUID {
	out := []uuid.UUID{}
	for _, pl := range r.plans {
		if pl.authless {
			out = append(out, pl.principal)
		}
	}
	return out
}

// LockPrincipals takes the auth.principals rows of ids FOR KEY SHARE, in id
// order: step 1 of the one lock order, for a caller that inserts rows naming
// them after it locks the organization. A membership verb holding one FOR
// UPDATE is waited for here, never after the organization row.
func (s *Store) LockPrincipals(ctx context.Context, tx audit.TxOps, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := foundIDs(ctx, txQuerier{tx}, `SELECT id FROM `+s.q+`.principals WHERE id = ANY($1::uuid[]) ORDER BY id FOR KEY SHARE`,
		uuidArray(sortedUnique(ids)))
	return err
}

// PlanRemoveAll plans the removal of every live membership of orgID.
func (s *Store) PlanRemoveAll(ctx context.Context, orgID uuid.UUID) (*OrgRemoval, error) {
	if !s.ownsUsers {
		return nil, ErrNotOwned
	}
	pairs, err := s.orgPairs(ctx, poolQuerier{s.pool}, orgID, false)
	if err != nil {
		return nil, err
	}
	return &OrgRemoval{orgID: orgID, plans: removalPlans(pairs)}, nil
}

// ApplyRemoveAll writes a planned removal inside the caller's audit.Commit. The
// caller holds the organization row FOR UPDATE (step 3) and the invite rows
// (step 4); this takes every public.memberships row of the organization, then
// every auth.memberships row, FOR UPDATE by id (steps 5 and 6). Rows that
// differ from the plan are ErrStateMoved: plan again.
func (s *Store) ApplyRemoveAll(ctx context.Context, tx audit.TxOps, r *OrgRemoval) error {
	pairs, err := s.orgPairs(ctx, txQuerier{tx}, r.orgID, true)
	if err != nil {
		return err
	}
	now := removalPlans(pairs)
	if len(now) != len(r.plans) {
		return errStateMoved
	}
	for i, pl := range r.plans {
		p := pairs[now[i].principal]
		if now[i].principal != pl.principal || now[i].authless != pl.authless || !pl.plan.matches(p) {
			return errStateMoved
		}
	}
	for _, pl := range r.plans {
		if err := s.write(ctx, tx, r.orgID, pairs[pl.principal], pl.plan.id, pl.plan.next, false, nil); err != nil {
			return err
		}
	}
	return nil
}

// principalPlan is one pair's planned removal. authless: the pair has no
// auth row, so the removal inserts one.
type principalPlan struct {
	principal uuid.UUID
	plan      pairPlan
	authless  bool
}

// removalPlans plans every pair whose effective status is live, ordered by
// principal id.
func removalPlans(pairs map[uuid.UUID]*pairRows) []principalPlan {
	out := []principalPlan{}
	for principal, p := range pairs {
		pl := planOf(p)
		switch pl.prev.status {
		case StatusActive, StatusSuspended, StatusInvited:
		default:
			continue
		}
		pl.next = state{status: StatusRemoved, role: pl.prev.role}
		out = append(out, principalPlan{principal: principal, plan: pl, authless: p.authID == nil})
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].principal[:], out[j].principal[:]) < 0 })
	return out
}

// orgPairs reads every membership row of orgID in both tables. With lock, the
// public rows, then the auth rows, FOR UPDATE by id.
func (s *Store) orgPairs(ctx context.Context, q querier, orgID uuid.UUID, lock bool) (map[uuid.UUID]*pairRows, error) {
	forUpdate := ""
	if lock {
		forUpdate = " FOR UPDATE"
	}
	pairs := map[uuid.UUID]*pairRows{}
	pair := func(principal uuid.UUID) *pairRows {
		if p, ok := pairs[principal]; ok {
			return p
		}
		p := &pairRows{principal: principal}
		pairs[principal] = p
		return p
	}
	rows, err := q.query(ctx, `SELECT id, user_id, role FROM public.memberships WHERE org_id = $1 ORDER BY id`+forUpdate, orgID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id, user uuid.UUID
		var role *string
		if err := rows.Scan(&id, &user, &role); err != nil {
			rows.Close()
			return nil, err
		}
		if role == nil {
			empty := ""
			role = &empty
		}
		p := pair(user)
		p.publicID, p.publicRole = &id, role
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = q.query(ctx, `SELECT id, principal_id, status, role, joined_at, invited_by FROM `+s.q+`.memberships
WHERE organization_id = $1 ORDER BY id`+forUpdate, orgID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id, principal uuid.UUID
		var status Status
		var role *string
		var joined *time.Time
		var inviter *uuid.UUID
		if err := rows.Scan(&id, &principal, &status, &role, &joined, &inviter); err != nil {
			rows.Close()
			return nil, err
		}
		p := pair(principal)
		p.authID, p.authStatus, p.authRole, p.authJoinedAt, p.authInvitedBy = &id, &status, role, joined, inviter
	}
	rows.Close()
	return pairs, rows.Err()
}

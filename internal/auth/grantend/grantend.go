// Package grantend ends live resource grants in the caller's audit.Commit: the
// SQL, audit rows and outbox events of a revoke, in one place. Membership
// removal, organization deprovision and the grant Store's own revoke call it
// directly, so a path that ends grants cannot forget the audit or outbox row.
//
// A plan reads the grant ids lock-free; Apply locks the rows FOR UPDATE in id
// order, refuses with ErrStateMoved when the live set differs from the plan,
// and marks them revoked. The caller holds the principal, user, organization
// and membership rows first (step 6b of the auth lock order).
package grantend

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
)

// ErrStateMoved: the live grants differ from the plan; plan again.
var ErrStateMoved = errors.New("grantend: the live grants changed since the plan")

// Reader is the read surface of a plan (a pool, or a verb's TxOps).
type Reader interface {
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
}

// ReaderFunc adapts a query function (a pool's, wrapped) to a Reader.
type ReaderFunc func(ctx context.Context, sql string, args ...any) (Rows, error)

func (f ReaderFunc) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	return f(ctx, sql, args...)
}

// Rows is the iterator a Reader returns.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Close()
	Err() error
}

// Actor is who ends the grants.
type Actor struct {
	PrincipalID *uuid.UUID
	RequestID   *string
	Caller      audit.Caller
}

type scope int

const (
	scopePair scope = iota + 1
	scopeOrg
	scopeGrant
)

type grantRow struct {
	id        uuid.UUID
	principal uuid.UUID
}

// Plan is the set of live grants one end covers.
type Plan struct {
	q     string
	scope scope
	org   uuid.UUID
	arg   uuid.UUID
	rows  []grantRow
}

// Len is the number of grants the plan ends.
func (p *Plan) Len() int { return len(p.rows) }

// IDs are the planned grant ids, ascending.
func (p *Plan) IDs() []uuid.UUID {
	out := make([]uuid.UUID, 0, len(p.rows))
	for _, r := range p.rows {
		out = append(out, r.id)
	}
	return out
}

// PlanPair plans the end of every live grant of one principal in one organization.
func PlanPair(ctx context.Context, r Reader, quotedSchema string, orgID, principalID uuid.UUID) (*Plan, error) {
	return read(ctx, r, &Plan{q: quotedSchema, scope: scopePair, org: orgID, arg: principalID})
}

// PlanOrg plans the end of every live grant of one organization.
func PlanOrg(ctx context.Context, r Reader, quotedSchema string, orgID uuid.UUID) (*Plan, error) {
	return read(ctx, r, &Plan{q: quotedSchema, scope: scopeOrg, org: orgID})
}

// PlanGrant plans the end of one grant, when it is live.
func PlanGrant(ctx context.Context, r Reader, quotedSchema string, orgID, grantID uuid.UUID) (*Plan, error) {
	return read(ctx, r, &Plan{q: quotedSchema, scope: scopeGrant, org: orgID, arg: grantID})
}

func (p *Plan) selectSQL(lock bool) string {
	sql := `SELECT id, principal_id FROM ` + p.q + `.resource_grants WHERE organization_id = $1 AND revoked_at IS NULL`
	switch p.scope {
	case scopePair:
		sql += ` AND principal_id = $2`
	case scopeGrant:
		sql += ` AND id = $2`
	}
	sql += ` ORDER BY id`
	if lock {
		sql += ` FOR UPDATE`
	}
	return sql
}

func (p *Plan) args() []any {
	if p.scope == scopeOrg {
		return []any{p.org}
	}
	return []any{p.org, p.arg}
}

func scan(rows Rows) ([]grantRow, error) {
	defer rows.Close()
	out := []grantRow{}
	for rows.Next() {
		var g grantRow
		if err := rows.Scan(&g.id, &g.principal); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].id[:], out[j].id[:]) < 0 })
	return out, nil
}

func read(ctx context.Context, r Reader, p *Plan) (*Plan, error) {
	rows, err := r.Query(ctx, p.selectSQL(false), p.args()...)
	if err != nil {
		return nil, err
	}
	if p.rows, err = scan(rows); err != nil {
		return nil, err
	}
	return p, nil
}

// EventFor is the resource_grant.revoked outbox event of one grant.
func EventFor(id uuid.UUID) audit.OutboxEvent {
	return audit.OutboxEvent{AggregateType: "resource_grant", AggregateID: provisioning.WireID("resource_grant", id.String()),
		EventType: "resource_grant.revoked", Change: map[string]any{"status": "revoked"}}
}

// EventType is the audit event type of one grant end.
const EventType = "auth.resource_grant.revoked"

// AuditFor is the audit row of one grant end: ids and closed values only.
func AuditFor(orgID, principalID, id uuid.UUID, a Actor) audit.AuditEvent {
	org, subject := orgID.String(), principalID.String()
	ev := audit.AuditEvent{EventType: EventType, Outcome: audit.OutcomeAllowed, OrganizationID: &org,
		SubjectPrincipalID: &subject, RequestID: a.RequestID,
		Attributes: map[string]any{"target_kind": "resource_grant", "target_id": id.String(), "reason_code": "grant_revoked",
			"resource_kind": "repository", "new_status": "revoked", "previous_status": "active", "changed_fields": []string{"revoked_at"}}}
	if a.PrincipalID != nil {
		actor := a.PrincipalID.String()
		ev.ActorPrincipalID = &actor
	}
	a.Caller.Annotate(ev.Attributes)
	return ev
}

// Events are the planned resource_grant.revoked events, one per grant.
func (p *Plan) Events() []audit.OutboxEvent {
	out := make([]audit.OutboxEvent, 0, len(p.rows))
	for _, g := range p.rows {
		out = append(out, EventFor(g.id))
	}
	return out
}

// Audit are the planned audit rows, one per grant, for audit.Mutation.Also.
func (p *Plan) Audit(a Actor) []audit.AuditEvent {
	out := make([]audit.AuditEvent, 0, len(p.rows))
	for _, g := range p.rows {
		out = append(out, AuditFor(p.org, g.principal, g.id, a))
	}
	return out
}

// Apply revokes the planned grants in the caller's transaction. A live set that
// differs from the plan is ErrStateMoved. revokedBy is the actor, NULL for a
// system caller.
func (p *Plan) Apply(ctx context.Context, tx audit.TxOps, revokedBy *uuid.UUID) error {
	if len(p.rows) == 0 {
		now, err := read(ctx, txReader{tx}, &Plan{q: p.q, scope: p.scope, org: p.org, arg: p.arg})
		if err != nil {
			return err
		}
		if len(now.rows) != 0 {
			return ErrStateMoved
		}
		return nil
	}
	rows, err := tx.Query(ctx, p.selectSQL(true), p.args()...)
	if err != nil {
		return err
	}
	now, err := scan(rows)
	if err != nil {
		return err
	}
	if len(now) != len(p.rows) {
		return ErrStateMoved
	}
	for i := range now {
		if now[i] != p.rows[i] {
			return ErrStateMoved
		}
	}
	var by any
	if revokedBy != nil {
		by = *revokedBy
	}
	tag, err := tx.Exec(ctx, `UPDATE `+p.q+`.resource_grants SET revoked_at = clock_timestamp(), revoked_by = $2, updated_at = clock_timestamp()
WHERE id = ANY($1::uuid[]) AND revoked_at IS NULL`, uuidArray(p.IDs()), by)
	if err != nil {
		return err
	}
	if int(tag.RowsAffected()) != len(p.rows) {
		return ErrStateMoved
	}
	return nil
}

// TxReader reads on a verb's transaction.
func TxReader(tx audit.TxOps) Reader { return txReader{tx} }

type txReader struct{ tx audit.TxOps }

func (t txReader) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	return t.tx.Query(ctx, sql, args...)
}

// uuidArray is the ids as one array literal: a statement inside audit.Commit
// has no parameter types to encode a []uuid.UUID with.
func uuidArray(ids []uuid.UUID) string {
	var b strings.Builder
	b.WriteByte('{')
	for i, id := range ids {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(id.String())
	}
	b.WriteByte('}')
	return b.String()
}

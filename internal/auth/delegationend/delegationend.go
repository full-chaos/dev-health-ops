// Package delegationend ends delegated (impersonation) sessions inside the
// caller's audit.Commit transaction. It is a leaf: platformrole and identity
// reach it without linking the session domain, and neither can be left
// unwired the way an interface can.
//
// A delegated session is a row of auth.sessions with an actor. The row's own
// writers (revoke, revoke-all, the organization verbs) reach it by id or by
// target; this package adds the two ways the target's writers cannot see: the
// actor's base session ended, or the actor lost the right to delegate.
package delegationend

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

// The audit events of a delegated session's life.
const (
	EventStarted = "auth.session.delegation_started"
	EventEnded   = "auth.session.delegation_ended"
)

// The revoked_reason of a delegated row this package ends.
const (
	ReasonBaseEnded  = "delegation_base_ended"
	ReasonActorEnded = "delegation_actor_ended"
	// ReasonSuperseded is a prior row the actor's next Start ended.
	ReasonSuperseded = "delegation_superseded"
)

// Row is one unrevoked delegated session.
type Row struct {
	ID, Target, Actor, Organization string
}

const rowColumns = `id::text, principal_id::text, actor_principal_id::text, organization_id::text`

func lock(ctx context.Context, tx audit.TxOps, q, where string, nowait bool, args ...any) ([]Row, error) {
	sql := `SELECT ` + rowColumns + ` FROM ` + q + `.sessions WHERE ` + where + `
		AND actor_principal_id IS NOT NULL AND revoked_at IS NULL ORDER BY id FOR UPDATE`
	if nowait {
		sql += ` NOWAIT`
	}
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Row
	for rows.Next() {
		var r Row
		if err := rows.Scan(&r.ID, &r.Target, &r.Actor, &r.Organization); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Lock locks, in id order, the unrevoked delegated rows among ids and the
// children of ids. With nowait a row another writer holds fails with
// lock_not_available, for a caller under the auth lock order's step 6a.
func Lock(ctx context.Context, tx audit.TxOps, q string, ids []string, nowait bool) ([]Row, error) {
	return lock(ctx, tx, q, `(id = ANY($1::uuid[]) OR delegated_from_session_id = ANY($1::uuid[]))`, nowait, ids)
}

// LockOfActor locks, in id order, the actor's unrevoked delegated rows.
func LockOfActor(ctx context.Context, tx audit.TxOps, q string, actor uuid.UUID, nowait bool) ([]Row, error) {
	return lock(ctx, tx, q, `actor_principal_id = $1`, nowait, actor)
}

// End revokes rows, with reason. The caller holds their locks. A delegated row
// has no refresh credential, so there is nothing else to end.
func End(ctx context.Context, tx audit.TxOps, q string, rows []Row, reason string) error {
	if len(rows) == 0 {
		return nil
	}
	ids := make([]string, len(rows))
	for i, r := range rows {
		ids[i] = r.ID
	}
	_, err := tx.Exec(ctx, `UPDATE `+q+`.sessions
		SET revoked_at = clock_timestamp(), revoked_reason = $2, updated_at = clock_timestamp()
		WHERE id = ANY($1::uuid[]) AND revoked_at IS NULL`, ids, reason)
	return err
}

// Record queues one EventEnded row per row, naming the real actor and the
// target (G-49), never the reason text.
func Record(ctx context.Context, tx audit.TxOps, rows []Row) error {
	for _, r := range rows {
		actor, target, org := r.Actor, r.Target, r.Organization
		event := audit.AuditEvent{
			EventType: EventEnded, Outcome: audit.OutcomeAllowed,
			ActorPrincipalID: &actor, SubjectPrincipalID: &target, OrganizationID: &org,
			RequestID: requestid.Audit(ctx),
			Attributes: map[string]any{
				"reason_code": "session_revoked", "target_kind": "session", "target_id": r.ID,
			},
		}
		if err := tx.AddAudit(event); err != nil {
			return err
		}
	}
	return nil
}

// EndOfActor ends every unrevoked delegated session the actor runs, with its
// audit rows, for a change that takes away the right to delegate: a platform
// role revoked, the principal disabled. It returns how many it ended.
func EndOfActor(ctx context.Context, tx audit.TxOps, q string, actor uuid.UUID) (int, error) {
	rows, err := LockOfActor(ctx, tx, q, actor, false)
	if err != nil {
		return 0, fmt.Errorf("delegationend: locking the sessions of an actor: %w", err)
	}
	if err := End(ctx, tx, q, rows, ReasonActorEnded); err != nil {
		return 0, fmt.Errorf("delegationend: ending the sessions of an actor: %w", err)
	}
	if err := Record(ctx, tx, rows); err != nil {
		return 0, err
	}
	return len(rows), nil
}

// Children are the rows of rows outside ids: the delegated rows a revoke of ids
// reached through their base session.
func Children(rows []Row, ids []string) []Row {
	direct := make(map[string]bool, len(ids))
	for _, id := range ids {
		direct[id] = true
	}
	var out []Row
	for _, r := range rows {
		if !direct[r.ID] {
			out = append(out, r)
		}
	}
	return out
}

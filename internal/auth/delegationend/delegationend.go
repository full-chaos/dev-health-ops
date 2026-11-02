// Package delegationend ends delegated (impersonation) sessions inside the
// caller's audit.Commit transaction. It is a leaf: platformrole and identity
// reach it without linking the session domain, and neither can be left
// unwired the way an interface can.
//
// A delegated session is a row of auth.sessions with an actor. The row's own
// writers (revoke, revoke-all, the organization verbs) reach it by id or by
// target; this package adds the two ways the target's writers cannot see: the
// actor's base session ended, or the actor lost the right to delegate.
//
// A delegated session also has go-api's record: a row of
// public.impersonation_sessions with the session's id, which go-api's readers
// and the OAuth actor check see, and impersonation_start and impersonation_stop
// rows of public.audit_logs. This package is auth-service's one writer of them,
// and a key compromise the one end that leaves them (KeepPublicRows).
package delegationend

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/publicaudit"
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

// The audit_logs actions and resource of a delegated session, go-api's names
// (internal/api/audit).
const (
	publicActionStart  = "impersonation_start"
	publicActionStop   = "impersonation_stop"
	publicResourceType = "session"
)

// PublicRows says whether Record ends go-api's rows.
type PublicRows bool

const (
	// EndPublicRows ends the public rows with the session rows.
	EndPublicRows PublicRows = true
	// KeepPublicRows leaves them, for a key compromise alone, on every install. It
	// runs as the migration login, which holds no write on go-api's tables by
	// design, and a security end must not fail. The row stays open to its
	// expires_at, at most 15 minutes; its one effect is that the actor's acr mint
	// refuses until then. The auth audit row holds the end, and no
	// impersonation_stop row is written.
	KeepPublicRows PublicRows = false
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
// target (G-49), never the reason text, and ends the public rows as public says.
// Every end of a delegated row reaches this function.
func Record(ctx context.Context, tx audit.TxOps, rows []Row, public PublicRows) error {
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
	if len(rows) == 0 || public == KeepPublicRows {
		return nil
	}
	return endPublicRows(ctx, tx, rows)
}

// endPublicRows ends the public rows of rows and writes one stop row for each it
// ended. A row go-api already ended, or no row, writes none. The row it ended
// holds foreign keys to the target's organization and to both users, under its
// lock, so a stop row cannot fail a security end.
func endPublicRows(ctx context.Context, tx audit.TxOps, rows []Row) error {
	byID := make(map[string]Row, len(rows))
	ids := make([]string, len(rows))
	for i, r := range rows {
		byID[r.ID], ids[i] = r, r.ID
	}
	ended, err := tx.Query(ctx, `UPDATE public.impersonation_sessions SET ended_at = clock_timestamp()
		WHERE id = ANY($1::uuid[]) AND ended_at IS NULL RETURNING id::text`, ids)
	if err != nil {
		return err
	}
	defer ended.Close()
	var endedIDs []string
	for ended.Next() {
		var id string
		if err := ended.Scan(&id); err != nil {
			return err
		}
		endedIDs = append(endedIDs, id)
	}
	if err := ended.Err(); err != nil {
		return err
	}
	ended.Close()
	for _, id := range endedIDs {
		r := byID[id]
		if err := publicRow(ctx, tx, publicActionStop, r.Organization, r.Actor, r.Target); err != nil {
			return err
		}
	}
	return nil
}

// EndLegacy ends the actor's open public rows that have no delegated session:
// rows go-api started. It runs in a Start, after the actor's delegated rows
// ended, as go-api's own start ends them, expired ones too. The target is empty
// on a row whose target user was deleted.
func EndLegacy(ctx context.Context, tx audit.TxOps, actor string) error {
	rows, err := tx.Query(ctx, `UPDATE public.impersonation_sessions SET ended_at = clock_timestamp()
		WHERE admin_user_id = $1::uuid AND ended_at IS NULL
		RETURNING target_org_id::text, COALESCE(target_user_id::text, '')`, actor)
	if err != nil {
		return err
	}
	defer rows.Close()
	type legacy struct{ org, target string }
	var ended []legacy
	for rows.Next() {
		var l legacy
		if err := rows.Scan(&l.org, &l.target); err != nil {
			return err
		}
		ended = append(ended, l)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()
	for _, l := range ended {
		if err := publicRow(ctx, tx, publicActionStop, l.org, actor, l.target); err != nil {
			return err
		}
	}
	return nil
}

// Begin writes go-api's record of a started delegation in the transaction of
// r's session row: the public row, with the session's id and times, and its
// impersonation_start row. role is the target's role, under the caller's
// membership lock.
func Begin(ctx context.Context, tx audit.TxOps, q string, r Row, role string) error {
	if _, err := tx.Exec(ctx, `INSERT INTO public.impersonation_sessions
		(id, admin_user_id, target_user_id, target_org_id, target_role, created_at, expires_at)
		SELECT id, actor_principal_id, principal_id, organization_id, $2::text, created_at, expires_at
		FROM `+q+`.sessions WHERE id = $1::uuid`, r.ID, role); err != nil {
		return err
	}
	return publicRow(ctx, tx, publicActionStart, r.Organization, r.Actor, r.Target)
}

// publicRow is go-api's audit_logs row of a delegated session: the actor in the
// target's organization, resource session/<target> (admin/impersonation.go).
func publicRow(ctx context.Context, tx audit.TxOps, action, org, actor, target string) error {
	orgID, err := uuid.Parse(org)
	if err != nil {
		return fmt.Errorf("delegationend: the organization of a public row: %w", err)
	}
	actorID, err := uuid.Parse(actor)
	if err != nil {
		return fmt.Errorf("delegationend: the actor of a public row: %w", err)
	}
	return publicaudit.Write(ctx, tx, publicaudit.Row{OrgID: orgID, UserID: &actorID, Action: action,
		ResourceType: publicResourceType, ResourceID: target})
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
	if err := Record(ctx, tx, rows, EndPublicRows); err != nil {
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

package invite

import (
	"bytes"
	"context"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
)

// OrgRevocation is the revocation of every pending invite of one organization,
// planned from a lock-free read for an organization deprovision's own
// audit.Commit. An invite is pending by its auth.invites row, or, for one
// go-api made before auth-service owned users, by its public.org_invites row
// alone; the second kind is taken over into auth.invites, as Revoke does.
type OrgRevocation struct {
	orgID uuid.UUID
	auth  []uuid.UUID
	pub   []uuid.UUID
	// inviters are the go-api-only invites' inviters: the take-over insert
	// names them (a foreign key, so a KEY SHARE lock on each principal).
	inviters []uuid.UUID
}

// Referenced are the principals the take-over inserts name, for the caller to
// lock before the organization row (membership.Store.LockPrincipals).
func (r *OrgRevocation) Referenced() []uuid.UUID { return append([]uuid.UUID(nil), r.inviters...) }

// Len is the number of invites the revocation ends.
func (r *OrgRevocation) Len() int { return len(r.auth) + len(r.pub) }

// Events are the planned invite.revoked events, one per invite.
func (r *OrgRevocation) Events() []audit.OutboxEvent {
	out := make([]audit.OutboxEvent, 0, r.Len())
	for _, id := range append(append([]uuid.UUID(nil), r.auth...), r.pub...) {
		out = append(out, eventFor(verbRevoke, id))
	}
	return out
}

// PlanRevokeAll plans the revocation of every pending invite of orgID.
func (s *Store) PlanRevokeAll(ctx context.Context, orgID uuid.UUID) (*OrgRevocation, error) {
	if !s.ownsUsers {
		return nil, ErrNotOwned
	}
	query := func(sql string, args ...any) (idRows, error) { return s.pool.Query(ctx, sql, args...) }
	return s.pendingOf(orgID, query, false)
}

// ApplyRevokeAll writes a planned revocation inside the caller's audit.Commit.
// The caller holds the organization row FOR UPDATE (step 3); this takes the
// pending auth.invites rows, then go-api's pending public-only rows, FOR UPDATE
// by id (step 4). Rows that differ from the plan are membership.ErrStateMoved:
// plan again.
func (s *Store) ApplyRevokeAll(ctx context.Context, tx audit.TxOps, r *OrgRevocation) error {
	query := func(sql string, args ...any) (idRows, error) {
		rows, err := tx.Query(ctx, sql, args...)
		return rows, err
	}
	locked, err := s.pendingOf(r.orgID, query, true)
	if err != nil {
		return err
	}
	if !equalIDs(locked.auth, r.auth) || !equalIDs(locked.pub, r.pub) {
		return membership.ErrStateMoved
	}
	if r.Len() == 0 {
		return nil
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return err
	}
	if len(r.auth) > 0 {
		if _, err := tx.Exec(ctx, `UPDATE `+s.q+`.invites SET status = 'revoked', updated_at = $2 WHERE id = ANY($1::uuid[])`,
			uuidArray(r.auth), now); err != nil {
			return err
		}
	}
	if len(r.pub) > 0 {
		if _, err := tx.Exec(ctx, `INSERT INTO `+s.q+`.invites
	(id, organization_id, email_normalized, role, token_hash, invited_by, status, expires_at, created_at, updated_at)
SELECT p.id, p.org_id, p.email, p.role, p.token_hash, (SELECT a.id FROM `+s.q+`.principals a WHERE a.id = p.invited_by_id),
       'revoked', p.expires_at, p.created_at, $2
FROM public.org_invites p WHERE p.id = ANY($1::uuid[])`, uuidArray(r.pub), now); err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `UPDATE public.org_invites SET status = 'revoked', updated_at = $2 WHERE id = ANY($1::uuid[])`,
		uuidArray(append(append([]uuid.UUID(nil), r.auth...), r.pub...)), now)
	return err
}

func (s *Store) pendingOf(orgID uuid.UUID, query func(string, ...any) (idRows, error), lock bool) (*OrgRevocation, error) {
	r := &OrgRevocation{orgID: orgID}
	lockAuth, publicLock := "", ""
	if lock {
		lockAuth, publicLock = " FOR UPDATE", " FOR UPDATE OF p"
	}
	for _, step := range []struct {
		sql      string
		out      *[]uuid.UUID
		inviters *[]uuid.UUID
	}{
		{`SELECT id, NULL::uuid FROM ` + s.q + `.invites WHERE organization_id = $1 AND status = 'pending' ORDER BY id` + lockAuth, &r.auth, nil},
		// go-api's pending invites with no auth row.
		{`SELECT p.id, p.invited_by_id FROM public.org_invites p WHERE p.org_id = $1 AND p.status = 'pending'
  AND NOT EXISTS (SELECT 1 FROM ` + s.q + `.invites a WHERE a.id = p.id) ORDER BY p.id` + publicLock, &r.pub, &r.inviters},
	} {
		rows, err := query(step.sql, orgID)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id uuid.UUID
			var inviter *uuid.UUID
			if err := rows.Scan(&id, &inviter); err != nil {
				rows.Close()
				return nil, err
			}
			*step.out = append(*step.out, id)
			if inviter != nil && step.inviters != nil {
				*step.inviters = append(*step.inviters, *inviter)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// idRows is the read surface pendingOf needs from the pool or the transaction.
type idRows interface {
	Next() bool
	Scan(dest ...any) error
	Close()
	Err() error
}

func equalIDs(a, b []uuid.UUID) bool {
	if len(a) != len(b) {
		return false
	}
	sa, sb := sortedIDs(a), sortedIDs(b)
	for i := range sa {
		if sa[i] != sb[i] {
			return false
		}
	}
	return true
}

func sortedIDs(ids []uuid.UUID) []uuid.UUID {
	out := append([]uuid.UUID(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i][:], out[j][:]) < 0 })
	return out
}

// uuidArray is ids as one array literal for an explicit ::uuid[] cast: a
// statement inside audit.Commit has no parameter types to encode a slice with.
func uuidArray(ids []uuid.UUID) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = id.String()
	}
	return "{" + strings.Join(parts, ",") + "}"
}

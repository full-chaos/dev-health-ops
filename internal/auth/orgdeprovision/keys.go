package orgdeprovision

import (
	"bytes"
	"context"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/grantend"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
)

// keyClass is serviceaccount.KeyClass (TestKeyClassIsTheServiceAccountClass).
const keyClass = "service_account_key"

// keyRevokedReason is the credential_registry.revoked_reason of a key a
// deprovision ends: the column holds the closed credential vocabulary, and the
// organization that owns the key asked for its end.
const keyRevokedReason = "owner_request"

// EventKeyRevoked is the audit event of one service-account key a deprovision
// revokes, the one serviceaccount.RevokeKey writes.
const EventKeyRevoked = "auth.service_account.key_revoked"

type keyRow struct {
	id      uuid.UUID
	account uuid.UUID
	owner   *uuid.UUID
}

// keyPlan is the organization's live service-account keys (pm answers-12680-1
// Q1 (b)). Keys are auth-owned data: the deprovision revokes them in its own
// commit, so a key of a deprovisioned organization is refused at once, before
// go-api's public.organizations row turns inactive.
type keyPlan struct {
	q    string
	org  uuid.UUID
	rows []keyRow
}

func (p *keyPlan) selectSQL(lock bool) string {
	sql := `SELECT c.id, c.principal_id, sa.owner_principal_id FROM ` + p.q + `.credential_registry c
JOIN ` + p.q + `.service_accounts sa ON sa.principal_id = c.principal_id
WHERE sa.organization_id = $1 AND c.credential_class = '` + keyClass + `' AND c.revoked_at IS NULL AND c.expires_at > clock_timestamp()
ORDER BY c.id`
	if lock {
		sql += ` FOR UPDATE OF c`
	}
	return sql
}

func (p *keyPlan) read(ctx context.Context, db grantend.Reader, lock bool) ([]keyRow, error) {
	rows, err := db.Query(ctx, p.selectSQL(lock), p.org)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []keyRow{}
	for rows.Next() {
		var k keyRow
		if err := rows.Scan(&k.id, &k.account, &k.owner); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].id[:], out[j].id[:]) < 0 })
	return out, nil
}

// accounts are the service-account principals the planned keys belong to: their
// audit rows name them (a foreign key, KEY SHARE at commit), so the deprovision
// takes them with the other principal rows, step 1 of the auth lock order.
func (p *keyPlan) accounts() []uuid.UUID {
	out := make([]uuid.UUID, 0, len(p.rows))
	for _, k := range p.rows {
		out = append(out, k.account)
	}
	return out
}

// events are one service_account.key_changed event per planned key.
func (p *keyPlan) events() []audit.OutboxEvent {
	out := make([]audit.OutboxEvent, 0, len(p.rows))
	for _, k := range p.rows {
		out = append(out, audit.OutboxEvent{AggregateType: "service_account", AggregateID: provisioning.WireID("service_account", k.account.String()),
			EventType: "service_account.key_changed", Change: map[string]any{"status": "revoked", "credential_id": "crd_" + k.id.String()}})
	}
	return out
}

// audit is one auth.service_account.key_revoked row per planned key, in the
// shape serviceaccount.RevokeKey writes.
func (p *keyPlan) audit(a Actor) []audit.AuditEvent {
	out := make([]audit.AuditEvent, 0, len(p.rows))
	org := p.org.String()
	for _, k := range p.rows {
		sub := k.account.String()
		ev := audit.AuditEvent{EventType: EventKeyRevoked, Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &sub, OrganizationID: &org,
			RequestID: a.RequestID, Attributes: map[string]any{"target_kind": "service_account", "target_id": sub, "reason_code": "credential_revoked",
				"changed_fields": []string{"revoked_at"}, "new_status": "revoked", "credential_class": keyClass, "credential_id": k.id.String()}}
		if k.owner != nil {
			ev.Attributes["principal_id"] = k.owner.String()
		}
		if a.PrincipalID != nil {
			actor := a.PrincipalID.String()
			ev.ActorPrincipalID = &actor
		}
		a.Caller.Annotate(ev.Attributes)
		out = append(out, ev)
	}
	return out
}

// apply revokes the planned keys in the deprovision's transaction, after the
// organization row and its memberships, sessions and grants: the key rows FOR
// UPDATE in id order. A live set that differs from the plan is
// membership.ErrStateMoved: plan again.
func (p *keyPlan) apply(ctx context.Context, tx audit.TxOps) error {
	now, err := p.read(ctx, grantend.TxReader(tx), true)
	if err != nil {
		return err
	}
	if len(now) != len(p.rows) {
		return membership.ErrStateMoved
	}
	for i := range now {
		if now[i].id != p.rows[i].id || now[i].account != p.rows[i].account {
			return membership.ErrStateMoved
		}
	}
	if len(now) == 0 {
		return nil
	}
	ids := make([]string, 0, len(now))
	for _, k := range now {
		ids = append(ids, k.id.String())
	}
	tag, err := tx.Exec(ctx, `UPDATE `+p.q+`.credential_registry SET revoked_at = clock_timestamp(), revoked_reason = $2, updated_at = clock_timestamp()
WHERE id = ANY($1::uuid[]) AND revoked_at IS NULL`, "{"+strings.Join(ids, ",")+"}", keyRevokedReason)
	if err != nil {
		return err
	}
	if int(tag.RowsAffected()) != len(now) {
		return membership.ErrStateMoved
	}
	return nil
}

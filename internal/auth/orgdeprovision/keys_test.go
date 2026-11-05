package orgdeprovision

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/grantend"
	"github.com/full-chaos/dev-health-ops/internal/auth/invite"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/auth/serviceaccount"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/auditwire"
)

func TestKeyClassIsTheServiceAccountClass(t *testing.T) {
	if keyClass != serviceaccount.KeyClass {
		t.Fatalf("keyClass %q, serviceaccount.KeyClass %q", keyClass, serviceaccount.KeyClass)
	}
}

// A key a deprovision ends names the deprovision as the reason, not the
// operator's owner_request (CHAOS-12941).
func TestKeyRevokedReasonIsOrgDeprovisioned(t *testing.T) {
	if keyRevokedReason != "org_deprovisioned" {
		t.Fatalf("keyRevokedReason = %q, want org_deprovisioned", keyRevokedReason)
	}
	if keyRevokedReason == serviceaccount.KeyReasonOwnerRequest {
		t.Fatal("the deprovision reason equals the operator reason")
	}
}

// pm answers-12680-1 Q1 (b): one key_revoked audit row and one key_changed
// event per live key, in serviceaccount.RevokeKey's shape, for the key's own
// account and the deprovisioned organization.
func TestKeyPlanWritesOneAuditRowAndOneEventPerKey(t *testing.T) {
	org, owner, actor, request := uuid.New(), uuid.New(), uuid.New(), uuid.NewString()
	p := &keyPlan{q: `"auth"`, org: org, rows: []keyRow{{id: uuid.New(), account: uuid.New(), owner: &owner}, {id: uuid.New(), account: uuid.New()}}}
	rows, events := p.audit(Actor{PrincipalID: &actor, RequestID: &request}), p.events()
	if len(rows) != 2 || len(events) != 2 {
		t.Fatalf("%d audit rows and %d events for 2 keys", len(rows), len(events))
	}
	for i, k := range p.rows {
		row, e := rows[i], events[i]
		auditwire.MustValidate(t, row)
		if err := audit.ValidateAttributes(row.Attributes); err != nil {
			t.Fatalf("key %d attributes: %v", i, err)
		}
		if row.EventType != EventKeyRevoked || *row.OrganizationID != org.String() || *row.SubjectPrincipalID != k.account.String() ||
			*row.ActorPrincipalID != actor.String() || *row.RequestID != request || row.Attributes["credential_id"] != k.id.String() ||
			row.Attributes["credential_class"] != serviceaccount.KeyClass || row.Attributes["new_status"] != "revoked" {
			t.Fatalf("key %d audit row = %+v", i, row)
		}
		if err := provisioning.CheckEvent(e.AggregateType, e.AggregateID, e.EventType, e.Change); err != nil {
			t.Fatalf("key %d event: %v", i, err)
		}
		if e.EventType != "service_account.key_changed" || e.AggregateID != provisioning.WireID("service_account", k.account.String()) ||
			e.Change["credential_id"] != "crd_"+k.id.String() || e.Change["status"] != "revoked" {
			t.Fatalf("key %d event = %+v", i, e)
		}
	}
	if rows[0].Attributes["principal_id"] != owner.String() {
		t.Fatalf("owner = %v", rows[0].Attributes["principal_id"])
	}
}

// The key read is the deprovisioned organization's live service-account keys
// only, and the locked read holds the key rows.
func TestKeyPlanReadsOnlyTheOrganizationsLiveKeys(t *testing.T) {
	p := &keyPlan{q: `"auth"`, org: uuid.New()}
	for _, lock := range []bool{false, true} {
		sql := p.selectSQL(lock)
		for _, want := range []string{"sa.organization_id = $1", "c.credential_class = 'service_account_key'", "c.revoked_at IS NULL",
			"c.expires_at > clock_timestamp()", "JOIN \"auth\".service_accounts sa ON sa.principal_id = c.principal_id"} {
			if !strings.Contains(sql, want) {
				t.Fatalf("lock=%v: %q lacks %q", lock, sql, want)
			}
		}
		if strings.Contains(sql, "FOR UPDATE OF c") != lock {
			t.Fatalf("lock=%v: %q", lock, sql)
		}
	}
}

// The deprovision's one commit carries the key ends: the key rows' audit rows
// with its own, and their events, and the key accounts are among the principal
// rows it locks first.
func TestOnceCommitsTheKeyEndsInTheDeprovision(t *testing.T) {
	var got audit.Mutation
	s := &Service{commit: func(_ context.Context, m audit.Mutation) error { got = m; return audit.ErrNoStateChange }}
	org, account := uuid.New(), uuid.New()
	keys := &keyPlan{q: `"auth"`, org: org, rows: []keyRow{{id: uuid.New(), account: account}}}
	p := plan{active: true, members: &membership.OrgRemoval{}, invites: &invite.OrgRevocation{}, grants: &grantend.Plan{}, keys: keys}
	if _, err := s.once(context.Background(), Input{OrgID: org, Authorize: func(context.Context, audit.TxOps, uuid.UUID) error { return nil }}, p); err != nil {
		t.Fatal(err)
	}
	var revoked, changed int
	for _, row := range got.Also {
		if row.EventType == EventKeyRevoked {
			revoked++
		}
	}
	for _, e := range got.Events {
		if e.EventType == "service_account.key_changed" {
			changed++
		}
	}
	if revoked != 1 || changed != 1 || got.Audit.EventType != EventDeprovisioned {
		t.Fatalf("commit: %d key_revoked rows, %d key_changed events, audit %q", revoked, changed, got.Audit.EventType)
	}
}

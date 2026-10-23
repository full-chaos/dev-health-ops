//go:build integration

package identity

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

func jitRequest() ProvisionRequest {
	key := ExternalKey{Issuer: "https://idp.example.com/" + uuid.NewString()[:8], Provider: Provider(uuid.NewString()), Subject: "jit-" + uuid.NewString()}
	return ProvisionRequest{Key: key, Email: "jit-" + uuid.NewString()[:8] + "@example.com", FullName: "Jit User", Actor: SelfSignUp(nil)}
}

// rowsOf counts the rows one new account and its join leave behind. Nothing
// else in the shared database carries the email, the key subject or the
// organization of the test.
func (e *env) jitRows(t *testing.T, ctx context.Context, req ProvisionRequest, org uuid.UUID) (principals, users, identities, memberships int) {
	t.Helper()
	lower, _ := NormalizeEmail(req.Email)
	users = e.count(t, ctx, `SELECT count(*) FROM auth.users WHERE email_lower = $1`, lower)
	principals = e.count(t, ctx, `SELECT count(*) FROM auth.principals p JOIN auth.users u ON u.principal_id = p.id WHERE u.email_lower = $1`, lower)
	identities = e.count(t, ctx, `SELECT count(*) FROM auth.external_identities WHERE subject = $1`, req.Key.Subject)
	memberships = e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1`, org)
	return
}

func TestProvisionAndJoinWritesOneAccountIdentityAndMembership(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org := e.seedOrg(t, ctx)
	req := jitRequest()
	ran := 0
	id, err := e.owned.ProvisionAndJoin(ctx, req, OrgJoin{OrgID: org, Role: "member"}, func(context.Context, audit.TxOps, uuid.UUID) error {
		ran++
		return nil
	})
	if err != nil {
		t.Fatalf("ProvisionAndJoin: %v", err)
	}
	if p, u, i, m := e.jitRows(t, ctx, req, org); p != 1 || u != 1 || i != 1 || m != 1 || ran != 1 {
		t.Fatalf("rows principal/user/identity/membership = %d/%d/%d/%d, hook ran %d; want 1/1/1/1, 1", p, u, i, m, ran)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2 AND status = 'active' AND role = 'member' AND joined_at IS NOT NULL`, org, id); n != 1 {
		t.Fatal("the membership is not active with its role")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events o JOIN auth.memberships m ON o.aggregate_id = 'mbr_' || m.id::text
WHERE o.event_type = 'membership.activated' AND m.organization_id = $1 AND m.principal_id = $2`, org, id); n != 1 {
		t.Fatalf("membership.activated outbox events = %d, want 1", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM public.users WHERE id = $1`, id); n != 0 {
		t.Fatal("the join wrote public.users")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM public.memberships WHERE user_id = $1`, id); n != 0 {
		t.Fatal("the join wrote public.memberships")
	}
}

func TestProvisionAndJoinFailureAfterTheMembershipWritesNothing(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org := e.seedOrg(t, ctx)
	req := jitRequest()
	boom := errors.New("forced failure after the membership")
	_, err := e.owned.ProvisionAndJoin(ctx, req, OrgJoin{OrgID: org, Role: "member"}, func(context.Context, audit.TxOps, uuid.UUID) error { return boom })
	if err == nil {
		t.Fatal("a failing hook was not an error")
	}
	if p, u, i, m := e.jitRows(t, ctx, req, org); p+u+i+m != 0 {
		t.Fatalf("rows left after a rollback: principal/user/identity/membership = %d/%d/%d/%d", p, u, i, m)
	}
}

func TestProvisionAndJoinRollsBackWhenTheMembershipInsertFails(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org := e.seedOrg(t, ctx)
	req := jitRequest()
	// A role outside the closed catalog fails the membership row's CHECK after
	// the principal, user and identity rows are written.
	if _, err := e.owned.ProvisionAndJoin(ctx, req, OrgJoin{OrgID: org, Role: "superuser"}, nil); err == nil {
		t.Fatal("a role outside the catalog was accepted")
	}
	if p, u, i, m := e.jitRows(t, ctx, req, org); p+u+i+m != 0 {
		t.Fatalf("rows left after a failed membership insert: %d/%d/%d/%d", p, u, i, m)
	}
	lower, _ := NormalizeEmail(req.Email)
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.principal.provisioned' AND request_id IS NULL
AND subject_principal_id IN (SELECT principal_id FROM auth.users WHERE email_lower = $1)`, lower); n != 0 {
		t.Fatal("an audit row survived the rollback")
	}
}

func TestProvisionAndJoinOfAnUnknownOrganizationWritesNothing(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	req := jitRequest()
	if _, err := e.owned.ProvisionAndJoin(ctx, req, OrgJoin{OrgID: uuid.New(), Role: "member"}, nil); err == nil {
		t.Fatal("an unknown organization was joined")
	}
	if p, u, i, _ := e.jitRows(t, ctx, req, uuid.New()); p+u+i != 0 {
		t.Fatalf("rows left: %d/%d/%d", p, u, i)
	}
}

func TestProvisionAndJoinOfAHeldKeyAddsNoMembership(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org := e.seedOrg(t, ctx)
	first := jitRequest()
	if _, err := e.owned.ProvisionFederated(ctx, first); err != nil {
		t.Fatal(err)
	}
	second := jitRequest()
	second.Key = first.Key
	if _, err := e.owned.ProvisionAndJoin(ctx, second, OrgJoin{OrgID: org, Role: "member"}, nil); !errors.Is(err, ErrIdentityTaken) {
		t.Fatalf("held key = %v, want ErrIdentityTaken", err)
	}
	if _, _, _, m := e.jitRows(t, ctx, second, org); m != 0 {
		t.Fatalf("memberships after a refused provision = %d", m)
	}
}

func TestJoinOrgJoinsOnceAndLeavesAMemberAlone(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org := e.seedOrg(t, ctx)
	id, err := e.owned.ProvisionFederated(ctx, jitRequest())
	if err != nil {
		t.Fatal(err)
	}
	hook := 0
	after := func(context.Context, audit.TxOps, uuid.UUID) error { hook++; return nil }
	joined, err := e.owned.JoinOrg(ctx, id, OrgJoin{OrgID: org, Role: "member"}, SelfSignUp(nil), after)
	if err != nil || !joined || hook != 1 {
		t.Fatalf("first join = %v, %v, hook %d", joined, err, hook)
	}
	joined, err = e.owned.JoinOrg(ctx, id, OrgJoin{OrgID: org, Role: "admin"}, SelfSignUp(nil), after)
	if err != nil || joined || hook != 1 {
		t.Fatalf("second join = %v, %v, hook %d; want no join and no hook", joined, err, hook)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2 AND role = 'member'`, org, id); n != 1 {
		t.Fatal("the member's row changed or was duplicated")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.membership.added' AND subject_principal_id = $1`, id); n != 1 {
		t.Fatalf("membership audit rows = %d, want 1", n)
	}
}

func TestJoinOrgFailureWritesNothing(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org := e.seedOrg(t, ctx)
	id, err := e.owned.ProvisionFederated(ctx, jitRequest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.owned.JoinOrg(ctx, id, OrgJoin{OrgID: org, Role: "member"}, SelfSignUp(nil),
		func(context.Context, audit.TxOps, uuid.UUID) error { return errors.New("forced failure") }); err == nil {
		t.Fatal("a failing hook was not an error")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1`, org); n != 0 {
		t.Fatalf("memberships after a rollback = %d", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.membership.added' AND subject_principal_id = $1`, id); n != 0 {
		t.Fatal("an audit row survived the rollback")
	}
}

func TestJoinOrgRefusals(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org := e.seedOrg(t, ctx)
	id, err := e.owned.ProvisionFederated(ctx, jitRequest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.JoinOrg(ctx, id, OrgJoin{OrgID: org, Role: "member"}, SelfSignUp(nil), nil); !errors.Is(err, ErrSignupNotAvailable) {
		t.Fatalf("join before the cutover = %v, want ErrSignupNotAvailable", err)
	}
	if _, err := e.owned.JoinOrg(ctx, uuid.New(), OrgJoin{OrgID: org, Role: "member"}, SelfSignUp(nil), nil); !errors.Is(err, ErrPrincipalUnavailable) {
		t.Fatalf("join of a missing principal = %v, want ErrPrincipalUnavailable", err)
	}
	subject := id.String()
	if err := audit.Commit(ctx, e.runtime, e.owned.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `UPDATE auth.principals SET disabled_at = now() WHERE id = $1`, id)
			return err
		},
		Audit: audit.AuditEvent{
			EventType: "auth.principal.disabled", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &subject,
			Attributes: map[string]any{"reason_code": "principal_disabled", "target_kind": "principal"},
		},
		OffStream: audit.OffStreamSessionLifecycle,
	}); err != nil {
		t.Fatalf("disable the principal: %v", err)
	}
	if _, err := e.owned.JoinOrg(ctx, id, OrgJoin{OrgID: org, Role: "member"}, SelfSignUp(nil), nil); !errors.Is(err, ErrPrincipalUnavailable) {
		t.Fatalf("join of a disabled principal = %v, want ErrPrincipalUnavailable", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1`, org); n != 0 {
		t.Fatalf("a refused join wrote %d memberships", n)
	}
}

func TestJoinOrgNeverReactivatesASuspendedMembership(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org := e.seedOrg(t, ctx)
	id, err := e.owned.ProvisionFederated(ctx, jitRequest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.owned.JoinOrg(ctx, id, OrgJoin{OrgID: org, Role: "member"}, SelfSignUp(nil), nil); err != nil {
		t.Fatal(err)
	}
	subject := id.String()
	err = audit.Commit(ctx, e.runtime, e.owned.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `UPDATE auth.memberships SET status = 'suspended' WHERE organization_id = $1 AND principal_id = $2`, org, id)
			return err
		},
		Audit: audit.AuditEvent{
			EventType: "auth.membership.status_changed", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &subject,
			Attributes: map[string]any{"reason_code": "membership_status_changed", "target_kind": "membership"},
		},
		OffStream: audit.OffStreamSessionLifecycle,
	})
	if err != nil {
		t.Fatalf("suspend the membership: %v", err)
	}
	if _, err := e.owned.JoinOrg(ctx, id, OrgJoin{OrgID: org, Role: "member"}, SelfSignUp(nil), nil); !errors.Is(err, ErrMembershipNotActive) {
		t.Fatalf("join onto a suspended membership = %v, want ErrMembershipNotActive", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2 AND status = 'suspended'`, org, id); n != 1 {
		t.Fatal("the suspended membership changed")
	}
}

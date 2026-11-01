//go:build integration

package platformrole

import (
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

func (e *env) ownerRows(t *testing.T, principal uuid.UUID) int {
	t.Helper()
	return e.count(t, ctxT(t), `SELECT count(*) FROM auth.platform_role_assignments WHERE principal_id = $1 AND role_key = 'platform_owner'`, principal)
}

// Q2: the API store refuses a platform_owner grant, writes no assignment and no
// outbox event, and records the refusal as a denied audit row.
func TestAPIGrantOfPlatformOwnerIsRefusedAndAudited(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	admin := e.user(t, ctx, true, true)
	target := e.user(t, ctx, true, false)
	outbox := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events`)
	if _, err := e.store.Grant(ctx, GrantInput{Actor: admin, Principal: target, Role: PlatformOwner}); !errors.Is(err, ErrPlatformOwnerCLIOnly) {
		t.Fatalf("API grant of platform_owner: %v, want ErrPlatformOwnerCLIOnly", err)
	}
	if n := e.ownerRows(t, target); n != 0 {
		t.Fatalf("the refused grant wrote %d owner rows", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events`); n != outbox {
		t.Fatalf("the refused grant wrote %d outbox rows", n-outbox)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.platform_owner.grant_refused'
		AND outcome = 'denied' AND actor_principal_id = $1 AND subject_principal_id = $2`, admin.String(), target.String()); n != 1 {
		t.Fatalf("grant refusal audit rows = %d, want 1", n)
	}
}

func TestAPIRevokeOfPlatformOwnerIsRefusedAndAudited(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	admin := e.user(t, ctx, true, true)
	target := e.user(t, ctx, true, false)
	if _, err := e.bootstrapStore(t).GrantOwner(ctx, target); err != nil {
		t.Fatalf("GrantOwner: %v", err)
	}
	if err := e.store.Revoke(ctx, RevokeInput{Actor: admin, Principal: target, Role: PlatformOwner}); !errors.Is(err, ErrPlatformOwnerCLIOnly) {
		t.Fatalf("API revoke of platform_owner: %v, want ErrPlatformOwnerCLIOnly", err)
	}
	if n := e.ownerRows(t, target); n != 1 {
		t.Fatalf("owner rows after the refused revoke = %d, want 1", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.platform_owner.revoke_refused'
		AND outcome = 'denied' AND actor_principal_id = $1 AND subject_principal_id = $2`, admin.String(), target.String()); n != 1 {
		t.Fatalf("revoke refusal audit rows = %d, want 1", n)
	}
}

// The CLI grant is permanent, self-granted (never a NULL granter), audited with
// no actor principal and the operator CLI as its source, and emits one outbox
// event and one aggregate revision.
func TestOperatorGrantOwnerCommitsAuditOutboxAndRevision(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	target := e.user(t, ctx, true, false)
	id, err := e.bootstrapStore(t).GrantOwner(ctx, target)
	if err != nil {
		t.Fatalf("GrantOwner: %v", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE id = $1 AND principal_id = $2
		AND role_key = 'platform_owner' AND granted_by = $2 AND expires_at IS NULL`, id, target); n != 1 {
		t.Fatal("the owner row is not a permanent self-granted platform_owner")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.platform_role.granted'
		AND outcome = 'allowed' AND actor_principal_id IS NULL AND subject_principal_id = $1
		AND attributes->>'source' = 'operator-cli' AND attributes->>'reason_code' = 'role_granted'`, target.String()); n != 1 {
		t.Fatalf("owner grant audit rows = %d, want 1", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = 'platform_role_assignment.granted' AND aggregate_id = $1`, "pra_"+id.String()); n != 1 {
		t.Fatalf("owner grant outbox rows = %d, want 1", n)
	}
	if rev := e.revision(t, ctx, id); rev != 1 {
		t.Fatalf("revision after the owner grant = %d, want 1", rev)
	}
}

func TestOperatorGrantOwnerRefusals(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	s := e.bootstrapStore(t)
	target := e.user(t, ctx, true, false)
	if _, err := s.GrantOwner(ctx, target); err != nil {
		t.Fatal(err)
	}
	disabled := e.user(t, ctx, false, false)
	grants := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.platform_role.granted'`)
	if _, err := s.GrantOwner(ctx, target); !errors.Is(err, ErrAlreadyHeld) {
		t.Errorf("a second owner grant: %v, want ErrAlreadyHeld", err)
	}
	if _, err := s.GrantOwner(ctx, disabled); !errors.Is(err, ErrPrincipalOff) {
		t.Errorf("owner grant to a disabled principal: %v, want ErrPrincipalOff", err)
	}
	if _, err := s.GrantOwner(ctx, uuid.New()); !errors.Is(err, ErrPrincipalMissing) {
		t.Errorf("owner grant to a missing principal: %v, want ErrPrincipalMissing", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.platform_role.granted'`) - grants; n != 0 {
		t.Errorf("refused owner grants left %d grant audit rows", n)
	}
	if n := e.ownerRows(t, disabled); n != 0 {
		t.Errorf("a refused owner grant wrote %d rows", n)
	}
}

func TestOperatorRevokeOwnerCommitsAuditOutboxAndRevision(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	s := e.bootstrapStore(t)
	target := e.user(t, ctx, true, false)
	id, err := s.GrantOwner(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RevokeOwner(ctx, target); err != nil {
		t.Fatalf("RevokeOwner: %v", err)
	}
	if n := e.ownerRows(t, target); n != 0 {
		t.Fatalf("owner rows after the revoke = %d", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.platform_role.revoked'
		AND outcome = 'allowed' AND actor_principal_id IS NULL AND subject_principal_id = $1
		AND attributes->>'source' = 'operator-cli' AND attributes->>'reason_code' = 'role_revoked'`, target.String()); n != 1 {
		t.Fatalf("owner revoke audit rows = %d, want 1", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = 'platform_role_assignment.revoked' AND aggregate_id = $1`, "pra_"+id.String()); n != 1 {
		t.Fatalf("owner revoke outbox rows = %d, want 1", n)
	}
	if rev := e.revision(t, ctx, id); rev != 2 {
		t.Fatalf("revision after the owner revoke = %d, want 2", rev)
	}
	if err := s.RevokeOwner(ctx, target); !errors.Is(err, ErrNotHeld) {
		t.Errorf("a second owner revoke: %v, want ErrNotHeld", err)
	}
}

// Q3: there is no last-owner guard. Revoking the only owner works at once, and
// the owner read then reports no owner (the exemption is off).
func TestRevokingTheOnlyOwnerWorksAndTheOwnerReadReportsNone(t *testing.T) {
	e := isolatedEnv(t)
	ctx := ctxT(t)
	s := e.bootstrapStore(t)
	if n, err := s.ActiveOwners(ctx); err != nil || n != 0 {
		t.Fatalf("ActiveOwners on a fresh database = %d (%v), want 0", n, err)
	}
	owner := e.user(t, ctx, true, false)
	if _, err := s.GrantOwner(ctx, owner); err != nil {
		t.Fatal(err)
	}
	if n, err := s.ActiveOwners(ctx); err != nil || n != 1 {
		t.Fatalf("ActiveOwners with one owner = %d (%v), want 1", n, err)
	}
	if err := s.RevokeOwner(ctx, owner); err != nil {
		t.Fatalf("revoking the only owner: %v", err)
	}
	if n, err := s.ActiveOwners(ctx); err != nil || n != 0 {
		t.Fatalf("ActiveOwners after revoking the only owner = %d (%v), want 0", n, err)
	}
	if held, err := e.store.Resolve(ctx, owner); err != nil || held.IsOwner() {
		t.Fatalf("Resolve after revoking the only owner: %v %v", held.Roles(), err)
	}
}

// Q2: the bootstrap grants platform_admin only. It never grants owner, and with
// no owner the owner read reports none.
func TestBootstrapDoesNotGrantOwner(t *testing.T) {
	e := isolatedEnv(t)
	ctx := ctxT(t)
	target := e.user(t, ctx, true, false)
	if _, err := e.bootstrapStore(t).Bootstrap(ctx, target); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if n := e.ownerRows(t, target); n != 0 {
		t.Fatalf("the bootstrap wrote %d owner rows", n)
	}
	if n, err := e.bootstrapStore(t).ActiveOwners(ctx); err != nil || n != 0 {
		t.Fatalf("ActiveOwners after the bootstrap = %d (%v), want 0", n, err)
	}
	if held, err := e.store.Resolve(ctx, target); err != nil || held.IsOwner() || !held.Allows(GrantAction) {
		t.Fatalf("Resolve after the bootstrap: %v %v", held.Roles(), err)
	}
}

// Q4: one principal holds platform_admin and platform_owner as two rows, each
// with its own audit row; revoking one leaves the other.
func TestOnePrincipalHoldsAdminAndOwnerAsSeparateRows(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	s := e.bootstrapStore(t)
	granter := e.user(t, ctx, true, true)
	target := e.user(t, ctx, true, false)
	adminRow, err := e.store.Grant(ctx, GrantInput{Actor: granter, Principal: target, Role: PlatformAdmin})
	if err != nil {
		t.Fatal(err)
	}
	ownerRow, err := s.GrantOwner(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	if adminRow == ownerRow {
		t.Fatal("admin and owner share an assignment id")
	}
	held, err := e.store.Resolve(ctx, target)
	if err != nil || !held.IsOwner() || !held.Allows(GrantAction) || len(held.Roles()) != 2 {
		t.Fatalf("Resolve with both roles: %v %v", held.Roles(), err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.platform_role.granted' AND subject_principal_id = $1`, target.String()); n != 2 {
		t.Fatalf("grant audit rows for the principal = %d, want 2", n)
	}
	if err := s.RevokeOwner(ctx, target); err != nil {
		t.Fatal(err)
	}
	held, err = e.store.Resolve(ctx, target)
	if err != nil || held.IsOwner() || !held.Allows(GrantAction) {
		t.Fatalf("Resolve after the owner revoke: %v %v", held.Roles(), err)
	}
}

// 5.4 reads the role live: the Authority and the SQL condition both follow the
// row, never a carried value.
func TestOwnerIsReadLiveNeverFromAClaim(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	s := e.bootstrapStore(t)
	target := e.user(t, ctx, true, false)
	holds := func() bool {
		var ok bool
		sql := `SELECT ` + rule.HoldsSQL(`auth`, `$1::uuid`, `'`+rule.PlatformOwner+`'`)
		if err := e.admin.QueryRow(ctx, sql, target).Scan(&ok); err != nil {
			t.Fatalf("HoldsSQL: %v", err)
		}
		return ok
	}
	check := func(step string, want bool) {
		t.Helper()
		held, err := e.store.Resolve(ctx, target)
		if err != nil || held.IsOwner() != want {
			t.Fatalf("%s: Resolve IsOwner = %v (%v), want %v", step, held.IsOwner(), err, want)
		}
		if got := holds(); got != want {
			t.Fatalf("%s: HoldsSQL = %v, want %v", step, got, want)
		}
	}
	check("before any grant", false)
	if _, err := s.GrantOwner(ctx, target); err != nil {
		t.Fatal(err)
	}
	check("after the grant", true)
	if err := s.RevokeOwner(ctx, target); err != nil {
		t.Fatal(err)
	}
	check("after the revoke", false)
	if _, err := s.GrantOwner(ctx, target); err != nil {
		t.Fatal(err)
	}
	check("after a re-grant", true)
}

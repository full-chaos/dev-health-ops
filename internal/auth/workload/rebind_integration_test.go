//go:build integration

package workload

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func tupleOf(b BindingSpec) BindingTuple {
	return BindingTuple{TrustDomain: b.TrustDomain, Issuer: b.Issuer, Namespace: b.Namespace, ServiceAccountName: b.ServiceAccountName}
}

func TestRebindMovesThePinWithOneAuditRowAndIsIdempotent(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n, b := pinnedBinding(t, e)
	newUID := uuid.NewString()
	rows := func() int {
		return e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.workload.binding_rebound' AND attributes->>'binding_id' = $1`, mustBindingID(t, e, b).String())
	}
	info, changed, err := e.writer.RebindBinding(ctx, tupleOf(b), newUID, Actor{})
	if err != nil || !changed {
		t.Fatalf("RebindBinding = %v changed=%t", err, changed)
	}
	if info.Workload != n {
		t.Fatalf("workload = %q, want %q", info.Workload, n)
	}
	moved := b
	moved.ServiceAccountUID = newUID
	if _, err := e.reader.Resolve(ctx, moved.input()); err != nil {
		t.Fatalf("new uid refused after rebind: %v", err)
	}
	if _, err := e.reader.Resolve(ctx, b.input()); RefusalReason(err) != string(ReasonUIDMismatch) {
		t.Fatalf("old uid after rebind = %v, want uid_mismatch", err)
	}
	if got := rows(); got != 1 {
		t.Fatalf("rebind audit rows = %d, want 1", got)
	}
	if c := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.workload.binding_rebound' AND attributes->>'binding_id' = $1 AND attributes->>'binding_disabled' = 'false'`, mustBindingID(t, e, b).String()); c != 1 {
		t.Fatalf("audit row lacks binding_disabled=false (%d)", c)
	}
	if _, changed, err := e.writer.RebindBinding(ctx, tupleOf(b), newUID, Actor{}); err != nil || changed {
		t.Fatalf("repeat = %v changed=%t", err, changed)
	}
	if got := rows(); got != 1 {
		t.Fatalf("rebind audit rows after repeat = %d, want 1", got)
	}
}

func mustBindingID(t *testing.T, e *env, b BindingSpec) uuid.UUID {
	t.Helper()
	info, err := e.writer.LookupBinding(ctxT(t), tupleOf(b))
	if err != nil {
		t.Fatal(err)
	}
	return info.ID
}

func TestRebindRefusals(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	_, b := pinnedBinding(t, e)
	unknown := tupleOf(tuple("ns-" + name()))
	if _, _, err := e.writer.RebindBinding(ctx, unknown, uuid.NewString(), Actor{}); !errors.Is(err, ErrBindingNotFound) {
		t.Fatalf("unknown tuple = %v", err)
	}
	for _, bad := range []string{"", "x", strings.ToUpper(uuid.NewString())} {
		if _, _, err := e.writer.RebindBinding(ctx, tupleOf(b), bad, Actor{}); !errors.Is(err, ErrBindingInvalid) {
			t.Fatalf("uid %q = %v, want ErrBindingInvalid", bad, err)
		}
	}
	before := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.workload.binding_rebound'`)
	if _, err := e.runtime.Exec(ctx, `UPDATE auth.workload_bindings SET service_account_uid = $1 WHERE namespace = $2`, uuid.NewString(), b.Namespace); err == nil {
		t.Fatal("the runtime role moved a pin")
	}
	if _, err := e.admin.Exec(ctx, `UPDATE auth.workload_bindings SET service_account_uid = $1 WHERE namespace = $2`, uuid.NewString(), b.Namespace); err == nil {
		t.Fatal("a pin moved outside audit.Commit")
	}
	if after := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.workload.binding_rebound'`); after != before {
		t.Fatalf("refusals wrote %d audit rows", after-before)
	}
}

func TestRebindSetsAPinOnAnUnpinnedBinding(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	_, b := registered(t, e, []string{"auth-service"})
	uid := uuid.NewString()
	if _, changed, err := e.writer.RebindBinding(ctx, tupleOf(b), uid, Actor{}); err != nil || !changed {
		t.Fatalf("RebindBinding = %v changed=%t", err, changed)
	}
	other := b.input()
	other.ServiceAccountUID = uuid.NewString()
	if _, err := e.reader.Resolve(ctx, other); RefusalReason(err) != string(ReasonUIDMismatch) {
		t.Fatalf("other uid after pinning = %v, want uid_mismatch", err)
	}
}

func TestRebindKeepsADisabledBindingDisabled(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	_, b := pinnedBinding(t, e)
	id := mustBindingID(t, e, b)
	if err := e.writer.DisableBinding(ctx, id, Actor{}); err != nil {
		t.Fatal(err)
	}
	newUID := uuid.NewString()
	if _, changed, err := e.writer.RebindBinding(ctx, tupleOf(b), newUID, Actor{}); err != nil || !changed {
		t.Fatalf("RebindBinding = %v changed=%t", err, changed)
	}
	moved := b
	moved.ServiceAccountUID = newUID
	if _, err := e.reader.Resolve(ctx, moved.input()); RefusalReason(err) != string(ReasonDisabled) {
		t.Fatalf("rebound disabled binding = %v, want disabled", err)
	}
	if c := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.workload.binding_rebound' AND attributes->>'binding_id' = $1 AND attributes->>'binding_disabled' = 'true'`, id.String()); c != 1 {
		t.Fatalf("audit row lacks binding_disabled=true (%d)", c)
	}
	if err := e.writer.EnableBinding(ctx, id, Actor{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.reader.Resolve(ctx, moved.input()); err != nil {
		t.Fatalf("binding-enable after rebind refused: %v", err)
	}
}

func TestWorkloadDisableStopsEveryBindingAndAuditsOnce(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n, b1 := registered(t, e, []string{"auth-service"})
	b2 := b1
	b2.ServiceAccountName = "sa-probe-8y"
	if _, err := e.writer.AddBinding(ctx, n, b2, Actor{}); err != nil {
		t.Fatal(err)
	}
	if err := e.writer.Disable(ctx, n, Actor{}); err != nil {
		t.Fatal(err)
	}
	if err := e.writer.Disable(ctx, n, Actor{}); err != nil {
		t.Fatal(err)
	}
	for _, b := range []BindingSpec{b1, b2} {
		if _, err := e.reader.Resolve(ctx, b.input()); RefusalReason(err) != string(ReasonDisabled) {
			t.Fatalf("binding %s after workload disable = %v", b.ServiceAccountName, err)
		}
	}
	info, _ := e.writer.Lookup(ctx, n)
	if c := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.workload.disabled' AND subject_principal_id = $1`, info.PrincipalID); c != 1 {
		t.Fatalf("disable audit rows = %d, want 1", c)
	}
}

func TestRebindTouchesOnlyTheNamedBinding(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n, first := pinnedBinding(t, e)
	second := first
	second.ServiceAccountName = "sa-probe-9z"
	second.ServiceAccountUID = uuid.NewString()
	if _, err := e.writer.AddBinding(ctx, n, second, Actor{}); err != nil {
		t.Fatal(err)
	}
	newUID := uuid.NewString()
	if _, changed, err := e.writer.RebindBinding(ctx, tupleOf(second), newUID, Actor{}); err != nil || !changed {
		t.Fatalf("RebindBinding = %v changed=%t", err, changed)
	}
	if _, err := e.reader.Resolve(ctx, first.input()); err != nil {
		t.Fatalf("rebound the wrong binding: the other binding now refuses its own uid: %v", err)
	}
	moved := second
	moved.ServiceAccountUID = newUID
	if _, err := e.reader.Resolve(ctx, moved.input()); err != nil {
		t.Fatalf("the named binding refuses its new uid: %v", err)
	}
}

//go:build integration

package workload

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// newPrincipal writes a principal of kind through audit.Commit and returns its id.
func newPrincipal(t *testing.T, e *env, kind string, disabled bool) uuid.UUID {
	t.Helper()
	return commitSeed(t, e, `INSERT INTO auth.principals (kind, disabled_at) VALUES ($1, CASE WHEN $2::boolean THEN now() END) RETURNING id`, kind, disabled)
}

func disableRowOnly(t *testing.T, e *env, principal uuid.UUID) {
	t.Helper()
	commitSeed(t, e, `UPDATE auth.workloads SET disabled_at = now() WHERE principal_id = $1 RETURNING principal_id`, principal)
}

func disablePrincipalOnly(t *testing.T, e *env, principal uuid.UUID) {
	t.Helper()
	commitSeed(t, e, `UPDATE auth.principals SET disabled_at = now() WHERE id = $1 RETURNING id`, principal)
}

// enableWrites is the state the enable verb can change: the two disabled
// marks, and the audit and outbox rows of the workload.
func enableWrites(t *testing.T, e *env, principal uuid.UUID) string {
	t.Helper()
	var state string
	if err := e.admin.QueryRow(ctxT(t), `SELECT (SELECT (disabled_at IS NULL)::text FROM auth.workloads WHERE principal_id = $1) || '/' ||
(SELECT (disabled_at IS NULL)::text FROM auth.principals WHERE id = $1) || '/' ||
(SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1)::text || '/' ||
(SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_id = $2)::text`, principal, wire(principal)).Scan(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

func rowsEnabled(t *testing.T, e *env, principal uuid.UUID) (row, prin bool) {
	t.Helper()
	if err := e.admin.QueryRow(ctxT(t), `SELECT (SELECT disabled_at IS NULL FROM auth.workloads WHERE principal_id = $1),
(SELECT disabled_at IS NULL FROM auth.principals WHERE id = $1)`, principal).Scan(&row, &prin); err != nil {
		t.Fatal(err)
	}
	return row, prin
}

type enableRow struct {
	attributes map[string]any
	actor      *uuid.UUID
}

func enableAuditRows(t *testing.T, e *env, principal uuid.UUID) []enableRow {
	t.Helper()
	rows, err := e.admin.Query(ctxT(t), `SELECT attributes, actor_principal_id FROM auth.security_audit_events
WHERE subject_principal_id = $1 AND event_type = 'auth.workload.enabled' ORDER BY occurred_at, id`, principal)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []enableRow
	for rows.Next() {
		var raw []byte
		var r enableRow
		if err := rows.Scan(&raw, &r.actor); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &r.attributes); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func enableEventsOf(t *testing.T, e *env, principal uuid.UUID) []string {
	t.Helper()
	rows, err := e.admin.Query(ctxT(t), `SELECT aggregate_type || ':' || event_type || ':' || (payload->'change'->>'status')
FROM auth.auth_outbox_events WHERE aggregate_id = $1 AND event_type LIKE '%.enabled' ORDER BY aggregate_type`, wire(principal))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// wantOneEnableRow checks the one audit row: the operator is its actor and
// its attributes are exactly want.
func wantOneEnableRow(t *testing.T, e *env, principal, operator uuid.UUID, want map[string]any) {
	t.Helper()
	got := enableAuditRows(t, e, principal)
	if len(got) != 1 {
		t.Fatalf("enable audit rows = %d, want 1", len(got))
	}
	if got[0].actor == nil || *got[0].actor != operator {
		t.Fatalf("audit actor %v, want the operator %s", got[0].actor, operator)
	}
	if !reflect.DeepEqual(got[0].attributes, want) {
		t.Fatalf("audit attributes %v, want %v", got[0].attributes, want)
	}
}

func enableAttrs(principal uuid.UUID, row, prin bool) map[string]any {
	m := map[string]any{"target_kind": "workload", "target_id": principal.String(), "reason_code": "workload_enabled"}
	if row {
		m["changed_fields"] = []any{"status", "disabled_at"}
		m["previous_status"] = "disabled"
		m["new_status"] = "active"
	}
	if prin {
		m["principal_enabled"] = "true"
		m["principal_id"] = principal.String()
	}
	return m
}

func createWorks(t *testing.T, e *env, n string) {
	t.Helper()
	if _, err := e.writer.CreateBootstrap(ctxT(t), n, mint(t), nil, time.Hour, Actor{}); err != nil {
		t.Fatalf("create after enable: %v", err)
	}
}

// TestEnableClearsOnlyTheWorkloadRowAndNamesTheOperator: the workload row
// alone is disabled; enable clears it, writes one audit row with the operator
// as actor and no principal attributes, and one workload.enabled event.
func TestEnableClearsOnlyTheWorkloadRowAndNamesTheOperator(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	for _, flag := range []bool{false, true} {
		n, principal := registerWorkload(t, e, []string{"auth-service"})
		operator := newPrincipal(t, e, "service_account", false)
		disableRowOnly(t, e, principal)
		plan, err := e.writer.EnableWorkload(ctx, n, EnableInput{Operator: operator, EnablePrincipal: flag})
		if err != nil {
			t.Fatalf("flag=%t: %v", flag, err)
		}
		if !plan.EnableRow || plan.EnablePrincipal || !plan.RowDisabled || plan.PrincipalDisabled || plan.PrincipalID != principal {
			t.Fatalf("flag=%t: plan %+v", flag, plan)
		}
		if row, prin := rowsEnabled(t, e, principal); !row || !prin {
			t.Fatalf("flag=%t: row enabled %t, principal enabled %t", flag, row, prin)
		}
		wantOneEnableRow(t, e, principal, operator, enableAttrs(principal, true, false))
		if ev := enableEventsOf(t, e, principal); !slices.Equal(ev, []string{"workload:workload.enabled:active"}) {
			t.Fatalf("flag=%t: events %v", flag, ev)
		}
		createWorks(t, e, n)
	}
}

// TestEnableRefusesADisabledPrincipalWithoutTheFlag: Store.Disable disables
// both rows; without -enable-principal nothing is written.
func TestEnableRefusesADisabledPrincipalWithoutTheFlag(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	for name, off := range map[string]func(string, uuid.UUID){
		"both rows":      func(n string, _ uuid.UUID) { _ = e.writer.Disable(ctx, n, Actor{}) },
		"principal only": func(_ string, principal uuid.UUID) { disablePrincipalOnly(t, e, principal) },
	} {
		n, principal := registerWorkload(t, e, []string{"auth-service"})
		operator := newPrincipal(t, e, "user", false)
		off(n, principal)
		before := enableWrites(t, e, principal)
		in := EnableInput{Operator: operator}
		if _, err := e.writer.PlanEnable(ctx, n, in); err != ErrBootstrapPrincipalDisabled {
			t.Fatalf("%s: plan = %v, want ErrBootstrapPrincipalDisabled", name, err)
		}
		if _, err := e.writer.EnableWorkload(ctx, n, in); err != ErrBootstrapPrincipalDisabled {
			t.Fatalf("%s: enable = %v, want ErrBootstrapPrincipalDisabled", name, err)
		}
		if after := enableWrites(t, e, principal); after != before {
			t.Fatalf("%s: a refused enable wrote: %s -> %s", name, before, after)
		}
	}
}

// TestEnableWithTheFlagEnablesBothRowsInOneAuditRow: one audit row, which
// carries principal_enabled "true" and the principal id, and two events.
func TestEnableWithTheFlagEnablesBothRowsInOneAuditRow(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n, principal := registerWorkload(t, e, []string{"auth-service"})
	operator := newPrincipal(t, e, "user", false)
	if err := e.writer.Disable(ctx, n, Actor{}); err != nil {
		t.Fatal(err)
	}
	before := enableWrites(t, e, principal)
	in := EnableInput{Operator: operator, EnablePrincipal: true}
	planned, err := e.writer.PlanEnable(ctx, n, in)
	if err != nil || !planned.EnableRow || !planned.EnablePrincipal {
		t.Fatalf("dry run plan %+v %v", planned, err)
	}
	if after := enableWrites(t, e, principal); after != before {
		t.Fatalf("the dry run wrote: %s -> %s", before, after)
	}
	plan, err := e.writer.EnableWorkload(ctx, n, in)
	if err != nil {
		t.Fatal(err)
	}
	if plan != planned {
		t.Fatalf("applied plan %+v, dry run %+v", plan, planned)
	}
	if row, prin := rowsEnabled(t, e, principal); !row || !prin {
		t.Fatalf("row enabled %t, principal enabled %t", row, prin)
	}
	wantOneEnableRow(t, e, principal, operator, enableAttrs(principal, true, true))
	if ev := enableEventsOf(t, e, principal); !slices.Equal(ev, []string{"principal:principal.enabled:active", "workload:workload.enabled:active"}) {
		t.Fatalf("events %v", ev)
	}
	createWorks(t, e, n)
}

func TestEnableWithTheFlagEnablesAPrincipalDisabledAlone(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n, principal := registerWorkload(t, e, []string{"auth-service"})
	operator := newPrincipal(t, e, "service_account", false)
	disablePrincipalOnly(t, e, principal)
	plan, err := e.writer.EnableWorkload(ctx, n, EnableInput{Operator: operator, EnablePrincipal: true})
	if err != nil {
		t.Fatal(err)
	}
	if plan.EnableRow || !plan.EnablePrincipal {
		t.Fatalf("plan %+v", plan)
	}
	if row, prin := rowsEnabled(t, e, principal); !row || !prin {
		t.Fatalf("row enabled %t, principal enabled %t", row, prin)
	}
	wantOneEnableRow(t, e, principal, operator, enableAttrs(principal, false, true))
	if ev := enableEventsOf(t, e, principal); !slices.Equal(ev, []string{"principal:principal.enabled:active"}) {
		t.Fatalf("events %v", ev)
	}
	createWorks(t, e, n)
}

// TestEnablePrincipalRefusesAPrincipalOfAnotherKind: the flag enables a
// workload-kind principal only; the kind is planted, as no verb makes one.
func TestEnablePrincipalRefusesAPrincipalOfAnotherKind(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	for _, kind := range []string{"user", "service_account"} {
		n, principal := registerWorkload(t, e, []string{"auth-service"})
		operator := newPrincipal(t, e, "user", false)
		if err := e.writer.Disable(ctx, n, Actor{}); err != nil {
			t.Fatal(err)
		}
		if _, err := e.admin.Exec(ctx, `UPDATE auth.principals SET kind = $2 WHERE id = $1`, principal, kind); err != nil {
			t.Fatal(err)
		}
		before := enableWrites(t, e, principal)
		in := EnableInput{Operator: operator, EnablePrincipal: true}
		if _, err := e.writer.PlanEnable(ctx, n, in); err != ErrPrincipalNotWorkload {
			t.Fatalf("%s: plan = %v, want ErrPrincipalNotWorkload", kind, err)
		}
		if _, err := e.writer.EnableWorkload(ctx, n, in); err != ErrPrincipalNotWorkload {
			t.Fatalf("%s: enable = %v, want ErrPrincipalNotWorkload", kind, err)
		}
		if after := enableWrites(t, e, principal); after != before {
			t.Fatalf("%s: a refused enable wrote: %s -> %s", kind, before, after)
		}
	}
}

func TestEnableRefusesABadOperatorByName(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n, principal := registerWorkload(t, e, []string{"auth-service"})
	disableRowOnly(t, e, principal)
	cases := map[string]struct {
		operator uuid.UUID
		want     error
	}{
		"unknown":                {uuid.New(), ErrOperatorNotFound},
		"disabled":               {newPrincipal(t, e, "user", true), ErrOperatorDisabled},
		"the workload principal": {principal, ErrOperatorIsSubject},
	}
	for name, c := range cases {
		before := enableWrites(t, e, principal)
		in := EnableInput{Operator: c.operator, EnablePrincipal: true}
		if _, err := e.writer.PlanEnable(ctx, n, in); err != c.want {
			t.Fatalf("%s: plan = %v, want %v", name, err, c.want)
		}
		if _, err := e.writer.EnableWorkload(ctx, n, in); err != c.want {
			t.Fatalf("%s: enable = %v, want %v", name, err, c.want)
		}
		if after := enableWrites(t, e, principal); after != before {
			t.Fatalf("%s: a refused enable wrote: %s -> %s", name, before, after)
		}
	}
	if _, err := e.writer.EnableWorkload(ctx, "t-absent", EnableInput{Operator: uuid.New()}); err != ErrNotFound {
		t.Fatalf("an unknown workload = %v, want ErrNotFound", err)
	}
}

// TestEnableRefusesAStateThatChangedBeforeTheLocks: a change between the
// unlocked read and the locks is refused, and the verb writes nothing.
func TestEnableRefusesAStateThatChangedBeforeTheLocks(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	cases := map[string]func(principal, operator uuid.UUID){
		"operator disabled":  func(_, operator uuid.UUID) { disablePrincipalOnly(t, e, operator) },
		"principal disabled": func(principal, _ uuid.UUID) { disablePrincipalOnly(t, e, principal) },
		"row enabled": func(principal, _ uuid.UUID) {
			commitSeed(t, e, `UPDATE auth.workloads SET disabled_at = NULL WHERE principal_id = $1 RETURNING principal_id`, principal)
		},
	}
	for name, change := range cases {
		n, principal := registerWorkload(t, e, []string{"auth-service"})
		operator := newPrincipal(t, e, "user", false)
		disableRowOnly(t, e, principal)
		store := *e.writer
		real := store.commit
		store.commit = func(ctx context.Context, m audit.Mutation) error {
			change(principal, operator)
			return real(ctx, m)
		}
		before := enableAuditRows(t, e, principal)
		if _, err := store.EnableWorkload(ctx, n, EnableInput{Operator: operator}); !errors.Is(err, ErrEnableStateChanged) {
			t.Fatalf("%s: enable = %v, want ErrEnableStateChanged", name, err)
		}
		if after := enableAuditRows(t, e, principal); len(after) != len(before) {
			t.Fatalf("%s: a refused enable wrote an audit row", name)
		}
		if ev := enableEventsOf(t, e, principal); len(ev) != 0 {
			t.Fatalf("%s: a refused enable wrote events %v", name, ev)
		}
	}
}

func TestEnableOfAnEnabledWorkloadWritesNothing(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n, principal := registerWorkload(t, e, []string{"auth-service"})
	operator := newPrincipal(t, e, "user", false)
	for _, flag := range []bool{false, true} {
		before := enableWrites(t, e, principal)
		plan, err := e.writer.EnableWorkload(ctx, n, EnableInput{Operator: operator, EnablePrincipal: flag})
		if err != nil || plan.EnableRow || plan.EnablePrincipal {
			t.Fatalf("flag=%t: plan %+v %v", flag, plan, err)
		}
		if after := enableWrites(t, e, principal); after != before {
			t.Fatalf("flag=%t: a no-op enable wrote: %s -> %s", flag, before, after)
		}
	}
}

// TestEnableRefusesWhileABootstrapValueIsLiveAndRevokeWhileDisabledOpensIt
// (D6505): a value live before the disable would be accepted again, so enable
// refuses (no row, no event, no audit row) until it is revoked. Rotate is
// refused while disabled; revoke and bootstrap-disable work, then enable does.
func TestEnableRefusesWhileABootstrapValueIsLiveAndRevokeWhileDisabledOpensIt(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	for _, revoke := range []string{"bootstrap-disable", "bootstrap-revoke"} {
		for _, both := range []bool{false, true} {
			n, principal := registerWorkload(t, e, []string{"auth-service"})
			operator := newPrincipal(t, e, "user", false)
			v := mint(t)
			created, err := e.writer.CreateBootstrap(ctx, n, v, nil, time.Hour, Actor{})
			if err != nil {
				t.Fatal(err)
			}
			if both {
				if err := e.writer.Disable(ctx, n, Actor{}); err != nil {
					t.Fatal(err)
				}
			} else {
				disableRowOnly(t, e, principal)
			}
			in := EnableInput{Operator: operator, EnablePrincipal: both}
			before, beforeBootstrap := enableWrites(t, e, principal), bootstrapWrites(t, e, principal)
			if _, err := e.writer.PlanEnable(ctx, n, in); err != ErrEnableLiveBootstrap {
				t.Fatalf("%s both=%t: plan = %v, want ErrEnableLiveBootstrap", revoke, both, err)
			}
			if _, err := e.writer.EnableWorkload(ctx, n, in); err != ErrEnableLiveBootstrap {
				t.Fatalf("%s both=%t: enable = %v, want ErrEnableLiveBootstrap", revoke, both, err)
			}
			if after := enableWrites(t, e, principal); after != before || bootstrapWrites(t, e, principal) != beforeBootstrap {
				t.Fatalf("%s both=%t: a refused enable wrote: %s -> %s", revoke, both, before, after)
			}
			if rows := enableAuditRows(t, e, principal); len(rows) != 0 {
				t.Fatalf("%s both=%t: a refused enable wrote %d audit rows", revoke, both, len(rows))
			}
			if _, err := e.writer.RotateBootstrap(ctx, n, mint(t), 0, time.Hour, Actor{}); err == nil {
				t.Fatalf("%s both=%t: rotate worked on a disabled workload; the refusal must not name revoke only", revoke, both)
			}
			if revoke == "bootstrap-disable" {
				if _, err := e.writer.DisableBootstrap(ctx, n, Actor{}); err != nil {
					t.Fatalf("bootstrap-disable while disabled: %v", err)
				}
			} else if err := e.writer.RevokeBootstrap(ctx, created.CredentialID, BootstrapReasonCompromised, Actor{}); err != nil {
				t.Fatalf("bootstrap-revoke while disabled: %v", err)
			}
			if _, err := e.writer.EnableWorkload(ctx, n, in); err != nil {
				t.Fatalf("%s both=%t: enable after the revoke: %v", revoke, both, err)
			}
			if row, prin := rowsEnabled(t, e, principal); !row || !prin {
				t.Fatalf("%s both=%t: row enabled %t, principal enabled %t", revoke, both, row, prin)
			}
			if _, err := e.reader.ResolveBootstrap(ctx, BootstrapDigest(v), time.Now()); err == nil {
				t.Fatalf("%s both=%t: the value live before the disable resolves after enable", revoke, both)
			}
			noSecretIn(t, e, principal, v)
			createWorks(t, e, n)
		}
	}
}

// TestAPrincipalOnlyEnableWritesItsAuditEventWithNoChangeFields: the row was
// not disabled, so the one audit row has no change fields of the workload
// row, but it exists, with the operator as actor and the enable action.
func TestAPrincipalOnlyEnableWritesItsAuditEventWithNoChangeFields(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n, principal := registerWorkload(t, e, []string{"auth-service"})
	operator := newPrincipal(t, e, "service_account", false)
	disablePrincipalOnly(t, e, principal)
	if _, err := e.writer.EnableWorkload(ctx, n, EnableInput{Operator: operator, EnablePrincipal: true}); err != nil {
		t.Fatal(err)
	}
	rows := enableAuditRows(t, e, principal)
	if len(rows) != 1 {
		t.Fatalf("auth.workload.enabled rows = %d, want 1", len(rows))
	}
	if rows[0].actor == nil || *rows[0].actor != operator || rows[0].attributes["reason_code"] != "workload_enabled" {
		t.Fatalf("audit row actor %v reason %v", rows[0].actor, rows[0].attributes["reason_code"])
	}
	for _, key := range []string{"changed_fields", "previous_status", "new_status"} {
		if _, ok := rows[0].attributes[key]; ok {
			t.Errorf("the principal-only audit row carries %s", key)
		}
	}
}

//go:build integration

package workload

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

var keyBase = time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)

func keyWorkload(t *testing.T, e *env) string {
	t.Helper()
	n := name()
	if _, err := e.writer.Register(ctxT(t), Spec{Name: n}, Actor{}); err != nil {
		t.Fatal(err)
	}
	return n
}

func activeKey(kid string, rotated time.Time) AssertionKeySpec {
	return AssertionKeySpec{Issuer: "https://issuer.test", KID: kid, Status: AssertionKeyActive, RotatedAt: rotated}
}

// outbox counts workload.assertion_key_changed rows: the verb writes an audit row only, so it must stay 0.
type keyCounts struct{ audit, outbox int }

func keyState(t *testing.T, e *env, n string) keyCounts {
	t.Helper()
	ctx := ctxT(t)
	info, err := e.writer.Lookup(ctx, n)
	if err != nil {
		t.Fatal(err)
	}
	return keyCounts{
		audit:  e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.workload.assertion_key_set' AND subject_principal_id = $1`, info.PrincipalID),
		outbox: e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = 'workload.assertion_key_changed' AND aggregate_id = $1`, wire(info.PrincipalID)),
	}
}

func keyRow(t *testing.T, e *env, n, kid string) (status string, retire *time.Time) {
	t.Helper()
	if err := e.admin.QueryRow(ctxT(t), `SELECT k.status, k.retire_after FROM auth.workload_assertion_keys k
JOIN auth.workloads w ON w.principal_id = k.workload_principal_id WHERE w.name = $1 AND k.kid = $2`, n, kid).Scan(&status, &retire); err != nil {
		t.Fatal(err)
	}
	return status, retire
}

// C1: a first key is one row and one audit row, and no provisioning event; the list shows its metadata; the table has no column that could hold a key.
func TestAssertionKeyFirstKeyWritesOneRowOneAuditRowOneEvent(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n := keyWorkload(t, e)
	id, err := e.writer.SetAssertionKey(ctx, n, activeKey("key-1", keyBase), Actor{})
	if err != nil || id == uuid.Nil {
		t.Fatalf("SetAssertionKey = %v, %v", id, err)
	}
	if got := keyState(t, e, n); got != (keyCounts{1, 0}) {
		t.Fatalf("audit/outbox rows = %+v, want 1/0", got)
	}
	var attrs string
	info, _ := e.writer.Lookup(ctx, n)
	if err := e.admin.QueryRow(ctx, `SELECT attributes::text FROM auth.security_audit_events WHERE event_type = 'auth.workload.assertion_key_set' AND subject_principal_id = $1`, info.PrincipalID).Scan(&attrs); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(attrs, "key-1") || strings.Contains(attrs, "https://issuer.test") {
		t.Errorf("the audit attributes carry the kid or the issuer")
	}
	if !strings.Contains(attrs, id.String()) {
		t.Errorf("the audit attributes lack the key row id")
	}
	if c := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_id = $1 AND event_type LIKE 'workload.assertion_key%'`, wire(info.PrincipalID)); c != 0 {
		t.Errorf("the verb wrote %d outbox events, want 0", c)
	}
	keys, err := e.writer.ListAssertionKeys(ctx, n)
	if err != nil || len(keys) != 1 || keys[0].KID != "key-1" || keys[0].Issuer != "https://issuer.test" ||
		keys[0].Status != AssertionKeyActive || !keys[0].RotatedAt.Equal(keyBase) || keys[0].RetireAfter != nil || keys[0].Workload != n || keys[0].ID != id {
		t.Fatalf("ListAssertionKeys = %+v, %v", keys, err)
	}
	rows, err := e.admin.Query(ctx, `SELECT column_name FROM information_schema.columns WHERE table_schema = 'auth' AND table_name = 'workload_assertion_keys' ORDER BY column_name`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, c)
	}
	want := "created_at id issuer kid retire_after rotated_at status updated_at workload_principal_id"
	if strings.Join(columns, " ") != want {
		t.Fatalf("columns = %v, want %s", columns, want)
	}
}

// C2: a second active key is refused; with -retire-previous-after the old one becomes retiring in the same transaction.
func TestAssertionKeyRotationDemotesTheActiveKeyInOneTransaction(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n := keyWorkload(t, e)
	if _, err := e.writer.SetAssertionKey(ctx, n, activeKey("old", keyBase), Actor{}); err != nil {
		t.Fatal(err)
	}
	before := keyState(t, e, n)
	next := activeKey("new", keyBase.Add(72*time.Hour))
	if _, err := e.writer.SetAssertionKey(ctx, n, next, Actor{}); !errors.Is(err, ErrAssertionKeyActiveExists) {
		t.Fatalf("a second active key = %v, want ErrAssertionKeyActiveExists", err)
	}
	if got := keyState(t, e, n); got != before {
		t.Fatalf("a refused call wrote rows: %+v -> %+v", before, got)
	}
	if c := e.count(t, ctx, `SELECT count(*) FROM auth.workload_assertion_keys k JOIN auth.workloads w ON w.principal_id = k.workload_principal_id WHERE w.name = $1`, n); c != 1 {
		t.Fatalf("a refused call left %d rows", c)
	}
	next.RetirePreviousAfter = AssertionOverlapCeiling
	if _, err := e.writer.SetAssertionKey(ctx, n, next, Actor{}); err != nil {
		t.Fatal(err)
	}
	if got := keyState(t, e, n); got != (keyCounts{before.audit + 1, 0}) {
		t.Fatalf("a rotation wrote %+v from %+v, want one audit row and no event", got, before)
	}
	status, retire := keyRow(t, e, n, "old")
	if status != "retiring" || retire == nil || !retire.Equal(next.RotatedAt.Add(AssertionOverlapCeiling)) {
		t.Fatalf("old key = %s %v", status, retire)
	}
	if status, retire := keyRow(t, e, n, "new"); status != "active" || retire != nil {
		t.Fatalf("new key = %s %v", status, retire)
	}
}

// C3: the overlap bound is exact; shape rules hold in the store.
func TestAssertionKeyOverlapBoundIsExactAndShapeRulesHold(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n := keyWorkload(t, e)
	if _, err := e.writer.SetAssertionKey(ctx, n, activeKey("a", keyBase), Actor{}); err != nil {
		t.Fatal(err)
	}
	retiring := func(kid string, after time.Time) AssertionKeySpec {
		return AssertionKeySpec{Issuer: "https://issuer.test", KID: kid, Status: AssertionKeyRetiring, RotatedAt: keyBase.Add(-time.Hour), RetireAfter: after}
	}
	if _, err := e.writer.SetAssertionKey(ctx, n, retiring("c-over", keyBase.Add(AssertionOverlapCeiling+time.Microsecond)), Actor{}); !errors.Is(err, ErrAssertionKeyOverlap) {
		t.Fatalf("1 µs past the bound = %v, want ErrAssertionKeyOverlap", err)
	}
	if _, err := e.writer.SetAssertionKey(ctx, n, retiring("c-at", keyBase.Add(AssertionOverlapCeiling)), Actor{}); err != nil {
		t.Fatalf("at the bound: %v", err)
	}
	if _, err := e.writer.SetAssertionKey(ctx, n, retiring("c-at", keyBase.Add(AssertionOverlapCeiling+time.Microsecond)), Actor{}); !errors.Is(err, ErrAssertionKeyOverlap) {
		t.Fatalf("pushing a retiring key past the bound = %v, want ErrAssertionKeyOverlap", err)
	}
	if status, retire := keyRow(t, e, n, "c-at"); status != "retiring" || retire == nil || !retire.Equal(keyBase.Add(AssertionOverlapCeiling)) {
		t.Fatalf("c-at = %s %v", status, retire)
	}
	bad := retiring("d", keyBase.Add(time.Hour))
	bad.RetireAfter = time.Time{}
	if _, err := e.writer.SetAssertionKey(ctx, n, bad, Actor{}); !errors.Is(err, ErrAssertionKeyInvalid) {
		t.Fatalf("retiring without a date = %v", err)
	}
	both := activeKey("e", keyBase.Add(time.Hour))
	both.RetireAfter = keyBase.Add(2 * time.Hour)
	if _, err := e.writer.SetAssertionKey(ctx, n, both, Actor{}); !errors.Is(err, ErrAssertionKeyInvalid) {
		t.Fatalf("active with a date = %v", err)
	}
}

func commitRaw(ctx context.Context, e *env, sql string, args ...any) error {
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		return err
	}
	return audit.Commit(ctx, e.admin, schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, sql, args...)
			return err
		},
		Audit:     audit.AuditEvent{EventType: "workload.test", Outcome: audit.OutcomeAllowed},
		OffStream: audit.OffStreamWorkloadRegistry,
	})
}

func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// C4: retired is terminal in the verb and in the table; at most one active kid per workload, even for a hand INSERT.
func TestAssertionKeyRetiredIsTerminalAndOneActivePerWorkload(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n := keyWorkload(t, e)
	if _, err := e.writer.SetAssertionKey(ctx, n, activeKey("k1", keyBase), Actor{}); err != nil {
		t.Fatal(err)
	}
	retired := activeKey("k1", keyBase)
	retired.Status = AssertionKeyRetired
	if _, err := e.writer.SetAssertionKey(ctx, n, retired, Actor{}); !errors.Is(err, ErrAssertionKeyLastActive) {
		t.Fatalf("active -> retired without a successor = %v, want ErrAssertionKeyLastActive", err)
	}
	if status, _ := keyRow(t, e, n, "k1"); status != "active" {
		t.Fatalf("k1 = %s after the refused retire", status)
	}
	next := activeKey("k1b", keyBase.Add(time.Hour))
	next.RetirePreviousAfter = time.Hour
	if _, err := e.writer.SetAssertionKey(ctx, n, next, Actor{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.writer.SetAssertionKey(ctx, n, retired, Actor{}); err != nil {
		t.Fatalf("retiring -> retired: %v", err)
	}
	if status, retire := keyRow(t, e, n, "k1"); status != "retired" || retire != nil {
		t.Fatalf("k1 = %s %v after retiring -> retired", status, retire)
	}
	if _, err := e.writer.SetAssertionKey(ctx, n, activeKey("k1", keyBase), Actor{}); !errors.Is(err, ErrAssertionKeyTerminal) {
		t.Fatalf("retired -> active = %v, want ErrAssertionKeyTerminal", err)
	}
	if err := commitRaw(ctx, e, `UPDATE auth.workload_assertion_keys SET status = 'active', retire_after = NULL WHERE kid = 'k1' AND workload_principal_id = (SELECT principal_id FROM auth.workloads WHERE name = $1)`, n); sqlState(err) != "23514" {
		t.Fatalf("a retired key reactivated through audit.Commit: state %q, err %v", sqlState(err), err)
	}
	if err := commitRaw(ctx, e, `UPDATE auth.workload_assertion_keys SET kid = 'renamed' WHERE kid = 'k1' AND workload_principal_id = (SELECT principal_id FROM auth.workloads WHERE name = $1)`, n); sqlState(err) != "23514" {
		t.Fatalf("an identity column changed: state %q, err %v", sqlState(err), err)
	}
	if status, _ := keyRow(t, e, n, "k1"); status != "retired" {
		t.Fatalf("k1 = %s after the refused writes", status)
	}
	if err := commitRaw(ctx, e, `INSERT INTO auth.workload_assertion_keys (id, workload_principal_id, issuer, kid, status, rotated_at)
SELECT gen_random_uuid(), principal_id, 'https://issuer.test', 'k3', 'active', now() FROM auth.workloads WHERE name = $1`, n); sqlState(err) != "23505" {
		t.Fatalf("a second active key by hand: state %q, err %v", sqlState(err), err)
	}
	var retiring AssertionKeySpec = AssertionKeySpec{Issuer: "https://issuer.test", KID: "k4", Status: AssertionKeyRetiring, RotatedAt: keyBase, RetireAfter: keyBase.Add(time.Hour)}
	if _, err := e.writer.SetAssertionKey(ctx, n, retiring, Actor{}); err != nil {
		t.Fatal(err)
	}
	if err := commitRaw(ctx, e, `UPDATE auth.workload_assertion_keys SET status = 'active', retire_after = NULL WHERE kid = 'k4' AND workload_principal_id = (SELECT principal_id FROM auth.workloads WHERE name = $1)`, n); sqlState(err) != "23514" {
		t.Fatalf("a retiring key made active by hand: state %q, err %v", sqlState(err), err)
	}
}

// C5: a repeat writes nothing; an unknown or a disabled workload is refused.
func TestAssertionKeyRepeatWritesNothingAndUnknownOrDisabledWorkloadIsRefused(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n := keyWorkload(t, e)
	spec := activeKey("r1", keyBase)
	id, err := e.writer.SetAssertionKey(ctx, n, spec, Actor{})
	if err != nil {
		t.Fatal(err)
	}
	before := keyState(t, e, n)
	again, err := e.writer.SetAssertionKey(ctx, n, spec, Actor{})
	if err != nil || again != id {
		t.Fatalf("repeat = %v, %v, want the same id %v", again, err, id)
	}
	if got := keyState(t, e, n); got != before {
		t.Fatalf("a repeat wrote rows: %+v -> %+v", before, got)
	}
	if _, err := e.writer.SetAssertionKey(ctx, "no-such-"+name(), spec, Actor{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown workload = %v, want ErrNotFound", err)
	}
	if err := e.writer.Disable(ctx, n, Actor{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.writer.SetAssertionKey(ctx, n, activeKey("r2", keyBase.Add(time.Hour)), Actor{}); !errors.Is(err, ErrAssertionKeyWorkloadDisabled) {
		t.Fatalf("disabled workload = %v, want ErrAssertionKeyWorkloadDisabled", err)
	}
	if got := keyState(t, e, n); got != before {
		t.Fatalf("a refused call wrote rows: %+v -> %+v", before, got)
	}
}

// C6: the table cannot be written outside audit.Commit, and the runtime role cannot read or write it.
func TestAssertionKeyTableIsGuardedAndClosedToTheRuntimeRole(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n := keyWorkload(t, e)
	if _, err := e.writer.SetAssertionKey(ctx, n, activeKey("g1", keyBase), Actor{}); err != nil {
		t.Fatal(err)
	}
	for label, sql := range map[string]string{
		"insert": fmt.Sprintf(`INSERT INTO auth.workload_assertion_keys (id, workload_principal_id, issuer, kid, status, rotated_at)
SELECT gen_random_uuid(), principal_id, 'https://issuer.test', 'g2', 'retired', now() FROM auth.workloads WHERE name = '%s'`, n),
		"update":   `UPDATE auth.workload_assertion_keys SET status = 'retired' WHERE kid = 'g1'`,
		"delete":   `DELETE FROM auth.workload_assertion_keys WHERE kid = 'g1'`,
		"truncate": `TRUNCATE auth.workload_assertion_keys`,
	} {
		if _, err := e.admin.Exec(ctx, sql); err == nil {
			t.Errorf("%s outside audit.Commit succeeded", label)
		}
	}
	if status, _ := keyRow(t, e, n, "g1"); status != "active" {
		t.Fatalf("g1 = %s", status)
	}
	for label, sql := range map[string]string{
		"select": `SELECT count(*) FROM auth.workload_assertion_keys`,
		"insert": `INSERT INTO auth.workload_assertion_keys (id, workload_principal_id, issuer, kid, status, rotated_at) VALUES (gen_random_uuid(), gen_random_uuid(), 'https://x.test', 'x', 'active', now())`,
		"update": `UPDATE auth.workload_assertion_keys SET status = 'retired'`,
		"delete": `DELETE FROM auth.workload_assertion_keys`,
	} {
		if _, err := e.runtime.Exec(ctx, sql); sqlState(err) != "42501" {
			t.Errorf("the runtime role's %s: state %q, err %v, want 42501", label, sqlState(err), err)
		}
	}
}

// C7: a key-shaped value is refused before any write, and no refusal carries it.
func TestAssertionKeyRefusesKeyShapedValuesAndLeaksNone(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n := keyWorkload(t, e)
	before := keyState(t, e, n)
	for label, spec := range map[string]AssertionKeySpec{
		"jwt-shaped kid": activeKey(credentialProbe(t), keyBase),
		"provider token": activeKey("ghp_"+strings.Repeat("A1b2", 9), keyBase),
		"credential url": {Issuer: "https://u:" + strings.Repeat("pw", 8) + "@issuer.test", KID: "k", Status: AssertionKeyActive, RotatedAt: keyBase},
	} {
		_, err := e.writer.SetAssertionKey(ctx, n, spec, Actor{})
		if !errors.Is(err, ErrAssertionKeyInvalid) {
			t.Errorf("%s: err = %v, want ErrAssertionKeyInvalid", label, err)
			continue
		}
		for _, value := range []string{spec.KID, spec.Issuer} {
			if len(value) > 8 && strings.Contains(err.Error(), value) {
				t.Errorf("%s: the error carries the value", label)
			}
		}
	}
	if got := keyState(t, e, n); got != before {
		t.Fatalf("refused calls wrote rows: %+v -> %+v", before, got)
	}
	if c := e.count(t, ctx, `SELECT count(*) FROM auth.workload_assertion_keys k JOIN auth.workloads w ON w.principal_id = k.workload_principal_id WHERE w.name = $1`, n); c != 0 {
		t.Fatalf("refused calls left %d rows", c)
	}
}

// C8: the table's own CHECK constraints hold for a writer that skips the verb.
func TestAssertionKeyTableConstraintsRefuseABadRowFromAnyWriter(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n := keyWorkload(t, e)
	for label, row := range map[string]string{
		"issuer without a scheme":         `'issuer.test', 'k', 'active', now(), NULL`,
		"issuer with a space":             `'https://issuer test', 'k', 'active', now(), NULL`,
		"kid with a space":                `'https://issuer.test', 'k k', 'active', now(), NULL`,
		"empty kid":                       `'https://issuer.test', '', 'active', now(), NULL`,
		"kid of 257 bytes":                `'https://issuer.test', repeat('k', 257), 'active', now(), NULL`,
		"unknown status":                  `'https://issuer.test', 'k', 'paused', now(), NULL`,
		"active with retire_after":        `'https://issuer.test', 'k', 'active', now(), now() + interval '1 hour'`,
		"retiring without retire_after":   `'https://issuer.test', 'k', 'retiring', now(), NULL`,
		"retiring with retire_after past": `'https://issuer.test', 'k', 'retiring', now(), now() - interval '1 hour'`,
		"retired with retire_after":       `'https://issuer.test', 'k', 'retired', now(), now() + interval '1 hour'`,
	} {
		sql := `INSERT INTO auth.workload_assertion_keys (id, workload_principal_id, issuer, kid, status, rotated_at, retire_after)
SELECT gen_random_uuid(), principal_id, ` + row + ` FROM auth.workloads WHERE name = $1`
		if err := commitRaw(ctx, e, sql, n); sqlState(err) != "23514" {
			t.Errorf("%s: state %q, err %v, want 23514", label, sqlState(err), err)
		}
	}
	if c := e.count(t, ctx, `SELECT count(*) FROM auth.workload_assertion_keys k JOIN auth.workloads w ON w.principal_id = k.workload_principal_id WHERE w.name = $1`, n); c != 0 {
		t.Fatalf("refused rows left %d rows", c)
	}
}

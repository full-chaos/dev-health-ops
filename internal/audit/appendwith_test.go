package audit

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

type countRow struct{ n int64 }

func (r countRow) Scan(dest ...any) error {
	*(dest[0].(*int64)) = r.n
	return nil
}

// fakeTx is a pgx.Tx that records statements; any method it does not override panics.
type fakeTx struct {
	pgx.Tx
	statements []string
	changed    int64
	failOn     string
	committed  bool
	rolledBack bool
}

func (f *fakeTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	f.statements = append(f.statements, sql)
	if f.failOn != "" && strings.Contains(sql, f.failOn) {
		return pgconn.CommandTag{}, errors.New("injected failure")
	}
	return pgconn.CommandTag{}, nil
}

func (f *fakeTx) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	f.statements = append(f.statements, sql)
	return countRow{f.changed}
}

func (f *fakeTx) Commit(context.Context) error   { f.committed = true; return nil }
func (f *fakeTx) Rollback(context.Context) error { f.rolledBack = true; return nil }

func init() {
	txStatusOf = func(pgx.Tx) byte { return 'T' }
}

func testSchema(t *testing.T) authschema.ValidatedIdentifier {
	t.Helper()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

func resetRequested() AuditEvent {
	return AuditEvent{EventType: EventPasswordResetRequested, Outcome: OutcomeAllowed,
		Attributes: map[string]any{"reason_code": "password_reset_requested", "target_kind": "principal"}}
}

func TestAppendWithRefusesAnEventTypeThatIsNotAppendOnlyBeforeBegin(t *testing.T) {
	called := false
	apply := func(context.Context, TxOps) error { called = true; return nil }
	for name, event := range map[string]AuditEvent{
		"a state-change type": {EventType: "organization.created", Outcome: OutcomeAllowed},
		"an unlisted type":    {EventType: "auth.password.maybe", Outcome: OutcomeAllowed},
	} {
		err := AppendWith(context.Background(), nil, authschema.ValidatedIdentifier{}, event, apply)
		if !errors.Is(err, ErrNotAppendOnly) {
			t.Errorf("%s: err = %v, want ErrNotAppendOnly", name, err)
		}
	}
	if called {
		t.Error("apply ran for a refused event type")
	}
}

func TestAppendWithValidatesTheRowAsAppendDoes(t *testing.T) {
	event := resetRequested()
	event.Attributes = map[string]any{"email": "someone@example.com"}
	err := AppendWith(context.Background(), nil, authschema.ValidatedIdentifier{}, event, func(context.Context, TxOps) error { return nil })
	if !errors.Is(err, ErrAttributeNotAllowed) {
		t.Fatalf("err = %v, want ErrAttributeNotAllowed", err)
	}
}

func TestAppendWithRunsApplyThenTheAuditRowInOneTransaction(t *testing.T) {
	tx := &fakeTx{}
	err := appendWithTx(context.Background(), tx, testSchema(t), resetRequested(),
		func(ctx context.Context, ops TxOps) error {
			_, err := ops.Exec(ctx, "SELECT 'apply'")
			return err
		})
	if err != nil {
		t.Fatalf("appendWithTx: %v", err)
	}
	var order []string
	for _, s := range tx.statements {
		switch {
		case strings.Contains(s, "set_config"):
			order = append(order, "arm")
		case strings.Contains(s, "'apply'"):
			order = append(order, "apply")
		case strings.Contains(s, "dev_health.audit_changed"):
			order = append(order, "count")
		case strings.Contains(s, "security_audit_events"):
			order = append(order, "audit")
		}
	}
	if got := strings.Join(order, ","); got != "arm,apply,count,audit" {
		t.Fatalf("statement order = %s, want arm,apply,count,audit", got)
	}
	if !tx.committed {
		t.Error("the transaction was not committed")
	}
}

func TestAppendWithRefusesAGuardedWrite(t *testing.T) {
	tx := &fakeTx{changed: 1}
	err := appendWithTx(context.Background(), tx, testSchema(t), resetRequested(),
		func(context.Context, TxOps) error { return nil })
	if !errors.Is(err, ErrGuardedWriteInAppend) {
		t.Fatalf("err = %v, want ErrGuardedWriteInAppend", err)
	}
	if tx.committed {
		t.Error("committed after a guarded write")
	}
	for _, s := range tx.statements {
		if strings.Contains(s, "INSERT INTO") {
			t.Errorf("audit row written after a guarded write: %s", s)
		}
	}
}

func TestAppendWithWritesNoRowWhenApplyFails(t *testing.T) {
	tx := &fakeTx{}
	err := appendWithTx(context.Background(), tx, testSchema(t), resetRequested(),
		func(context.Context, TxOps) error { return errors.New("apply failed") })
	if !errors.Is(err, ErrMutationFailed) {
		t.Fatalf("err = %v, want ErrMutationFailed", err)
	}
	if tx.committed {
		t.Error("committed after apply failed")
	}
	for _, s := range tx.statements {
		if strings.Contains(s, "INSERT INTO") {
			t.Errorf("audit row written after apply failed: %s", s)
		}
	}
}

func TestAppendWithFailsWholeWhenTheAuditInsertFails(t *testing.T) {
	tx := &fakeTx{failOn: "security_audit_events"}
	err := appendWithTx(context.Background(), tx, testSchema(t), resetRequested(),
		func(context.Context, TxOps) error { return nil })
	if !errors.Is(err, ErrMutationFailed) {
		t.Fatalf("err = %v, want ErrMutationFailed", err)
	}
	if tx.committed {
		t.Error("committed after the audit insert failed")
	}
}

func TestPasswordResetRequestedIsAppendOnly(t *testing.T) {
	if !appendOnlyEventTypes[EventPasswordResetRequested] {
		t.Fatal("auth.password.reset_requested is not append-only")
	}
	m := Mutation{
		Apply:     func(context.Context, TxOps) error { return nil },
		Audit:     resetRequested(),
		OffStream: OffStreamLinkCredentialLifecycle,
	}
	if err := m.validate(); !errors.Is(err, ErrMutationFailed) {
		t.Fatalf("Commit accepts the append-only event: %v", err)
	}
}

func TestResetRequestedAttributesAreAllowlisted(t *testing.T) {
	for _, reason := range []string{"password_reset_requested", "principal_not_found"} {
		attrs := map[string]any{"reason_code": reason, "target_kind": "principal"}
		if err := ValidateAttributes(attrs); err != nil {
			t.Errorf("%s: %v", reason, err)
		}
	}
}

func TestAppendWithRefusesATransactionTheApplyEnded(t *testing.T) {
	saved := txStatusOf
	txStatusOf = func(pgx.Tx) byte { return 'I' }
	t.Cleanup(func() { txStatusOf = saved })
	tx := &fakeTx{}
	err := appendWithTx(context.Background(), tx, testSchema(t), resetRequested(), func(context.Context, TxOps) error { return nil })
	if !errors.Is(err, ErrMutationFailed) {
		t.Fatalf("err = %v, want ErrMutationFailed", err)
	}
	if tx.committed {
		t.Error("committed a transaction the apply ended")
	}
	for _, s := range tx.statements {
		if strings.Contains(s, "INSERT INTO") {
			t.Errorf("audit row written outside the transaction: %s", s)
		}
	}
}

//go:build integration

package audit

import (
	"context"
	"errors"
	"testing"
	"time"
)

const attemptInsert = `INSERT INTO auth.login_attempts (identifier_hash) VALUES (decode(repeat('ab', 32), 'hex'))`

func countRows(t *testing.T, ctx context.Context, env *auditFixture, query string, args ...any) int {
	t.Helper()
	var n int
	if err := env.pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("counting: %v", err)
	}
	return n
}

func TestAppendWithCommitsTheWritesAndTheRowTogether(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	writer := runtimeRolePool(t, ctx, env.pool)
	err := AppendWith(ctx, writer, env.schema, resetRequested(), func(ctx context.Context, tx TxOps) error {
		_, err := tx.Exec(ctx, attemptInsert)
		return err
	})
	if err != nil {
		t.Fatalf("AppendWith: %v", err)
	}
	if n := countRows(t, ctx, env, `SELECT count(*) FROM auth.login_attempts`); n != 1 {
		t.Errorf("login_attempts rows = %d, want 1", n)
	}
	if n := countRows(t, ctx, env, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1`, EventPasswordResetRequested); n != 1 {
		t.Errorf("audit rows = %d, want 1", n)
	}
}

func TestAppendWithFaultAtTheAuditInsertLeavesNoWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	writer := runtimeRolePool(t, ctx, env.pool)
	event := resetRequested()
	notAUUID := "not-a-uuid"
	event.ActorPrincipalID = &notAUUID
	err := AppendWith(ctx, writer, env.schema, event, func(ctx context.Context, tx TxOps) error {
		_, err := tx.Exec(ctx, attemptInsert)
		return err
	})
	if !errors.Is(err, ErrMutationFailed) {
		t.Fatalf("err = %v, want ErrMutationFailed", err)
	}
	if n := countRows(t, ctx, env, `SELECT count(*) FROM auth.login_attempts`); n != 0 {
		t.Errorf("login_attempts rows = %d after an audit insert fault, want 0", n)
	}
	if n := countRows(t, ctx, env, `SELECT count(*) FROM auth.security_audit_events`); n != 0 {
		t.Errorf("audit rows = %d, want 0", n)
	}
}

func TestAppendWithFaultInApplyLeavesNoAuditRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	writer := runtimeRolePool(t, ctx, env.pool)
	err := AppendWith(ctx, writer, env.schema, resetRequested(), func(ctx context.Context, tx TxOps) error {
		if _, err := tx.Exec(ctx, attemptInsert); err != nil {
			return err
		}
		return errors.New("apply fault after a write")
	})
	if !errors.Is(err, ErrMutationFailed) {
		t.Fatalf("err = %v, want ErrMutationFailed", err)
	}
	if n := countRows(t, ctx, env, `SELECT count(*) FROM auth.login_attempts`); n != 0 {
		t.Errorf("login_attempts rows = %d, want 0", n)
	}
	if n := countRows(t, ctx, env, `SELECT count(*) FROM auth.security_audit_events`); n != 0 {
		t.Errorf("audit rows = %d, want 0", n)
	}
}

func TestAppendWithRollsBackAGuardedWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	writer := runtimeRolePool(t, ctx, env.pool)
	err := AppendWith(ctx, writer, env.schema, resetRequested(), func(ctx context.Context, tx TxOps) error {
		_, err := tx.Exec(ctx, `INSERT INTO auth.organizations (id, name, slug) VALUES (gen_random_uuid(), 'guarded', 'guarded')`)
		return err
	})
	if !errors.Is(err, ErrGuardedWriteInAppend) {
		t.Fatalf("err = %v, want ErrGuardedWriteInAppend", err)
	}
	if n := countRows(t, ctx, env, `SELECT count(*) FROM auth.organizations WHERE slug = 'guarded'`); n != 0 {
		t.Errorf("guarded row survived: %d", n)
	}
	if n := countRows(t, ctx, env, `SELECT count(*) FROM auth.security_audit_events`); n != 0 {
		t.Errorf("audit rows = %d, want 0", n)
	}
}

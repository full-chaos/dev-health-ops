package admin

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// lockDB begins a lockTx that records every statement in order.
type lockDB struct {
	pgDB
	tx *lockTx
}

func (d *lockDB) Begin(context.Context) (pgx.Tx, error) { return d.tx, nil }

type lockTx struct {
	pgx.Tx
	statements []string
	args       [][]any
	lockErr    error
}

func (x *lockTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	x.statements, x.args = append(x.statements, sql), append(x.args, args)
	if sql == rule.SharedWriteLockSQL && x.lockErr != nil {
		return pgconn.CommandTag{}, x.lockErr
	}
	return pgconn.NewCommandTag("SELECT 1"), nil
}

func (x *lockTx) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	x.statements, x.args = append(x.statements, sql), append(x.args, args)
	return noRows{}, nil
}

func (x *lockTx) Commit(context.Context) error   { return nil }
func (x *lockTx) Rollback(context.Context) error { return nil }

type noRows struct{ pgx.Rows }

func (noRows) Next() bool { return false }
func (noRows) Close()     {}
func (noRows) Err() error { return nil }

// While the platform roles are owned, the guarded write's first statement is
// the platform role write lock taken shared, before the target read: a grant
// in flight is waited for and then seen. Off, the transaction is today's.
func TestGuardedWriteTakesThePlatformRoleWriteLockFirstWhileOwned(t *testing.T) {
	caller := &policy.User{UserID: uuid.NewString(), ID: uuid.New()}
	target := uuid.New()
	for _, owned := range []bool{true, false} {
		tx := &lockTx{}
		h := &handlers{store: pgStore{Pool: &lockDB{tx: tx}, PlatformRolesOwned: owned}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
		recorder := httptest.NewRecorder()
		h.superuserGuardedWrite(context.Background(), recorder, caller, nil, "update_user", func(g *superuserTx) func() {
			if g.refuse(target) {
				return nil
			}
			return func() { recorder.WriteHeader(http.StatusOK) }
		})
		targets, err := h.store.targetsSuperuserSQL()
		if err != nil {
			t.Fatal(err)
		}
		want := []string{targets}
		if owned {
			want = []string{rule.SharedWriteLockSQL, targets}
		}
		if recorder.Code != http.StatusOK || len(tx.statements) != len(want) {
			t.Fatalf("owned %v: status %d, statements %q, want %q", owned, recorder.Code, tx.statements, want)
		}
		for i := range want {
			if tx.statements[i] != want[i] {
				t.Fatalf("owned %v: statement %d is %q, want %q", owned, i, tx.statements[i], want[i])
			}
		}
		if owned && (tx.statements[0] != "SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 0))" || len(tx.args[0]) != 1 || tx.args[0][0] != "auth.platform_role_assignments:write") {
			t.Fatalf("owned: lock %q %v, want the shared transaction lock on auth.platform_role_assignments:write", tx.statements[0], tx.args[0])
		}
	}
}

// A failed lock answers 500 and runs no part of the write.
func TestGuardedWriteRefusesWhenThePlatformRoleWriteLockFails(t *testing.T) {
	tx := &lockTx{lockErr: errors.New("lock failed")}
	h := &handlers{store: pgStore{Pool: &lockDB{tx: tx}, PlatformRolesOwned: true}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	recorder := httptest.NewRecorder()
	ran := false
	h.superuserGuardedWrite(context.Background(), recorder, &policy.User{ID: uuid.New()}, nil, "update_user", func(*superuserTx) func() {
		ran = true
		return func() {}
	})
	if ran || recorder.Code != http.StatusInternalServerError || len(tx.statements) != 1 {
		t.Fatalf("write ran %v, status %d, statements %q; want no write, 500, the lock only", ran, recorder.Code, tx.statements)
	}
}

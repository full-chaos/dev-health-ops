package session

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type lockoutStmtTx struct {
	pgx.Tx
	stmts []string
}

func (f *lockoutStmtTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	f.stmts = append(f.stmts, sql)
	return pgconn.CommandTag{}, nil
}

func (f *lockoutStmtTx) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	f.stmts = append(f.stmts, sql)
	return &lockoutOneRow{left: 1}, nil
}

// lockoutOneRow serves the zero-count row the INSERT .. ON CONFLICT leaves.
type lockoutOneRow struct {
	pgx.Rows
	left int
}

func (r *lockoutOneRow) Next() bool {
	r.left--
	return r.left >= 0
}

func (r *lockoutOneRow) Scan(dest ...any) error {
	*dest[0].(*uuid.UUID) = uuid.New()
	*dest[1].(*int64) = 0
	return nil
}
func (*lockoutOneRow) Err() error { return nil }
func (*lockoutOneRow) Close()     {}

// The failed-login counter takes the row lock in one upsert, then writes.
func TestRecordFailedAttemptStatementOrder(t *testing.T) {
	h := handlers{Deps: Deps{Now: time.Now, NewUUID: uuid.New}}
	tx := &lockoutStmtTx{}
	if err := h.recordFailedAttempt(context.Background(), tx, "a@example.com"); err != nil {
		t.Fatal(err)
	}
	if len(tx.stmts) != 2 {
		t.Fatalf("statements %q", tx.stmts)
	}
	if s := tx.stmts[0]; !strings.Contains(s, "INSERT INTO login_attempts") || !strings.Contains(s, "ON CONFLICT (email) DO UPDATE") || !strings.Contains(s, "RETURNING") {
		t.Errorf("first statement is not the locking upsert: %s", s)
	}
	if s := tx.stmts[1]; !strings.HasPrefix(s, "UPDATE login_attempts") {
		t.Errorf("second statement is not the UPDATE: %s", s)
	}
}

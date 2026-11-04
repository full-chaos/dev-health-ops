package audit

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// groupDB is a fake transaction opener: it records each transaction's
// statements and rows, holds each Commit for one receive from hold (a
// closed hold releases them all), and fails the insert of a row whose
// request id is its bad value.
type groupDB struct {
	mu       sync.Mutex
	begins   int
	commits  [][]string // the request ids of each committed transaction
	firsts   []string   // the first statement of each transaction
	inCommit int        // rows held in a Commit
	hold     chan struct{}
	bad      string
}

type groupTx struct {
	pgx.Tx
	db    *groupDB
	stmts int
	rows  []string
}

func (db *groupDB) begin(context.Context) (pgx.Tx, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.begins++
	return &groupTx{db: db}, nil
}

func (tx *groupTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()
	if tx.stmts == 0 {
		tx.db.firsts = append(tx.db.firsts, sql)
	}
	tx.stmts++
	if strings.Contains(sql, "INSERT INTO") {
		id, _ := args[7].(*string)
		if id == nil {
			return pgconn.CommandTag{}, errors.New("no request id")
		}
		if *id == tx.db.bad {
			return pgconn.CommandTag{}, errors.New("insert refused")
		}
		tx.rows = append(tx.rows, *id)
	}
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (tx *groupTx) Commit(context.Context) error {
	tx.db.mu.Lock()
	hold := tx.db.hold
	tx.db.inCommit += len(tx.rows)
	tx.db.mu.Unlock()
	if hold != nil {
		<-hold
	}
	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()
	tx.db.inCommit -= len(tx.rows)
	tx.db.commits = append(tx.db.commits, tx.rows)
	return nil
}

func (tx *groupTx) Rollback(context.Context) error { return nil }

func (db *groupDB) state() (inCommit int, commits [][]string, begins int) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.inCommit, append([][]string(nil), db.commits...), db.begins
}

func (db *groupDB) committedRows() map[string]int {
	_, commits, _ := db.state()
	rows := map[string]int{}
	for _, c := range commits {
		for _, id := range c {
			rows[id]++
		}
	}
	return rows
}

func groupEvent(id string) AuditEvent {
	return AuditEvent{EventType: EventAuthorizeHighRiskDecided, Outcome: OutcomeAllowed, Attributes: highRiskAttrs(), RequestID: &id}
}

func newTestGroupWriter(t *testing.T, db *groupDB, flushers, maxRows int) *GroupWriter {
	t.Helper()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewGroupWriter(GroupWriterConfig{Begin: db.begin, Schema: schema, Flushers: flushers, MaxRows: maxRows})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// waitFor polls cond for up to 5 s.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func appendAsync(w *GroupWriter, ctx context.Context, id string) <-chan error {
	out := make(chan error, 1)
	go func() { out <- w.Append(ctx, groupEvent(id)) }()
	return out
}

func idOf(i int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", i) }

// TestGroupWriterCoalescesConcurrentCalls: 64 calls arrive while the first
// commits are held; the rest share the next transactions.
func TestGroupWriterCoalescesConcurrentCalls(t *testing.T) {
	const calls, flushers = 64, 2
	db := &groupDB{hold: make(chan struct{})}
	w := newTestGroupWriter(t, db, flushers, 0)
	results := make([]<-chan error, calls)
	for i := range results {
		results[i] = appendAsync(w, context.Background(), idOf(i))
	}
	waitFor(t, "every call to be held or queued", func() bool {
		in, _, _ := db.state()
		return in+w.queued() == calls
	})
	close(db.hold)
	for i, r := range results {
		if err := <-r; err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	_, commits, _ := db.state()
	if len(commits) > 2*flushers {
		t.Fatalf("commits = %d for %d calls, want <= %d", len(commits), calls, 2*flushers)
	}
	if rows := db.committedRows(); len(rows) != calls {
		t.Fatalf("committed rows = %d, want %d", len(rows), calls)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.active != 0 || len(w.pending) != 0 {
		t.Fatalf("after the calls: active = %d, pending = %d, want 0 and 0", w.active, len(w.pending))
	}
}

// TestGroupWriterAnswersOnlyAfterCommit: no call of a group held in Commit
// has returned, the leading call nor a waiting one.
func TestGroupWriterAnswersOnlyAfterCommit(t *testing.T) {
	db := &groupDB{hold: make(chan struct{})}
	w := newTestGroupWriter(t, db, 1, 0)
	first := appendAsync(w, context.Background(), idOf(0))
	waitFor(t, "the first commit to start", func() bool { in, _, _ := db.state(); return in == 1 })
	group := []<-chan error{appendAsync(w, context.Background(), idOf(1)), appendAsync(w, context.Background(), idOf(2))}
	waitFor(t, "two queued calls", func() bool { return w.queued() == 2 })
	db.hold <- struct{}{}
	if err := <-first; err != nil {
		t.Fatalf("first: %v", err)
	}
	waitFor(t, "the group commit to start", func() bool { in, _, _ := db.state(); return in == 2 })
	for i, r := range group {
		select {
		case err := <-r:
			t.Fatalf("call %d returned (%v) while its commit was held", i+1, err)
		case <-time.After(50 * time.Millisecond):
		}
	}
	close(db.hold)
	for i, r := range group {
		if err := <-r; err != nil {
			t.Fatalf("call %d: %v", i+1, err)
		}
	}
}

// TestGroupWriterIsolatesAFailedCall: one call's refused row fails that call
// only; the other call of its group is committed by the call-by-call retry.
func TestGroupWriterIsolatesAFailedCall(t *testing.T) {
	db := &groupDB{hold: make(chan struct{}), bad: idOf(1)}
	w := newTestGroupWriter(t, db, 1, 0)
	first := appendAsync(w, context.Background(), idOf(0))
	waitFor(t, "the first commit to start", func() bool { in, _, _ := db.state(); return in == 1 })
	bad := appendAsync(w, context.Background(), idOf(1))
	good := appendAsync(w, context.Background(), idOf(2))
	waitFor(t, "two queued calls", func() bool { return w.queued() == 2 })
	close(db.hold)
	if err := <-first; err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := <-bad; !errors.Is(err, ErrMutationFailed) {
		t.Fatalf("bad: %v, want ErrMutationFailed", err)
	}
	if err := <-good; err != nil {
		t.Fatalf("good: %v, want nil (the retry commits it alone)", err)
	}
	rows := db.committedRows()
	if rows[idOf(1)] != 0 || rows[idOf(2)] != 1 || rows[idOf(0)] != 1 {
		t.Fatalf("committed rows = %v", rows)
	}
}

// TestGroupWriterArmsEveryTransaction: the commit guard is armed by the
// first statement of each transaction.
func TestGroupWriterArmsEveryTransaction(t *testing.T) {
	db := &groupDB{}
	w := newTestGroupWriter(t, db, 0, 0)
	for i := range 3 {
		if err := w.Append(context.Background(), groupEvent(idOf(i))); err != nil {
			t.Fatal(err)
		}
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(db.firsts) != db.begins || db.begins == 0 {
		t.Fatalf("transactions = %d, first statements = %d", db.begins, len(db.firsts))
	}
	for _, sql := range db.firsts {
		if !strings.Contains(sql, "set_config('"+commitGuardSetting+"', pg_current_xact_id()::text, true)") {
			t.Fatalf("first statement %q does not arm the commit guard", sql)
		}
	}
}

// TestGroupWriterBoundsRowsPerTransaction: no transaction holds more than
// MaxRows rows.
func TestGroupWriterBoundsRowsPerTransaction(t *testing.T) {
	const maxRows, calls = 4, 10
	db := &groupDB{hold: make(chan struct{})}
	w := newTestGroupWriter(t, db, 1, maxRows)
	first := appendAsync(w, context.Background(), idOf(0))
	waitFor(t, "the first commit to start", func() bool { in, _, _ := db.state(); return in == 1 })
	results := make([]<-chan error, calls)
	for i := range results {
		results[i] = appendAsync(w, context.Background(), idOf(i+1))
	}
	waitFor(t, "every call queued", func() bool { return w.queued() == calls })
	close(db.hold)
	for _, r := range append(results, first) {
		if err := <-r; err != nil {
			t.Fatal(err)
		}
	}
	_, commits, _ := db.state()
	for _, c := range commits {
		if len(c) > maxRows {
			t.Fatalf("a transaction held %d rows, want <= %d", len(c), maxRows)
		}
	}
	if rows := db.committedRows(); len(rows) != calls+1 {
		t.Fatalf("committed rows = %d, want %d", len(rows), calls+1)
	}
}

// TestGroupWriterDropsACancelledCall: a call whose context ends while it is
// queued returns the context error and its rows are never written; the
// writer goes on serving.
func TestGroupWriterDropsACancelledCall(t *testing.T) {
	db := &groupDB{hold: make(chan struct{})}
	w := newTestGroupWriter(t, db, 1, 0)
	first := appendAsync(w, context.Background(), idOf(0))
	waitFor(t, "the first commit to start", func() bool { in, _, _ := db.state(); return in == 1 })
	ctx, cancel := context.WithCancel(context.Background())
	cancelled := appendAsync(w, ctx, idOf(1))
	waitFor(t, "the call queued", func() bool { return w.queued() == 1 })
	cancel()
	select {
	case err := <-cancelled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled call: %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the cancelled call did not return while the commit was held")
	}
	close(db.hold)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := w.Append(context.Background(), groupEvent(idOf(2))); err != nil {
		t.Fatalf("a later call: %v", err)
	}
	if rows := db.committedRows(); rows[idOf(1)] != 0 || rows[idOf(2)] != 1 {
		t.Fatalf("committed rows = %v", rows)
	}
}

// TestGroupWriterNeverWritesACancelledQueuedCall: a call whose context has
// ended while it is queued commits no row and gets the context error, one-row
// and multi-row calls alike: when the call itself holds the flusher slot (an
// idle writer hands it the slot at once, so its lead and its context are ready
// together), and when another call leads the transaction it would join.
func TestGroupWriterNeverWritesACancelledQueuedCall(t *testing.T) {
	events := func(rows int) []AuditEvent {
		out := make([]AuditEvent, rows)
		for r := range out {
			out[r] = groupEvent(idOf(100 + r))
		}
		return out
	}
	cancelledRows := func(db *groupDB, rows int) int {
		got, committed := db.committedRows(), 0
		for r := range rows {
			committed += got[idOf(100+r)]
		}
		return committed
	}
	for _, rows := range []int{1, 2} {
		t.Run(fmt.Sprintf("%d rows, handed the slot", rows), func(t *testing.T) {
			committed := 0
			for i := range 50 {
				db := &groupDB{}
				w := newTestGroupWriter(t, db, 1, 0)
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if err := w.Append(ctx, events(rows)...); !errors.Is(err, context.Canceled) {
					t.Fatalf("iteration %d: %v, want context.Canceled", i, err)
				}
				committed += cancelledRows(db, rows)
				if err := w.Append(context.Background(), groupEvent(idOf(1))); err != nil {
					t.Fatalf("iteration %d: a later call: %v", i, err)
				}
			}
			if committed != 0 {
				t.Fatalf("cancelled queued calls committed %d rows over 50 runs, want 0", committed)
			}
		})
		t.Run(fmt.Sprintf("%d rows, another call leads", rows), func(t *testing.T) {
			committed := 0
			for i := range 50 {
				db := &groupDB{hold: make(chan struct{})}
				w := newTestGroupWriter(t, db, 1, 0)
				first := appendAsync(w, context.Background(), idOf(0))
				waitFor(t, "the first commit to start", func() bool { in, _, _ := db.state(); return in == 1 })
				live := appendAsync(w, context.Background(), idOf(1))
				waitFor(t, "the live call queued", func() bool { return w.queued() == 1 })
				ctx, cancel := context.WithCancel(context.Background())
				cancelled := make(chan error, 1)
				go func() { cancelled <- w.Append(ctx, events(rows)...) }()
				waitFor(t, "the call queued behind the live call", func() bool { return w.queued() == 2 })
				// The leader's release races the cancelled call's own exit:
				// the live call then leads a transaction the cancelled call
				// is still queued for.
				cancel()
				close(db.hold)
				for _, r := range []<-chan error{first, live} {
					if err := <-r; err != nil {
						t.Fatalf("iteration %d: %v", i, err)
					}
				}
				if err := <-cancelled; !errors.Is(err, context.Canceled) {
					t.Fatalf("iteration %d: cancelled call: %v, want context.Canceled", i, err)
				}
				committed += cancelledRows(db, rows)
				if got := db.committedRows()[idOf(1)]; got != 1 {
					t.Fatalf("iteration %d: the live call's row committed %d times, want 1", i, got)
				}
			}
			if committed != 0 {
				t.Fatalf("cancelled queued calls committed %d rows over 50 runs, want 0", committed)
			}
		})
	}
}

// TestGroupWriterRefusesBeforeItQueues: a refused set opens no transaction.
func TestGroupWriterRefusesBeforeItQueues(t *testing.T) {
	db := &groupDB{}
	w := newTestGroupWriter(t, db, 0, 0)
	bad := groupEvent(idOf(0))
	bad.Attributes = map[string]any{"password": "x"}
	if err := w.Append(context.Background(), bad); !errors.Is(err, ErrAttributeNotAllowed) {
		t.Fatalf("bad attribute: %v, want ErrAttributeNotAllowed", err)
	}
	if err := w.Append(context.Background(), AuditEvent{EventType: "organization.created", Outcome: OutcomeAllowed}); !errors.Is(err, ErrNotAppendOnly) {
		t.Fatalf("state-change type: %v, want ErrNotAppendOnly", err)
	}
	if err := w.Append(context.Background()); !errors.Is(err, ErrMutationFailed) {
		t.Fatalf("no rows: %v, want ErrMutationFailed", err)
	}
	if _, _, begins := db.state(); begins != 0 || w.queued() != 0 {
		t.Fatalf("begins = %d, queued = %d, want 0 and 0", begins, w.queued())
	}
}

func TestNewGroupWriterNeedsAnOpener(t *testing.T) {
	if _, err := NewGroupWriter(GroupWriterConfig{}); err == nil {
		t.Fatal("a writer with no opener was built")
	}
}

func BenchmarkGroupWriterCommitsPerCall(b *testing.B) {
	db := &groupDB{}
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		b.Fatal(err)
	}
	w, err := NewGroupWriter(GroupWriterConfig{Begin: db.begin, Schema: schema})
	if err != nil {
		b.Fatal(err)
	}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := w.Append(context.Background(), groupEvent(idOf(0))); err != nil {
				b.Fatal(err)
			}
		}
	})
	_, commits, _ := db.state()
	b.ReportMetric(float64(len(commits))/float64(b.N), "commits/call")
}

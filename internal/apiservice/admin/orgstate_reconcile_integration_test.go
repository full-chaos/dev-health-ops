//go:build integration

package admin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/orgreactivate"
)

const reconciledLine = "admin: organization is_active reconciled to auth state"

// passAuth answers by organization. onRead runs after the answer of read n is taken, outside the lock.
type passAuth struct {
	mu      sync.Mutex
	answers map[uuid.UUID]orgreactivate.State
	errs    map[uuid.UUID]error
	reads   int
	onRead  func(n int, org uuid.UUID)
}

func newPassAuth() *passAuth {
	return &passAuth{answers: map[uuid.UUID]orgreactivate.State{}, errs: map[uuid.UUID]error{}}
}

func (a *passAuth) set(org uuid.UUID, status string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.errs, org)
	a.answers[org] = orgreactivate.State{Status: status, Revision: 1}
}

func (a *passAuth) fail(org uuid.UUID, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.errs[org] = err
}

func (a *passAuth) readCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.reads
}

func (a *passAuth) State(_ context.Context, org uuid.UUID) (orgreactivate.State, error) {
	a.mu.Lock()
	a.reads++
	n, state, err := a.reads, a.answers[org], a.errs[org]
	hook := a.onRead
	a.mu.Unlock()
	if hook != nil {
		hook(n, org)
	}
	return state, err
}

type lockedLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *lockedLog) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func (l *lockedLog) count(part string) int { return strings.Count(l.text(), part) }

func reconcilerFor(pool *pgxpool.Pool, state orgreactivate.StateReader) (*OrgStateReconciler, *lockedLog) {
	log := &lockedLog{}
	return NewOrgStateReconciler(pool, state, slog.New(slog.NewTextHandler(log, nil))), log
}

// boundedPass ends a pass that waits on a lock and reports whether it ran into that bound.
func boundedPass(ctx context.Context, r *OrgStateReconciler) (bounded bool) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	r.pass(ctx)
	return ctx.Err() != nil
}

func updatedAt(ctx context.Context, t *testing.T, pool *pgxpool.Pool, org uuid.UUID) time.Time {
	t.Helper()
	var at time.Time
	if err := pool.QueryRow(ctx, `SELECT updated_at FROM organizations WHERE id = $1`, org).Scan(&at); err != nil {
		t.Fatal(err)
	}
	return at
}

func wantOneFixedLine(t *testing.T, log *lockedLog, org uuid.UUID, from, to bool) {
	t.Helper()
	if got := log.count(reconciledLine); got != 1 {
		t.Fatalf("%d fixed lines, want 1:\n%s", got, log.text())
	}
	if want := fmt.Sprintf("org_id=%s from=%v to=%v", org, from, to); log.count(want) != 1 {
		t.Fatalf("no line with %q:\n%s", want, log.text())
	}
}

func wantNoWarn(t *testing.T, log *lockedLog) {
	t.Helper()
	if log.count("level=WARN") != 0 {
		t.Fatalf("a WARN line in a clean pass:\n%s", log.text())
	}
}

// probeRow takes lock on the row with NOWAIT in its own transaction and returns the error.
func probeRow(ctx context.Context, t *testing.T, pool *pgxpool.Pool, org uuid.UUID, lock string) error {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var one int
	return tx.QueryRow(ctx, `SELECT 1 FROM organizations WHERE id = $1 FOR `+lock+` NOWAIT`, org).Scan(&one)
}

// C1 red: a change in auth after go-api's PATCH leaves the row stale until a pass.
func TestReconcileFixesAnAuthSuspendAfterAGoAPIPatch(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	auth := &liveAuth{status: "suspended", rev: 1}
	org := seedOrg(ctx, t, pool, false)
	if code, answered := patchLive(ctx, liveHandlers(pool, auth), org, `{"is_active": true}`); code != http.StatusOK || !answered {
		t.Fatalf("PATCH answered %d is_active=%v", code, answered)
	}
	if err := auth.set("suspended", "native"); err != nil {
		t.Fatal(err)
	}
	if !storedActive(ctx, t, pool, org) {
		t.Fatal("the row moved before any pass")
	}
	r, log := reconcilerFor(pool, auth)
	boundedPass(ctx, r)
	if storedActive(ctx, t, pool, org) {
		t.Fatal("the pass left is_active true while auth holds suspended")
	}
	wantOneFixedLine(t, log, org, true, false)
	wantNoWarn(t, log)
}

// C2: the other direction.
func TestReconcileFollowsAnAuthReactivate(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	auth := newPassAuth()
	org := seedOrg(ctx, t, pool, false)
	auth.set(org, "active")
	r, log := reconcilerFor(pool, auth)
	boundedPass(ctx, r)
	if !storedActive(ctx, t, pool, org) {
		t.Fatal("the pass left is_active false while auth holds active")
	}
	wantOneFixedLine(t, log, org, false, true)
}

// C4: an equal row: no write, no line, and the row is free during the read.
func TestReconcileLeavesAnEqualRowUntouchedAndUnlocked(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	auth := newPassAuth()
	org := seedOrg(ctx, t, pool, true)
	auth.set(org, "active")
	before := updatedAt(ctx, t, pool, org)
	auth.onRead = func(int, uuid.UUID) {
		if err := probeRow(ctx, t, pool, org, "UPDATE"); err != nil {
			t.Errorf("the row is locked during the read of an equal row: %v", err)
		}
	}
	r, log := reconcilerFor(pool, auth)
	boundedPass(ctx, r)
	if auth.readCount() != 1 {
		t.Fatalf("%d reads of one equal row, want 1", auth.readCount())
	}
	if !updatedAt(ctx, t, pool, org).Equal(before) || log.count(reconciledLine) != 0 {
		t.Fatalf("an equal row was written:\n%s", log.text())
	}
	wantNoWarn(t, log)
}

// C5: a refused row, first by id, is skipped and counted; a 404 never writes false; the next row is fixed. The WARN
// names no org.
func TestReconcileSkipsARefusedRowAndFixesTheNext(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	auth := newPassAuth()
	refused, drifted := seedOrg(ctx, t, pool, true), seedOrg(ctx, t, pool, true)
	if bytes.Compare(refused[:], drifted[:]) > 0 {
		refused, drifted = drifted, refused
	}
	auth.fail(refused, fmt.Errorf("%w: status 404", orgreactivate.ErrRefused))
	auth.set(drifted, "suspended")
	r, log := reconcilerFor(pool, auth)
	boundedPass(ctx, r)
	if !storedActive(ctx, t, pool, refused) {
		t.Fatal("a 404 wrote false")
	}
	if storedActive(ctx, t, pool, drifted) {
		t.Fatal("a refused row stopped the pass")
	}
	if log.count("level=WARN") != 1 || log.count("refused=1") != 1 || log.count("stopped=false") != 1 {
		t.Fatalf("want one WARN with the count:\n%s", log.text())
	}
	for _, line := range strings.Split(log.text(), "\n") {
		if strings.Contains(line, "level=WARN") && (strings.Contains(line, refused.String()) || strings.Contains(line, drifted.String())) {
			t.Fatalf("the WARN names an organization: %s", line)
		}
	}
}

// C6: auth down: no write, no more reads in this pass; the next pass fixes it.
func TestReconcileStopsOnUnavailableAndTheNextPassFixes(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	auth := newPassAuth()
	a, b := seedOrg(ctx, t, pool, true), seedOrg(ctx, t, pool, true)
	unavailable := fmt.Errorf("%w: status 503", orgreactivate.ErrUnavailable)
	auth.fail(a, unavailable)
	auth.fail(b, unavailable)
	r, log := reconcilerFor(pool, auth)
	boundedPass(ctx, r)
	if auth.readCount() != 1 {
		t.Fatalf("%d reads after the failure, want the 1 that failed", auth.readCount())
	}
	if !storedActive(ctx, t, pool, a) || !storedActive(ctx, t, pool, b) {
		t.Fatal("a failed read wrote the row")
	}
	if log.count("level=WARN") != 1 || log.count("stopped=true") != 1 {
		t.Fatalf("want one WARN:\n%s", log.text())
	}
	auth.set(a, "suspended")
	auth.set(b, "suspended")
	boundedPass(ctx, r)
	if storedActive(ctx, t, pool, a) || storedActive(ctx, t, pool, b) {
		t.Fatal("the next pass did not fix the rows")
	}
}

// C7: auth moves between the two reads: the read under the lock decides, and here it matches the row.
func TestReconcileDecidesOnTheReadUnderTheLock(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	auth := newPassAuth()
	org := seedOrg(ctx, t, pool, true)
	auth.set(org, "suspended")
	auth.onRead = func(n int, _ uuid.UUID) {
		if n == 1 {
			auth.set(org, "active")
		}
	}
	before := updatedAt(ctx, t, pool, org)
	r, log := reconcilerFor(pool, auth)
	boundedPass(ctx, r)
	if auth.readCount() != 2 {
		t.Fatalf("%d reads, want 2", auth.readCount())
	}
	if !storedActive(ctx, t, pool, org) || !updatedAt(ctx, t, pool, org).Equal(before) || log.count(reconciledLine) != 0 {
		t.Fatalf("the first read decided the write:\n%s", log.text())
	}
}

// C8: a row another transaction holds is skipped with no wait; the next pass fixes it.
func TestReconcileSkipsARowAnotherTransactionHolds(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	auth := newPassAuth()
	org := seedOrg(ctx, t, pool, true)
	auth.set(org, "suspended")
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `SELECT 1 FROM organizations WHERE id = $1 FOR UPDATE`, org); err != nil {
		t.Fatal(err)
	}
	r, log := reconcilerFor(pool, auth)
	// A pass that waits on this row has no other end: this is a hang detector, not a time budget.
	if boundedPass(ctx, r) {
		t.Fatal("the pass waited on a held row")
	}
	if !storedActive(ctx, t, pool, org) || log.count(reconciledLine) != 0 {
		t.Fatalf("a held row was written:\n%s", log.text())
	}
	wantNoWarn(t, log)
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	boundedPass(ctx, r)
	if storedActive(ctx, t, pool, org) {
		t.Fatal("the next pass did not fix the row")
	}
}

// C9: a pass and a PATCH in both orders: the row ends equal to auth.
func TestReconcileAndAPatchEndEqualToAuth(t *testing.T) {
	t.Run("the pass holds the row, the PATCH waits", func(t *testing.T) {
		ctx, pool := startOrgsDB(t)
		auth := &liveAuth{status: "suspended", rev: 1, holdRead: 2, held: make(chan struct{}), release: make(chan struct{})}
		org := seedOrg(ctx, t, pool, true)
		r, _ := reconcilerFor(pool, auth)
		passDone := make(chan struct{})
		go func() {
			defer close(passDone)
			r.pass(ctx)
		}()
		<-auth.held
		patchDone := make(chan struct{})
		go func() {
			defer close(patchDone)
			_, _ = patchLive(ctx, liveHandlers(pool, auth), org, `{"is_active": true}`)
		}()
		for lockWaiters(ctx, t, pool) == 0 {
			time.Sleep(10 * time.Millisecond)
		}
		close(auth.release)
		<-passDone
		<-patchDone
		if got, want := storedActive(ctx, t, pool, org), auth.current() == "active"; got != want {
			t.Fatalf("row is_active=%v, auth holds %s", got, auth.current())
		}
	})
	t.Run("the PATCH holds the row, the pass skips it", func(t *testing.T) {
		ctx, pool := startOrgsDB(t)
		auth := &liveAuth{status: "suspended", rev: 1, holdRead: 2, held: make(chan struct{}), release: make(chan struct{})}
		org := seedOrg(ctx, t, pool, false)
		patchDone := make(chan struct{})
		go func() {
			defer close(patchDone)
			_, _ = patchLive(ctx, liveHandlers(pool, auth), org, `{"is_active": true}`)
		}()
		<-auth.held
		r, log := reconcilerFor(pool, auth)
		boundedPass(ctx, r)
		if log.count(reconciledLine) != 0 || log.count("level=WARN") != 0 {
			t.Fatalf("the pass did not skip a held row:\n%s", log.text())
		}
		close(auth.release)
		<-patchDone
		if got, want := storedActive(ctx, t, pool, org), auth.current() == "active"; got != want {
			t.Fatalf("row is_active=%v, auth holds %s", got, auth.current())
		}
	})
}

// C10: a row deleted between the page and the lock: no write, no error.
func TestReconcileSkipsARowDeletedBeforeTheLock(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	auth := newPassAuth()
	org := seedOrg(ctx, t, pool, true)
	auth.set(org, "suspended")
	auth.onRead = func(int, uuid.UUID) {
		if _, err := pool.Exec(ctx, `DELETE FROM organizations WHERE id = $1`, org); err != nil {
			t.Errorf("delete: %v", err)
		}
	}
	r, log := reconcilerFor(pool, auth)
	boundedPass(ctx, r)
	if log.count(reconciledLine) != 0 {
		t.Fatalf("a line for a deleted row:\n%s", log.text())
	}
	wantNoWarn(t, log)
}

// C11: more rows than one page, with the drift on the last id.
func TestReconcileWalksEveryPage(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	auth := newPassAuth()
	var last uuid.UUID
	for i := 0; i <= orgStateReconcilePage; i++ {
		org := seedOrg(ctx, t, pool, true)
		auth.set(org, "active")
		if bytes.Compare(org[:], last[:]) > 0 {
			last = org
		}
	}
	auth.set(last, "suspended")
	r, log := reconcilerFor(pool, auth)
	boundedPass(ctx, r)
	if storedActive(ctx, t, pool, last) {
		t.Fatal("the row after the first page was not fixed")
	}
	if got, want := auth.readCount(), orgStateReconcilePage+2; got != want {
		t.Fatalf("%d reads, want %d: one per row and one under the lock", got, want)
	}
	wantOneFixedLine(t, log, last, true, false)
}

// C12: two passes on one drifted row: the one that comes second writes nothing and logs nothing.
func TestTwoPassesOnOneRowWriteOneLine(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	auth := newPassAuth()
	org := seedOrg(ctx, t, pool, true)
	auth.set(org, "suspended")
	first, firstLog := reconcilerFor(pool, auth)
	second, secondLog := reconcilerFor(pool, auth)
	var writtenAt time.Time
	auth.onRead = func(n int, _ uuid.UUID) {
		if n == 1 {
			boundedPass(ctx, second)
			writtenAt = updatedAt(ctx, t, pool, org)
		}
	}
	boundedPass(ctx, first)
	if got := firstLog.count(reconciledLine) + secondLog.count(reconciledLine); got != 1 {
		t.Fatalf("%d fixed lines, want 1:\n%s%s", got, firstLog.text(), secondLog.text())
	}
	if !updatedAt(ctx, t, pool, org).Equal(writtenAt) {
		t.Fatal("the later pass wrote the row again")
	}
}

// Condition (a): the lock leaves foreign key inserts free and still orders against a PATCH.
func TestReconcileLockLeavesForeignKeyInsertsFree(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	auth := newPassAuth()
	org := seedOrg(ctx, t, pool, true)
	auth.set(org, "suspended")
	probed := false
	auth.onRead = func(n int, _ uuid.UUID) {
		if n != 2 {
			return
		}
		probed = true
		if err := probeRow(ctx, t, pool, org, "KEY SHARE"); err != nil {
			t.Errorf("FOR KEY SHARE NOWAIT during the read under the lock: %v", err)
		}
		var pgErr *pgconn.PgError
		if err := probeRow(ctx, t, pool, org, "NO KEY UPDATE"); !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
			t.Errorf("FOR NO KEY UPDATE NOWAIT during the read under the lock: %v, want 55P03", err)
		}
	}
	r, _ := reconcilerFor(pool, auth)
	boundedPass(ctx, r)
	if !probed {
		t.Fatal("no read under the lock")
	}
}

// Condition (b): a status that is neither active nor suspended changes nothing and counts as refused, at either read.
func TestReconcileRefusesAnUnknownStatus(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	auth := newPassAuth()
	first, second := seedOrg(ctx, t, pool, true), seedOrg(ctx, t, pool, true)
	auth.set(first, "deleted")
	auth.set(second, "suspended")
	auth.onRead = func(_ int, org uuid.UUID) {
		if org == second {
			auth.set(second, "deleted")
		}
	}
	r, log := reconcilerFor(pool, auth)
	boundedPass(ctx, r)
	if !storedActive(ctx, t, pool, first) || !storedActive(ctx, t, pool, second) {
		t.Fatal("an unknown status wrote the row")
	}
	if log.count("level=WARN") != 1 || log.count("refused=2") != 1 {
		t.Fatalf("want one WARN with refused=2:\n%s", log.text())
	}
}

// Condition (c), S4: after a deprovision with a failed purge auth keeps the row and answers suspended (org.Store.read
// maps disabled_at to the status; deprovision never deletes it), so the row left active goes false.
func TestReconcileClosesADeprovisionWithAFailedPurge(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	auth := newPassAuth()
	org := seedOrg(ctx, t, pool, true)
	auth.set(org, "suspended")
	r, log := reconcilerFor(pool, auth)
	boundedPass(ctx, r)
	if storedActive(ctx, t, pool, org) {
		t.Fatal("the row of a deprovisioned organization stayed active")
	}
	wantOneFixedLine(t, log, org, true, false)
}

//go:build integration

package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/orgreactivate"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/workloadclient"
)

const reconciledLine = "admin: organization is_active reconciled to auth state"

// passAuth answers by organization. onRead runs after the answer of read n is taken, outside the lock. first holds
// errors an org's next reads answer, one per read, before its usual answer.
type passAuth struct {
	mu      sync.Mutex
	answers map[uuid.UUID]orgreactivate.State
	errs    map[uuid.UUID]error
	first   map[uuid.UUID][]error
	order   []uuid.UUID
	reads   int
	onRead  func(n int, org uuid.UUID)
}

func newPassAuth() *passAuth {
	return &passAuth{answers: map[uuid.UUID]orgreactivate.State{}, errs: map[uuid.UUID]error{}, first: map[uuid.UUID][]error{}}
}

func (a *passAuth) limit(org uuid.UUID, errs ...error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.first[org] = append(a.first[org], errs...)
}

// readOrder is the org of every read so far, in order.
func (a *passAuth) readOrder() []uuid.UUID {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]uuid.UUID(nil), a.order...)
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
	a.order = append(a.order, org)
	n, state, err := a.reads, a.answers[org], a.errs[org]
	if queued := a.first[org]; len(queued) > 0 {
		err, a.first[org] = queued[0], queued[1:]
	}
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

// passClock is the pacer's clock in these cells: it stands still, and a wait moves it at once. waits holds every wait
// asked; onWait runs at each one.
type passClock struct {
	mu     sync.Mutex
	base   time.Time
	offset time.Duration
	waits  []time.Duration
	onWait func(d time.Duration)
}

func (c *passClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.base.Add(c.offset)
}

func (c *passClock) wait(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.offset += d
	c.waits = append(c.waits, d)
	hook := c.onWait
	c.mu.Unlock()
	if hook != nil {
		hook(d)
	}
	return nil
}

func (c *passClock) waited() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.waits...)
}

func reconcilerFor(pool *pgxpool.Pool, state orgreactivate.StateReader) (*OrgStateReconciler, *lockedLog) {
	r, log, _ := reconcilerWithClock(pool, state)
	return r, log
}

func reconcilerWithClock(pool *pgxpool.Pool, state orgreactivate.StateReader) (*OrgStateReconciler, *lockedLog, *passClock) {
	log := &lockedLog{}
	clock := &passClock{base: time.Now()}
	r := NewOrgStateReconciler(pool, state, slog.New(slog.NewTextHandler(log, nil)))
	r.now, r.wait = clock.now, clock.wait
	return r, log, clock
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

// The general per-route bucket auth-service gives the state route: authconfig's defaults
// (AUTH_SERVICE_RATE_LIMIT_PER_SECOND 20, AUTH_SERVICE_RATE_LIMIT_BURST 40), unset in prod.
const (
	authStateRoutePerSecond = 20
	authStateRouteBurst     = 40
	limiterOrgs             = 3 * authStateRouteBurst
)

// limitedAuth answers the state route behind the REAL auth rate-limit middleware; served holds every org it answered
// 200 for.
type limitedAuth struct {
	mu      sync.Mutex
	states  map[uuid.UUID]string
	served  map[uuid.UUID]bool
	limited int
}

func newLimitedAuth() *limitedAuth {
	return &limitedAuth{states: map[uuid.UUID]string{}, served: map[uuid.UUID]bool{}}
}

func (a *limitedAuth) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		OrganizationID string `json:"organization_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	org, err := uuid.Parse(req.OrganizationID)
	a.mu.Lock()
	status, known := a.states[org]
	if err == nil && known {
		a.served[org] = true
	}
	a.mu.Unlock()
	if err != nil || !known {
		http.Error(w, "unknown organization", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, `{"organization_id":%q,"status":%q,"revision":1}`, org.String(), status)
}

func (a *limitedAuth) servedCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.served)
}

func (a *limitedAuth) limitedCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.limited
}

// limitedStateClient is go-api's real state client of a server that puts bucket in front of auth.
func limitedStateClient(t *testing.T, auth *limitedAuth, bucket *httpapi.Bucket) *orgreactivate.Client {
	t.Helper()
	limited := httpapi.RateLimitWith(bucket, httpapi.WriteError)(auth)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != orgreactivate.StatePath {
			http.NotFound(w, r)
			return
		}
		rec := httptest.NewRecorder()
		limited.ServeHTTP(rec, r)
		if rec.Code == http.StatusTooManyRequests {
			auth.mu.Lock()
			auth.limited++
			auth.mu.Unlock()
		}
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
	}))
	t.Cleanup(server.Close)
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	wc, _ := workloadclient.New(t)
	client, err := orgreactivate.NewWorkload(base, wc, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// seedLimiterOrgs seeds n active orgs that auth holds active, except the highest id, which auth holds suspended.
func seedLimiterOrgs(ctx context.Context, t *testing.T, pool *pgxpool.Pool, auth *limitedAuth, n int) uuid.UUID {
	t.Helper()
	var last uuid.UUID
	for range n {
		org := seedOrg(ctx, t, pool, true)
		auth.states[org] = "active"
		if bytes.Compare(org[:], last[:]) > 0 {
			last = org
		}
	}
	auth.states[last] = "suspended"
	return last
}

// realClockPass runs one pass of a reconciler on the real clock; the bound is a hang detector, not a time budget.
func realClockPass(ctx context.Context, t *testing.T, r *OrgStateReconciler) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	r.pass(ctx)
	if ctx.Err() != nil {
		t.Fatal("the pass did not end")
	}
}

// CHAOS-13575 C-real: the REAL auth limiter at its defaults and more orgs than its burst, the drift on the last id:
// one pass covers every row, fixes the drifted one and refuses none.
func TestReconcileCoversMoreOrgsThanTheRealLimiterBurst(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	auth := newLimitedAuth()
	last := seedLimiterOrgs(ctx, t, pool, auth, limiterOrgs)
	client := limitedStateClient(t, auth, httpapi.NewBucket(authStateRoutePerSecond, authStateRouteBurst, nil))
	log := &lockedLog{}
	r := NewOrgStateReconciler(pool, client, slog.New(slog.NewTextHandler(log, nil)))
	realClockPass(ctx, t, r)
	if storedActive(ctx, t, pool, last) {
		t.Fatalf("the drifted row at the end of the id order was not fixed (429 answers: %d):\n%s", auth.limitedCount(), log.text())
	}
	if got := auth.servedCount(); got != limiterOrgs {
		t.Fatalf("auth answered %d of %d orgs (429 answers: %d):\n%s", got, limiterOrgs, auth.limitedCount(), log.text())
	}
	if log.count("level=WARN") != 0 {
		t.Fatalf("the pass refused or stopped:\n%s", log.text())
	}
	wantOneFixedLine(t, log, last, true, false)
}

// CHAOS-13575 C-two: two go-api replicas reconcile against ONE auth pod bucket: both cover every row.
func TestTwoReconcilersShareOneLimiterBucket(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	bucket := httpapi.NewBucket(authStateRoutePerSecond, authStateRouteBurst, nil)
	auths := [2]*limitedAuth{newLimitedAuth(), newLimitedAuth()}
	last := seedLimiterOrgs(ctx, t, pool, auths[0], limiterOrgs)
	for org, status := range auths[0].states {
		auths[1].states[org] = status
	}
	var logs [2]*lockedLog
	var wg sync.WaitGroup
	for i := range auths {
		logs[i] = &lockedLog{}
		r := NewOrgStateReconciler(pool, limitedStateClient(t, auths[i], bucket), slog.New(slog.NewTextHandler(logs[i], nil)))
		wg.Add(1)
		go func() {
			defer wg.Done()
			realClockPass(ctx, t, r)
		}()
	}
	wg.Wait()
	if storedActive(ctx, t, pool, last) {
		t.Fatal("the drifted row was not fixed")
	}
	for i, auth := range auths {
		if got := auth.servedCount(); got != limiterOrgs {
			t.Errorf("replica %d: auth answered %d of %d orgs (429 answers: %d):\n%s", i, got, limiterOrgs, auth.limitedCount(), logs[i].text())
		}
		if logs[i].count("level=WARN") != 0 {
			t.Errorf("replica %d refused or stopped:\n%s", i, logs[i].text())
		}
	}
	if got := logs[0].count(reconciledLine) + logs[1].count(reconciledLine); got != 1 {
		t.Fatalf("%d fixed lines, want 1", got)
	}
}

// sortedOrgs seeds n active orgs and returns them in id order.
func sortedOrgs(ctx context.Context, t *testing.T, pool *pgxpool.Pool, n int) []uuid.UUID {
	t.Helper()
	orgs := make([]uuid.UUID, n)
	for i := range orgs {
		orgs[i] = seedOrg(ctx, t, pool, true)
	}
	sort.Slice(orgs, func(i, j int) bool { return bytes.Compare(orgs[i][:], orgs[j][:]) < 0 })
	return orgs
}

func limited(retryAfter time.Duration) error {
	return &orgreactivate.RateLimited{RetryAfter: retryAfter}
}

// CHAOS-13575 C-pace: every auth call, the re-read under the lock included, comes at least one call gap after the
// one before it on the pacer's clock.
func TestReconcilePacesEveryAuthCall(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	auth := newPassAuth()
	orgs := sortedOrgs(ctx, t, pool, 4)
	for _, org := range orgs {
		auth.set(org, "active")
	}
	auth.set(orgs[0], "suspended")
	r, log, clock := reconcilerWithClock(pool, auth)
	var at []time.Time
	auth.onRead = func(int, uuid.UUID) { at = append(at, clock.now()) }
	boundedPass(ctx, r)
	if len(at) != len(orgs)+1 {
		t.Fatalf("%d reads, want %d", len(at), len(orgs)+1)
	}
	for i := 1; i < len(at); i++ {
		if gap := at[i].Sub(at[i-1]); gap < orgStateReconcileCallGap {
			t.Fatalf("read %d came %v after the one before, want >= %v", i+1, gap, orgStateReconcileCallGap)
		}
	}
	if orgStateReconcileCallGap != time.Second/5 {
		t.Fatalf("call gap %v, want the 5 calls/s of the design", orgStateReconcileCallGap)
	}
	if storedActive(ctx, t, pool, orgs[0]) {
		t.Fatal("the drifted row was not fixed")
	}
	wantNoWarn(t, log)
}

// CHAOS-13575 C-ra: a 429 waits its Retry-After (bounded), or the fixed wait when it names none, then reads the SAME
// row again; the row is then covered and nothing is refused.
func TestReconcileWaitsOnA429AndReadsTheSameRowAgain(t *testing.T) {
	for name, tc := range map[string]struct {
		retryAfter time.Duration
		want       time.Duration
	}{
		"Retry-After 3s":   {3 * time.Second, 3 * time.Second},
		"Retry-After 1h":   {time.Hour, orgStateReconcileRetryAfterMax},
		"no Retry-After":   {0, orgStateReconcileRateLimitWait},
		"at the bound 10s": {10 * time.Second, 10 * time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, pool := startOrgsDB(t)
			auth := newPassAuth()
			org := seedOrg(ctx, t, pool, true)
			auth.set(org, "suspended")
			auth.limit(org, limited(tc.retryAfter), limited(tc.retryAfter))
			r, log, clock := reconcilerWithClock(pool, auth)
			boundedPass(ctx, r)
			if storedActive(ctx, t, pool, org) {
				t.Fatalf("the row was not fixed after the 429s:\n%s", log.text())
			}
			if got := auth.readCount(); got != 4 {
				t.Fatalf("%d reads, want 2 answered 429, then the read and the re-read under the lock", got)
			}
			if n := countWaits(clock.waited(), tc.want); n != 2 {
				t.Fatalf("waits %v, want two of %v", clock.waited(), tc.want)
			}
			wantNoWarn(t, log)
		})
	}
	if orgStateReconcileRetryAfterMax != 10*time.Second || orgStateReconcileRateLimitWait != time.Second {
		t.Fatal("the wait bounds are not the design's 10 s clamp and 1 s fallback")
	}
}

func countWaits(waits []time.Duration, d time.Duration) int {
	n := 0
	for _, w := range waits {
		if w == d {
			n++
		}
	}
	return n
}

// CHAOS-13575 C-lock: a 429 on the re-read under the lock releases the lock and writes nothing before the wait; the
// fix then runs again and writes once.
func TestReconcileReleasesTheLockBeforeA429Wait(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	auth := newPassAuth()
	org := seedOrg(ctx, t, pool, true)
	auth.set(org, "suspended")
	auth.limit(org, nil, limited(2*time.Second))
	r, log, clock := reconcilerWithClock(pool, auth)
	probed := false
	clock.onWait = func(d time.Duration) {
		if d != 2*time.Second {
			return
		}
		probed = true
		if err := probeRow(ctx, t, pool, org, "NO KEY UPDATE"); err != nil {
			t.Errorf("the row is locked during the 429 wait: %v", err)
		}
		if !storedActive(ctx, t, pool, org) {
			t.Error("the 429 under the lock wrote the row")
		}
	}
	boundedPass(ctx, r)
	if !probed {
		t.Fatalf("no 429 wait; waits %v", clock.waited())
	}
	if storedActive(ctx, t, pool, org) {
		t.Fatal("the row was not fixed after the wait")
	}
	wantOneFixedLine(t, log, org, true, false)
	wantNoWarn(t, log)
}

// CHAOS-13575 C-stop/resume/reset: a row still answered 429 after the tries bound stops the pass with the count; the
// next pass resumes at that row, not at the lowest id; after it walks to the end the one after starts at the lowest.
func TestReconcileStopsOnA429BoundAndTheNextPassResumes(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	auth := newPassAuth()
	orgs := sortedOrgs(ctx, t, pool, 3)
	for _, org := range orgs {
		auth.set(org, "active")
	}
	errs := make([]error, orgStateReconcileRateLimitTries)
	for i := range errs {
		errs[i] = limited(time.Second)
	}
	auth.limit(orgs[1], errs...)
	r, log := reconcilerFor(pool, auth)
	boundedPass(ctx, r)
	if got, want := auth.readOrder(), append([]uuid.UUID{orgs[0]}, repeat(orgs[1], orgStateReconcileRateLimitTries)...); !equalOrgs(got, want) {
		t.Fatalf("first pass read %v, want %v", got, want)
	}
	if log.count("level=WARN") != 1 || log.count(fmt.Sprintf("rate_limited=%d", orgStateReconcileRateLimitTries)) != 1 ||
		log.count("refused=0") != 1 || log.count("stopped=true") != 1 {
		t.Fatalf("want one WARN with refused=0 rate_limited=%d stopped=true:\n%s", orgStateReconcileRateLimitTries, log.text())
	}
	for _, org := range orgs {
		if strings.Contains(log.text(), org.String()) {
			t.Fatalf("the WARN names an organization:\n%s", log.text())
		}
	}
	auth.set(orgs[1], "suspended")
	before := len(auth.readOrder())
	boundedPass(ctx, r)
	second := auth.readOrder()[before:]
	if len(second) == 0 || second[0] != orgs[1] {
		t.Fatalf("second pass read %v, want it to resume at the stopped row %s", second, orgs[1])
	}
	if storedActive(ctx, t, pool, orgs[1]) {
		t.Fatal("the resumed pass did not fix the stopped row")
	}
	before = len(auth.readOrder())
	boundedPass(ctx, r)
	if third := auth.readOrder()[before:]; !equalOrgs(third, orgs) {
		t.Fatalf("third pass read %v, want every row from the lowest id %v", third, orgs)
	}
	if orgStateReconcileRateLimitTries != 5 {
		t.Fatal("the tries bound is not the design's 5")
	}
}

func repeat(org uuid.UUID, n int) []uuid.UUID {
	out := make([]uuid.UUID, n)
	for i := range out {
		out[i] = org
	}
	return out
}

func equalOrgs(a, b []uuid.UUID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// passReads runs one pass and returns the orgs it read, in order.
func passReads(ctx context.Context, r *OrgStateReconciler, auth *passAuth) []uuid.UUID {
	before := len(auth.readOrder())
	boundedPass(ctx, r)
	return auth.readOrder()[before:]
}

// CHAOS-13575 C-poison: a row that stops every pass (a row-specific outage) does not freeze the rows before it: the
// resumed pass that covers no row resets the cursor, so the pass after it reads them again and fixes a drift there.
func TestReconcileAPoisonRowDoesNotFreezeTheRowsBeforeIt(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	auth := newPassAuth()
	orgs := sortedOrgs(ctx, t, pool, 4)
	for _, org := range orgs {
		auth.set(org, "active")
	}
	poison := orgs[2]
	auth.fail(poison, fmt.Errorf("%w: status 503", orgreactivate.ErrUnavailable))
	r, log := reconcilerFor(pool, auth)
	if got := passReads(ctx, r, auth); !equalOrgs(got, orgs[:3]) {
		t.Fatalf("pass 1 read %v, want the rows up to the poison row %v", got, orgs[:3])
	}
	if got := passReads(ctx, r, auth); !equalOrgs(got, orgs[2:3]) {
		t.Fatalf("pass 2 read %v, want it to resume at the poison row only", got)
	}
	auth.set(orgs[0], "suspended")
	if got := passReads(ctx, r, auth); !equalOrgs(got, []uuid.UUID{orgs[0], orgs[0], orgs[1], orgs[2]}) {
		t.Fatalf("pass 3 read %v, want it to start at the lowest id again (fix re-read included)", got)
	}
	if storedActive(ctx, t, pool, orgs[0]) {
		t.Fatal("a drift before the poison row was not fixed")
	}
	if log.count("stopped=true") != 3 {
		t.Fatalf("want 3 stopped passes:\n%s", log.text())
	}
}

// CHAOS-13575 C-keep: a resumed pass that covers at least one row before it stops again keeps its cursor: the next
// pass resumes after that row, not at the lowest id.
func TestReconcileAResumedPassThatCoversARowKeepsItsCursor(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	auth := newPassAuth()
	orgs := sortedOrgs(ctx, t, pool, 4)
	for _, org := range orgs {
		auth.set(org, "active")
	}
	exhaust := func(org uuid.UUID) {
		errs := make([]error, orgStateReconcileRateLimitTries)
		for i := range errs {
			errs[i] = limited(time.Second)
		}
		auth.limit(org, errs...)
	}
	exhaust(orgs[1])
	r, _ := reconcilerFor(pool, auth)
	passReads(ctx, r, auth)
	exhaust(orgs[2])
	if got, want := passReads(ctx, r, auth), append([]uuid.UUID{orgs[1]}, repeat(orgs[2], orgStateReconcileRateLimitTries)...); !equalOrgs(got, want) {
		t.Fatalf("pass 2 read %v, want %v", got, want)
	}
	if got := passReads(ctx, r, auth); !equalOrgs(got, orgs[2:]) {
		t.Fatalf("pass 3 read %v, want it to resume at %s", got, orgs[2])
	}
}

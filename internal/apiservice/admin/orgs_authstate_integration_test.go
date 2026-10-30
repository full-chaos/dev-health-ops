//go:build integration

package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/orgreactivate"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// liveAuth is a fake auth-service whose organization state follows the calls it accepts. It does not check
// if_revision: the cells test the order of go-api's local writes, not the revision guard.
type liveAuth struct {
	mu     sync.Mutex
	status string
	rev    int64
	reads  int
	calls  []string
	// afterCall, when set, runs under the lock after a call set the state (another auth change landing).
	afterCall func(a *liveAuth)
	// holdRead is the number of the State read that is answered, then held until release closes (0: none); held
	// closes when that read has its answer.
	holdRead int
	held     chan struct{}
	release  chan struct{}
	// hangFrom is the number of the first State read that blocks on its context (0: none).
	hangFrom int
}

func (a *liveAuth) State(ctx context.Context, _ uuid.UUID) (orgreactivate.State, error) {
	a.mu.Lock()
	a.reads++
	n := a.reads
	state := orgreactivate.State{Status: a.status, Revision: a.rev}
	a.mu.Unlock()
	if a.hangFrom != 0 && n >= a.hangFrom {
		<-ctx.Done()
		return orgreactivate.State{}, ctx.Err()
	}
	if a.holdRead != 0 && n == a.holdRead {
		close(a.held)
		<-a.release
	}
	return state, nil
}

func (a *liveAuth) set(status, verb string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.status = status
	a.rev++
	a.calls = append(a.calls, verb)
	if a.afterCall != nil {
		a.afterCall(a)
	}
	return nil
}

func (a *liveAuth) Reactivate(_ context.Context, _, _ uuid.UUID, _ *int64) error {
	return a.set("active", "reactivate")
}

func (a *liveAuth) Suspend(_ context.Context, _, _ uuid.UUID, _ *int64) error {
	return a.set("suspended", "suspend")
}

func (a *liveAuth) current() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.status
}

func (a *liveAuth) requests() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.reads + len(a.calls)
}

func startOrgsDB(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	return ctx, pool
}

func seedOrg(ctx context.Context, t *testing.T, pool *pgxpool.Pool, active bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'community', 'stripe', $3, now(), now())`, id, "authstate-"+id.String()[:8], active); err != nil {
		t.Fatal(err)
	}
	return id
}

func storedActive(ctx context.Context, t *testing.T, pool *pgxpool.Pool, org uuid.UUID) bool {
	t.Helper()
	var active bool
	if err := pool.QueryRow(ctx, `SELECT is_active FROM organizations WHERE id = $1`, org).Scan(&active); err != nil {
		t.Fatal(err)
	}
	return active
}

func liveHandlers(pool *pgxpool.Pool, auth *liveAuth) *handlers {
	h := &handlers{store: pgStore{Pool: pool, Now: time.Now}, logger: slog.New(slog.DiscardHandler)}
	if auth != nil {
		h.orgReactivator, h.orgSuspender, h.orgState = auth, auth, auth
	}
	return h
}

func withSuperuser(ctx context.Context) context.Context {
	return policy.WithUser(ctx, &policy.User{IsSuperuser: true, UserID: validUUID, ID: uuid.MustParse(validUUID)})
}

func patchLive(ctx context.Context, h *handlers, org uuid.UUID, body string) (int, bool) {
	r := answerWith(h.updateOrganization, http.MethodPatch, func(req *http.Request) *http.Request {
		req = req.WithContext(withSuperuser(ctx))
		req = pathValues("org_id", org.String())(req)
		return withBody(body)(req)
	})
	var out struct {
		IsActive bool `json:"is_active"`
	}
	_ = json.Unmarshal(r.Body.Bytes(), &out)
	return r.Code, out.IsActive
}

func statusFor(active bool) string {
	if active {
		return "active"
	}
	return "suspended"
}

// C1: the call is accepted, then another auth change lands; the re-read under the row lock sees it, so the row and the
// answer follow auth, not the PATCH value.
func TestAnOrgPatchStoresTheAuthStateWhenItMovesAfterTheCall(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	auth := &liveAuth{status: "suspended", rev: 1, afterCall: func(a *liveAuth) { a.status, a.rev = "suspended", a.rev+1 }}
	org := seedOrg(ctx, t, pool, false)
	code, answered := patchLive(ctx, liveHandlers(pool, auth), org, `{"is_active": true}`)
	if code != http.StatusOK || answered || storedActive(ctx, t, pool, org) {
		t.Fatalf("answer %d is_active=%v stored=%v, want 200 with false in both: auth holds suspended", code, answered, storedActive(ctx, t, pool, org))
	}
}

func lockWaiters(ctx context.Context, t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var waiting int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&waiting); err != nil {
		t.Fatal(err)
	}
	return waiting
}

// C2: two PATCHes (a reactivate and a suspend) overlap. The first one's re-read is answered and then held while the
// second one's call changes auth; the second then either waits on the row lock (the fix) or writes at once (a race).
// Either way the first one writes last, and the row must still end equal to auth. 20 iterations, both orders.
func TestConcurrentOrgPatchesEndEqualToAuth(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	for i := 0; i < 20; i++ {
		firstActive := i%2 == 0
		auth := &liveAuth{status: statusFor(!firstActive), rev: 1, holdRead: 2, held: make(chan struct{}), release: make(chan struct{})}
		org := seedOrg(ctx, t, pool, !firstActive)
		h := liveHandlers(pool, auth)
		var wg sync.WaitGroup
		codes := make(chan int, 2)
		send := func(active bool) chan struct{} {
			done := make(chan struct{})
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer close(done)
				code, _ := patchLive(ctx, h, org, fmt.Sprintf(`{"is_active": %v}`, active))
				codes <- code
			}()
			return done
		}
		firstDone := send(firstActive)
		select {
		case <-auth.held:
		case <-time.After(30 * time.Second):
			close(auth.release)
			t.Fatalf("iteration %d: the first PATCH never reached its re-read", i)
		}
		secondDone := send(!firstActive)
		deadline := time.Now().Add(30 * time.Second)
		for settled := false; !settled; {
			select {
			case <-secondDone:
				settled = true
			case <-time.After(20 * time.Millisecond):
				settled = lockWaiters(ctx, t, pool) > 0
			}
			if time.Now().After(deadline) {
				close(auth.release)
				t.Fatalf("iteration %d: the second PATCH neither finished nor waited on a lock", i)
			}
		}
		close(auth.release)
		<-firstDone
		<-secondDone
		wg.Wait()
		close(codes)
		for code := range codes {
			if code != http.StatusOK {
				t.Fatalf("iteration %d: a PATCH answered %d, want 200", i, code)
			}
		}
		if got, want := storedActive(ctx, t, pool, org), auth.current() == "active"; got != want {
			t.Fatalf("iteration %d: row is_active=%v, auth holds %s", i, got, auth.current())
		}
	}
}

// C3: the switch off: the PATCH value is stored and auth saw no request.
func TestAnOrgPatchWithTheSwitchOffIsUnchanged(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	auth := &liveAuth{status: "suspended", rev: 1}
	for _, active := range []bool{true, false} {
		org := seedOrg(ctx, t, pool, !active)
		code, answered := patchLive(ctx, liveHandlers(pool, nil), org, fmt.Sprintf(`{"is_active": %v}`, active))
		if code != http.StatusOK || answered != active || storedActive(ctx, t, pool, org) != active {
			t.Fatalf("PATCH %v: answer %d is_active=%v, want 200 with the PATCH value stored", active, code, answered)
		}
	}
	if auth.requests() != 0 {
		t.Fatalf("fake auth saw %d requests, want none", auth.requests())
	}
}

// C4: the re-read hangs. The request returns once its own limit ends, the row lock is gone (a second transaction
// takes it at once), and the row holds the state the accepted call set.
func TestAnOrgPatchReReadTimeoutWritesTheAcceptedState(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	auth := &liveAuth{status: "suspended", rev: 1, hangFrom: 2}
	org := seedOrg(ctx, t, pool, false)
	reqCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	code, answered := patchLive(reqCtx, liveHandlers(pool, auth), org, `{"is_active": true}`)
	if reqCtx.Err() != nil {
		t.Fatalf("the request did not return within its own context: %v", reqCtx.Err())
	}
	if code != http.StatusOK || !answered || !storedActive(ctx, t, pool, org) {
		t.Fatalf("answer %d is_active=%v stored=%v, want 200 with the accepted state true", code, answered, storedActive(ctx, t, pool, org))
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var id uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM organizations WHERE id = $1 FOR UPDATE NOWAIT`, org).Scan(&id); err != nil {
		t.Fatalf("the row is still locked after the request returned: %v", err)
	}
}

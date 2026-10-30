package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/orgreactivate"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// lockDB is a pgDB that holds one organization row and accepts the write transaction: the statements of the
// transaction are recorded in the shared log, and the row changes only at COMMIT.
type lockDB struct {
	log     *orderLog
	active  bool
	pending bool
	sqls    []string
	writes  []bool
}

func (d *lockDB) Begin(context.Context) (pgx.Tx, error) {
	d.log.add("db:begin")
	return &lockTx{d: d}, nil
}

func (d *lockDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errNoWrite
}

func (d *lockDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errNoWrite
}

func (d *lockDB) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	d.log.add("db:select")
	d.sqls = append(d.sqls, strings.Join(strings.Fields(sql), " "))
	return orgRow{&reactivationDB{found: true, active: d.active}}
}

type lockTx struct {
	pgx.Tx
	d *lockDB
}

func (t *lockTx) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	t.d.log.add("tx:select")
	t.d.sqls = append(t.d.sqls, strings.Join(strings.Fields(sql), " "))
	return orgRow{&reactivationDB{found: true, active: t.d.active}}
}

func (t *lockTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.d.log.add("tx:exec")
	t.d.sqls = append(t.d.sqls, strings.Join(strings.Fields(sql), " "))
	if strings.HasPrefix(strings.TrimSpace(sql), "UPDATE organizations") {
		t.d.pending = args[6].(bool)
		t.d.writes = append(t.d.writes, t.d.pending)
	}
	return pgconn.CommandTag{}, nil
}

func (t *lockTx) Commit(context.Context) error {
	t.d.log.add("tx:commit")
	t.d.active = t.d.pending
	return nil
}

func (t *lockTx) Rollback(context.Context) error { return nil }

// hangingState answers its first read at once and blocks every later read on its context.
type hangingState struct {
	log      *orderLog
	first    orgreactivate.State
	calls    int
	ctxErr   error
	hadLimit bool
}

func (s *hangingState) State(ctx context.Context, _ uuid.UUID) (orgreactivate.State, error) {
	s.log.add("auth-state")
	s.calls++
	if s.calls == 1 {
		return s.first, nil
	}
	_, s.hadLimit = ctx.Deadline()
	<-ctx.Done()
	s.ctxErr = ctx.Err()
	return orgreactivate.State{}, ctx.Err()
}

func lockHandlers(db *lockDB, life *fakeLifecycle, state orgreactivate.StateReader, logs *bytes.Buffer) *handlers {
	h := &handlers{store: pgStore{Pool: db, Now: time.Now}, orgState: state, logger: slog.New(slog.NewTextHandler(logs, nil))}
	if life != nil {
		h.orgReactivator, h.orgSuspender = life, life
	}
	return h
}

func patchOrgIn(ctx context.Context, h *handlers, body string) (code int, text string) {
	r := answerWith(h.updateOrganization, http.MethodPatch, func(req *http.Request) *http.Request {
		req = req.WithContext(policy.WithUser(ctx, &policy.User{IsSuperuser: true, UserID: validUUID, ID: uuid.MustParse(validUUID)}))
		req = pathValues("org_id", validUUID)(req)
		return withBody(body)(req)
	})
	return r.Code, r.Body.String()
}

func answeredIsActive(t *testing.T, code int, body string) bool {
	t.Helper()
	if code != http.StatusOK {
		t.Fatalf("answer %d %s, want 200", code, body)
	}
	var out struct {
		IsActive bool `json:"is_active"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("answer %s: %v", body, err)
	}
	return out.IsActive
}

// (a) The call is accepted at revision 1, then another change lands: the re-read under the row lock answers suspended.
// The row and the answer follow auth, not the PATCH value.
func TestPatchWithTheSwitchOnStoresTheAuthStateReadUnderTheLock(t *testing.T) {
	log := &orderLog{}
	db := &lockDB{log: log, active: false}
	state := &fakeAuthState{log: log, seq: []orgreactivate.State{{Status: "suspended", Revision: 1}, {Status: "suspended", Revision: 3}}}
	var logs bytes.Buffer
	h := lockHandlers(db, &fakeLifecycle{log: log}, state, &logs)
	code, body := patchOrg(h, `{"is_active": true, "name": "Renamed"}`)
	if got := answeredIsActive(t, code, body); got {
		t.Fatalf("answer is_active = true, want false: auth holds suspended")
	}
	if len(db.writes) != 1 || db.writes[0] || db.active {
		t.Fatalf("writes %v, stored is_active %v, want one write of false", db.writes, db.active)
	}
	if state.calls != 2 {
		t.Fatalf("%d auth state reads, want 2 (before the call, under the lock)", state.calls)
	}
	lock, reread, write := log.index("tx:select"), log.indexAfter("auth-state", "tx:select"), log.index("tx:exec")
	if lock < 0 || reread < lock || write < reread {
		t.Fatalf("order %v, want the row lock, then the auth re-read, then the write", log.steps)
	}
	if !strings.Contains(strings.Join(db.sqls, "\n"), "FOR UPDATE") {
		t.Fatalf("sql %q, want a SELECT ... FOR UPDATE", db.sqls)
	}
	if !strings.Contains(logs.String(), "auth state moved after the call") {
		t.Fatalf("log %q, want the info line for a moved state", logs.String())
	}
}

func (l *orderLog) indexAfter(step, after string) int {
	from := l.index(after)
	if from < 0 {
		return -1
	}
	for i := from + 1; i < len(l.steps); i++ {
		if l.steps[i] == step {
			return i
		}
	}
	return -1
}

// (b) The re-read fails after an accepted call: the state the call set is written, the other fields stay, the answer is
// 200, and the warning names the verb and the reason class, not the error text.
func TestPatchWithTheSwitchOnWritesTheAcceptedStateWhenTheReReadFails(t *testing.T) {
	for name, tc := range map[string]struct {
		body   string
		accept bool
		reason string
	}{
		"reactivate": {`{"is_active": true, "name": "Renamed"}`, true, "reason=error"},
		"suspend":    {`{"is_active": false, "name": "Renamed"}`, false, "reason=error"},
	} {
		log := &orderLog{}
		db := &lockDB{log: log, active: !tc.accept}
		state := &fakeAuthState{log: log, seq: []orgreactivate.State{{Status: "active", Revision: 1}},
			errs: map[int]error{2: errors.New("secret-bearing boom")}}
		var logs bytes.Buffer
		code, body := patchOrg(lockHandlers(db, &fakeLifecycle{log: log}, state, &logs), tc.body)
		if got := answeredIsActive(t, code, body); got != tc.accept {
			t.Fatalf("%s: answer is_active = %v, want %v", name, got, tc.accept)
		}
		if len(db.writes) != 1 || db.writes[0] != tc.accept || db.active != tc.accept {
			t.Fatalf("%s: writes %v, stored %v, want the accepted state %v", name, db.writes, db.active, tc.accept)
		}
		out := logs.String()
		if !strings.Contains(out, "auth state re-read failed after an accepted change") || !strings.Contains(out, tc.reason) ||
			strings.Contains(out, "secret-bearing boom") {
			t.Fatalf("%s: log %q, want the warning with the reason class and no error text", name, out)
		}
	}
}

// (c) The re-read hangs: it runs under its own limit, so the request returns, the request's own context is still
// live, and the accepted state is written.
func TestPatchWithTheSwitchOnBoundsTheReRead(t *testing.T) {
	log := &orderLog{}
	db := &lockDB{log: log, active: false}
	state := &hangingState{log: log, first: orgreactivate.State{Status: "suspended", Revision: 1}}
	var logs bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	code, body := patchOrgIn(ctx, lockHandlers(db, &fakeLifecycle{log: log}, state, &logs), `{"is_active": true}`)
	if got := answeredIsActive(t, code, body); !got {
		t.Fatal("answer is_active = false, want true (the state the accepted call set)")
	}
	if !state.hadLimit || !errors.Is(state.ctxErr, context.DeadlineExceeded) {
		t.Fatalf("re-read had its own limit = %v, ended with %v, want a deadline of its own", state.hadLimit, state.ctxErr)
	}
	if ctx.Err() != nil {
		t.Fatalf("the request context ended (%v): the re-read used it as its limit", ctx.Err())
	}
	if len(db.writes) != 1 || !db.writes[0] || !db.active {
		t.Fatalf("writes %v, stored %v, want the accepted state true", db.writes, db.active)
	}
	if !strings.Contains(logs.String(), "reason=timeout") {
		t.Fatalf("log %q, want the reason class timeout", logs.String())
	}
}

// (d) The switch off: no lifecycle client, so no auth state read, no lock, and the PATCH value is stored.
func TestPatchWithTheSwitchOffReadsNoAuthStateAndStoresThePatchValue(t *testing.T) {
	for _, active := range []bool{true, false} {
		log := &orderLog{}
		db := &lockDB{log: log, active: !active}
		state := &fakeAuthState{log: log, seq: []orgreactivate.State{{Status: "suspended", Revision: 1}}}
		var logs bytes.Buffer
		body := `{"is_active": false}`
		if active {
			body = `{"is_active": true}`
		}
		code, text := patchOrg(lockHandlers(db, nil, state, &logs), body)
		if got := answeredIsActive(t, code, text); got != active {
			t.Fatalf("answer is_active = %v, want the PATCH value %v", got, active)
		}
		if state.calls != 0 || len(db.writes) != 1 || db.writes[0] != active {
			t.Fatalf("auth state reads %d, writes %v, want none and the PATCH value %v", state.calls, db.writes, active)
		}
		if strings.Contains(strings.Join(db.sqls, "\n"), "FOR UPDATE") {
			t.Fatalf("sql %q: the switch-off path takes a row lock", db.sqls)
		}
	}
}

// (e) A PATCH without is_active reads no auth state, whatever the switch.
func TestPatchWithoutIsActiveReadsNoAuthState(t *testing.T) {
	for _, body := range []string{`{"name": "Renamed"}`, `{"name": "Renamed", "is_active": null}`} {
		log := &orderLog{}
		db := &lockDB{log: log, active: true}
		state := &fakeAuthState{log: log, seq: []orgreactivate.State{{Status: "suspended", Revision: 1}}}
		var logs bytes.Buffer
		code, text := patchOrg(lockHandlers(db, &fakeLifecycle{log: log}, state, &logs), body)
		if got := answeredIsActive(t, code, text); !got {
			t.Fatalf("%s: answer is_active = false, want the stored true", body)
		}
		if state.calls != 0 || len(log.steps) == 0 || log.index("auth") != -1 || log.index("auth-suspend") != -1 {
			t.Fatalf("%s: auth state reads %d, steps %v, want no auth traffic", body, state.calls, log.steps)
		}
		if strings.Contains(strings.Join(db.sqls, "\n"), "FOR UPDATE") {
			t.Fatalf("%s: sql %q: a PATCH without is_active takes a row lock", body, db.sqls)
		}
	}
}

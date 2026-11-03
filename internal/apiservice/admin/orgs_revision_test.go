package admin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/orgreactivate"
)

// fakeAuthState is auth-service's state read: seq holds the answer of each successive read (the last repeats), errs the
// error of read n (1-based).
type fakeAuthState struct {
	log   *orderLog
	seq   []orgreactivate.State
	errs  map[int]error
	calls int
}

func (f *fakeAuthState) State(_ context.Context, _ uuid.UUID) (orgreactivate.State, error) {
	f.log.add("auth-state")
	f.calls++
	if err := f.errs[f.calls]; err != nil {
		return orgreactivate.State{}, err
	}
	i := f.calls - 1
	if i >= len(f.seq) {
		i = len(f.seq) - 1
	}
	return f.seq[i], nil
}

// fakeLifecycle is both mutating calls of auth-service; revs records the if_revision each was sent.
type fakeLifecycle struct {
	log   *orderLog
	err   error
	revs  []*int64
	verbs []string
}

func (f *fakeLifecycle) Reactivate(_ context.Context, _, _ uuid.UUID, rev *int64) error {
	f.log.add("auth")
	f.verbs, f.revs = append(f.verbs, "reactivate"), append(f.revs, rev)
	return f.err
}

func (f *fakeLifecycle) Suspend(_ context.Context, _, _ uuid.UUID, rev *int64) error {
	f.log.add("auth-suspend")
	f.verbs, f.revs = append(f.verbs, "suspend"), append(f.revs, rev)
	return f.err
}

func newRevisionHandlers(db *reactivationDB, life *fakeLifecycle, state orgreactivate.StateReader) *handlers {
	h := &handlers{store: pgStore{Pool: db}, orgState: state, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if life != nil {
		h.orgReactivator, h.orgSuspender = life, life
	}
	return h
}

var errStateRead = errors.New("boom")

// The PATCH reads auth's state first and sends the revision it read: the read comes before the mutating call, which
// comes before the write, in both directions, and the call goes out whatever status the read saw (a no-op there is
// still guarded by the revision).
func TestPatchSendsTheRevisionItRead(t *testing.T) {
	for name, tc := range map[string]struct {
		body, authStatus, step string
		verb                   string
	}{
		"reactivate a suspended org":   {`{"is_active": true}`, "suspended", "auth", "reactivate"},
		"reactivate an active org":     {`{"is_active": true}`, "active", "auth", "reactivate"},
		"suspend an active org":        {`{"is_active": false}`, "active", "auth-suspend", "suspend"},
		"suspend a suspended org":      {`{"is_active": false}`, "suspended", "auth-suspend", "suspend"},
		"reactivate with other fields": {`{"is_active": true, "name": "Renamed"}`, "suspended", "auth", "reactivate"},
	} {
		log := &orderLog{}
		db := &reactivationDB{log: log, found: true, active: true}
		life := &fakeLifecycle{log: log}
		state := &fakeAuthState{log: log, seq: []orgreactivate.State{{Status: tc.authStatus, Revision: 7}}}
		patchOrg(newRevisionHandlers(db, life, state), tc.body)
		if len(life.verbs) != 1 || life.verbs[0] != tc.verb || life.revs[0] == nil || *life.revs[0] != 7 {
			t.Errorf("%s: calls %v revisions %v, want one %s call sent revision 7", name, life.verbs, life.revs, tc.verb)
			continue
		}
		if state.calls != 1 {
			t.Errorf("%s: %d state reads, want 1", name, state.calls)
		}
		s, c, w := log.index("auth-state"), log.index(tc.step), log.index("db:begin")
		if s < 0 || c < 0 || w < 0 || !(s < c && c < w) {
			t.Errorf("%s: order %v, want the state read, the call, then the write", name, log.steps)
		}
	}
}

// A 409 state_changed is answered 409 to the admin caller after ONE re-read for the answer only: no write, no second
// mutating call, in either direction; a failed re-read still answers 409.
func TestPatchAnswers409AfterOneReReadAndNeverRetries(t *testing.T) {
	conflict := fmt.Errorf("%w: status 409", orgreactivate.ErrStateChanged)
	for name, body := range map[string]string{"reactivate": `{"is_active": true, "name": "Renamed"}`, "suspend": `{"is_active": false}`} {
		for reread, errs := range map[string]map[int]error{"re-read ok": nil, "re-read fails": {2: errStateRead}} {
			log := &orderLog{}
			db := &reactivationDB{log: log, found: true, active: true}
			life := &fakeLifecycle{log: log, err: conflict}
			state := &fakeAuthState{log: log, errs: errs, seq: []orgreactivate.State{{Status: "suspended", Revision: 7}, {Status: "active", Revision: 8}}}
			code, text := patchOrg(newRevisionHandlers(db, life, state), body)
			if code != http.StatusConflict || strings.TrimSpace(text) == authUnavailableBody {
				t.Fatalf("%s %s: %d %s, want 409", name, reread, code, text)
			}
			if len(life.verbs) != 1 {
				t.Fatalf("%s %s: %d mutating calls %v, want exactly the one that was refused", name, reread, len(life.verbs), life.verbs)
			}
			if state.calls != 2 {
				t.Fatalf("%s %s: %d state reads, want the read and one re-read", name, reread, state.calls)
			}
			if log.index("db:begin") != -1 || log.count("db:exec") != 0 {
				t.Fatalf("%s %s: the write ran after the conflict: %v", name, reread, log.steps)
			}
		}
	}
}

// A failed state read is the fail-closed 503: nothing is called, nothing is written.
func TestPatchFailsClosedWhenTheStateReadFails(t *testing.T) {
	for name, err := range map[string]error{"down": orgreactivate.ErrUnavailable, "refused": orgreactivate.ErrRefused, "other": errStateRead} {
		for _, body := range []string{`{"is_active": true}`, `{"is_active": false}`} {
			log := &orderLog{}
			db := &reactivationDB{log: log, found: true, active: true}
			life := &fakeLifecycle{log: log}
			state := &fakeAuthState{log: log, errs: map[int]error{1: err}, seq: []orgreactivate.State{{Status: "active"}}}
			code, text := patchOrg(newRevisionHandlers(db, life, state), body)
			if code != http.StatusServiceUnavailable || strings.TrimSpace(text) != authUnavailableBody {
				t.Fatalf("%s %s: %d %s, want 503 %s", name, body, code, text, authUnavailableBody)
			}
			if len(life.verbs) != 0 || log.index("db:begin") != -1 {
				t.Fatalf("%s %s: a call or a write followed the failed read: %v", name, body, log.steps)
			}
		}
	}
}

// Any other refusal or outage of the mutating call stays the 503 of today, with no re-read.
func TestPatchOtherAuthFailuresStay503WithNoReRead(t *testing.T) {
	for name, err := range map[string]error{"down": orgreactivate.ErrUnavailable, "refused": orgreactivate.ErrRefused, "other": errStateRead} {
		log := &orderLog{}
		db := &reactivationDB{log: log, found: true, active: true}
		life := &fakeLifecycle{log: log, err: err}
		state := &fakeAuthState{log: log, seq: []orgreactivate.State{{Status: "suspended", Revision: 3}}}
		code, text := patchOrg(newRevisionHandlers(db, life, state), `{"is_active": true}`)
		if code != http.StatusServiceUnavailable || strings.TrimSpace(text) != authUnavailableBody {
			t.Fatalf("%s: %d %s, want 503 %s", name, code, text, authUnavailableBody)
		}
		if state.calls != 1 || len(life.verbs) != 1 || log.index("db:begin") != -1 {
			t.Fatalf("%s: state reads %d, calls %v, order %v, want one read, one call, no write", name, state.calls, life.verbs, log.steps)
		}
	}
}

// Switch off: no client, so no state read and no call; the database sees the same two statements as before, whether or
// not a state reader was wired.
func TestPatchWithTheSwitchOffReadsNoAuthState(t *testing.T) {
	for _, body := range []string{`{"is_active": true}`, `{"is_active": false}`} {
		log := &orderLog{}
		db := &reactivationDB{log: log, found: true, active: true}
		state := &fakeAuthState{log: log, seq: []orgreactivate.State{{Status: "active"}}}
		patchOrg(newRevisionHandlers(db, nil, state), body)
		if state.calls != 0 || log.index("auth") != -1 || log.index("auth-suspend") != -1 {
			t.Fatalf("%s: auth was touched with the switch off: %v", body, log.steps)
		}
		if len(db.sqls) != 2 || db.sqls[1] != "BEGIN" {
			t.Fatalf("%s: database calls = %q, want the one organization read and then BEGIN", body, db.sqls)
		}
	}
	for _, body := range []string{`{"name": "N"}`, `{"is_active": null}`} {
		log := &orderLog{}
		state := &fakeAuthState{log: log, seq: []orgreactivate.State{{Status: "active"}}}
		patchOrg(newRevisionHandlers(&reactivationDB{log: log, found: true, active: true}, &fakeLifecycle{log: log}, state), body)
		if state.calls != 0 {
			t.Fatalf("%s: a PATCH with no is_active read auth's state", body)
		}
	}
}

// A verb client with no state reader is a wiring fault: it fails closed (no call, no write) rather than sending a call
// with no revision.
func TestPatchWithAClientAndNoStateReaderFailsClosed(t *testing.T) {
	log := &orderLog{}
	db := &reactivationDB{log: log, found: true, active: true}
	life := &fakeLifecycle{log: log}
	code, _ := patchOrg(newRevisionHandlers(db, life, nil), `{"is_active": true}`)
	if code != http.StatusInternalServerError || len(life.verbs) != 0 || log.index("db:begin") != -1 {
		t.Fatalf("code %d calls %v order %v, want 500 with no call and no write", code, life.verbs, log.steps)
	}
}

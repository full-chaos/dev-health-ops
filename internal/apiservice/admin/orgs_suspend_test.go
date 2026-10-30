package admin

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/orgreactivate"
)

type fakeAuthSuspender struct {
	log   *orderLog
	err   error
	calls int
	org   uuid.UUID
	actor uuid.UUID
}

func (f *fakeAuthSuspender) Suspend(_ context.Context, org, actor uuid.UUID, _ *int64) error {
	f.log.add("auth-suspend")
	f.calls++
	f.org, f.actor = org, actor
	return f.err
}

func newLifecycleHandlers(db *reactivationDB, re orgreactivate.Reactivator, su orgreactivate.Suspender) *handlers {
	return &handlers{store: pgStore{Pool: db}, orgReactivator: re, orgSuspender: su,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

// With the switch off (no clients) a PATCH is_active=false is what it is today: the one read of the row, then the write
// transaction; auth-service is not touched.
func TestPatchIsActiveFalseWithTheSwitchOffMakesNoAuthCallAndTheSameSQL(t *testing.T) {
	log := &orderLog{}
	db := &reactivationDB{log: log, found: true, active: true}
	patchOrg(newLifecycleHandlers(db, nil, nil), `{"is_active": false}`)
	if len(db.sqls) != 2 || !strings.HasPrefix(db.sqls[0], "SELECT id, slug, name, description, settings, tier, managed_by, is_active, created_at, updated_at FROM organizations WHERE id = $1") ||
		db.sqls[1] != "BEGIN" {
		t.Fatalf("database calls = %q, want the one organization read and then BEGIN", db.sqls)
	}
	if log.index("auth") != -1 || log.index("auth-suspend") != -1 {
		t.Fatalf("an auth call was made with the switch off: %v", log.steps)
	}
}

// With the switch on, is_active=false of a found organization calls the suspend route exactly once, before any write,
// whatever the first read saw (an active or an already inactive organization), and never the reactivate route.
func TestPatchIsActiveFalseCallsAuthSuspendBeforeAnyWrite(t *testing.T) {
	for name, active := range map[string]bool{"read saw active": true, "read saw inactive": false} {
		for _, body := range []string{`{"is_active": false}`, `{"is_active": false, "name": "Renamed"}`} {
			log := &orderLog{}
			db := &reactivationDB{log: log, found: true, active: active}
			re, su := &fakeAuthReactivator{log: log}, &fakeAuthSuspender{log: log}
			patchOrg(newLifecycleHandlers(db, re, su), body)
			if su.calls != 1 || su.org != uuid.MustParse(validUUID) || su.actor != uuid.MustParse(validUUID) || re.calls != 0 {
				t.Fatalf("%s %s: suspend calls=%d reactivate calls=%d org=%s actor=%s, want one suspend for the org by the acting superuser",
					name, body, su.calls, re.calls, su.org, su.actor)
			}
			if a, w := log.index("auth-suspend"), log.index("db:begin"); a < 0 || w < 0 || a > w {
				t.Fatalf("%s %s: order %v, want the suspend call before the write transaction", name, body, log.steps)
			}
		}
	}
}

// Fail closed: a refusal, an outage or any other error is a 503 with auth-service's own unavailable text, and the write
// never starts.
func TestPatchIsActiveFalseFailsClosedWhenAuthRefusesOrIsDown(t *testing.T) {
	for name, err := range map[string]error{"down": orgreactivate.ErrUnavailable, "refused": orgreactivate.ErrRefused, "other": errors.New("boom")} {
		log := &orderLog{}
		db := &reactivationDB{log: log, found: true, active: true}
		h := newLifecycleHandlers(db, &fakeAuthReactivator{log: log}, &fakeAuthSuspender{log: log, err: err})
		code, body := patchOrg(h, `{"is_active": false, "name": "Renamed"}`)
		if code != http.StatusServiceUnavailable || strings.TrimSpace(body) != authUnavailableBody {
			t.Fatalf("%s: %d %s, want 503 %s", name, code, body, authUnavailableBody)
		}
		if log.index("db:begin") != -1 || log.count("db:exec") != 0 {
			t.Fatalf("%s: the write ran after auth failed: %v", name, log.steps)
		}
	}
}

// Each direction reaches only its own route, and only for a found organization with is_active present.
func TestPatchCallsOnlyTheRouteOfItsDirection(t *testing.T) {
	for name, tc := range map[string]struct {
		body       string
		found      bool
		suspends   int
		reactivate int
	}{
		"true on an active org":    {`{"is_active": true}`, true, 0, 1},
		"false on a suspended org": {`{"is_active": false}`, true, 1, 0},
		"no is_active":             {`{"name": "N"}`, true, 0, 0},
		"null is_active":           {`{"is_active": null}`, true, 0, 0},
		"false on a missing org":   {`{"is_active": false}`, false, 0, 0},
		"true on a missing org":    {`{"is_active": true}`, false, 0, 0},
	} {
		log := &orderLog{}
		db := &reactivationDB{log: log, found: tc.found, active: true}
		re, su := &fakeAuthReactivator{log: log}, &fakeAuthSuspender{log: log}
		patchOrg(newLifecycleHandlers(db, re, su), tc.body)
		if su.calls != tc.suspends || re.calls != tc.reactivate {
			t.Errorf("%s: suspend calls=%d reactivate calls=%d, want %d and %d", name, su.calls, re.calls, tc.suspends, tc.reactivate)
		}
	}
	log := &orderLog{}
	h := newLifecycleHandlers(&reactivationDB{log: log}, &fakeAuthReactivator{log: log}, &fakeAuthSuspender{log: log})
	if code, body := patchOrg(h, `{"is_active": false}`); code != http.StatusNotFound || !strings.Contains(body, "Organization not found") {
		t.Fatalf("a missing org: %d %s, want the 404 of today", code, body)
	}
}

// A direction whose client is nil makes no call: the suspender alone (no reactivator) leaves is_active=true as it was.
func TestAnAbsentClientMakesNoCallForItsDirection(t *testing.T) {
	log := &orderLog{}
	db := &reactivationDB{log: log, found: true, active: false}
	su := &fakeAuthSuspender{log: log}
	patchOrg(newLifecycleHandlers(db, nil, su), `{"is_active": true}`)
	if su.calls != 0 || log.index("db:begin") < 0 {
		t.Fatalf("suspend calls=%d order %v, want no call and the write for is_active=true", su.calls, log.steps)
	}
}

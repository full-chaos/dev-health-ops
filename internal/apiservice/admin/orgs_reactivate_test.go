package admin

import (
	"context"
	"errors"
	"io"
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

const authUnavailableBody = `{"detail":{"message":"Authentication service unavailable"}}`

// orderLog is the one ordered record of what the handler did to the database
// and to auth-service.
type orderLog struct{ steps []string }

func (l *orderLog) add(step string) { l.steps = append(l.steps, step) }

func (l *orderLog) index(step string) int {
	for i, s := range l.steps {
		if s == step {
			return i
		}
	}
	return -1
}

func (l *orderLog) count(step string) int {
	n := 0
	for _, s := range l.steps {
		if s == step {
			n++
		}
	}
	return n
}

// reactivationDB is a pgDB that holds one organization row and refuses every
// write: BEGIN is where the PATCH would write, so it is recorded and fails.
type reactivationDB struct {
	log    *orderLog
	found  bool
	active bool
	// seq, when set, is the is_active each successive read of the row sees (a suspend that lands between reads).
	seq  []bool
	sqls []string
}

var errNoWrite = errors.New("reactivationDB: no write in this test")

func (d *reactivationDB) Begin(context.Context) (pgx.Tx, error) {
	d.log.add("db:begin")
	d.sqls = append(d.sqls, "BEGIN")
	return nil, errNoWrite
}

func (d *reactivationDB) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	d.log.add("db:exec")
	return pgconn.CommandTag{}, errNoWrite
}

func (d *reactivationDB) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	d.log.add("db:query")
	return nil, errNoWrite
}

func (d *reactivationDB) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	d.log.add("db:select")
	d.sqls = append(d.sqls, strings.Join(strings.Fields(sql), " "))
	if len(d.seq) > 0 {
		d.active, d.seq = d.seq[0], d.seq[1:]
	}
	return orgRow{d}
}

type orgRow struct{ d *reactivationDB }

func (r orgRow) Scan(dest ...any) error {
	if !r.d.found {
		return pgx.ErrNoRows
	}
	now := time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC)
	for _, target := range dest {
		switch p := target.(type) {
		case *uuid.UUID:
			*p = uuid.MustParse(validUUID)
		case *string:
			*p = "x"
		case **string:
			*p = nil
		case *[]byte:
			*p = []byte("{}")
		case *bool:
			*p = r.d.active
		case *time.Time:
			*p = now
		default:
			return errors.New("orgRow: unexpected scan target")
		}
	}
	return nil
}

type fakeAuthReactivator struct {
	log   *orderLog
	err   error
	calls int
	org   uuid.UUID
	actor uuid.UUID
}

func (f *fakeAuthReactivator) Reactivate(_ context.Context, org, actor uuid.UUID) error {
	f.log.add("auth")
	f.calls++
	f.org, f.actor = org, actor
	return f.err
}

// patchOrg is a PATCH by the superuser validUUID with body.
func patchOrg(h *handlers, body string) (code int, text string) {
	r := answerWith(h.updateOrganization, http.MethodPatch, func(req *http.Request) *http.Request {
		req = req.WithContext(policy.WithUser(req.Context(), &policy.User{IsSuperuser: true, UserID: validUUID, ID: uuid.MustParse(validUUID)}))
		req = pathValues("org_id", validUUID)(req)
		return withBody(body)(req)
	})
	return r.Code, r.Body.String()
}

func newReactivationHandlers(db *reactivationDB, auth orgreactivate.Reactivator) *handlers {
	return &handlers{store: pgStore{Pool: db}, orgReactivator: auth, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

// With the switch off (no reactivator) the PATCH is what it is today: one
// read of the row, then the write transaction; auth-service is not touched.
func TestPatchIsActiveWithTheSwitchOffMakesNoAuthCallAndTheSameSQL(t *testing.T) {
	log := &orderLog{}
	db := &reactivationDB{log: log, found: true, active: false}
	h := newReactivationHandlers(db, nil)
	patchOrg(h, `{"is_active": true}`)
	if len(db.sqls) != 2 || !strings.HasPrefix(db.sqls[0], "SELECT id, slug, name, description, settings, tier, managed_by, is_active, created_at, updated_at FROM organizations WHERE id = $1") ||
		db.sqls[1] != "BEGIN" {
		t.Fatalf("database calls = %q, want the one organization read and then BEGIN", db.sqls)
	}
	if log.index("auth") != -1 {
		t.Fatal("an auth call was made with the switch off")
	}
}

func TestPatchIsActiveTrueWithTheSwitchOnCallsAuthBeforeAnyWrite(t *testing.T) {
	log := &orderLog{}
	db := &reactivationDB{log: log, found: true, active: false}
	auth := &fakeAuthReactivator{log: log}
	h := newReactivationHandlers(db, auth)
	for _, body := range []string{`{"is_active": true}`, `{"is_active": true, "name": "Renamed"}`} {
		*log, auth.calls = orderLog{}, 0
		patchOrg(h, body)
		if auth.calls != 1 || auth.org != uuid.MustParse(validUUID) || auth.actor != uuid.MustParse(validUUID) {
			t.Fatalf("%s: auth calls=%d org=%s actor=%s, want one call for the org by the acting superuser", body, auth.calls, auth.org, auth.actor)
		}
		if a, w := log.index("auth"), log.index("db:begin"); a < 0 || w < 0 || a > w {
			t.Fatalf("%s: order %v, want the auth call before the write transaction", body, log.steps)
		}
	}
}

// Fail closed: a refusal, an outage or any other error is a 503 with
// auth-service's own unavailable text, and the write never starts.
func TestPatchIsActiveTrueFailsClosedWhenAuthRefusesOrIsDown(t *testing.T) {
	for name, err := range map[string]error{
		"down":    orgreactivate.ErrUnavailable,
		"refused": orgreactivate.ErrRefused,
		"other":   errors.New("boom"),
	} {
		log := &orderLog{}
		db := &reactivationDB{log: log, found: true, active: false}
		h := newReactivationHandlers(db, &fakeAuthReactivator{log: log, err: err})
		code, body := patchOrg(h, `{"is_active": true, "name": "Renamed"}`)
		if code != http.StatusServiceUnavailable || strings.TrimSpace(body) != authUnavailableBody {
			t.Fatalf("%s: %d %s, want 503 %s", name, code, body, authUnavailableBody)
		}
		if log.index("db:begin") != -1 || log.count("db:exec") != 0 {
			t.Fatalf("%s: the write ran after auth failed: %v", name, log.steps)
		}
	}
}

// Every is_active=true request of a found organization reaches auth, whatever the first read saw: a suspend that lands
// between that read and the write would otherwise let go-api write is_active=true with no auth call.
func TestPatchWithTheSwitchOnCallsAuthForEveryIsActiveTrueOfAFoundOrg(t *testing.T) {
	for name, tc := range map[string]struct {
		body   string
		found  bool
		active bool
		auth   int
	}{
		"true on an active org":   {`{"is_active": true}`, true, true, 1},
		"false on an active org":  {`{"is_active": false}`, true, true, 0},
		"false on a suspended":    {`{"is_active": false}`, true, false, 0},
		"no is_active":            {`{"name": "N"}`, true, false, 0},
		"null is_active":          {`{"is_active": null}`, true, false, 0},
		"true on a missing org":   {`{"is_active": true}`, false, false, 0},
		"true on a suspended org": {`{"is_active": true}`, true, false, 1},
	} {
		log := &orderLog{}
		db := &reactivationDB{log: log, found: tc.found, active: tc.active}
		auth := &fakeAuthReactivator{log: log}
		patchOrg(newReactivationHandlers(db, auth), tc.body)
		if auth.calls != tc.auth {
			t.Errorf("%s: %d auth calls, want %d", name, auth.calls, tc.auth)
		}
	}
	log := &orderLog{}
	h := newReactivationHandlers(&reactivationDB{log: log}, &fakeAuthReactivator{log: log})
	if code, body := patchOrg(h, `{"is_active": true}`); code != http.StatusNotFound || !strings.Contains(body, "Organization not found") {
		t.Fatalf("a missing org: %d %s, want the 404 of today", code, body)
	}
}

// The race: the gate's read sees an active organization, a suspend lands, and the write would turn it active
// again. Auth is called exactly once, before any write, whatever the first read saw.
func TestPatchIsActiveTrueCallsAuthEvenWhenTheFirstReadSawActive(t *testing.T) {
	log := &orderLog{}
	db := &reactivationDB{log: log, found: true, seq: []bool{true, false, false}}
	auth := &fakeAuthReactivator{log: log}
	patchOrg(newReactivationHandlers(db, auth), `{"is_active": true}`)
	if auth.calls != 1 {
		t.Fatalf("%d auth calls, want exactly 1 although the first read saw an active organization", auth.calls)
	}
	if a, w := log.index("auth"), log.index("db:begin"); a < 0 || w < 0 || a > w {
		t.Fatalf("order %v, want the auth call before the write transaction", log.steps)
	}
}

// An already-active organization with the switch on: auth is called, and the update runs after it.
func TestPatchIsActiveTrueOnAnActiveOrgCallsAuthThenWrites(t *testing.T) {
	log := &orderLog{}
	db := &reactivationDB{log: log, found: true, active: true}
	auth := &fakeAuthReactivator{log: log}
	patchOrg(newReactivationHandlers(db, auth), `{"is_active": true}`)
	if auth.calls != 1 || log.index("db:begin") < 0 || log.index("auth") > log.index("db:begin") {
		t.Fatalf("auth calls=%d order %v, want the auth call and then the write transaction", auth.calls, log.steps)
	}
}

// With the first read active, an auth refusal or outage is a 503 and no write.
func TestPatchIsActiveTrueOnAnActiveOrgFailsClosedWhenAuthFails(t *testing.T) {
	for name, err := range map[string]error{"down": orgreactivate.ErrUnavailable, "refused": orgreactivate.ErrRefused} {
		log := &orderLog{}
		db := &reactivationDB{log: log, found: true, active: true}
		code, body := patchOrg(newReactivationHandlers(db, &fakeAuthReactivator{log: log, err: err}), `{"is_active": true}`)
		if code != http.StatusServiceUnavailable || strings.TrimSpace(body) != authUnavailableBody {
			t.Fatalf("%s: %d %s, want 503 %s", name, code, body, authUnavailableBody)
		}
		if log.index("db:begin") != -1 {
			t.Fatalf("%s: the write ran after auth failed: %v", name, log.steps)
		}
	}
}

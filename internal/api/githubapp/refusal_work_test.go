package githubapp

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
)

// The two install-callback refusals that answer "installer does not have access"
// (an installation the installer cannot access, and one another organization
// holds) must run the same local database work, so response time does not tell
// which one a caller met. These tests drive installCallback end to end against a
// fake transaction and compare the statements each refusal ran. The count is
// timing-independent: it is the work, not a measurement of it.

const (
	callerOrg = "11111111-1111-1111-1111-111111111111"
	otherOrg  = "22222222-2222-2222-2222-222222222222"
)

type refusalRecorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *refusalRecorder) add(call string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
}

func (r *refusalRecorder) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

// storedRow is what github_app_installations holds for the installation: no
// row, a row no organization holds, one the caller holds, or one another
// organization holds.
type storedRow struct {
	exists bool
	owner  string
}

type fakeTx struct {
	pgx.Tx
	rec    *refusalRecorder
	stored storedRow
}

type fakeRow struct{ err error }

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*(dest[0].(*string)) = "row-id"
	return nil
}

func (t fakeTx) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	if !strings.Contains(sql, "SELECT id::text FROM github_app_installations") {
		t.rec.add("unexpected query: " + sql)
		return fakeRow{err: errors.New("unexpected query")}
	}
	t.rec.add("lookup")
	if !t.stored.exists {
		return fakeRow{err: pgx.ErrNoRows}
	}
	return fakeRow{}
}

// Exec emulates the guarded claim: it affects a row only when a row exists, the
// claim is allowed (a sixth argument, when present, must be true) and the row is
// unowned or the caller's.
func (t fakeTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if !strings.Contains(sql, "UPDATE github_app_installations") {
		t.rec.add("unexpected exec: " + sql)
		return pgconn.CommandTag{}, errors.New("unexpected exec")
	}
	t.rec.add("update")
	claim := true
	if len(args) >= 6 {
		claim, _ = args[5].(bool)
	}
	if claim && t.stored.exists && (t.stored.owner == "" || t.stored.owner == callerOrg) {
		return pgconn.NewCommandTag("UPDATE 1"), nil
	}
	return pgconn.NewCommandTag("UPDATE 0"), nil
}

func (t fakeTx) Rollback(context.Context) error {
	t.rec.add("rollback")
	return nil
}

func (t fakeTx) Commit(context.Context) error {
	t.rec.add("commit")
	return nil
}

func (t fakeTx) Begin(context.Context) (pgx.Tx, error) {
	t.rec.add("savepoint")
	return t, nil
}

type callbackRun struct {
	status int
	body   string
	header http.Header
	calls  []string
	logs   string
}

// runCallback posts a callback for installation 101 as callerOrg. The GitHub
// stub lists installation 101 for the installer only when accessible.
func runCallback(t *testing.T, accessible bool, stored storedRow, beginErr error) callbackRun {
	t.Helper()
	github := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/login/oauth/access_token") {
			_, _ = w.Write([]byte(`{"access_token":"token"}`))
			return
		}
		if accessible {
			_, _ = w.Write([]byte(`{"installations":[{"id":101,"account":{"login":"acme","type":"Organization"}}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"installations":[]}`))
	}))
	t.Cleanup(github.Close)

	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	signer := Signer{Secret: "secret", Issuer: "issuer", Audience: "audience"}
	state, err := signer.Mint(callerOrg, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	var logs strings.Builder
	rec := &refusalRecorder{}
	h := handlers{
		Deps: Deps{
			Logger: slog.New(slog.NewTextHandler(&logs, nil)), Now: func() time.Time { return now }, Signer: signer,
			HTTPClient: github.Client(), GitHubURL: github.URL, GitHubAPIURL: github.URL,
			Config: Config{AppID: "1", ClientID: "id", ClientSecret: "secret", PrivateKey: func() (string, error) { return "key", nil }},
		},
		begin: func(context.Context) (pgx.Tx, error) {
			rec.add("begin")
			if beginErr != nil {
				return nil, beginErr
			}
			return fakeTx{rec: rec, stored: stored}, nil
		},
	}
	payload := `{"installation_id":101,"state":"` + state + `","code":"oauth-code"}`
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	body, outcome, _, err := pybody.Read(request)
	if err != nil || outcome != pybody.Ready {
		t.Fatalf("read the body: %v %v", outcome, err)
	}
	ctx := policy.WithUser(policy.WithBody(context.Background(), body), &policy.User{OrgID: callerOrg})
	response := httptest.NewRecorder()
	h.installCallback(response, request.WithContext(ctx))
	return callbackRun{status: response.Code, body: response.Body.String(), header: response.Header(), calls: rec.seen(), logs: logs.String()}
}

func TestBothInstallRefusalsRunTheSameLocalWork(t *testing.T) {
	linked := runCallback(t, true, storedRow{exists: true, owner: otherOrg}, nil)
	if linked.status != http.StatusForbidden {
		t.Fatalf("an installation another organization holds answered %d: %s", linked.status, linked.body)
	}
	want := []string{"begin", "lookup", "update", "rollback"}
	if !reflect.DeepEqual(linked.calls, want) {
		t.Fatalf("the linked-elsewhere refusal ran %v, want %v", linked.calls, want)
	}
	for name, stored := range map[string]storedRow{
		"no row":               {},
		"another organization": {exists: true, owner: otherOrg},
		"the caller's own":     {exists: true, owner: callerOrg},
		"no organization":      {exists: true},
	} {
		t.Run(name, func(t *testing.T) {
			got := runCallback(t, false, stored, nil)
			if got.status != linked.status || got.body != linked.body {
				t.Fatalf("answered %d %q, want %d %q", got.status, got.body, linked.status, linked.body)
			}
			if !reflect.DeepEqual(got.calls, want) {
				t.Fatalf("the not-accessible refusal ran %v for stored state %q, want %v", got.calls, name, want)
			}
			for header, values := range linked.header {
				if header == "Content-Length" {
					continue
				}
				if !reflect.DeepEqual(got.header[header], values) {
					t.Errorf("header %s is %v, want %v", header, got.header[header], values)
				}
			}
			if len(got.header) != len(linked.header) {
				t.Errorf("headers %v, want %v", got.header, linked.header)
			}
		})
	}
}

func TestTheNotAccessibleRefusalWritesNothing(t *testing.T) {
	for name, stored := range map[string]storedRow{"unowned row": {exists: true}, "no row": {}} {
		got := runCallback(t, false, stored, nil)
		for _, call := range got.calls {
			if call == "commit" || call == "savepoint" {
				t.Errorf("%s: the not-accessible refusal ran %q: %v", name, call, got.calls)
			}
		}
	}
}

func TestTheNotAccessibleAnswerSurvivesAFailedLocalTransaction(t *testing.T) {
	want := runCallback(t, false, storedRow{}, nil)
	got := runCallback(t, false, storedRow{}, errors.New("database unavailable"))
	if got.status != want.status || got.body != want.body {
		t.Fatalf("answered %d %q, want %d %q", got.status, got.body, want.status, want.body)
	}
	if strings.Contains(got.body, "database") || strings.Contains(got.logs, "installation_id") || strings.Contains(got.logs, callerOrg) {
		t.Fatalf("the answer or the log carries more than the class: body %q log %q", got.body, got.logs)
	}
}

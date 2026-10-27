package authapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"regexp"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// closedPool is the error a request gets when the process closes the pool under
// it, from a real pgx pool (lazy: no database).
func closedPool(t *testing.T) error {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://u@127.0.0.1:1/db")
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	pool.Close()
	_, err = pool.Exec(context.Background(), "select 1")
	if err == nil {
		t.Fatal("a closed pool answered")
	}
	return err
}

func expectRetry503(t *testing.T, name string, r *httptest.ResponseRecorder) {
	t.Helper()
	if r.Code != http.StatusServiceUnavailable {
		t.Errorf("%s = %d, want 503: %s", name, r.Code, r.Body)
		return
	}
	if got := r.Header().Get("Retry-After"); got != "1" {
		t.Errorf("%s 503 Retry-After = %q, want 1", name, got)
	}
	if r.Header().Get("X-Dho-Unhandled-Error") != "" {
		t.Errorf("%s 503 carries the unhandled-error marker", name)
	}
}

// A request that meets a closed pool at shutdown is a 503 the client retries,
// on login, refresh and the credential routes, whichever layer sees the error.
func TestAClosedPoolAnswers503WithRetryAfter(t *testing.T) {
	down := fmt.Errorf("%w: load: %w", session.ErrStore, closedPool(t))

	login := newCompatFixture(t)
	login.loginUsers.err = down
	r := login.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, ""), nil)
	expectRetry503(t, "login store", r)

	refresh := newCompatFixture(t)
	refresh.sessions.result = session.Result{Reason: session.ReasonStoreUnavailable}
	refresh.sessions.err = down
	r = refresh.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+credential()+`"}`, nil)
	expectRetry503(t, "refresh store", r)

	limited := storeErr{fmt.Errorf("rate limit: %w", closedPool(t))}
	compat := newCompatFixture(t)
	compat.counters = limited
	compat.buildCompat()
	cred := newCredFixtureWith(t, limited)
	for name, call := range map[string]*httptest.ResponseRecorder{
		"login limiter":   compat.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, "wrong password", ""), nil),
		"refresh limiter": compat.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+credential()+`"}`, nil),
		"forgot":          cred.call(http.MethodPost, PatternForgotPassword, `{"email":"sentinel-person@example.com"}`, nil),
		"register":        cred.call(http.MethodPost, PatternRegister, registerBody, nil),
		"change password": cred.call(http.MethodPost, PatternChangePassword, `{"current_password":"a-password-1","new_password":"Brand-new-pass-77!"}`, cred.bearer(cred.grant())),
	} {
		expectRetry503(t, name, call)
	}
}

func TestAnUnclassifiedStoreErrorStaysA500(t *testing.T) {
	f := newCompatFixture(t)
	f.loginUsers.err = errors.New("unexpected row shape")
	r := f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, ""), nil)
	if r.Code != http.StatusInternalServerError || r.Header().Get("Retry-After") != "" {
		t.Fatalf("status %d Retry-After %q, want a plain 500", r.Code, r.Header().Get("Retry-After"))
	}
}

var requestID = regexp.MustCompile(`(?m)^X-Auth-Request-Id: .*\r\n`)

// The go-api wire of every writer that renders the dependency 503: before this
// change the bytes were the same minus the Retry-After line (only
// UnavailableRetry carried it). Any other byte is a wire change.
func TestGoAPIRefusalWireIsPinned(t *testing.T) {
	dump := func(rec *httptest.ResponseRecorder) string {
		b, err := httputil.DumpResponse(rec.Result(), true)
		if err != nil {
			t.Fatal(err)
		}
		return requestID.ReplaceAllString(string(b), "")
	}
	const body503 = `{"detail":{"message":"Authentication service unavailable"}}`
	want503 := "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 59\r\nContent-Type: application/json\r\nRetry-After: 1\r\n\r\n" + body503
	for name, c := range map[httpapi.Code]string{
		httpapi.CodeUnavailable: want503,
		httpapi.CodeRateLimited: "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 69\r\nContent-Type: application/json\r\n\r\n" +
			`{"detail":{"message":"Rate limit exceeded. Please try again later."}}`,
		httpapi.CodeInternal: "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 34\r\nContent-Type: application/json\r\nX-Dho-Unhandled-Error: 1\r\n\r\n" +
			`{"detail":"Internal Server Error"}`,
	} {
		rec := httptest.NewRecorder()
		goAPIRefusal(rec, httptest.NewRequest(http.MethodGet, "/", nil), name)
		if got := dump(rec); got != c {
			t.Errorf("goAPIRefusal(%s):\n got %q\nwant %q", name, got, c)
		}
	}
	rec := httptest.NewRecorder()
	UnavailableRetry().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if got := dump(rec); got != want503 {
		t.Errorf("UnavailableRetry:\n got %q\nwant %q", got, want503)
	}
	f := newCompatFixture(t)
	f.loginUsers.err = pgDown
	r := f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, ""), nil)
	if got := dump(r); got != want503 {
		t.Errorf("unavailableCompat:\n got %q\nwant %q", got, want503)
	}
}

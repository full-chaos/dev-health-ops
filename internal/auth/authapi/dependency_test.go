package authapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"

	"github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

type storeErr struct{ err error }

func (s storeErr) Increment(context.Context, httpapi.Hit) (int64, error) { return 0, s.err }
func (s storeErr) Peek(context.Context, httpapi.Hit) (int64, error)      { return 0, s.err }
func (storeErr) Backend() string                                         { return "redis" }

func expectDown(t *testing.T, name string, response *httptest.ResponseRecorder, body string) {
	t.Helper()
	if got := response.Code; got != http.StatusServiceUnavailable {
		t.Errorf("%s = %d, want 503: %s", name, got, body)
	}
	if strings.Contains(body, "sentinel") || strings.Contains(body, "5432") {
		t.Errorf("%s 503 names the dependency: %s", name, body)
	}
}

var pgDown = fmt.Errorf("%w: %w", session.ErrStore, &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED})

func TestAPostgresFailureOnLoginAnswers503(t *testing.T) {
	f := newCompatFixture(t)
	f.loginUsers.err = pgDown
	r := f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, ""), nil)
	expectDown(t, "login", r, r.Body.String())
	if !strings.Contains(r.Body.String(), textAuthUnavailable) {
		t.Errorf("login 503 body %s", r.Body)
	}
}

func TestAPostgresFailureOnTheLoginAuditAnswers503(t *testing.T) {
	f := newCompatFixture(t)
	f.appendErr = pgDown
	r := f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, "wrong", ""), nil)
	expectDown(t, "login audit", r, r.Body.String())
}

func TestAPostgresFailureOnCompatRefreshAnswers503(t *testing.T) {
	f := newCompatFixture(t)
	f.sessions.result = session.Result{Reason: session.ReasonStoreUnavailable}
	f.sessions.err = pgDown
	r := f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+credential()+`"}`, nil)
	expectDown(t, "compat refresh", r, r.Body.String())
}

func TestANonDependencyFailureStillAnswers500(t *testing.T) {
	f := newCompatFixture(t)
	f.loginUsers.err = errors.New("unexpected row shape")
	r := f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, ""), nil)
	if r.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500", r.Code)
	}
}

func TestAClientCancelOnLoginIsNotA503(t *testing.T) {
	f := newCompatFixture(t)
	f.loginUsers.err = fmt.Errorf("lookup: %w", context.Canceled)
	r := f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, ""), nil)
	if r.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want the existing 500 for a cancelled client", r.Code)
	}
}

func TestAStoreDeadlineOnLoginAnswers503(t *testing.T) {
	f := newCompatFixture(t)
	f.loginUsers.err = fmt.Errorf("lookup: %w", context.DeadlineExceeded)
	r := f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, ""), nil)
	expectDown(t, "login deadline", r, r.Body.String())
}

func TestAValkeyFailureAnswers503OnEveryLimitedRoute(t *testing.T) {
	down := storeErr{fmt.Errorf("rate limit script: %w", valkey.ErrClosing)}
	compat := newCompatFixture(t)
	compat.counters = down
	compat.buildCompat()
	cred := newCredFixtureWith(t, down)
	link, _ := linkFixtureWith(t, down)
	calls := map[string]*httptest.ResponseRecorder{
		"login":   compat.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, "wrong password", ""), nil),
		"refresh": compat.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+credential()+`"}`, nil),
		"forgot":  cred.call(http.MethodPost, PatternForgotPassword, `{"email":"sentinel-person@example.com"}`, nil),
		"reset":   cred.call(http.MethodPost, PatternResetPassword, `{"token":"sentinel-token","new_password":"another-password-1"}`, nil),
		"link":    link.do(http.MethodPost, PatternSocialLink, linkBody, link.bearer(link.grant())),
	}
	for name, r := range calls {
		expectDown(t, name, r, r.Body.String())
	}
	if compat.attempts.failures != 0 {
		t.Error("the login handler ran with the counter store down")
	}
}

func TestAMalformedLoginIsChargedBeforeItsAuditAndABurstIsLimited(t *testing.T) {
	f := newCompatFixture(t)
	for i := 0; i < loginIPLimit.Count; i++ {
		if r := f.do(http.MethodPost, PatternCompatLogin, `{}`, nil); r.Code != http.StatusUnprocessableEntity {
			t.Fatalf("malformed login %d = %d, want 422", i, r.Code)
		}
	}
	audited := len(f.appended)
	if audited != loginIPLimit.Count {
		t.Fatalf("%d audit rows for %d refusals", audited, loginIPLimit.Count)
	}
	r := f.do(http.MethodPost, PatternCompatLogin, `{}`, nil)
	if r.Code != http.StatusTooManyRequests {
		t.Fatalf("malformed login over the budget = %d, want 429: %s", r.Code, r.Body)
	}
	if len(f.appended) != audited {
		t.Fatal("a limited malformed login wrote an audit row")
	}
}

func TestAMalformedLoginWhileTheCounterStoreIsDownAnswers503WithoutAnAuditRow(t *testing.T) {
	f := newCompatFixture(t)
	f.counters = storeErr{valkey.ErrClosing}
	f.buildCompat()
	r := f.do(http.MethodPost, PatternCompatLogin, `{}`, nil)
	expectDown(t, "malformed login", r, r.Body.String())
	if len(f.appended) != 0 {
		t.Fatal("an unlimited refusal wrote an audit row")
	}
}

func TestAMalformedLoginWhilePostgresIsDownAnswers503(t *testing.T) {
	f := newCompatFixture(t)
	f.appendErr = pgDown
	r := f.do(http.MethodPost, PatternCompatLogin, `{"email":"x@example.invalid","password":"p"}`, nil)
	expectDown(t, "invalid login under pg-cut", r, r.Body.String())
}

type brokenBody struct{ err error }

func (b brokenBody) Read([]byte) (int, error) { return 0, b.err }

// io.EOF is in the dependency class, but the class applies to store-call
// errors only: a login body that cannot be read, or an empty one, never
// becomes a 503.
func TestAnUnreadableOrEmptyLoginBodyIsNeverA503(t *testing.T) {
	for _, c := range []struct {
		name string
		body io.Reader
		want int
	}{
		{"truncated body", brokenBody{io.ErrUnexpectedEOF}, http.StatusInternalServerError},
		{"eof body", brokenBody{io.EOF}, http.StatusUnprocessableEntity},
		{"empty body", strings.NewReader(""), http.StatusUnprocessableEntity},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newCompatFixture(t)
			req := httptest.NewRequest(http.MethodPost, PatternCompatLogin, c.body)
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			f.server.ServeHTTP(w, req)
			if w.Code != c.want {
				t.Fatalf("status %d, want %d: %s", w.Code, c.want, w.Body)
			}
		})
	}
}

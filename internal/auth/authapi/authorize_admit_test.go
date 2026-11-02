package authapi

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

type admitCountedBody struct {
	r io.Reader
	n int
}

func (b *admitCountedBody) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.n += n
	return n, err
}

func (b *admitCountedBody) Close() error { return nil }

// countingVerifier counts the workload token verifications the caller gate makes.
type countingVerifier struct {
	inner workloadVerifier
	calls int
}

func (c *countingVerifier) Verify(token string, now time.Time) (*authverify.WorkloadClaims, error) {
	c.calls++
	return c.inner.Verify(token, now)
}

type admitFixture struct {
	*authorizeHandlerFixture
	routes   []httpapi.Route
	server   http.Handler
	verifier *countingVerifier
}

// newAdmitFixture mounts the two authorize routes behind the real route chain
// (rate limit, in-flight cap, body bound), with a counting caller verifier.
func newAdmitFixture(t *testing.T) *admitFixture {
	t.Helper()
	h := newAuthorizeHandlerFixture(t)
	callers := h.callers()
	counting := &countingVerifier{inner: callers.verifier}
	callers.verifier = counting
	routes, err := AuthorizeRoutes(AuthorizeDeps{
		Callers: callers, References: h.references(), State: h.state, Evaluator: authorization.NewEvaluator(),
		ProfileRows: authorizeProfileDoc(t, PatternAuthorize, PatternAuthorizeBatch),
	})
	if err != nil {
		t.Fatal(err)
	}
	server, err := httpapi.NewServer(httpapi.ServerOptions{
		Address: "127.0.0.1:0", Logger: slog.New(slog.DiscardHandler), Routes: routes,
		RequestTimeout: 5 * time.Second, MaxBodyBytes: 1 << 20, RateLimit: 1000, RateLimitBurst: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &admitFixture{authorizeHandlerFixture: h, routes: routes, server: server.Handler(), verifier: counting}
}

func (a *admitFixture) do(handler http.Handler, pattern, bearer string, payload []byte, chunked bool) (*httptest.ResponseRecorder, *admitCountedBody) {
	body := &admitCountedBody{r: strings.NewReader(string(payload))}
	req := httptest.NewRequest(http.MethodPost, pattern, body)
	req.ContentLength = int64(len(payload))
	if chunked {
		req.ContentLength = -1
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	rec := httptest.NewRecorder()
	requestid.Middleware(handler).ServeHTTP(rec, req)
	return rec, body
}

func (a *admitFixture) payload(pattern string) []byte {
	q := a.question("settings.write", "organization", "acme", a.org)
	if pattern == PatternAuthorizeBatch {
		return mustJSON(a.t, map[string]any{"schema_version": "authorization_batch_request.v1", "requests": []map[string]any{q}})
	}
	return mustJSON(a.t, q)
}

// The caller gate runs before the body is read, whatever the framing.
func TestAuthorizeRefusedCallerReadsNoBodyByte(t *testing.T) {
	for _, pattern := range []string{PatternAuthorize, PatternAuthorizeBatch} {
		for _, chunked := range []bool{true, false} {
			for name, token := range map[string]func(*admitFixture) string{
				"no bearer": func(*admitFixture) string { return "" },
				"wrong audience": func(a *admitFixture) string {
					return a.workloadToken(func(c *contracts.WorkloadTokenClaims) { c.Aud = "dev-health-api" })
				},
			} {
				a := newAdmitFixture(t)
				rec, body := a.do(a.server, pattern, token(a), a.payload(pattern), chunked)
				if rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), reasonInvalidCredentials) || body.n != 0 {
					t.Errorf("%s chunked=%v %s: status %d body %q, body bytes read %d; want 401 invalid_credentials and 0 bytes read", pattern, chunked, name, rec.Code, rec.Body, body.n)
				}
			}
		}
	}
}

func TestAuthorizeAllowedCallerStillGetsTheSizeBound(t *testing.T) {
	for _, pattern := range []string{PatternAuthorize, PatternAuthorizeBatch} {
		limit := map[string]int{PatternAuthorize: maxAuthorizeBody, PatternAuthorizeBatch: maxAuthorizeBatchBody}[pattern]
		for _, chunked := range []bool{true, false} {
			a := newAdmitFixture(t)
			rec, _ := a.do(a.server, pattern, a.workloadToken(nil), []byte(strings.Repeat(" ", limit+1)), chunked)
			if rec.Code != http.StatusRequestEntityTooLarge || !strings.Contains(rec.Body.String(), `"payload_too_large"`) || a.state.reads != 0 {
				t.Errorf("%s chunked=%v: status %d body %q, state transactions %d; want 413 payload_too_large and 0", pattern, chunked, rec.Code, rec.Body, a.state.reads)
			}
		}
	}
}

func TestAuthorizeAllowedCallerChunkedBodyIsDecided(t *testing.T) {
	for _, pattern := range []string{PatternAuthorize, PatternAuthorizeBatch} {
		a := newAdmitFixture(t)
		rec, _ := a.do(a.server, pattern, a.workloadToken(nil), a.payload(pattern), true)
		if rec.Code != http.StatusOK {
			t.Errorf("%s: an allowed caller with a chunked body got %d %s; want 200", pattern, rec.Code, rec.Body)
		}
		if a.verifier.calls != 1 {
			t.Errorf("%s: the caller token was verified %d times; want once", pattern, a.verifier.calls)
		}
	}
}

// A handler that finds no admitted caller in its request context fails closed:
// it never verifies the bearer itself.
func TestAuthorizeHandlerWithoutAnAdmittedCallerFailsClosed(t *testing.T) {
	for _, route := range []string{PatternAuthorize, PatternAuthorizeBatch} {
		a := newAdmitFixture(t)
		var handler http.Handler
		for _, r := range a.routes {
			if r.Pattern == route {
				handler = r.Handler
			}
		}
		rec, _ := a.do(handler, route, a.workloadToken(nil), a.payload(route), false)
		if rec.Code != http.StatusInternalServerError || a.verifier.calls != 0 || a.state.reads != 0 {
			t.Errorf("%s: no admitted caller gave status %d, %d Authenticate calls, %d state transactions; want 500, 0, 0", route, rec.Code, a.verifier.calls, a.state.reads)
		}
	}
}

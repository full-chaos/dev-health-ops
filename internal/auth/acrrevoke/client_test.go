package acrrevoke

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/acrlisting"
)

const (
	tokenPath  = "/api/v1/oauth/token"
	subjectJWT = "subject-jwt-one"
	accessOne  = "access-one"
	orgOne     = "0b6f3a52-8c1e-4d7a-9a4e-3f2d1c0b9e87"
	credOne    = "cred_A-1"
	actorOne   = "5f0c2d1e-7a3b-4c9d-8e2f-1a0b3c4d5e6f"
)

type call struct {
	method, path, auth, contentType, body string
	form                                  url.Values
}

type fakeACR struct {
	srv *httptest.Server

	mu          sync.Mutex
	calls       []call
	exchangeRC  int
	exchangeRaw string
	revokeRC    int
	revokeRaw   string
	location    string
}

func newFake(t *testing.T) *fakeACR {
	t.Helper()
	f := &fakeACR{revokeRC: http.StatusNoContent}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeACR) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	raw, _ := io.ReadAll(r.Body)
	c := call{method: r.Method, path: r.URL.EscapedPath(), auth: r.Header.Get("Authorization"), contentType: r.Header.Get("Content-Type"), body: string(raw)}
	if r.URL.Path == tokenPath {
		c.form, _ = url.ParseQuery(string(raw))
		f.calls = append(f.calls, c)
		if f.exchangeRC != 0 {
			w.WriteHeader(f.exchangeRC)
			_, _ = w.Write([]byte(f.exchangeRaw))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": accessOne, "token_type": "Bearer", "expires_in": 600})
		return
	}
	f.calls = append(f.calls, c)
	if f.location != "" {
		w.Header().Set("Location", f.location)
	}
	w.WriteHeader(f.revokeRC)
	_, _ = w.Write([]byte(f.revokeRaw))
}

func (f *fakeACR) snapshot() []call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]call(nil), f.calls...)
}

func subjectFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(subjectJWT+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func newClient(t *testing.T, f *fakeACR) *Client {
	t.Helper()
	base, _ := url.Parse(f.srv.URL)
	endpoint, _ := url.Parse(f.srv.URL + tokenPath)
	c, err := New(Config{BaseURL: base, TokenEndpoint: endpoint, SubjectTokenFile: subjectFile(t), HTTPClient: f.srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func acrError(code string, status int) string {
	raw, _ := json.Marshal(map[string]any{"schema_version": "error.v1", "request_id": "req-1",
		"error": map[string]any{"code": code, "message": "m", "http_status": status, "retryable": false}})
	return string(raw)
}

func TestRevokeSendsTheExchangeThenTheRevokeOnTheWire(t *testing.T) {
	f := newFake(t)
	if err := newClient(t, f).Revoke(context.Background(), orgOne, credOne, ActorAdmin, actorOne); err != nil {
		t.Fatalf("Revoke = %v", err)
	}
	calls := f.snapshot()
	if len(calls) != 2 {
		t.Fatalf("%d calls, want 2: %+v", len(calls), calls)
	}
	exchange, revoke := calls[0], calls[1]
	if exchange.method != http.MethodPost || exchange.path != tokenPath || exchange.auth != "" || exchange.contentType != "application/x-www-form-urlencoded" {
		t.Fatalf("exchange = %+v", exchange)
	}
	want := url.Values{"grant_type": {"urn:ietf:params:oauth:grant-type:token-exchange"}, "subject_token": {subjectJWT},
		"subject_token_type": {"urn:ietf:params:oauth:token-type:jwt"}, "resource": {"urn:acr:org:" + orgOne}}
	if exchange.form.Encode() != want.Encode() {
		t.Fatalf("exchange form = %v, want %v", exchange.form, want)
	}
	if revoke.method != http.MethodPost || revoke.path != "/internal/v1/credentials/"+credOne+"/revoke" ||
		revoke.auth != "Bearer "+accessOne || revoke.contentType != "application/json" {
		t.Fatalf("revoke = %+v", revoke)
	}
	if wantBody := `{"schema_version":"credential_revoke_on_behalf_request.v1","actor_type":"admin","actor_id":"` + actorOne + `"}`; revoke.body != wantBody {
		t.Fatalf("revoke body = %s, want %s", revoke.body, wantBody)
	}
	if strings.Contains(revoke.body, subjectJWT) || strings.Contains(revoke.path, subjectJWT) || strings.Contains(revoke.auth, subjectJWT) {
		t.Fatal("the subject token left the exchange")
	}
}

func TestRevokeSendsTheUserActor(t *testing.T) {
	f := newFake(t)
	if err := newClient(t, f).Revoke(context.Background(), orgOne, credOne, ActorUser, actorOne); err != nil {
		t.Fatal(err)
	}
	if got := f.snapshot()[1].body; !strings.Contains(got, `"actor_type":"user"`) {
		t.Fatalf("body = %s", got)
	}
}

func TestRevokeMapsEachAnswer(t *testing.T) {
	cases := []struct {
		name   string
		status int
		raw    string
		want   error
		kind   acrlisting.Kind
	}{
		{"revoked", http.StatusNoContent, "", nil, ""},
		{"refused", http.StatusNotFound, acrError("not_found", 404), ErrNotFound, ""},
		{"already revoked", http.StatusConflict, acrError("credential_lifecycle_conflict", 409), ErrConflict, ""},
		{"404 that is not acr's refusal", http.StatusNotFound, "404 page not found\n", nil, acrlisting.KindRejected},
		{"404 with another code", http.StatusNotFound, acrError("route_not_found", 404), nil, acrlisting.KindRejected},
		{"404 envelope of another status", http.StatusNotFound, acrError("not_found", 400), nil, acrlisting.KindRejected},
		{"404 envelope of another schema", http.StatusNotFound, strings.Replace(acrError("not_found", 404), "error.v1", "error.v2", 1), nil, acrlisting.KindRejected},
		{"404 envelope past the answer bound", http.StatusNotFound, acrError("not_found", 404) + strings.Repeat(" ", maxAnswerBytes), nil, acrlisting.KindRejected},
		{"409 with another code", http.StatusConflict, acrError("conflict", 409), nil, acrlisting.KindRejected},
		{"bad request", http.StatusBadRequest, acrError("invalid_request", 400), nil, acrlisting.KindRejected},
		{"200 is not 204", http.StatusOK, "{}", nil, acrlisting.KindRejected},
		{"unauthorized", http.StatusUnauthorized, acrError("invalid_token", 401), nil, acrlisting.KindUnauthorized},
		{"forbidden", http.StatusForbidden, acrError("insufficient_scope", 403), nil, acrlisting.KindForbidden},
		{"rate limited", http.StatusTooManyRequests, "", nil, acrlisting.KindRateLimited},
		{"unavailable", http.StatusServiceUnavailable, acrError("upstream_unavailable", 503), nil, acrlisting.KindUnavailable},
		{"server error", http.StatusInternalServerError, "", nil, acrlisting.KindUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t)
			f.revokeRC, f.revokeRaw = tc.status, tc.raw
			err := newClient(t, f).Revoke(context.Background(), orgOne, credOne, ActorUser, actorOne)
			if tc.kind == "" {
				if !errors.Is(err, tc.want) || (tc.want == nil && err != nil) {
					t.Fatalf("err = %v, want %v", err, tc.want)
				}
				return
			}
			var failure *Error
			if !errors.As(err, &failure) || failure.Kind != tc.kind || failure.Status != tc.status {
				t.Fatalf("err = %#v, want kind %s status %d", err, tc.kind, tc.status)
			}
			if errors.Is(err, ErrNotFound) || errors.Is(err, ErrConflict) {
				t.Fatalf("err = %v reads as acr's refusal", err)
			}
		})
	}
}

func TestRevokeFollowsNoRedirect(t *testing.T) {
	f := newFake(t)
	f.revokeRC, f.location = http.StatusTemporaryRedirect, "/internal/v1/credentials/other/revoke"
	err := newClient(t, f).Revoke(context.Background(), orgOne, credOne, ActorUser, actorOne)
	var failure *Error
	if !errors.As(err, &failure) || failure.Kind != acrlisting.KindRejected || failure.Status != http.StatusTemporaryRedirect {
		t.Fatalf("err = %v", err)
	}
	if n := len(f.snapshot()); n != 2 {
		t.Fatalf("%d calls: the redirect was followed", n)
	}
}

func TestRevokeExchangeRefusalSendsNoRevoke(t *testing.T) {
	cases := []struct {
		name string
		rc   int
		raw  string
		kind acrlisting.Kind
	}{
		{"server error", http.StatusInternalServerError, "", acrlisting.KindExchange},
		{"invalid target", http.StatusBadRequest, `{"error":"invalid_target"}`, acrlisting.KindInvalidTarget},
		{"invalid request", http.StatusBadRequest, `{"error":"invalid_request"}`, acrlisting.KindInvalidRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t)
			f.exchangeRC, f.exchangeRaw = tc.rc, tc.raw
			err := newClient(t, f).Revoke(context.Background(), orgOne, credOne, ActorUser, actorOne)
			var failure *Error
			if !errors.As(err, &failure) || failure.Kind != tc.kind || failure.Status != tc.rc {
				t.Fatalf("err = %#v", err)
			}
			if calls := f.snapshot(); len(calls) != 1 || calls[0].path != tokenPath {
				t.Fatalf("calls = %+v", calls)
			}
		})
	}
}

func TestRevokeRefusesBadInputBeforeAnyRequest(t *testing.T) {
	long := strings.Repeat("a", 201)
	cases := []struct{ name, org, cred, actorType, actor string }{
		{"upper-case org", strings.ToUpper(orgOne), credOne, ActorUser, actorOne},
		{"empty org", "", credOne, ActorUser, actorOne},
		{"empty credential", orgOne, "", ActorUser, actorOne},
		{"slash in credential", orgOne, "a/b", ActorUser, actorOne},
		{"dot-dot credential", orgOne, "..", ActorUser, actorOne},
		{"long credential", orgOne, strings.Repeat("a", 129), ActorUser, actorOne},
		{"unknown actor type", orgOne, credOne, "operator", actorOne},
		{"empty actor", orgOne, credOne, ActorUser, ""},
		{"actor with edge space", orgOne, credOne, ActorUser, " " + actorOne},
		{"actor too long", orgOne, credOne, ActorUser, long},
		{"actor not utf-8", orgOne, credOne, ActorUser, "\xff"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t)
			if err := newClient(t, f).Revoke(context.Background(), tc.org, tc.cred, tc.actorType, tc.actor); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("err = %v", err)
			}
			if n := len(f.snapshot()); n != 0 {
				t.Fatalf("%d requests sent", n)
			}
		})
	}
	f := newFake(t)
	if err := newClient(t, f).Revoke(context.Background(), orgOne, strings.Repeat("a", 128), ActorUser, strings.Repeat("é", 200)); err != nil {
		t.Fatalf("the longest valid id and actor: %v", err)
	}
}

func TestRevokeErrorsNameNoTokenOrCredential(t *testing.T) {
	f := newFake(t)
	f.revokeRC, f.revokeRaw = http.StatusBadGateway, "upstream says "+accessOne+" "+credOne
	err := newClient(t, f).Revoke(context.Background(), orgOne, credOne, ActorUser, actorOne)
	if err == nil {
		t.Fatal("no error")
	}
	for _, secret := range []string{accessOne, subjectJWT, credOne, actorOne, "upstream"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error %q names %q", err, secret)
		}
	}
}

func TestRevokeCancelledContextUnwrapsToTheContextError(t *testing.T) {
	f := newFake(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := newClient(t, f).Revoke(ctx, orgOne, credOne, ActorUser, actorOne)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

func TestNewRefusesUnusableConfig(t *testing.T) {
	good, _ := url.Parse("https://acr.example.test")
	token, _ := url.Parse("https://acr.example.test/api/v1/oauth/token")
	plain, _ := url.Parse("http://acr.example.test")
	query, _ := url.Parse("https://acr.example.test/?a=b")
	loopback, _ := url.Parse("http://127.0.0.1:8080")
	cases := []struct {
		name string
		cfg  Config
		ok   bool
	}{
		{"good", Config{BaseURL: good, TokenEndpoint: token, SubjectTokenFile: "/var/run/token"}, true},
		{"loopback http", Config{BaseURL: loopback, TokenEndpoint: loopback, SubjectTokenFile: "/t"}, true},
		{"plain http base", Config{BaseURL: plain, TokenEndpoint: token, SubjectTokenFile: "/t"}, false},
		{"plain http endpoint", Config{BaseURL: good, TokenEndpoint: plain, SubjectTokenFile: "/t"}, false},
		{"query on base", Config{BaseURL: query, TokenEndpoint: token, SubjectTokenFile: "/t"}, false},
		{"no base", Config{TokenEndpoint: token, SubjectTokenFile: "/t"}, false},
		{"no endpoint", Config{BaseURL: good, SubjectTokenFile: "/t"}, false},
		{"blank subject file", Config{BaseURL: good, TokenEndpoint: token, SubjectTokenFile: "  "}, false},
	}
	for _, tc := range cases {
		_, err := New(tc.cfg)
		if (err == nil) != tc.ok || (!tc.ok && !errors.Is(err, ErrInvalidConfig)) {
			t.Errorf("%s: err = %v", tc.name, err)
		}
	}
}

func TestNewWithoutAnHTTPClientUsesTheGuardedClient(t *testing.T) {
	base, _ := url.Parse("https://acr.example.test")
	c, err := New(Config{BaseURL: base, TokenEndpoint: base, SubjectTokenFile: "/t"})
	if err != nil {
		t.Fatal(err)
	}
	if c.http == nil || c.http.CheckRedirect == nil || c.http.Timeout != defaultTimeout {
		t.Fatalf("client = %+v", c.http)
	}
}

package sessionissue

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// testToken is an auth-session-issuer credential built at run time.
func testToken(t *testing.T) string {
	t.Helper()
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	return "svc_authsess_" + base64.RawURLEncoding.EncodeToString(random)
}

func baseOf(t *testing.T, raw string) *url.URL {
	t.Helper()
	base, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return base
}

func endUserRequest() *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil)
	r.RemoteAddr = "198.51.100.23:4711"
	r.Header.Set("User-Agent", "Mozilla/5.0 (issue test)")
	return r
}

func TestIssueSendsTheSessionAndAnswersThePair(t *testing.T) {
	token := testToken(t)
	principal, org := uuid.New(), uuid.New()
	var seen *http.Request
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Clone(context.Background())
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"a.b.c","refresh_token":"dhr1_x","token_type":"bearer","expires_in":3600}`)
	}))
	defer server.Close()
	client, err := New(baseOf(t, server.URL), token, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := client.Issue(context.Background(), endUserRequest(), Request{PrincipalID: principal, OrganizationID: &org, AMR: []string{"pwd"}})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if pair.Access != "a.b.c" || pair.Refresh != "dhr1_x" {
		t.Fatalf("pair %+v", pair)
	}
	if seen.Method != http.MethodPost || seen.URL.Path != Path {
		t.Fatalf("called %s %s, want POST %s", seen.Method, seen.URL.Path, Path)
	}
	if seen.Header.Get("Authorization") != "Bearer "+token {
		t.Fatal("the service credential was not presented as a bearer")
	}
	if seen.Header.Get("User-Agent") != "Mozilla/5.0 (issue test)" || seen.Header.Get("X-Forwarded-For") != "198.51.100.23" {
		t.Fatalf("end user not forwarded: UA %q XFF %q", seen.Header.Get("User-Agent"), seen.Header.Get("X-Forwarded-For"))
	}
	if body["principal_id"] != principal.String() || body["organization_id"] != org.String() {
		t.Fatalf("body %v", body)
	}
	if amr, _ := body["amr"].([]any); len(amr) != 1 || amr[0] != "pwd" {
		t.Fatalf("amr %v", body["amr"])
	}
}

func TestIssueWithoutAnOrganizationSendsNull(t *testing.T) {
	var raw []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ = io.ReadAll(r.Body)
		_, _ = io.WriteString(w, `{"access_token":"a","refresh_token":"r","token_type":"bearer"}`)
	}))
	defer server.Close()
	client, err := New(baseOf(t, server.URL), testToken(t), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Issue(context.Background(), endUserRequest(), Request{PrincipalID: uuid.New(), AMR: []string{"fed"}}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"organization_id":null`) {
		t.Fatalf("body %s, want a null organization", raw)
	}
}

func TestIssueFailuresAreUnavailable(t *testing.T) {
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	down := "http://" + closed.Addr().String()
	_ = closed.Close()
	answers := map[string]http.HandlerFunc{
		"refused 401": func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) },
		"refused 403": func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusForbidden) },
		"down 503":    func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) },
		"not json":    func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "<html>") },
		"no refresh": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"access_token":"a","token_type":"bearer"}`)
		},
		"not bearer": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"access_token":"a","refresh_token":"r","token_type":"mac"}`)
		},
		"redirect": func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/elsewhere", http.StatusFound) },
		"too large": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"access_token":"`+strings.Repeat("a", maxAnswerBytes)+`","refresh_token":"r","token_type":"bearer"}`)
		},
		"slow": func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-time.After(2 * time.Second):
			case <-r.Context().Done():
			}
		},
	}
	for name, answer := range answers {
		server := httptest.NewServer(answer)
		client, err := New(baseOf(t, server.URL), testToken(t), 200*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.Issue(context.Background(), endUserRequest(), Request{PrincipalID: uuid.New(), AMR: []string{"pwd"}})
		server.Close()
		if !errors.Is(err, ErrUnavailable) {
			t.Errorf("%s: err %v, want ErrUnavailable", name, err)
		}
	}
	client, err := New(baseOf(t, down), testToken(t), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Issue(context.Background(), endUserRequest(), Request{PrincipalID: uuid.New(), AMR: []string{"pwd"}}); !errors.Is(err, ErrUnavailable) {
		t.Errorf("unreachable: err %v, want ErrUnavailable", err)
	}
}

func TestNewRefusesAnythingButAnIssuerCredential(t *testing.T) {
	base := baseOf(t, "http://auth-service:8095")
	good := testToken(t)
	for name, token := range map[string]string{
		"empty":           "",
		"acr credential":  "svc_acr_" + good[len("svc_authsess_"):],
		"short":           good[:len(good)-1],
		"trailing space":  good + " ",
		"not base64url":   "svc_authsess_" + strings.Repeat("+", 43),
		"another service": "svc_worker_" + good[len("svc_authsess_"):],
	} {
		if _, err := New(base, token, 0); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := New(nil, good, 0); err == nil {
		t.Error("no base URL accepted")
	}
	if _, err := New(base, good, 0); err != nil {
		t.Fatalf("a good credential: %v", err)
	}
}

// The service credential never goes to an ambient HTTP proxy: the client
// dials auth-service itself.
func TestIssueIgnoresTheEnvironmentsHTTPProxy(t *testing.T) {
	var proxied atomicCount
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxied.add()
		_, _ = io.WriteString(w, `{"access_token":"forged","refresh_token":"forged","token_type":"bearer"}`)
	}))
	defer proxy.Close()
	t.Setenv("HTTP_PROXY", proxy.URL)
	t.Setenv("HTTPS_PROXY", proxy.URL)
	t.Setenv("NO_PROXY", "")
	client, err := New(baseOf(t, "http://auth-service.invalid:8095"), testToken(t), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := client.Issue(context.Background(), endUserRequest(), Request{PrincipalID: uuid.New(), AMR: []string{"pwd"}})
	if proxied.get() != 0 {
		t.Fatalf("the issue call went through the environment's proxy %d time(s)", proxied.get())
	}
	if !errors.Is(err, ErrUnavailable) || pair.Access != "" {
		t.Fatalf("issue to an unresolvable host = %+v, %v; want ErrUnavailable and no pair", pair, err)
	}
}

type atomicCount struct {
	mu sync.Mutex
	n  int
}

func (c *atomicCount) add() { c.mu.Lock(); c.n++; c.mu.Unlock() }

func (c *atomicCount) get() int { c.mu.Lock(); defer c.mu.Unlock(); return c.n }

// A redirect from the issue route is never followed: the other origin sees no
// request (no credential leaves with it) and the call is ErrUnavailable.
func TestIssueNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	var reached atomicCount
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached.add()
		_, _ = io.WriteString(w, `{"access_token":"a","refresh_token":"r","token_type":"bearer"}`)
	}))
	defer other.Close()
	redirecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+Path, http.StatusTemporaryRedirect)
	}))
	defer redirecting.Close()
	client, err := New(baseOf(t, redirecting.URL), testToken(t), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if client.http.CheckRedirect == nil || client.http.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
		t.Fatal("the client's own CheckRedirect does not answer http.ErrUseLastResponse")
	}
	pair, err := client.Issue(context.Background(), endUserRequest(), Request{PrincipalID: uuid.New(), AMR: []string{"pwd"}})
	if reached.get() != 0 {
		t.Fatalf("the redirect target saw %d request(s)", reached.get())
	}
	if !errors.Is(err, ErrUnavailable) || pair.Access != "" {
		t.Fatalf("redirect = %+v, %v; want ErrUnavailable and no pair", pair, err)
	}
}

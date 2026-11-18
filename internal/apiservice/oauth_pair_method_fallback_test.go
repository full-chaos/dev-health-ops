package apiservice

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/acr"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

const pairPattern = "/api/v1/auth/oauth/{first}/{second}"

// registeredMethods are the seven methods the /oauth/{first}/{second} pattern registers.
var registeredMethods = []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
	http.MethodDelete, http.MethodOptions}

// fallbackMethods are methods no route of the api listener registers; CONNECT is origin-form.
var fallbackMethods = []string{"TRACE", http.MethodConnect, "PROPFIND", "CUSTOM"}

// methodRoutes are the api listener's routes with the guarded mounts (sso among them) in.
func methodRoutes(t *testing.T) ([]httpapi.Route, http.Handler) {
	t.Helper()
	auth, err := policy.NewAuthenticator(newProbeStore(), quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	routes := Routes(Deps{Pool: &pgxpool.Pool{}, Guard: policy.NewGuard(auth, quietLogger()), Auth: auth}, quietLogger())
	server, err := NewServer(apiTestConfig(), quietLogger(), routes, policy.NewScope(auth, quietLogger()).Layers()...)
	if err != nil {
		t.Fatal(err)
	}
	return routes, server.Handler()
}

type methodAnswer struct {
	status int
	allow  string
	body   string
}

func askMethod(handler http.Handler, method, path string) methodAnswer {
	return askMethodAs(handler, method, path, nil)
}

func askMethodAs(handler http.Handler, method, path string, headers map[string]string) methodAnswer {
	request := httptest.NewRequest(method, path, strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return methodAnswer{recorder.Code, recorder.Header().Get("Allow"), strings.TrimSpace(recorder.Body.String())}
}

// methodCallers are the credential shapes the listener's auth layers judge before any route runs.
var methodCallers = []struct {
	name    string
	headers map[string]string
}{
	{"anonymous", nil},
	{"invalid bearer", map[string]string{"Authorization": "Bearer not-a-token"}},
	{"non-bearer scheme", map[string]string{"Authorization": "Basic Zm9vOmJhcg=="}},
	{"invalid bearer, foreign org", map[string]string{"Authorization": "Bearer not-a-token", "X-Org-Id": "22000000-0000-4000-8000-0000000000ff"}},
	{"malformed org header", map[string]string{"X-Org-Id": "not-a-uuid"}},
	{"session cookie", map[string]string{"Cookie": "access_token=not-a-token"}},
}

const (
	notFoundBody   = `{"detail":"Not Found"}`
	notAllowedBody = `{"detail":"Method Not Allowed"}`
)

// TestOAuthPairAnswersUnregisteredMethodsAsItsOwnRoutes is the route x method
// table of the overlapping /oauth/{first}/{second} dispatch: a method no route
// registers (TRACE, CONNECT, PROPFIND, a custom one) gets the answer the pair
// gives a registered method that none of its routes serves on that path (PUT):
// 405 with the path's own Allow, or 404 where no route takes the path. The
// registered methods are pinned in the same table, so a fix cannot move them.
func TestOAuthPairAnswersUnregisteredMethodsAsItsOwnRoutes(t *testing.T) {
	_, handler := methodRoutes(t)
	const provider = "22000000-0000-4000-8000-000000000003"
	rows := []struct {
		name string
		path string
		// served are the methods a pair route takes on this path: not a 404 or 405.
		served []string
		// refused is the answer of every other method on this path, PUT first.
		refused *methodAnswer
	}{
		{"authorize", "/api/v1/auth/oauth/" + provider + "/authorize", []string{http.MethodPost, http.MethodGet},
			&methodAnswer{http.StatusMethodNotAllowed, "POST", notAllowedBody}},
		{"by-type authorize", "/api/v1/auth/oauth/github/authorize", []string{http.MethodPost, http.MethodGet},
			&methodAnswer{http.StatusMethodNotAllowed, "POST", notAllowedBody}},
		{"callback", "/api/v1/auth/oauth/" + provider + "/callback", []string{http.MethodPost},
			&methodAnswer{http.StatusMethodNotAllowed, "POST", notAllowedBody}},
		{"providers/authorize", "/api/v1/auth/oauth/providers/authorize", []string{http.MethodPost, http.MethodGet},
			&methodAnswer{http.StatusMethodNotAllowed, "POST", notAllowedBody}},
		// PATCH /oauth/providers/{provider_id} is retired: 404 for every method.
		{"retired provider id", "/api/v1/auth/oauth/providers/" + provider, nil,
			&methodAnswer{http.StatusNotFound, "", notFoundBody}},
		{"no route", "/api/v1/auth/oauth/" + provider + "/unknown", nil,
			&methodAnswer{http.StatusNotFound, "", notFoundBody}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			served := map[string]bool{}
			for _, method := range row.served {
				served[method] = true
			}
			want := askMethod(handler, http.MethodPut, row.path)
			if want != *row.refused {
				t.Fatalf("PUT %s = %+v, want %+v", row.path, want, *row.refused)
			}
			if want.status != http.StatusNotFound && want.status != http.StatusMethodNotAllowed {
				t.Fatalf("PUT %s = %+v: not a refusal", row.path, want)
			}
			for _, method := range registeredMethods {
				got := askMethod(handler, method, row.path)
				if served[method] {
					if got.status == http.StatusNotFound || got.status == http.StatusMethodNotAllowed {
						t.Errorf("registered %s %s = %+v, want a served answer", method, row.path, got)
					}
					continue
				}
				if got != want {
					t.Errorf("registered %s %s = %+v, want %+v", method, row.path, got, want)
				}
			}
			for _, method := range fallbackMethods {
				if got := askMethod(handler, method, row.path); got != want {
					t.Errorf("unregistered %s %s = %+v, want %+v", method, row.path, got, want)
				}
			}
		})
	}
}

// TestOAuthPairUnregisteredMethodsMeetTheAuthLayersFirst: per caller, an
// unregistered method gets exactly what PUT (registered, served by no pair
// route) gets on the same path, so the listener's auth layers judge it first
// and the pair never serves it as GET or POST: the answer is a refusal.
func TestOAuthPairUnregisteredMethodsMeetTheAuthLayersFirst(t *testing.T) {
	_, handler := methodRoutes(t)
	const provider = "22000000-0000-4000-8000-000000000003"
	paths := []string{
		"/api/v1/auth/oauth/" + provider + "/authorize",
		"/api/v1/auth/oauth/github/authorize",
		"/api/v1/auth/oauth/" + provider + "/callback",
		"/api/v1/auth/oauth/providers/authorize",
		"/api/v1/auth/oauth/providers/" + provider,
		"/api/v1/auth/oauth/" + provider + "/unknown",
	}
	for _, caller := range methodCallers {
		for _, path := range paths {
			want := askMethodAs(handler, http.MethodPut, path, caller.headers)
			t.Logf("%s PUT %s = %+v", caller.name, path, want)
			if want.status < 400 {
				t.Errorf("%s PUT %s = %+v: not a refusal", caller.name, path, want)
			}
			for _, method := range fallbackMethods {
				if got := askMethodAs(handler, method, path, caller.headers); got != want {
					t.Errorf("%s %s %s = %+v, want %+v", caller.name, method, path, got, want)
				}
			}
		}
	}
}

// TestUnregisteredMethodsOnPlainRoutesKeepTheirServedAllow is the control: a
// pattern that registers its methods one route each answers an unregistered
// method 405 with the methods it serves, before and after the pair fix.
func TestUnregisteredMethodsOnPlainRoutesKeepTheirServedAllow(t *testing.T) {
	_, handler := methodRoutes(t)
	const provider = "22000000-0000-4000-8000-000000000003"
	rows := []struct {
		path, allow string
	}{
		{"/api/v1/auth/sso/providers", "GET, POST"},
		{"/api/v1/auth/sso/providers/" + provider, "DELETE, GET, PATCH"},
		{"/api/v1/auth/saml/" + provider + "/acs", "POST"},
		{"/api/v1/auth/oidc/" + provider + "/authorize", "POST"},
	}
	for _, row := range rows {
		for _, method := range fallbackMethods {
			want := methodAnswer{http.StatusMethodNotAllowed, row.allow, notAllowedBody}
			if got := askMethod(handler, method, row.path); got != want {
				t.Errorf("%s %s = %+v, want %+v", method, row.path, got, want)
			}
		}
	}
}

// TestOnlyThePairRegistersEveryMethod is the census behind the pair fix: on the
// api listener, the /oauth/{first}/{second} dispatch is the one pattern
// registered for all seven methods, so it is the one whose router 405 named
// every method.
func TestOnlyThePairRegistersEveryMethod(t *testing.T) {
	methods := map[string]map[string]bool{}
	routes, _ := methodRoutes(t)
	for _, route := range routes {
		if methods[route.Pattern] == nil {
			methods[route.Pattern] = map[string]bool{}
		}
		methods[route.Pattern][route.Method] = true
	}
	var every []string
	for pattern, set := range methods {
		all := true
		for _, method := range registeredMethods {
			all = all && set[method]
		}
		if all {
			every = append(every, pattern)
		}
	}
	sort.Strings(every)
	if len(every) != 1 || every[0] != pairPattern {
		t.Fatalf("patterns registered for every method = %v, want only %s", every, pairPattern)
	}
}

// TestOnlyTheOAuthPairSetsMethodFallback: the sso pair is the one route set of
// the api listeners that dispatches methods itself; no other route sets the flag.
func TestOnlyTheOAuthPairSetsMethodFallback(t *testing.T) {
	routes, _ := methodRoutes(t)
	routes = append(routes, internalRoutesWith(acr.Deps{})...)
	flagged := map[string]int{}
	for _, route := range routes {
		if route.MethodFallback {
			flagged[route.Pattern]++
		}
	}
	if len(flagged) != 1 || flagged[pairPattern] != len(registeredMethods) {
		t.Fatalf("MethodFallback patterns = %v, want only %s on its %d routes", flagged, pairPattern, len(registeredMethods))
	}
}

type countingBody struct {
	io.Reader
	bytes int
}

func (b *countingBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.bytes += n
	return n, err
}

func (b *countingBody) Close() error { return nil }

// TestOAuthPairUnregisteredMethodsLeaveAStreamedBodyUnread: a method no route
// registers is refused with the pair's own 405 or 404 without reading its
// body, whatever the framing (an undeclared-length stream, a declared length
// over the listener's bound), as the router's own 405 did; PUT, a registered
// method, keeps its route chain's eager body bound (a read, a 413).
func TestOAuthPairUnregisteredMethodsLeaveAStreamedBodyUnread(t *testing.T) {
	_, handler := methodRoutes(t)
	const provider = "22000000-0000-4000-8000-000000000003"
	paths := []string{
		"/api/v1/auth/oauth/" + provider + "/authorize",
		"/api/v1/auth/oauth/github/authorize",
		"/api/v1/auth/oauth/" + provider + "/callback",
		"/api/v1/auth/oauth/providers/authorize",
		"/api/v1/auth/oauth/providers/" + provider,
		"/api/v1/auth/oauth/" + provider + "/unknown",
	}
	ask := func(method, path string, length int64, transfer []string) (methodAnswer, int) {
		body := &countingBody{Reader: strings.NewReader("{}")}
		request := httptest.NewRequest(method, path, nil)
		request.Body, request.ContentLength, request.TransferEncoding = body, length, transfer
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		return methodAnswer{recorder.Code, recorder.Header().Get("Allow"), strings.TrimSpace(recorder.Body.String())}, body.bytes
	}
	framings := []struct {
		name     string
		length   int64
		transfer []string
	}{
		{"chunked", -1, []string{"chunked"}},
		{"undeclared", -1, nil},
		{"declared", 2, nil},
		{"declared over the bound", maxBodyBytes + 1, nil},
	}
	for _, path := range paths {
		want, _ := ask(http.MethodPut, path, 2, nil)
		if want.status != http.StatusNotFound && want.status != http.StatusMethodNotAllowed {
			t.Fatalf("PUT %s = %+v: not the pair's refusal", path, want)
		}
		for _, framing := range framings {
			for _, method := range fallbackMethods {
				got, read := ask(method, path, framing.length, framing.transfer)
				if got != want || read != 0 {
					t.Errorf("%s %s %s = %+v after reading %d bytes, want %+v and no read", framing.name, method, path, got, read, want)
				}
			}
		}
		if _, read := ask(http.MethodPut, path, -1, []string{"chunked"}); read == 0 {
			t.Errorf("chunked PUT %s read no body: the registered route chain's eager bound moved", path)
		}
		if got, read := ask(http.MethodPut, path, maxBodyBytes+1, nil); got.status != http.StatusRequestEntityTooLarge || read != 0 {
			t.Errorf("PUT %s declared over the bound = %+v after reading %d bytes, want the eager 413 unread", path, got, read)
		}
	}
}

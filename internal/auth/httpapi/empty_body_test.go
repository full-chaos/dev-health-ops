package httpapi

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// EmptyBody is the explicit "this route takes no body": a declared body and one of
// undeclared length are both refused, before the handler, and a bodiless request is
// served. MaxBodyBytes of zero would mean the server default, not this.
func TestAnEmptyBodyRouteRefusesEveryBody(t *testing.T) {
	served := 0
	route := Route{Method: http.MethodGet, Pattern: "/x", EmptyBody: true, Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) { served++ })}
	chain := MaxBodyWith(0, WriteError)(route.Handler)
	for name, body := range map[string]io.Reader{
		"declared":   strings.NewReader("a"),
		"undeclared": io.MultiReader(strings.NewReader("a")),
	} {
		request := httptest.NewRequest(http.MethodGet, "/x", body)
		if name == "undeclared" {
			request.ContentLength = -1
		}
		response := httptest.NewRecorder()
		chain.ServeHTTP(response, request)
		if response.Code != http.StatusRequestEntityTooLarge || served != 0 {
			t.Fatalf("%s: status %d, served %d", name, response.Code, served)
		}
	}
	response := httptest.NewRecorder()
	chain.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/x", nil))
	if response.Code != http.StatusOK || served != 1 {
		t.Fatalf("positive control: status %d, served %d", response.Code, served)
	}
}

// routeChain itself applies the policy over a server default that would admit the
// body.
func TestRouteChainAppliesTheEmptyBodyPolicyOverTheDefault(t *testing.T) {
	options := ServerOptions{MaxBodyBytes: 1 << 20, RateLimit: 1000, RateLimitBurst: 1000}
	for name, tc := range map[string]struct {
		empty bool
		want  int
	}{"the policy": {true, http.StatusRequestEntityTooLarge}, "the default": {false, http.StatusOK}} {
		route := Route{Method: http.MethodGet, Pattern: "/x", EmptyBody: tc.empty, Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})}
		handler := routeChain(route, options, slog.New(slog.DiscardHandler), WriteError, make(chan struct{}, 4))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/x", strings.NewReader("a")))
		if response.Code != tc.want {
			t.Errorf("%s: status %d, want %d", name, response.Code, tc.want)
		}
	}
}

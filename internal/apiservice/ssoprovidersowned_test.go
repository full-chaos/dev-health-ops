package apiservice

import (
	"net/http"
	"strings"
	"testing"
)

func parseSSOProvidersOwnedFor(t *testing.T, values map[string]string) (bool, error) {
	t.Helper()
	lookup := usersOwnedLookup(values)
	proxy, err := ParseAuthProxyConfig(lookup)
	if err != nil {
		t.Fatalf("proxy config: %v", err)
	}
	return ParseSSOProvidersOwned(lookup, proxy)
}

func TestSSOProvidersOwnedOffNeedsNoProxy(t *testing.T) {
	for _, values := range []map[string]string{nil, {"AUTH_SSO_PROVIDERS_OWNED": "false"}, {"AUTH_SSO_PROVIDERS_OWNED": ""}} {
		on, err := parseSSOProvidersOwnedFor(t, values)
		if err != nil || on {
			t.Fatalf("%v: on=%v err=%v, want off and no error", values, on, err)
		}
	}
}

func TestSSOProvidersOwnedGarbageIsABootError(t *testing.T) {
	for _, value := range []string{"maybe", "yes", "1", "on"} {
		_, err := parseSSOProvidersOwnedFor(t, map[string]string{"AUTH_SSO_PROVIDERS_OWNED": value})
		if err == nil || !strings.Contains(err.Error(), "AUTH_SSO_PROVIDERS_OWNED") {
			t.Fatalf("%q: err = %v, want a boot error naming AUTH_SSO_PROVIDERS_OWNED", value, err)
		}
	}
}

func TestSSOProvidersOwnedNeedsTheSessionsProxy(t *testing.T) {
	on, err := parseSSOProvidersOwnedFor(t, map[string]string{"AUTH_SSO_PROVIDERS_OWNED": "true"})
	if err == nil || on || !strings.Contains(err.Error(), "AUTH_PROXY_SESSIONS") {
		t.Fatalf("no sessions proxy: on=%v err=%v, want a refusal naming AUTH_PROXY_SESSIONS", on, err)
	}
	on, err = parseSSOProvidersOwnedFor(t, map[string]string{"AUTH_SSO_PROVIDERS_OWNED": "true", "AUTH_PROXY_URL": "http://auth-service:8095", "AUTH_PROXY_SESSIONS": "true"})
	if err != nil || !on {
		t.Fatalf("sessions proxy on: on=%v err=%v, want on", on, err)
	}
}

// Owned with no workload client: every provider route answers the named 503, never the stub's 500 and never a write.
func TestSSOProvidersOwnedWithNoWorkloadClientAnswersTheNamed503OnEveryRoute(t *testing.T) {
	withSSOLicence(t)
	client, err := ssoProvidersClient(AuthProxyConfig{}, true, nil)
	if err != nil || client == nil {
		t.Fatalf("client = %v, %v, want the unavailable client", client, err)
	}
	if off, err := ssoProvidersClient(AuthProxyConfig{}, false, nil); off != nil || err != nil {
		t.Fatalf("not owned: %v, %v, want nil", off, err)
	}
	handler, minter := adminConfigServerScoped(t, nil, withBoundaryCallers, ssoRoutes(client), nil)
	admin := boundaryCallers(minter)["org admin"].token
	for key := range ssoRowActions {
		got := ssoDo(handler, key, admin)
		if got.status != http.StatusServiceUnavailable || !strings.Contains(got.body, "SSO provider service unavailable") {
			t.Errorf("%s: %d %s, want the named 503", key, got.status, got.body)
		}
	}
}

// The list bounds on the real route: absent, 0, the cap and past it are echoed as asked; a negative value is the legacy bare 500
// with no call to auth-service.
func TestSSOListBoundsOnTheRealRoute(t *testing.T) {
	withSSOLicence(t)
	client := &ssoCellClient{}
	handler, minter := adminConfigServerScoped(t, nil, withBoundaryCallers, ssoRoutes(client), nil)
	member := boundaryCallers(minter)["org member"].token
	list := func(query string) cellAnswer {
		return ssoDo(handler, "GET /api/v1/auth/sso/providers"+query, member)
	}
	for query, want := range map[string]string{"": `"limit":50,"offset":0`, "?limit=0": `"limit":0,"offset":0`, "?limit=1000&offset=3": `"limit":1000,"offset":3`, "?limit=5000": `"limit":5000,"offset":0`} {
		if got := list(query); got.status != http.StatusOK || !strings.HasSuffix(got.body, want+"}") {
			t.Errorf("%q: %d %s, want 200 ending %s", query, got.status, got.body, want)
		}
	}
	calls := len(client.calls)
	for _, query := range []string{"?limit=-1", "?offset=-1"} {
		if got := list(query); got.status != http.StatusInternalServerError {
			t.Errorf("%q: %d %s, want the bare 500", query, got.status, got.body)
		}
	}
	if len(client.calls) != calls {
		t.Fatalf("a negative bound reached auth-service: %v", client.calls[calls:])
	}
	if got := list("?limit=abc"); got.status != http.StatusUnprocessableEntity {
		t.Errorf("limit=abc: %d %s, want 422", got.status, got.body)
	}
}

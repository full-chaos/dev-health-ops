package apiservice

import (
	"strings"
	"testing"
)

func usersOwnedLookup(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func parseUsersOwnedFor(t *testing.T, values map[string]string) (bool, error) {
	t.Helper()
	lookup := usersOwnedLookup(values)
	proxy, err := ParseAuthProxyConfig(lookup)
	if err != nil {
		t.Fatalf("proxy config: %v", err)
	}
	return ParseUsersOwned(lookup, proxy)
}

func TestUsersOwnedOffNeedsNoProxy(t *testing.T) {
	for _, values := range []map[string]string{nil, {"AUTH_USERS_OWNED": "false"}, {"AUTH_USERS_OWNED": ""}} {
		on, err := parseUsersOwnedFor(t, values)
		if err != nil || on {
			t.Fatalf("%v: on=%v err=%v, want off and no error", values, on, err)
		}
	}
}

func TestUsersOwnedGarbageIsABootError(t *testing.T) {
	_, err := parseUsersOwnedFor(t, map[string]string{"AUTH_USERS_OWNED": "maybe"})
	if err == nil || !strings.Contains(err.Error(), "AUTH_USERS_OWNED") {
		t.Fatalf("err = %v, want a boot error naming AUTH_USERS_OWNED", err)
	}
}

func TestUsersOwnedNeedsTheSessionsLocalLoginAndCredentialsProxies(t *testing.T) {
	all := map[string]string{
		"AUTH_PROXY_URL": "http://auth-service:8095", "AUTH_PROXY_SESSIONS": "true",
		"AUTH_PROXY_LOCAL_LOGIN": "true", "AUTH_PROXY_CREDENTIALS": "true", "AUTH_USERS_OWNED": "true",
	}
	if on, err := parseUsersOwnedFor(t, all); err != nil || !on {
		t.Fatalf("all three proxies on: on=%v err=%v, want on and no error", on, err)
	}
	for _, missing := range []string{"AUTH_PROXY_LOCAL_LOGIN", "AUTH_PROXY_CREDENTIALS"} {
		values := map[string]string{}
		for key, value := range all {
			if key != missing {
				values[key] = value
			}
		}
		on, err := parseUsersOwnedFor(t, values)
		if err == nil || on || !strings.Contains(err.Error(), missing) {
			t.Fatalf("without %s: on=%v err=%v, want a refusal naming it", missing, on, err)
		}
	}
	// With sessions off the proxy config itself refuses any other aggregate,
	// so the switch is refused with no proxy at all.
	if on, err := parseUsersOwnedFor(t, map[string]string{"AUTH_USERS_OWNED": "true"}); err == nil || on {
		t.Fatalf("no proxy at all: on=%v err=%v, want a refusal", on, err)
	}
}

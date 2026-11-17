package devhealth_test

import (
	"fmt"
	"strings"
	"testing"
)

// The named principal reference kind of POST /v1/authorize (CHAOS-13945): authService.namedPrincipalCaller renders
// AUTH_AUTHORIZE_NAMED_PRINCIPAL_CALLER. Empty renders nothing.

const namedPrincipalCaller = "prn__fYQz6HDTDeOd_M7zDN0fQ"

func TestAuthNamedPrincipalOffRendersNothing(t *testing.T) {
	t.Parallel()

	_, base, err := renderAuth(t, phaseArgs(2)...)
	if err != nil {
		t.Fatalf("default render: %v\n%.400s", err, base)
	}
	docs, empty, err := renderAuth(t, phaseArgs(2, "--set-string", "authService.namedPrincipalCaller=")...)
	if err != nil {
		t.Fatalf("empty key render: %v\n%.400s", err, empty)
	}
	if empty != base {
		t.Fatal("an empty authService.namedPrincipalCaller renders differently from the default")
	}
	container, _ := authServicePod(t, docs)
	if _, ok := envValue(container, "AUTH_AUTHORIZE_NAMED_PRINCIPAL_CALLER"); ok {
		t.Fatal("the default render sets AUTH_AUTHORIZE_NAMED_PRINCIPAL_CALLER")
	}
}

func TestAuthNamedPrincipalOnRendersTheCallerUnchanged(t *testing.T) {
	t.Parallel()

	docs, out, err := renderAuth(t, phaseArgs(2, "--set-string", "authService.namedPrincipalCaller="+namedPrincipalCaller)...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	container, _ := authServicePod(t, docs)
	if value, ok := envValue(container, "AUTH_AUTHORIZE_NAMED_PRINCIPAL_CALLER"); !ok || value != namedPrincipalCaller {
		t.Fatalf("AUTH_AUTHORIZE_NAMED_PRINCIPAL_CALLER = %q (set %v), want %q", value, ok, namedPrincipalCaller)
	}
}

func TestAuthNamedPrincipalExtraEnvIsRefused(t *testing.T) {
	t.Parallel()

	for _, spelling := range []string{"AUTH_AUTHORIZE_NAMED_PRINCIPAL_CALLER", strings.ToLower("AUTH_AUTHORIZE_NAMED_PRINCIPAL_CALLER")} {
		_, out, err := renderAuth(t, phaseArgs(2, "--set-json", fmt.Sprintf(`authService.extraEnv=[{"name":%q,"value":"x"}]`, spelling))...)
		if err == nil {
			t.Fatalf("authService.extraEnv %s rendered", spelling)
		}
		if !strings.Contains(out, "authService.extraEnv must not set") {
			t.Fatalf("authService.extraEnv %s: the refusal does not name extraEnv: %.300s", spelling, out)
		}
	}
}

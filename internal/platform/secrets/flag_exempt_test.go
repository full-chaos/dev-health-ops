package secrets

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

func lookupWarnings(t *testing.T, name, value string) string {
	t.Helper()
	ResetRegistered()
	t.Cleanup(ResetRegistered)
	var logs bytes.Buffer
	var mu sync.Mutex
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	previous := SetWarner(func(msg string, args ...any) { mu.Lock(); defer mu.Unlock(); logger.Warn(msg, args...) })
	t.Cleanup(func() { SetWarner(previous) })
	t.Setenv(name, value)
	if got, ok := ProcessLookup(name); !ok || got != value {
		t.Fatalf("ProcessLookup(%s) = %q, %v", name, got, ok)
	}
	mu.Lock()
	defer mu.Unlock()
	return logs.String()
}

func TestAuthProxyFlagsAreNotRegisteredAsSecrets(t *testing.T) {
	for _, name := range []string{
		"AUTH_PROXY_SESSIONS", "AUTH_PROXY_LOCAL_LOGIN", "AUTH_PROXY_CREDENTIALS",
		"AUTH_PROXY_SOCIAL", "AUTH_PROXY_OIDC", "AUTH_PROXY_SAML", "AUTH_PROXY_ORG_PROVISIONING",
	} {
		if LooksSecret(name) {
			t.Errorf("%s is a boolean routing flag; LooksSecret must be false", name)
		}
		if out := lookupWarnings(t, name, "true"); out != "" {
			t.Errorf("%s: want no warning, got: %s", name, out)
		}
	}
}

func TestNamesNextToAnAuthProxyFlagStillRegisterAndWarn(t *testing.T) {
	for _, name := range []string{
		"AUTH_PROXY_CREDENTIALS_X", "X_AUTH_PROXY_CREDENTIALS", "AUTH_PROXY_CREDENTIAL",
		"AUTH_PROXY_CREDENTIALS2", "SERVICE_CREDENTIAL", "SERVICE_CREDENTIALS", "AUTH_PROXY_TOKEN",
	} {
		if !LooksSecret(name) {
			t.Errorf("%s must still look secret", name)
		}
		out := lookupWarnings(t, name, "true")
		if strings.Count(out, "secret_shorter_than_minimum_not_redactable_by_value") != 1 || !strings.Contains(out, name) {
			t.Errorf("%s: want one short-secret warning naming it, got: %s", name, out)
		}
	}
}

func TestARealSecretNameStaysRedactedByValue(t *testing.T) {
	value := "a-long-enough-secret-value-0123"
	if out := lookupWarnings(t, "AUTH_SERVICE_PASSWORD", value); out != "" {
		t.Fatalf("a long secret must not warn: %s", out)
	}
	if got := RedactRegistered("x " + value + " y"); strings.Contains(got, value) {
		t.Fatalf("a registered secret leaked: %q", got)
	}
}

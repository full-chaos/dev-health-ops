package authruntime

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

// ssoProviderStartEnv is the prod shape with the SSO provider routes on: users owned, the sign-in settings,
// AUTH_MEMBERSHIP_ROUTES and AUTH_SSO_PROVIDERS_OWNED.
func ssoProviderStartEnv() map[string]string {
	return provisioningEnvironment(usersAdminEnv(true))
}

// ssoProviderRoutesLine is the one start line of the SSO provider routes, decoded. It fails unless there is exactly one,
// and when any other line names the routes (the earlier "sso provider routes: ..." forms).
func ssoProviderRoutesLine(t *testing.T, logs string) map[string]any {
	t.Helper()
	var found []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) != nil {
			continue
		}
		msg, _ := record["msg"].(string)
		switch {
		case msg == "sso provider routes":
			found = append(found, record)
		case strings.HasPrefix(msg, "sso provider routes"):
			t.Fatalf("a second SSO provider routes line %q:\n%s", msg, logs)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%d \"sso provider routes\" start lines, want 1:\n%s", len(found), logs)
	}
	return found[0]
}

// The start log says how many SSO provider routes are mounted and, when none is, why. The gate is all or nothing, so the
// count is 7/7 or 0/7. Each case drives the real start path (authconfig.Load, configure). There is no start refusal
// (product rule 3); a key the cipher refuses is a WARN, every other reason an INFO.
func TestSSOProviderStartLineNamesTheMountCountAndTheReason(t *testing.T) {
	provisioningProfileRows = nil
	all := fmt.Sprintf("%d/%d", authapi.SSOProviderRouteCount(), authapi.SSOProviderRouteCount())
	none := fmt.Sprintf("0/%d", authapi.SSOProviderRouteCount())
	for _, c := range []struct {
		name         string
		mutate       func(map[string]string)
		level        string
		mounted      string
		reason       any
		mountedPaths bool
	}{
		{"mounted", func(map[string]string) {}, "INFO", all, nil, true},
		{"switch off", func(env map[string]string) { delete(env, authconfig.EnvSSOProvidersOwned) }, "INFO", none, "sso_providers_not_owned", false},
		{"cipher key unusable", func(env map[string]string) { delete(env, authconfig.EnvSettingsEncryptionKey) }, "WARN", none, "sso_cipher_unusable", false},
		{"membership routes off", func(env map[string]string) { delete(env, authconfig.EnvMembershipRoutes) }, "INFO", none, "membership_routes_off", false},
	} {
		env := ssoProviderStartEnv()
		c.mutate(env)
		line := ssoProviderRoutesLine(t, startLog(t, env))
		if line["level"] != c.level || line["mounted"] != c.mounted || line["reason"] != c.reason {
			t.Errorf("%s: %v, want level %s mounted %s reason %v", c.name, line, c.level, c.mounted, c.reason)
		}
		// The line says what the listener serves: mounted routes answer the credential gate, unmounted ones 404.
		answers := apiAnswers(t, env, authapi.SSOProviderPatterns())
		for _, pattern := range authapi.SSOProviderPatterns() {
			if mounted := answers[pattern] != http.StatusNotFound; mounted != c.mountedPaths {
				t.Errorf("%s: %s answered %d, want mounted=%v", c.name, pattern, answers[pattern], c.mountedPaths)
			}
		}
	}
	plantEmbeddedWithout(t, authapi.PatternSSOProviderDelete)
	missing := ssoProviderRoutesLine(t, startLog(t, ssoProviderStartEnv()))
	if missing["level"] != "INFO" || missing["mounted"] != none || missing["reason"] != authapi.ProvisioningRowReason ||
		missing["route"] != "POST "+authapi.PatternSSOProviderDelete {
		t.Errorf("rows missing: %v, want INFO mounted %s reason %s route POST %s", missing, none, authapi.ProvisioningRowReason,
			authapi.PatternSSOProviderDelete)
	}
}

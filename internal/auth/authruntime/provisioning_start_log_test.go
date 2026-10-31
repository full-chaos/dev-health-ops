package authruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

// withoutProfileRow plants the embedded endpoint-profile document without the
// auth-service row of route.
func withoutProfileRow(t *testing.T, route string) {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(authv1.AuthServiceEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	var kept []any
	for _, item := range document["rows"].([]any) {
		row := item.(map[string]any)
		if row["route"] == route && row["service"] == authapi.AccessService {
			continue
		}
		kept = append(kept, item)
	}
	document["rows"] = kept
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	provisioningProfileRows = raw
	t.Cleanup(func() { provisioningProfileRows = nil })
}

// startLogEntry runs auth-service with env and returns the decoded start
// failure line ("configure runtime dependencies"), failing when it exits other
// than 1 or writes no such line.
func startLogEntry(t *testing.T, env map[string]string) (map[string]any, string) {
	t.Helper()
	var logs bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if code := Execute(ctx, nil, lookupFrom(env), IO{Stdout: &logs, Stderr: &logs}); code != 1 {
		t.Fatalf("exit %d, want 1; log:\n%s", code, logs.String())
	}
	for _, line := range strings.Split(logs.String(), "\n") {
		if !strings.Contains(line, `"msg":"configure runtime dependencies"`) {
			continue
		}
		entry := map[string]any{}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("decode %q: %v", line, err)
		}
		return entry, logs.String()
	}
	t.Fatalf("no start failure line; log:\n%s", logs.String())
	return nil, ""
}

// A missing provisioning row stops auth-service at start. The start failure
// line must say which route and which credential classes the row lacks, so a
// deploy that ships a short document is diagnosed from the log alone. It names
// a route pattern and class names, never a value.
func TestAMissingProvisioningRowNamesTheRouteInTheStartLog(t *testing.T) {
	mailEnv(t, smtpMail())
	withoutProfileRow(t, authapi.PatternMembershipAdd)
	env := provisioningEnvironment(compatEnvironment(map[string]string{
		authconfig.EnvDatabaseURI:    "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile: "/nonexistent",
		authconfig.EnvSigningKeyID:   "start-log-test",
		authconfig.EnvUsersOwned:     "true",
	}))
	entry, logs := startLogEntry(t, env)
	if entry["error_category"] != "dependency_configuration_failed" || entry["reason"] != "auth_provisioning_profile_row_missing" {
		t.Fatalf("start log category=%v reason=%v, want dependency_configuration_failed and auth_provisioning_profile_row_missing; log:\n%s",
			entry["error_category"], entry["reason"], logs)
	}
	if entry["route"] != "POST "+authapi.PatternMembershipAdd {
		t.Fatalf("start log route=%v, want POST %s; log:\n%s", entry["route"], authapi.PatternMembershipAdd, logs)
	}
	classes, _ := entry["required_classes"].([]any)
	if len(classes) != 1 || classes[0] != "internal_svc_authsess_token" {
		t.Fatalf("start log required_classes=%v, want [internal_svc_authsess_token]; log:\n%s", entry["required_classes"], logs)
	}
	for _, secret := range []string{"probe-invite-signing-key", "probe-invite-previous-key"} {
		if strings.Contains(logs, secret) {
			t.Fatalf("the start log carries a secret value; log:\n%s", logs)
		}
	}
}

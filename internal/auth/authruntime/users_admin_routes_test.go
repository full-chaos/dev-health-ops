package authruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

var usersAdminRoutePatterns = []string{authapi.PatternUsersCreate, authapi.PatternUsersUpdate, authapi.PatternUsersPassword,
	authapi.PatternUsersDelete}

// plantUsersAdminRows plants the embedded endpoint-profile document plus a row for each user-admin route (a copy of
// the deprovision row, which admits the go-api workload token only).
func plantUsersAdminRows(t *testing.T) {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(authv1.AuthServiceEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	rows := document["rows"].([]any)
	var base map[string]any
	for _, item := range rows {
		if row := item.(map[string]any); row["route"] == authapi.PatternOrgDeprovision {
			base = row
		}
	}
	if base == nil {
		t.Fatal("no deprovision row in the embedded document")
	}
	for _, pattern := range usersAdminRoutePatterns {
		clone := map[string]any{}
		for k, v := range base {
			clone[k] = v
		}
		clone["route"], clone["id"] = pattern, "POST "+pattern+" [dev-health-auth-service @api]"
		rows = append(rows, clone)
	}
	document["rows"] = rows
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	provisioningProfileRows = raw
	t.Cleanup(func() { provisioningProfileRows = nil })
}

// apiAnswers configures auth-service from env and answers an empty POST to each path on the API listener.
func apiAnswers(t *testing.T, env map[string]string, paths []string) map[string]int {
	t.Helper()
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	components, err := configure(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	t.Cleanup(func() {
		for _, component := range components {
			_ = component.Shutdown(context.Background())
		}
	})
	var api http.Handler
	for _, component := range components {
		if announced, ok := component.(announcedListener); ok {
			if server, ok := announced.listener.(*httpapi.Server); ok {
				api = server.Handler()
			}
		}
	}
	if api == nil {
		t.Fatal("configure built no API listener")
	}
	codes := map[string]int{}
	for _, path := range paths {
		recorder := httptest.NewRecorder()
		api.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}")))
		codes[path] = recorder.Code
	}
	return codes
}

func usersAdminEnv(owned bool) map[string]string {
	env := map[string]string{
		authconfig.EnvDatabaseURI:        "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile:     "/nonexistent",
		authconfig.EnvSigningKeyID:       "users-admin-test",
		authconfig.EnvRateLimitPerSecond: "100000",
		authconfig.EnvRateLimitBurst:     "100000",
		authconfig.EnvAPIAddress:         "127.0.0.1:0",
		authconfig.EnvOperatorAddress:    "127.0.0.1:1",
	}
	if owned {
		env[authconfig.EnvUsersOwned] = "true"
	}
	// The session and sign-in routes on (as on prod), AUTH_MEMBERSHIP_ROUTES absent (as on prod).
	return ssoEnvironment(compatEnvironment(env))
}

// The split the users routes ride on: with AUTH_MEMBERSHIP_ROUTES absent (prod) and users owned, the four user-admin
// routes mount with their rows and no membership, invite, organization, service account or grant route mounts. With
// users not owned, or without the rows, none of the four mounts.
func TestUsersAdminRoutesMountOnUsersOwnedApartFromTheProvisioningSwitch(t *testing.T) {
	provisioning := []string{authapi.PatternMembershipAdd, authapi.PatternMembershipRole, authapi.PatternMembershipRemove,
		authapi.PatternMembershipTransfer, authapi.PatternInviteCreate, authapi.PatternInviteAccept, authapi.PatternInviteRevoke,
		authapi.PatternOrgDeprovision, authapi.PatternOrgReactivate, authapi.PatternOrgSuspend, authapi.PatternOrgState,
		authapi.PatternOrgOnboard, authapi.PatternOrgCreate, authapi.PatternOrgRename, authapi.PatternServiceAccountCreate,
		authapi.PatternServiceAccountDisable, authapi.PatternServiceAccountEnable, authapi.PatternServiceAccountOwner,
		authapi.PatternServiceAccountKeyIssue, authapi.PatternServiceAccountKeyRotate, authapi.PatternServiceAccountKeyRevoke,
		authapi.PatternServiceAccountKeyList, authapi.PatternGrantCreate, authapi.PatternGrantUpdate, authapi.PatternGrantRevoke}
	plantUsersAdminRows(t)
	split := apiAnswers(t, usersAdminEnv(true), append(append([]string{}, usersAdminRoutePatterns...), provisioning...))
	for _, pattern := range usersAdminRoutePatterns {
		if split[pattern] == http.StatusNotFound {
			t.Errorf("owned, rows, no AUTH_MEMBERSHIP_ROUTES: %s is not mounted", pattern)
		}
	}
	for _, pattern := range provisioning {
		if split[pattern] != http.StatusNotFound {
			t.Errorf("owned, rows, no AUTH_MEMBERSHIP_ROUTES: %s answered %d, want 404 (not mounted)", pattern, split[pattern])
		}
	}
	for _, pattern := range usersAdminRoutePatterns {
		if got := apiAnswers(t, usersAdminEnv(false), []string{pattern})[pattern]; got != http.StatusNotFound {
			t.Errorf("users not owned, rows: %s answered %d, want 404 (not mounted)", pattern, got)
		}
	}
	plantEmbeddedWithout(t, authapi.PatternUsersDelete)
	for _, pattern := range usersAdminRoutePatterns {
		if got := apiAnswers(t, usersAdminEnv(true), []string{pattern})[pattern]; got != http.StatusNotFound {
			t.Errorf("owned, embedded document without the users/delete row: %s answered %d, want 404 (none mounts)", pattern, got)
		}
	}
}

// plantEmbeddedWithout plants the embedded endpoint-profile document without the rows for routes.
func plantEmbeddedWithout(t *testing.T, routes ...string) {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(authv1.AuthServiceEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	var kept []any
	for _, item := range document["rows"].([]any) {
		if !slices.Contains(routes, item.(map[string]any)["route"].(string)) {
			kept = append(kept, item)
		}
	}
	document["rows"] = kept
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	provisioningProfileRows = raw
	t.Cleanup(func() { provisioningProfileRows = nil })
}

// The embedded document holds the four rows: with users owned (prod) every user-admin route is mounted, and a POST
// with no bearer is the credential gate's 401.
func TestUsersAdminRoutesMountWithTheEmbeddedRows(t *testing.T) {
	provisioningProfileRows = nil
	answers := apiAnswers(t, usersAdminEnv(true), usersAdminRoutePatterns)
	for _, pattern := range usersAdminRoutePatterns {
		if answers[pattern] != http.StatusUnauthorized {
			t.Errorf("owned, embedded document: %s answered %d, want 401 (mounted, no bearer)", pattern, answers[pattern])
		}
	}
}

// startLog configures auth-service from env and returns its start log (JSON lines).
func startLog(t *testing.T, env map[string]string) string {
	t.Helper()
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	var logs bytes.Buffer
	components, err := configure(context.Background(), cfg, slog.New(slog.NewJSONHandler(&logs, nil)))
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	for _, component := range components {
		_ = component.Shutdown(context.Background())
	}
	return logs.String()
}

// usersRoutesLine is the one INFO start line of the user-admin routes, decoded; it fails unless there is exactly one.
func usersRoutesLine(t *testing.T, logs string) map[string]any {
	t.Helper()
	var found []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil && record["msg"] == "users routes" {
			found = append(found, record)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%d \"users routes\" start lines, want 1:\n%s", len(found), logs)
	}
	if found[0]["level"] != "INFO" {
		t.Fatalf("the users routes line is level %v, want INFO", found[0]["level"])
	}
	return found[0]
}

// The start log says how many user-admin routes are mounted and, when none is, why: users not owned, or the route
// whose endpoint-profile row is missing. The gate is all or nothing, so the count is 4/4 or 0/4. There is no start
// refusal (product rule 3).
func TestUsersAdminStartLineNamesTheMountCountAndTheReason(t *testing.T) {
	provisioningProfileRows = nil
	mounted := usersRoutesLine(t, startLog(t, usersAdminEnv(true)))
	if mounted["mounted"] != "4/4" || mounted["reason"] != nil {
		t.Errorf("owned, embedded document: %v, want mounted 4/4 and no reason", mounted)
	}
	off := usersRoutesLine(t, startLog(t, usersAdminEnv(false)))
	if off["mounted"] != "0/4" || off["reason"] != "users_not_owned" {
		t.Errorf("not owned: %v, want mounted 0/4 reason users_not_owned", off)
	}
	plantEmbeddedWithout(t, authapi.PatternUsersDelete)
	missing := usersRoutesLine(t, startLog(t, usersAdminEnv(true)))
	if missing["mounted"] != "0/4" || missing["reason"] != authapi.ProvisioningRowReason || missing["route"] != "POST "+authapi.PatternUsersDelete {
		t.Errorf("owned, no users/delete row: %v, want mounted 0/4 reason %s route POST %s", missing, authapi.ProvisioningRowReason, authapi.PatternUsersDelete)
	}
}

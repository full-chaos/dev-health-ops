package authruntime

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
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
	provisioningProfileRows = nil
	for _, pattern := range usersAdminRoutePatterns {
		if got := apiAnswers(t, usersAdminEnv(true), []string{pattern})[pattern]; got != http.StatusNotFound {
			t.Errorf("owned, embedded document without the rows: %s answered %d, want 404 (not mounted)", pattern, got)
		}
	}
}

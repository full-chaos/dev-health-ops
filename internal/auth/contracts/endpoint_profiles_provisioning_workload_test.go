package contracts

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// provisioningRoutes are the internal provisioning routes go-api calls.
var provisioningRoutes = []string{
	"/internal/v1/memberships/add", "/internal/v1/memberships/role", "/internal/v1/memberships/remove",
	"/internal/v1/memberships/transfer", "/internal/v1/invites/create", "/internal/v1/invites/accept",
	"/internal/v1/organizations/deprovision", "/internal/v1/organizations/reactivate",
}

func provisioningRows(t *testing.T) map[string]map[string]any {
	t.Helper()
	rows := map[string]map[string]any{}
	for _, item := range loadAuthServiceDocument(t)["rows"].([]any) {
		row := item.(map[string]any)
		if row["service"] == "dev-health-auth-service" && row["method"] == "POST" && slices.Contains(provisioningRoutes, row["route"].(string)) {
			rows[row["route"].(string)] = row
		}
	}
	if len(rows) != len(provisioningRoutes) {
		t.Fatalf("%d provisioning rows, want %d", len(rows), len(provisioningRoutes))
	}
	return rows
}

// Every provisioning route accepts the go-api workload token only: the session
// issuer credential's memberships:write scope is retired. A one-class row
// declares no class_disambiguator, and the row's validator text names the
// workload path and not CheckScope.
func TestTheProvisioningRowsAcceptOnlyTheWorkloadToken(t *testing.T) {
	registry := realRegistry(t)
	for route, row := range provisioningRows(t) {
		var classes []string
		for _, class := range row["accepted_credential_classes"].([]any) {
			classes = append(classes, class.(string))
		}
		if !slices.Equal(classes, []string{"workload_token"}) {
			t.Fatalf("%s: accepted classes %v, want [workload_token]", route, classes)
		}
		if _, has := row["class_disambiguator"]; has {
			t.Fatalf("%s: a one-class row declares a class_disambiguator", route)
		}
		profile := map[string]any{"accepted_credential_classes": row["accepted_credential_classes"]}
		if err := CheckClassSelector(profile, registry); err != nil {
			t.Fatalf("%s: the selector gate refuses the row: %v", route, err)
		}
		description, _ := row["primary_validator"].(map[string]any)["description"].(string)
		if !strings.Contains(description, "SessionIssueWorkload.authorize") || strings.Contains(description, "CheckScope") {
			t.Fatalf("%s: the validator description does not name only the workload path: %s", route, description)
		}
		anchor, _ := row["primary_validator"].(map[string]any)["anchor"].(map[string]any)
		if anchor["path"] != "internal/auth/authapi/issue_workload.go" {
			t.Fatalf("%s: primary validator anchor %v, want issue_workload.go", route, anchor)
		}
	}
}

// credential-classes.json names every provisioning route as a workload_token
// consumer at the row's source line, and no longer as a consumer of the
// session issuer credential, which holds sessions:issue only.
func TestTheRegistryNamesTheProvisioningRoutesAsWorkloadConsumersOnly(t *testing.T) {
	document := loadClassRegistryDocument(t)
	registry := indexRegistry(document)
	for route, row := range provisioningRows(t) {
		source := row["source"].(map[string]any)
		key := anchorKey(source["file"].(string), int(source["line"].(float64)))
		if !registry.consumers["workload_token"][key] {
			t.Errorf("%s: workload_token has no consumer anchored at %s", route, key)
		}
		if registry.consumers["internal_svc_authsess_token"][key] {
			t.Errorf("%s: internal_svc_authsess_token still has a consumer anchored at %s", route, key)
		}
	}
	for _, class := range document.Classes {
		switch class.ClassID {
		case "workload_token":
			var named int
			for _, consumer := range class.Consumers {
				if strings.Contains(consumer.Description, "/internal/v1/") && strings.Contains(consumer.Description, "AUTH_MEMBERSHIP_ROUTES") &&
					strings.Contains(consumer.Description, "AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT") {
					named++
				}
			}
			if named != len(provisioningRoutes) {
				t.Errorf("%d workload_token provisioning consumers name both variables, want %d", named, len(provisioningRoutes))
			}
		case "internal_svc_authsess_token":
			for _, consumer := range class.Consumers {
				if strings.Contains(consumer.Description, "memberships:write") || strings.Contains(consumer.Description, "/internal/v1/memberships") ||
					strings.Contains(consumer.Description, "/internal/v1/invites") || strings.Contains(consumer.Description, "/internal/v1/organizations") {
					t.Errorf("internal_svc_authsess_token still names a provisioning consumer: %s", consumer.Description)
				}
			}
		}
	}
	raw := loadRawClassRegistry(t)
	if strings.Contains(raw, "memberships:write") || strings.Contains(raw, "servicecred/scope.go") {
		t.Error("credential-classes.json still names memberships:write or servicecred/scope.go")
	}
}

func loadRawClassRegistry(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(ContractsDir(testRoot(t)), "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

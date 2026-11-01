package contracts

import (
	"slices"
	"strings"
	"testing"
)

// provisioningRoutes are the internal provisioning routes go-api calls.
var provisioningRoutes = []string{
	"/internal/v1/memberships/add", "/internal/v1/memberships/role", "/internal/v1/memberships/remove",
	"/internal/v1/memberships/transfer", "/internal/v1/invites/create", "/internal/v1/invites/accept",
	"/internal/v1/organizations/deprovision", "/internal/v1/organizations/reactivate", "/internal/v1/organizations/state",
	"/internal/v1/organizations/onboard", "/internal/v1/organizations/create",
	"/internal/v1/service-accounts/create", "/internal/v1/service-accounts/disable", "/internal/v1/service-accounts/enable",
	"/internal/v1/service-accounts/owner",
	"/internal/v1/grants/create", "/internal/v1/grants/update", "/internal/v1/grants/revoke",
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

// Every provisioning route accepts the go-api workload token beside the
// session issuer credential (which stays until it is retired), chosen by the
// JOSE typ of its header, and the selector gate accepts the row.
func TestTheProvisioningRowsAcceptTheWorkloadTokenAndKeepTheServiceCredential(t *testing.T) {
	registry := realRegistry(t)
	for route, row := range provisioningRows(t) {
		var classes []string
		for _, class := range row["accepted_credential_classes"].([]any) {
			classes = append(classes, class.(string))
		}
		slices.Sort(classes)
		if !slices.Equal(classes, []string{"internal_svc_authsess_token", "workload_token"}) {
			t.Fatalf("%s: accepted classes %v", route, classes)
		}
		selector, _ := row["class_disambiguator"].(map[string]any)
		if selector["kind"] != "jwt_header_typ" || selector["name"] != "workload+jwt" {
			t.Fatalf("%s: class_disambiguator %v, want jwt_header_typ workload+jwt", route, selector)
		}
		profile := map[string]any{"accepted_credential_classes": row["accepted_credential_classes"], "class_disambiguator": row["class_disambiguator"]}
		if err := CheckClassSelector(profile, registry); err != nil {
			t.Fatalf("%s: the selector gate refuses the row: %v", route, err)
		}
		description, _ := row["primary_validator"].(map[string]any)["description"].(string)
		if !strings.Contains(description, "SessionIssueWorkload.authorize") {
			t.Fatalf("%s: the validator description does not name the workload path", route)
		}
	}
}

// credential-classes.json names every provisioning route as a consumer of both
// classes at the row's source line, and the workload consumer names the
// subject variable that turns it on.
func TestTheRegistryNamesTheProvisioningRoutesAsConsumersOfBothClasses(t *testing.T) {
	document := loadClassRegistryDocument(t)
	registry := indexRegistry(document)
	for route, row := range provisioningRows(t) {
		source := row["source"].(map[string]any)
		key := anchorKey(source["file"].(string), int(source["line"].(float64)))
		for _, class := range []string{"workload_token", "internal_svc_authsess_token"} {
			if !registry.consumers[class][key] {
				t.Errorf("%s: %s has no consumer anchored at %s", route, class, key)
			}
		}
	}
	for _, class := range document.Classes {
		if class.ClassID != "workload_token" {
			continue
		}
		var named int
		for _, consumer := range class.Consumers {
			if strings.Contains(consumer.Description, "/internal/v1/") && strings.Contains(consumer.Description, "AUTH_MEMBERSHIP_ROUTES") &&
				strings.Contains(consumer.Description, "AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT") {
				named++
			}
		}
		if named != len(provisioningRoutes) {
			t.Fatalf("%d workload_token provisioning consumers name both variables, want %d", named, len(provisioningRoutes))
		}
	}
}

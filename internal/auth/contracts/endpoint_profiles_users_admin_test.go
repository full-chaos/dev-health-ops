package contracts

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/routescan"
)

// usersAdminRouteTwins maps each internal user-admin route to the legacy go-api row whose write it serves: the route's
// tenant class is copied from that row, never chosen.
var usersAdminRouteTwins = map[string]string{
	"/internal/v1/users/create":   "POST /api/v1/admin/users [dev-health-ops-api @api]",
	"/internal/v1/users/update":   "PATCH /api/v1/admin/users/{user_id} [dev-health-ops-api @api]",
	"/internal/v1/users/password": "POST /api/v1/admin/users/{user_id}/password [dev-health-ops-api @api]",
	"/internal/v1/users/delete":   "DELETE /api/v1/admin/users/{user_id} [dev-health-ops-api @api]",
}

func usersAdminRows(t *testing.T) map[string]map[string]any {
	t.Helper()
	rows := map[string]map[string]any{}
	for _, item := range loadAuthServiceDocument(t)["rows"].([]any) {
		row := item.(map[string]any)
		route, _ := row["route"].(string)
		if _, ok := usersAdminRouteTwins[route]; ok && row["service"] == "dev-health-auth-service" && row["method"] == "POST" {
			if _, dup := rows[route]; dup {
				t.Fatalf("%s: two rows", route)
			}
			rows[route] = row
		}
	}
	if len(rows) != len(usersAdminRouteTwins) {
		t.Fatalf("%d user-admin rows, want %d", len(rows), len(usersAdminRouteTwins))
	}
	return rows
}

// Each user-admin row: protected, the go-api workload token only (no disambiguator), the provisioning workload path as
// its validator, its legacy go-api row's tenant class, and a source that names its route by symbol (routescan.RowSymbol).
func TestTheUsersAdminRowsAcceptOnlyTheWorkloadToken(t *testing.T) {
	registry := realRegistry(t)
	legacy := map[string]map[string]any{}
	for _, item := range loadGoAPIDocument(t)["rows"].([]any) {
		row := item.(map[string]any)
		legacy[row["id"].(string)] = row
	}
	root := testRoot(t)
	for route, row := range usersAdminRows(t) {
		if row["classification"] != "protected" {
			t.Errorf("%s: classification %v, want protected", route, row["classification"])
		}
		var classes []string
		for _, class := range row["accepted_credential_classes"].([]any) {
			classes = append(classes, class.(string))
		}
		if !slices.Equal(classes, []string{"workload_token"}) {
			t.Errorf("%s: accepted classes %v, want [workload_token]", route, classes)
		}
		if _, has := row["class_disambiguator"]; has {
			t.Errorf("%s: a one-class row declares a class_disambiguator", route)
		}
		if err := CheckClassSelector(map[string]any{"accepted_credential_classes": row["accepted_credential_classes"]}, registry); err != nil {
			t.Errorf("%s: the selector gate refuses the row: %v", route, err)
		}
		validator, _ := row["primary_validator"].(map[string]any)
		description, _ := validator["description"].(string)
		if !strings.Contains(description, "SessionIssueWorkload.authorize") || strings.Contains(description, "CheckScope") {
			t.Errorf("%s: the validator description does not name only the workload path: %s", route, description)
		}
		if anchor, _ := validator["anchor"].(map[string]any); anchor["path"] != "internal/auth/authapi/issue_workload.go" {
			t.Errorf("%s: primary validator anchor %v, want issue_workload.go", route, anchor)
		}
		twin, ok := legacy[usersAdminRouteTwins[route]]
		if !ok {
			t.Fatalf("%s: no legacy go-api row %s", route, usersAdminRouteTwins[route])
		}
		if row["tenant_requirement"] != twin["tenant_requirement"] {
			t.Errorf("%s: tenant %v, the legacy row %s has %v", route, row["tenant_requirement"], usersAdminRouteTwins[route], twin["tenant_requirement"])
		}
		file := row["source"].(map[string]any)["file"].(string)
		raw, err := os.ReadFile(filepath.Join(root, file))
		if err != nil {
			t.Fatal(err)
		}
		regs, err := routescan.ScanWithMux(filepath.Join(root, filepath.Dir(file)))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := routescan.RowSymbol(regs, raw, filepath.ToSlash(filepath.Join(root, file)), "POST", route, ""); err != nil {
			t.Errorf("%s: %s", route, strings.ReplaceAll(err.Error(), filepath.ToSlash(root)+"/", ""))
		}
	}
}

// go-api is the only consumer of the user-admin rows: credential-classes.json names, at each row's source line, one
// workload_token consumer for go-api's admin users write on AUTH_USERS_OWNED (not AUTH_MEMBERSHIP_ROUTES), and no
// consumer of any other class.
func TestTheRegistryNamesGoAPIAsTheOnlyConsumerOfTheUsersAdminRows(t *testing.T) {
	document := loadClassRegistryDocument(t)
	for route, row := range usersAdminRows(t) {
		source := row["source"].(map[string]any)
		key := anchorKey(source["file"].(string), int(source["line"].(float64)))
		var named []string
		for _, class := range document.Classes {
			for _, consumer := range class.Consumers {
				if consumer.Anchor == nil || consumer.Anchor.Repo != "ops" || anchorKey(consumer.Anchor.Path, consumer.Anchor.Line) != key {
					continue
				}
				named = append(named, class.ClassID)
				description := consumer.Description
				if class.ClassID != "workload_token" {
					continue
				}
				if !strings.Contains(description, "auth-service POST "+route+" ") || !strings.Contains(description, "go-api's admin users") ||
					!strings.Contains(description, "AUTH_USERS_OWNED") || !strings.Contains(description, "AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT") ||
					strings.Contains(description, "AUTH_MEMBERSHIP_ROUTES") {
					t.Errorf("%s: the workload_token consumer does not name go-api's call on AUTH_USERS_OWNED alone: %s", route, description)
				}
			}
		}
		if !slices.Equal(named, []string{"workload_token"}) {
			t.Errorf("%s: consumers at %s are %v, want exactly one workload_token consumer", route, key, named)
		}
	}
}

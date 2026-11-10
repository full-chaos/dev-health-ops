package contracts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// provisioningRoutes are the internal provisioning routes go-api calls.
var provisioningRoutes = []string{
	"/internal/v1/memberships/add", "/internal/v1/memberships/role", "/internal/v1/memberships/remove",
	"/internal/v1/memberships/transfer", "/internal/v1/invites/create", "/internal/v1/invites/accept", "/internal/v1/invites/revoke",
	"/internal/v1/organizations/deprovision", "/internal/v1/organizations/reactivate", "/internal/v1/organizations/state",
	"/internal/v1/organizations/suspend", "/internal/v1/organizations/rename",
	"/internal/v1/organizations/onboard", "/internal/v1/organizations/create",
	"/internal/v1/service-accounts/create", "/internal/v1/service-accounts/disable", "/internal/v1/service-accounts/enable",
	"/internal/v1/service-accounts/owner",
	"/internal/v1/service-accounts/keys/issue", "/internal/v1/service-accounts/keys/rotate",
	"/internal/v1/service-accounts/keys/revoke", "/internal/v1/service-accounts/keys/list",
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

// Every provisioning route accepts the go-api workload token only: the session issuer credential's memberships:write
// scope is retired. A one-class row declares no class_disambiguator, and the row's validator text names the workload
// path and not CheckScope.
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

// credential-classes.json names every provisioning route as a workload_token consumer at the row's source line, and no
// longer as a consumer of the session issuer credential, which holds sessions:issue only.
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
			// The eight read routes mount with the provisioning routes, on the same two switches.
			if want := len(provisioningRoutes) + len(ReadOperations()); named != want {
				t.Errorf("%d workload_token provisioning consumers name both variables, want %d", named, want)
			}
		case "internal_svc_authsess_token":
			for _, consumer := range class.Consumers {
				if strings.Contains(consumer.Description, "memberships:write") || strings.Contains(consumer.Description, ":read scope") ||
					strings.Contains(consumer.Description, "/internal/v1/principals") || strings.Contains(consumer.Description, "/internal/v1/memberships") ||
					strings.Contains(consumer.Description, "/internal/v1/invites") || strings.Contains(consumer.Description, "/internal/v1/organizations") ||
					strings.Contains(consumer.Description, "/internal/v1/service-accounts") || strings.Contains(consumer.Description, "/internal/v1/grants") {
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
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "contracts", "auth", "v1", "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// A key route issues a value only on issue and rotate, and those two rows say the value authenticates nowhere yet.
func TestTheKeyRowsNameTheIssuedKeyAndItsUse(t *testing.T) {
	const gap = "a key this route issues authenticates nowhere yet"
	rows := provisioningRows(t)
	for route, issues := range map[string]bool{
		"/internal/v1/service-accounts/keys/issue": true, "/internal/v1/service-accounts/keys/rotate": true,
		"/internal/v1/service-accounts/keys/revoke": false, "/internal/v1/service-accounts/keys/list": false,
	} {
		issued, _ := rows[route]["issued_credential"].([]any)
		gaps, _ := rows[route]["gaps"].([]any)
		if issues != (len(issued) == 1) || issues != slices.Contains(gaps, any(gap)) {
			t.Errorf("%s: issued %v, gaps %v; want the issued key and the gap line: %v", route, issued, gaps, issues)
		}
		if issues && issued[0].(map[string]any)["class_id"] != "service_account_key" {
			t.Errorf("%s issues %v", route, issued[0])
		}
	}
}

// Issuing is live and use is not: the class stays unconsumed, with no consumer and no validator, until the use of a key lands.
func TestTheKeyClassStaysUnconsumedUntilItsUseLands(t *testing.T) {
	var document struct {
		Classes []struct {
			ClassID    string `json:"class_id"`
			Status     string `json:"status"`
			Consumers  []any  `json:"consumers"`
			Validators []any  `json:"validators"`
		} `json:"classes"`
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "contracts", "auth", "v1", "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	for _, class := range document.Classes {
		if class.ClassID != "service_account_key" {
			continue
		}
		if class.Status != "unconsumed" || len(class.Consumers) != 0 || len(class.Validators) != 0 {
			t.Fatalf("service_account_key is %s with %d consumers and %d validators, want unconsumed with none", class.Status, len(class.Consumers), len(class.Validators))
		}
		return
	}
	t.Fatal("no service_account_key class")
}

// A provisioning consumer of credential-classes.json is anchored at a line that names the route its description names: where
// the anchored line holds a quoted /internal/v1/ route, it is that route. A consumer moved onto the line of a sibling route
// (which has its own pinned text) is caught here and nowhere else.
func TestAProvisioningConsumerAnchorLineNamesTheRouteItDescribes(t *testing.T) {
	problems, checked := provisioningConsumerProblems(t, loadClassRegistryDocument(t), testRoot(t))
	for _, problem := range problems {
		t.Error(problem)
	}
	for _, class := range []string{"workload_token"} {
		if checked[class] < 7 {
			t.Errorf("%s: %d provisioning consumers anchored at a quoted route line, want at least 7 (the grant and service account routes)", class, checked[class])
		}
	}
}

// provisioningConsumerProblems checks each provisioning consumer anchor whose PINNED text quotes a route: the text
// TestCredentialClassAnchorsPointAtTheirPinnedLineText proves is in its symbol, so a moved line is never read or
// skipped here. The file must exist under root; checked counts the anchors measured per class.
func provisioningConsumerProblems(t *testing.T, document classRegistryDocument, root string) (problems []string, checked map[string]int) {
	t.Helper()
	pinnedText := map[string]string{}
	for _, pin := range loadAnchorPins(t) {
		pinnedText[anchorKey(pin.Path, pin.Line)] = pin.Text
	}
	described := regexp.MustCompile(`auth-service POST (/internal/v1/[a-z0-9/_-]+)`)
	quoted := regexp.MustCompile(`"(/internal/v1/[a-z0-9/_-]+)"`)
	checked = map[string]int{}
	for _, class := range document.Classes {
		for _, consumer := range class.Consumers {
			m := described.FindStringSubmatch(consumer.Description)
			if m == nil || consumer.Anchor == nil || consumer.Anchor.Repo != "ops" {
				continue
			}
			if _, err := os.Stat(filepath.Join(root, consumer.Anchor.Path)); err != nil {
				problems = append(problems, fmt.Sprintf("%s %s: anchor file %s: %v", class.ClassID, m[1], consumer.Anchor.Path, err))
				continue
			}
			text, pinned := pinnedText[anchorKey(consumer.Anchor.Path, consumer.Anchor.Line)]
			if !pinned {
				problems = append(problems, fmt.Sprintf("%s %s: anchor %s:%d has no pin in credentialanchors/credential-anchor-lines.json", class.ClassID, m[1], consumer.Anchor.Path, consumer.Anchor.Line))
				continue
			}
			if q := quoted.FindStringSubmatch(text); q != nil {
				if q[1] != m[1] {
					problems = append(problems, fmt.Sprintf("%s consumer of %s is anchored at %s:%d, a line that names %s", class.ClassID, m[1], consumer.Anchor.Path, consumer.Anchor.Line, q[1]))
				}
				checked[class.ClassID]++
			}
		}
	}
	return problems, checked
}

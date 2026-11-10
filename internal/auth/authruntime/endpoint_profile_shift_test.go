package authruntime

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/routescan"
)

// authRowFiles are the files the auth-service rows cite: each row's source and its anchors.
func authRowFiles(rows []map[string]any) []string {
	var files []string
	var walk func(node any)
	walk = func(node any) {
		switch value := node.(type) {
		case map[string]any:
			if path, ok := value["path"].(string); ok {
				files = append(files, path)
			}
			for _, child := range value {
				walk(child)
			}
		case []any:
			for _, child := range value {
				walk(child)
			}
		}
	}
	for _, row := range rows {
		files = append(files, row["source"].(map[string]any)["file"].(string))
		walk(row["primary_validator"])
		walk(row["issued_credential"])
	}
	return files
}

// Lines inserted at the top of the provisioning files move every line in them and change no code. The auth-service
// row check must hold on such a tree: a row is tied to its registration (or its pattern constant) by symbol.
func TestAuthRowAnchorsSurviveALineShift(t *testing.T) {
	rows := loadAuthServiceRows(t)
	root := routescan.ShiftedTree(t, "../../..", []string{"internal/auth/authapi"}, authRowFiles(rows), map[string]string{
		"internal/auth/authapi/provisioning.go":                "package ",
		"internal/auth/authapi/provisioning_serviceaccount.go": "package ",
		"internal/auth/authapi/provisioning_read.go":           "package ",
		"internal/auth/authapi/provisioning_users.go":          "package ",
	})
	problems, checked, _ := authRowAnchorProblems(t, root, rows)
	if len(problems) != 0 {
		t.Errorf("a line shift broke %d auth-service anchors:\n%s", len(problems), strings.Join(problems, "\n"))
	}
	if checked < 150 {
		t.Fatalf("only %d rows and anchors were checked on the shifted tree: the measurement did not happen", checked)
	}
}

// A row whose route its source does not name, or whose registration is written twice, fails; the line it cites never
// decides.
func TestAuthRowFailsWhenItsSourceDoesNotResolveToOnePlace(t *testing.T) {
	var base map[string]any
	for _, row := range loadAuthServiceRows(t) {
		if row["route"] == "/internal/v1/memberships/add" {
			base = row
		}
	}
	if base == nil {
		t.Fatal("the memberships add row is missing")
	}
	gone := map[string]any{}
	for key, value := range base {
		gone[key] = value
	}
	gone["route"] = "/internal/v1/memberships/gone"
	problems, _, _ := authRowAnchorProblems(t, "../../..", []map[string]any{gone})
	if len(problems) == 0 || !strings.Contains(problems[0], "source anchor stale: internal/auth/authapi/provisioning.go holds no registration or constant of POST /internal/v1/memberships/gone") {
		t.Errorf("a route its source does not name: %v", problems)
	}
	root := routescan.ShiftedTree(t, "../../..", []string{"internal/auth/authapi"}, authRowFiles([]map[string]any{base}), nil)
	path := filepath.Join(root, "internal/auth/authapi/provisioning.go")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	const entry = "{Method: http.MethodPost, Pattern: PatternMembershipAdd, Handler: http.HandlerFunc(h.membershipAdd)},"
	if !strings.Contains(string(raw), entry) {
		t.Fatal("the memberships add registration is missing")
	}
	if err := os.WriteFile(path, []byte(strings.Replace(string(raw), entry, entry+"\n\t\t"+entry, 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	problems, _, _ = authRowAnchorProblems(t, root, []map[string]any{base})
	if len(problems) == 0 || !strings.Contains(problems[0], "source anchor ambiguous: internal/auth/authapi/provisioning.go holds 2 registrations of POST /internal/v1/memberships/add") {
		t.Errorf("a registration written twice: %v", problems)
	}
}

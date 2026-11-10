package authruntime

import (
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
	problems, checked := authRowAnchorProblems(t, root, rows)
	if len(problems) != 0 {
		t.Errorf("a line shift broke %d auth-service anchors:\n%s", len(problems), strings.Join(problems, "\n"))
	}
	if checked < 150 {
		t.Fatalf("only %d rows and anchors were checked on the shifted tree: the measurement did not happen", checked)
	}
}

package copyproject

import (
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// The session store imports this package, and identity imports session in its
// tests through membership: it must reach neither identity nor session.
func TestCopyprojectImportsNoAuthDomainPackage(t *testing.T) {
	parsed, err := parser.ParseFile(token.NewFileSet(), "copyproject.go", nil, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range parsed.Imports {
		path := strings.Trim(spec.Path.Value, `"`)
		for _, banned := range []string{"/auth/identity", "/auth/session", "/auth/membership"} {
			if strings.Contains(path, banned) {
				t.Errorf("imports %s", path)
			}
		}
	}
}

func TestInsertSQLNamesTheSchemaOnce(t *testing.T) {
	for name, sql := range map[string]string{
		"principal": InsertPrincipalSQL("auth"), "user": InsertUserSQL("auth"), "organization": InsertOrganizationSQL("auth"),
	} {
		if !strings.Contains(sql, "INSERT INTO auth.") {
			t.Errorf("%s insert: %s", name, sql)
		}
	}
}

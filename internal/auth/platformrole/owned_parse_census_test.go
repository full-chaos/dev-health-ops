package platformrole

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// ownedParseCallers are the files that read AUTH_PLATFORM_ROLES_OWNED; each
// reads it through OwnedFromLookup or the leaf parse it is (rule.OwnedFromLookup).
var ownedParseCallers = []string{
	"cmd/auth-migrate/main.go",
	"cmd/auth-migrate/restorefence.go",
	"internal/adminops/users.go",
	"internal/apiservice/service.go",
	"internal/auth/authconfig/config.go",
	"internal/auth/platformrole/platformrole.go",
	"internal/auth/usersbackfill/command.go",
	"internal/auth/usersbackfill/platformrole_command.go",
	"internal/mintcli/edgetoken/main.go",
	"internal/queryapi/server/query_route.go",
	"internal/rivermigrate/rivermigrate.go",
}

// ownedNameDeclarations spell the switch's name: the constant, the option
// registries that list it for --help, and query-api's settings constant (its
// settings census resolves only string constants of the server package; a
// test there holds it equal to rule.OwnedEnv).
var ownedNameDeclarations = []string{
	"internal/auth/platformrole/rule/rule.go",
	"internal/platform/config/queryapi_options.go",
	"internal/queryapi/server/query_route.go",
}

// The switch has one parse (rule.OwnedFromLookup, which OwnedFromLookup is):
// every process that reads it calls one of the two, and no other file spells
// its name.
func TestPlatformRolesOwnedIsParsedByOneFunction(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	callers, named := map[string]bool{}, map[string]bool{}
	call := regexp.MustCompile(`\b(platformrole|rule)\.OwnedFromLookup\(`)
	for _, dir := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			body, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			rel := filepath.ToSlash(strings.TrimPrefix(path, root+string(filepath.Separator)))
			if call.Match(body) {
				callers[rel] = true
			}
			if strings.Contains(string(body), `"`+OwnedEnv+`"`) {
				named[rel] = true
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := keys(callers); strings.Join(got, "\n") != strings.Join(ownedParseCallers, "\n") {
		t.Errorf("OwnedFromLookup callers:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(ownedParseCallers, "\n"))
	}
	if got := keys(named); strings.Join(got, "\n") != strings.Join(ownedNameDeclarations, "\n") {
		t.Errorf("files that spell %s:\n%s\nwant:\n%s", OwnedEnv, strings.Join(got, "\n"), strings.Join(ownedNameDeclarations, "\n"))
	}
}

func keys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

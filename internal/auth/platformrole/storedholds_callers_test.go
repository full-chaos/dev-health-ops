package platformrole

import (
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The stored holding is no authority read, so only go-api's admin users view and its superuser TARGET protections read
// it: rule.StoredHoldsSQL only through policy.PlatformRoleRead.StoredSuperuser, that only from the admin store's
// userColumns (the five view reads) and the three target reads (impersonation target, guarded-write targets, transfer
// target). A new caller could decide by it as if it were the authority rule.
func TestStoredHoldingIsReadOnlyByTheAdminUsersView(t *testing.T) {
	sites := func(call *regexp.Regexp) []string {
		t.Helper()
		var out []string
		err := filepath.WalkDir(filepath.Join("..", ".."), func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			src := string(raw)
			for _, at := range call.FindAllStringIndex(src, -1) {
				if strings.HasSuffix(src[:at[0]], "func ") {
					continue
				}
				fn := "(top level)"
				if start := strings.LastIndex(src[:at[0]], "\nfunc "); start >= 0 {
					line := src[start+1:]
					fn = strings.TrimPrefix(line[:strings.IndexByte(line, '(')], "func ")
					if strings.HasPrefix(line, "func (") {
						rest := line[strings.IndexByte(line, ')')+2:]
						fn = rest[:strings.IndexByte(rest, '(')]
					}
				}
				out = append(out, filepath.ToSlash(path)+" "+fn)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(out)
		return out
	}
	for name, c := range map[string]struct {
		call *regexp.Regexp
		want []string
	}{
		"StoredHoldsSQL": {regexp.MustCompile(`\bStoredHoldsSQL\(`), []string{"../../api/policy/store.go StoredSuperuser"}},
		"StoredSuperuser": {regexp.MustCompile(`\.StoredSuperuser\(`), []string{"../../apiservice/admin/guards.go targetsSuperuserSQL",
			"../../apiservice/admin/orgs_provision.go isSuperuserSQL", "../../apiservice/admin/store.go targetUserSQL",
			"../../apiservice/admin/userstore.go userColumns"}},
		"targetUserSQL":       {regexp.MustCompile(`\.targetUserSQL\(`), []string{"../../apiservice/admin/store.go userByID"}},
		"targetsSuperuserSQL": {regexp.MustCompile(`\.targetsSuperuserSQL\(`), []string{"../../apiservice/admin/guards.go refuse"}},
		"isSuperuserSQL":      {regexp.MustCompile(`\.isSuperuserSQL\(`), []string{"../../apiservice/admin/orgs_provision.go isSuperuser"}},
		"userColumns": {regexp.MustCompile(`\.userColumns\(`), []string{"../../apiservice/admin/userstore.go fullUserByID",
			"../../apiservice/admin/userstore.go listAllUsers", "../../apiservice/admin/userstore.go listUsersByOrg",
			"../../apiservice/admin/userstore.go userByEmail", "../../apiservice/admin/userstore.go userByUsername"}},
	} {
		if got := sites(c.call); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s production call sites = %v, want %v", name, got, c.want)
		}
	}
}

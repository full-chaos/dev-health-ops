package platformrole

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A grant's target is a user principal, enabled unless the grant allows a disabled one (the user-admin routes, pm
// D6914); every other grant (the operator CLI) still refuses a disabled target.
func TestGrantTargetRefusal(t *testing.T) {
	for _, c := range []struct {
		name           string
		kind           string
		enabled, allow bool
		want           error
	}{
		{"disabled user, allowed", "user", false, true, nil},
		{"disabled user, not allowed", "user", false, false, ErrPrincipalOff},
		{"enabled user", "user", true, false, nil},
		{"service account, allowed", "service_account", true, true, ErrNotHumanUser},
		{"disabled service account, allowed", "service_account", false, true, ErrNotHumanUser},
	} {
		if got := grantTargetRefusal(c.kind, c.enabled, c.allow); !errors.Is(got, c.want) || (c.want == nil && got != nil) {
			t.Errorf("%s: grantTargetRefusal = %v, want %v", c.name, got, c.want)
		}
	}
}

// AllowDisabledTarget is set in one place: the user-admin update's platform role step. A caller that sets it anywhere
// else would grant to a disabled principal.
func TestOnlyTheUserAdminUpdateGrantsToADisabledTarget(t *testing.T) {
	var sites []string
	err := filepath.WalkDir(filepath.Join("..", ".."), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for range strings.Count(string(src), "AllowDisabledTarget:") {
			sites = append(sites, filepath.ToSlash(path))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sites) != 1 || sites[0] != "../../auth/authapi/provisioning_users.go" {
		t.Fatalf("AllowDisabledTarget is set at %v, want once in internal/auth/authapi/provisioning_users.go", sites)
	}
}

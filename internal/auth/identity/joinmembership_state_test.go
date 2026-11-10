package identity

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A suspended organization refuses every join but go-api's admin create (pm D6914); an active one refuses none.
func TestJoinOrgStateRefusesASuspendedOrganizationButForAnAdminCreate(t *testing.T) {
	for _, c := range []struct {
		name   string
		active bool
		m      membershipJoin
		want   error
	}{
		{"admin create, suspended", false, membershipJoin{AdminCreate: true}, nil},
		{"plain join, suspended", false, membershipJoin{}, ErrOrganizationSuspended},
		{"sign-in join, suspended", false, membershipJoin{SignIn: true}, ErrOrganizationSuspended},
		{"plain join, active", true, membershipJoin{}, nil},
		{"admin create, active", true, membershipJoin{AdminCreate: true}, nil},
	} {
		if got := joinOrgStateRefusal(c.active, c.m); !errors.Is(got, c.want) || (c.want == nil && got != nil) {
			t.Errorf("%s: joinOrgStateRefusal = %v, want %v", c.name, got, c.want)
		}
	}
}

// AdminCreate is set in one place: go-api's admin create (AdminCreateUser). A join that sets it anywhere else would
// land in a suspended organization.
func TestOnlyTheAdminCreateJoinsASuspendedOrganization(t *testing.T) {
	var sites []string
	err := filepath.WalkDir(filepath.Join("..", ".."), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for range strings.Count(string(src), "AdminCreate: true") {
			sites = append(sites, filepath.ToSlash(path))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sites) != 1 || sites[0] != "../../auth/identity/adminusers.go" {
		t.Fatalf("AdminCreate: true is set at %v, want once in internal/auth/identity/adminusers.go", sites)
	}
	if body := funcBody(t, "adminusers.go", "func (s *Store) AdminCreateUser("); !strings.Contains(body, "AdminCreate: true") {
		t.Error("AdminCreate: true is not in AdminCreateUser")
	}
}

package entitlement_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	entitlementPkg   = "github.com/full-chaos/dev-health-ops/internal/auth/entitlement"
	authorizationPkg = "github.com/full-chaos/dev-health-ops/internal/auth/authorization"
)

func deps(t *testing.T, pkg string) []string {
	t.Helper()
	out, err := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", pkg).CombinedOutput()
	if err != nil {
		t.Fatalf("go list %s: %v\n%s", pkg, err, out)
	}
	return strings.Fields(string(out))
}

func TestPackageDoesNotImportAuthorization(t *testing.T) {
	for _, dep := range deps(t, entitlementPkg) {
		if dep == authorizationPkg {
			t.Fatal("entitlement must not import internal/auth/authorization")
		}
		if dep == "net/http" {
			t.Fatal("entitlement must not import net/http")
		}
	}
}

func TestAuthorizationDoesNotImportPackage(t *testing.T) {
	if _, err := os.Stat(filepath.Join("..", "authorization")); err != nil {
		t.Skip("internal/auth/authorization is not on this base yet")
	}
	for _, dep := range deps(t, authorizationPkg) {
		if dep == entitlementPkg {
			t.Fatal("authorization must not import internal/auth/entitlement")
		}
	}
}

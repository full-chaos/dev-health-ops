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

func TestPackageImportsNoTokenOrClaimsPackage(t *testing.T) {
	const base = "github.com/full-chaos/dev-health-ops/internal/auth/"
	forbidden := map[string]bool{}
	for _, p := range []string{
		"edgetoken", "session", "sessionid", "servicecred", "accessverify", "authruntime", "authapi",
		"workloadexchange", "identity/local", "identity/oidc", "identity/saml",
	} {
		forbidden[base+p] = true
	}
	for _, dep := range deps(t, entitlementPkg) {
		if forbidden[dep] {
			t.Fatalf("entitlement must not import the token/claims package %s", dep)
		}
	}
}

func TestNoAuthPackageImportsEntitlement(t *testing.T) {
	out, err := exec.Command("go", "list", "-f", "{{.ImportPath}} {{join .Imports \" \"}}", "github.com/full-chaos/dev-health-ops/internal/auth/...").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] == entitlementPkg {
			continue
		}
		for _, imp := range fields[1:] {
			if imp == entitlementPkg {
				t.Fatalf("%s imports internal/auth/entitlement; the PDP reaches it through its own interface", fields[0])
			}
		}
	}
}

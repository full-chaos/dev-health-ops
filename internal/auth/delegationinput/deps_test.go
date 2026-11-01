package delegationinput_test

import (
	"os/exec"
	"strings"
	"testing"
)

const (
	self = "github.com/full-chaos/dev-health-ops/internal/auth/delegationinput"
	base = "github.com/full-chaos/dev-health-ops/internal/auth/"
)

func deps(t *testing.T, pkg string) []string {
	t.Helper()
	out, err := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", pkg).CombinedOutput()
	if err != nil {
		t.Fatalf("go list %s: %v\n%s", pkg, err, out)
	}
	return strings.Fields(string(out))
}

func TestAuthorizationDoesNotImportTheAdapter(t *testing.T) {
	for _, d := range deps(t, base+"authorization") {
		if d == self {
			t.Fatal("authorization imports delegationinput")
		}
	}
}

func TestAdapterImportsNoTokenClaimsOrEntitlementPackage(t *testing.T) {
	for _, d := range deps(t, self) {
		for _, p := range []string{"edgetoken", "servicecred", "accessverify", "authruntime", "authapi", "workloadexchange", "entitlement"} {
			if d == base+p {
				t.Fatalf("imports %s", d)
			}
		}
	}
}

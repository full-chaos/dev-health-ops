package contracts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// go-api no longer mounts the OAuth provider create and patch routes. No ops route contract may name the path: a row
// for a route nothing mounts describes a surface that is not there. (The two-segment /api/v1/auth/oauth/{first}/{second}
// rows stay: the sign-in routes use them.)
func TestNoContractNamesTheRetiredOAuthProviderPath(t *testing.T) {
	root := testRoot(t)
	for name, path := range map[string]string{
		"endpoint profiles": filepath.Join(root, "contracts", "auth", "v1", "endpoint-profiles.go-api.json"),
		"ingress planes":    filepath.Join(root, "contracts", "ingress", "v1", "planes.json"),
		"compose router":    filepath.Join(root, "contracts", "ingress", "v1", "compose-router.nginx.conf"),
	} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if strings.Contains(string(raw), "/api/v1/auth/oauth/providers") {
			t.Errorf("%s (%s) still names the retired /api/v1/auth/oauth/providers path", name, path)
		}
	}
}

package scripts_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// startupOnlyMarker tags the one JWT_SECRET_KEY line the live e2e keeps: dho api refuses to start without
// the variable until the HS256 verifier is gone. The value is random per run and no minter reads it.
const startupOnlyMarker = "go-api startup only"

func TestE2EScriptsDoNotUseTheHS256Secret(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	files := []string{
		"ci/run_live_backend_e2e.sh",
		"ci/lib/e2e_fixture.sh",
		"ci/lib/go_api_prove_e2e.sh",
		"internal/cigates/livee2e/livee2e.go",
		"internal/cigates/livee2e/cli.go",
	}
	for _, rel := range files {
		raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(raw), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "//") {
				continue
			}
			for _, word := range []string{"GO_API_EDGE_JWT_SECRET", "live-e2e jwt-secret", "SigningMethodHS", "JWTSecret"} {
				if strings.Contains(line, word) {
					t.Errorf("%s:%d uses %s", rel, i+1, word)
				}
			}
			if strings.Contains(line, "JWT_SECRET_KEY") && !strings.Contains(line, startupOnlyMarker) {
				t.Errorf("%s:%d uses JWT_SECRET_KEY outside the marked go-api startup line", rel, i+1)
			}
		}
	}
}

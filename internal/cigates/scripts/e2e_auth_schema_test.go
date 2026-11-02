package scripts_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// The live e2e verifies its access token against a session row in the auth schema, and `dho migrate postgres` does
// not create that schema: auth-migrate does. The runner must apply it before it seeds a session and mints a token.
func TestLiveE2EAppliesTheAuthSchemaBeforeSeedingASession(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "ci/run_live_backend_e2e.sh"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	migrate := strings.Index(text, `"${BIN_DIR}/auth-migrate"`)
	seed := strings.Index(text, "AUTH_TOKEN=\"$(generate_auth_token)\"")
	if migrate < 0 {
		t.Fatal(`ci/run_live_backend_e2e.sh never runs "${BIN_DIR}/auth-migrate"`)
	}
	if seed < 0 || migrate > seed {
		t.Fatal("auth-migrate must run before generate_auth_token seeds the session")
	}
	for _, name := range []string{"AUTH_MIGRATION_DATABASE_URI", "AUTH_RUNTIME_DATABASE_ROLE"} {
		if !strings.Contains(text, name) {
			t.Errorf("the auth-migrate step does not set %s", name)
		}
	}
}

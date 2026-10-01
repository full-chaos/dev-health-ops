package synccli

import (
	"errors"

	"context"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"net/http"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CHAOS-7132: with no SETTINGS_ENCRYPTION_KEY the verb refuses up front and says so, instead of failing
// the first resolve with a bare "provider credential is invalid".
func TestResolveJiraStoredSettingsRefusesWithoutAnEncryptionKey(t *testing.T) {
	_, err := resolveJiraStoredSettings(context.Background(), new(pgxpool.Pool), providerfoundation.FernetDecryptor{},
		http.DefaultClient, nil, "org", envOverrides{})
	if err == nil || !strings.Contains(err.Error(), "encryption_key_not_configured") || !strings.Contains(err.Error(), "SETTINGS_ENCRYPTION_KEY") {
		t.Fatalf("err = %v, want a refusal naming encryption_key_not_configured and SETTINGS_ENCRYPTION_KEY", err)
	}
}

// The strict verb: there the Atlassian leg IS the work, so the failure that is only DEGRADED in the
// worker's discovery (an "Invalid Organization Ari" from the gateway) must still exit non-zero and
// say read_failed, with nothing written and no secret in the text (CHAOS-7132).
func TestStrictVerbExitsNonZeroWhenTheAtlassianLegFails(t *testing.T) {
	rec := &recorded{}
	failure := errors.New("search atlassian teams: Exception while fetching data (/team/teamSearchV2) : Invalid Organization Ari: some-uuid")
	code, stdout, stderr := run(t, validEnv(), stubDeps(rec, failingClient{err: failure}, nil), "--provider", "jira", "--org", "o")
	if code != cli.ExitFailure {
		t.Fatalf("exit %d, want %d: a failed Atlassian leg is a failed verb", code, cli.ExitFailure)
	}
	if stdout != "" || !strings.Contains(stderr, `"code":"read_failed"`) || strings.Contains(stderr, tokenValue) {
		t.Errorf("stdout=%q stderr=%q", stdout, stderr)
	}
}

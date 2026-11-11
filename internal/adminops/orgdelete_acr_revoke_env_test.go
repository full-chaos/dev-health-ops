package adminops

import (
	"bytes"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/acrrevoke"
	"github.com/full-chaos/dev-health-ops/internal/cli"
)

var acrRevokeRequired = []string{acrrevoke.EnvURL, acrrevoke.EnvTokenURL, acrrevoke.EnvSubjectFile}

// The CLI reads ACR_REVOKE_* as go-api does: all unset is no client (the delete goes on); a present but blank value,
// alone or with others, is a configuration error that names the variable and never a value; a usable set is a client.
func TestOrgsDeleteReadsABlankPresentACRRevokeSetAsAConfigurationError(t *testing.T) {
	for mask := 1; mask < 1<<len(acrRevokeRequired); mask++ {
		env := map[string]string{}
		var present []string
		for i, name := range acrRevokeRequired {
			if mask&(1<<i) != 0 {
				env[name] = ""
				present = append(present, name)
			}
		}
		var stderr bytes.Buffer
		config, _, ok := deleteConfigFrom(cli.Env{Lookup: func(name string) (string, bool) { v, set := env[name]; return v, set }, Stderr: &stderr}, "")
		if ok || config.ACROrgRevoker != nil {
			t.Fatalf("present blank %v: config accepted (revoker %v), want a configuration error", present, config.ACROrgRevoker)
		}
		if !strings.Contains(stderr.String(), "configuration error") || !strings.Contains(stderr.String(), "ACR_REVOKE_") {
			t.Fatalf("present blank %v: stderr %q, want a configuration error naming the variable", present, stderr.String())
		}
	}
	var stderr bytes.Buffer
	config, _, ok := deleteConfigFrom(cli.Env{Lookup: func(string) (string, bool) { return "", false }, Stderr: &stderr}, "")
	if !ok || config.ACROrgRevoker != nil {
		t.Fatalf("all unset: ok %v revoker %v, want accepted with no client", ok, config.ACROrgRevoker)
	}
	usable := map[string]string{acrrevoke.EnvURL: "https://acr.internal.test", acrrevoke.EnvTokenURL: "https://acr.internal.test/api/v1/oauth/token", acrrevoke.EnvSubjectFile: "/var/run/dev-health/acr-revoke/token"}
	config, _, ok = deleteConfigFrom(cli.Env{Lookup: func(name string) (string, bool) { v, set := usable[name]; return v, set }, Stderr: &stderr}, "")
	if !ok || config.ACROrgRevoker == nil {
		t.Fatalf("all set: ok %v revoker %v, want a client", ok, config.ACROrgRevoker)
	}
}

package queryapiservice

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/workloadinput"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
)

// The PDP gate reads its settings, the 5.17 cell list included, through the declared-settings reader, which answers
// only for a declared name: an undeclared one would read as unset and keep the gate off whatever the deployment sets.
func TestThePDPSettingsAreDeclared(t *testing.T) {
	names := []string{pdp.EnvMode, pdp.EnvAuthProxyURL, workloadinput.EnvSubjectFile, workloadinput.EnvBootstrapFile, workloadinput.EnvIssuer,
		authzenforce.QueryAPI.Env()}
	lookup := func(name string) (string, bool) {
		for _, n := range names {
			if n == name {
				return "x", true
			}
		}
		return "", false
	}
	cfg, err := config.Load(config.Spec{Service: config.QueryAPIServiceName, LookupEnv: lookup})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if value, ok := cfg.Setting(name); !ok || value != "x" {
			t.Errorf("%s is not a declared query-api setting", name)
		}
	}
}

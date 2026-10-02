package githubapp

import (
	"context"
	"math/big"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

// A caller-supplied client of the default redirect policy never carries the installer's token or the app JWT to
// another origin (D4124).
func TestASuppliedClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	h := handlers{Deps: Deps{HTTPClient: probe.Client()}}
	request, err := http.NewRequest(http.MethodGet, probe.Base.URL+"/user/installations", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer SECRET-TOKEN")
	_, _, _ = h.fetchJSON(request)
	probe.Assert(t)
}

// The OAuth code exchange carries client_secret in the form BODY, which a 307 would send again to any host.
func TestTheCodeExchangeNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	h := handlers{Deps: Deps{HTTPClient: probe.Client(), GitHubURL: probe.Base.URL, GitHubAPIURL: probe.Base.URL,
		Config: Config{ClientID: "id", ClientSecret: "SECRET"}}}
	_, _ = h.verifyInstallerAccess(context.Background(), big.NewInt(1), "code")
	probe.Assert(t)
}

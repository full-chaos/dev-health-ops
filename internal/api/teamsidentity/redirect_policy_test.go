package teamsidentity

import (
	"net/http"
	"testing"
)

// CHAOS-7454 (r1 class): every discovery call carries a stored provider credential; neither shared client
// may follow a redirect.
func TestDiscoveryClientsRefuseRedirects(t *testing.T) {
	for name, doer := range map[string]any{"discoveryHTTPClient": discoveryHTTPClient, "discoveryAppExchangeClient": discoveryAppExchangeClient} {
		client, ok := doer.(*http.Client)
		if !ok || client.CheckRedirect == nil || client.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
			t.Errorf("%s follows redirects", name)
		}
	}
}

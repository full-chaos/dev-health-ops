package pushcli

import (
	"net/http"
	"testing"
)

// The ingest client carries the ingest token (Authorization) and refuses every redirect: a 3xx is the response (CHAOS-7910).
func TestTheIngestClientRefusesRedirects(t *testing.T) {
	client := newIngestClient().http
	if client.CheckRedirect == nil || client.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
		t.Fatal("the ingest client follows redirects")
	}
}

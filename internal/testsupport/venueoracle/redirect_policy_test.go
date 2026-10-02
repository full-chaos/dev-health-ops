package venueoracle

import (
	"net/http"
	"testing"
)

// The venue client returns a redirect as the response and never follows it (CHAOS-7910).
func TestTheVenueClientRefusesRedirects(t *testing.T) {
	if noRedirects.CheckRedirect == nil || noRedirects.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
		t.Fatal("the venue client follows redirects")
	}
}

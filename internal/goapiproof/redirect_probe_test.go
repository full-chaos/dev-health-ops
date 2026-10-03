package goapiproof

import (
	"errors"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

// The leg client carries the leg's credential: a redirect to another origin is refused (errRedirectRefused) and the other
// origin sees no request (CHAOS-7910).
func TestTheLegClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	request, err := http.NewRequest(http.MethodGet, probe.Base.URL+"/leg", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer SECRET")
	if _, err := NewLegClient(0).Do(request); !errors.Is(err, errRedirectRefused) {
		t.Fatalf("Do across a redirect: err = %v, want errRedirectRefused", err)
	}
	probe.Assert(t)
}

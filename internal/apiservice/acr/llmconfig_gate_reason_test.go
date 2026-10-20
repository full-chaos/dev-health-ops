package acr

import (
	"io"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
)

// The marked no-config 404 names a closed byo_llm gate in a second header, so
// acr can refuse the turn with a reason instead of reading "no config". The
// X-Org-Llm-Config: none marker stays on both answers: the worker client
// reads only that marker.
func TestNoConfigNotFoundNamesTheClosedGateInAHeader(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source *fakeSource
		reason string
	}{
		{name: "gate closed", source: &fakeSource{cfg: llmorgsettings.OrgConfig{GateClosed: true}}, reason: "byo_llm_gate_closed"},
		{name: "saved config of another purpose only", source: &fakeSource{cfg: llmorgsettings.OrgConfig{UnresolvedPurpose: llmorgsettings.PurposeInvestigate}}, reason: "byo_no_investigate_config"},
		{name: "nothing stored", source: &fakeSource{}, reason: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response, _ := call(t, &fakeVerifier{}, tc.source, io.Discard, testOrg, "Bearer "+testToken)
			if response.StatusCode != 404 || response.Header.Get(NoConfigHeader) != NoConfigValue {
				t.Fatalf("status %d marker %q, want the marked 404", response.StatusCode, response.Header.Get(NoConfigHeader))
			}
			if got := response.Header.Get("X-Org-Llm-Config-Reason"); got != tc.reason {
				t.Fatalf("X-Org-Llm-Config-Reason = %q, want %q", got, tc.reason)
			}
		})
	}
}

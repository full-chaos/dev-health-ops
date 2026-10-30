package admin

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/providerrevocation"
)

// The revocation worker must leave a setup row alone exactly as long as the
// callback's own drain does: a younger row's callback may still be in flight.
func TestRevocationWorkerSetupCutoffMatchesTheCallback(t *testing.T) {
	if providerrevocation.SetupStaleAfter != setupRevocationStaleAfter {
		t.Fatalf("worker setup cutoff %s, callback cutoff %s", providerrevocation.SetupStaleAfter, setupRevocationStaleAfter)
	}
}

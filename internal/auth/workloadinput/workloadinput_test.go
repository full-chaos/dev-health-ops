package workloadinput

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/httpguard"
)

// The workload exchange rides the shared outbound transport (no environment proxy), not a transport of its own.
func TestExchangeTransportIsTheSharedOutboundTransport(t *testing.T) {
	if Transport() != httpguard.SharedTransport() {
		t.Fatal("the workload exchange does not use httpguard.SharedTransport()")
	}
}

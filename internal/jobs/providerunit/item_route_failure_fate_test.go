package providerunit

import (
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CHAOS-14235 C1: a decode failure that names its route keeps the unit's fate of the bare sentinel.
func TestAnItemDecodeFailureThatNamesItsRouteKeepsTheUnitFate(t *testing.T) {
	bare := providerfoundation.ErrNormalizationInvalid
	routed := providerfoundation.RouteFailure("/repos/{owner}/{repo}/commits", providerfoundation.ReasonBodyDecode, bare)
	if got, want := causeClass(routed, false), causeClass(bare, false); got != want {
		t.Fatalf("causeClass = %q, want %q", got, want)
	}
	if got, want := retryMaxAttempts(routed), retryMaxAttempts(bare); got != want {
		t.Fatalf("retryMaxAttempts = %d, want %d", got, want)
	}
	categoryRouted, terminalRouted := deterministicTerminalCategory(routed)
	categoryBare, terminalBare := deterministicTerminalCategory(bare)
	if categoryRouted != categoryBare || terminalRouted != terminalBare {
		t.Fatalf("deterministicTerminalCategory = %q %v, want %q %v", categoryRouted, terminalRouted, categoryBare, terminalBare)
	}
	if !errors.Is(routed, providerfoundation.ErrNormalizationInvalid) {
		t.Fatal("the routed error lost ErrNormalizationInvalid")
	}
}

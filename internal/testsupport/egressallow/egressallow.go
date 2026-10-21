// Package egressallow lets a package's tests send provider traffic to their own local servers and doubles through the
// provider egress guard: the guard refuses loopback and any RoundTripper it cannot reach into by default. A TestMain
// calls LocalTests before m.Run; a test that proves a refusal sets its own list with egressguard.SetAllowForTest.
package egressallow

import (
	"net/netip"

	"github.com/full-chaos/dev-health-ops/internal/egressguard"
)

// LocalTests allows 127.0.0.0/8 and ::1 and passes test-double RoundTrippers through, for this test binary; restore
// puts the previous settings back.
func LocalTests() (restore func()) {
	restoreAllow := egressguard.SetAllowForTest(netip.MustParsePrefix("127.0.0.0/8"), netip.MustParsePrefix("::1/128"))
	restoreDoubles := egressguard.SetUnguardedTransportsForTest(true)
	return func() { restoreDoubles(); restoreAllow() }
}

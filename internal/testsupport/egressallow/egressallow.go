// Package egressallow lets a package's tests dial their own loopback servers (httptest) through the provider egress
// guard, which refuses loopback by default. A TestMain calls Loopback before m.Run; a test that proves a refusal sets
// its own list with egressguard.SetAllowForTest.
package egressallow

import (
	"net/netip"

	"github.com/full-chaos/dev-health-ops/internal/egressguard"
)

// Loopback allows 127.0.0.0/8 and ::1 for this test binary; restore puts the previous list back.
func Loopback() (restore func()) {
	return egressguard.SetAllowForTest(netip.MustParsePrefix("127.0.0.0/8"), netip.MustParsePrefix("::1/128"))
}

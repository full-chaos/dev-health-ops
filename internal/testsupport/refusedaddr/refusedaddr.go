// Package refusedaddr gives tests an address whose dials are refused and stay refused.
package refusedaddr

import "testing"

// Addr returns a 127.0.0.1 address with a bound socket that never listens: the kernel refuses every dial (ECONNREFUSED), and no
// other listener, in this process or another parallel test, can be handed the port until the test ends. A port that was bound,
// closed and then dialled can be taken by another test's listener in between. A test that must open a listener on the address
// later uses Hold.
func Addr(t testing.TB) string {
	t.Helper()
	return Hold(t).Addr()
}

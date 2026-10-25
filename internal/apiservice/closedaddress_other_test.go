//go:build !unix

package apiservice

import "testing"

func closedAddress(t *testing.T) string {
	t.Helper()
	t.Skip("closedAddress needs a bound, non-listening socket (unix only)")
	return ""
}

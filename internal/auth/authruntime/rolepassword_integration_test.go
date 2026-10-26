//go:build integration

package authruntime

import (
	"crypto/rand"
	"encoding/hex"
	"testing"
)

// testRolePassword is a login password for a test role, built at run time so no fixed password stands in the
// sources (TestNoFixedCredentialLiteralInThePackage). Hex keeps it safe inside a quoted SQL literal.
func testRolePassword(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	return "rt-" + hex.EncodeToString(raw)
}

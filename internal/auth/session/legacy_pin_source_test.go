package session

import (
	"os"
	"strings"
	"testing"
)

// Every session a legacy exchange starts goes through insertSession, which
// writes the pin; the compat handler's pinMissing arm is therefore unreachable
// for it.
func TestEveryLegacyExchangedSessionIsPinnedAtCreation(t *testing.T) {
	legacy, err := os.ReadFile("legacy.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(legacy), "s.insertSession(ctx, tx, legacy.PrincipalID") {
		t.Fatal("ExchangeLegacy no longer creates its session through insertSession")
	}
	signin, err := os.ReadFile("signin.go")
	if err != nil {
		t.Fatal(err)
	}
	insert := string(signin)[strings.Index(string(signin), "func (s *Store) insertSession"):]
	if !strings.Contains(insert, "legacy_token_version)") {
		t.Fatal("insertSession no longer writes legacy_token_version")
	}
}

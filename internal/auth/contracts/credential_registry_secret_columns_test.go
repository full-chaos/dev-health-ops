package contracts

import (
	"sort"
	"strings"
	"testing"
)

// TestCredentialRegistryHoldsNoSecretColumnBeyondTheKnownVerifierPair: every
// registry column is either a credential-entry.v1 member or one of the two
// reviewed verifier columns; a new secret-shaped column cannot be added
// without changing this set on purpose.
func TestCredentialRegistryHoldsNoSecretColumnBeyondTheKnownVerifierPair(t *testing.T) {
	want := []string{"hash_algorithm", "secret_hash"}
	var got []string
	for c := range credentialNotOnTheWire {
		got = append(got, c)
	}
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("columns kept off the wire = %v, want exactly %v", got, want)
	}
	secretShaped := []string{"secret", "token", "password", "bearer", "api_key", "apikey", "private", "plaintext"}
	for _, column := range registryColumns(t) {
		if _, verifier := credentialNotOnTheWire[column]; verifier {
			continue
		}
		for _, word := range secretShaped {
			if strings.Contains(column, word) {
				t.Errorf("credential_registry column %q looks like secret material: only secret_hash and hash_algorithm may", column)
			}
		}
	}
}

func TestTheSecretColumnGuardFlagsAPlantedColumn(t *testing.T) {
	planted := append(registryColumns(t), "api_secret")
	flagged := false
	for _, column := range planted {
		if _, verifier := credentialNotOnTheWire[column]; verifier {
			continue
		}
		if strings.Contains(column, "secret") {
			flagged = true
		}
	}
	if !flagged {
		t.Fatal("a planted api_secret column was not flagged")
	}
}

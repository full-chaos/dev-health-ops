package credshape

import (
	"strings"
	"testing"
)

// The service-account key's prefix is a credential_prefix marker of
// credential-classes.json, so the contract-derived scanner refuses a value of
// its shape (in any job payload or contract example) and probes it.
func TestTheScannerMatchesAServiceAccountKey(t *testing.T) {
	scanner, err := Default()
	if err != nil {
		t.Fatal(err)
	}
	value := "fcsak_" + strings.Repeat("Kq", 21) + "Z"
	if rule := scanner.Match(value); rule != "prefix:fcsak_" {
		t.Fatalf("a service-account key matched rule %q, want prefix:fcsak_", rule)
	}
	if rule := scanner.Match("token " + value + " leaked"); rule == "" {
		t.Fatal("a service-account key inside text was not matched")
	}
	probes, err := scanner.Probes()
	if err != nil {
		t.Fatal(err)
	}
	probed := false
	for _, probe := range probes {
		if strings.HasPrefix(probe, "fcsak_") {
			probed = true
		}
	}
	if !probed {
		t.Fatal("no probe carries the service-account key prefix")
	}
}

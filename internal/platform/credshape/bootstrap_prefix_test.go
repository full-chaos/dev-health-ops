package credshape

import (
	"strings"
	"testing"
)

// The workload bootstrap credential's prefix is a credential_prefix marker of
// credential-classes.json, so the contract-derived scanner refuses a value of
// its shape (in any job payload or contract example) and probes it.
func TestTheScannerMatchesAWorkloadBootstrapValue(t *testing.T) {
	scanner, err := Default()
	if err != nil {
		t.Fatal(err)
	}
	value := "fcwlb_" + strings.Repeat("Kq", 21) + "Z"
	if rule := scanner.Match(value); rule != "prefix:fcwlb_" {
		t.Fatalf("a bootstrap value matched rule %q, want prefix:fcwlb_", rule)
	}
	if rule := scanner.Match("token " + value + " leaked"); rule == "" {
		t.Fatal("a bootstrap value inside text was not matched")
	}
	probes, err := scanner.Probes()
	if err != nil {
		t.Fatal(err)
	}
	probed := false
	for _, probe := range probes {
		if strings.HasPrefix(probe, "fcwlb_") {
			probed = true
		}
	}
	if !probed {
		t.Fatal("no probe carries the bootstrap prefix")
	}
}

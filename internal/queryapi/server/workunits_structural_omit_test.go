package server

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
)

// R1 (acp, CHAOS-13090): the wire of a unit whose structural evidence is omitted has NO "structural" key; every other
// unit keeps the key, an empty list included (the unrestricted shape, byte for byte).
func TestWorkUnitWire_StructuralKeyOmittedOnlyForAnOmittedUnit(t *testing.T) {
	for _, tc := range []struct {
		name    string
		omitted bool
		want    bool
	}{
		{"omitted", true, false},
		{"served, no entries", false, true},
	} {
		wire := toWorkUnitInvestmentWire(investmentexplain.WorkUnitInvestment{
			WorkUnitID: "u1",
			Evidence:   investmentexplain.WorkUnitEvidenceOutput{StructuralOmitted: tc.omitted},
		})
		body, err := json.Marshal(wire)
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Contains(string(body), `"structural"`); got != tc.want {
			t.Fatalf("%s: structural key present = %t, want %t: %s", tc.name, got, tc.want, body)
		}
		if !tc.omitted && !strings.Contains(string(body), `"structural":[]`) {
			t.Fatalf("%s: an empty served list must stay []: %s", tc.name, body)
		}
	}
}

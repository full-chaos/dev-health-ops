package policycatalog

import (
	"strings"
	"testing"
)

// ReadOnly is the low-risk organization actions, and every one of them is named "*.read": a low-risk action that
// writes would let a delegated bearer write, so it fails here first.
func TestReadOnlyIsTheLowRiskOrganizationReads(t *testing.T) {
	reads := 0
	for _, a := range Default().Actions() {
		want := a.Scope == ScopeOrganization && a.RiskClass == RiskLow
		if a.ReadOnly() != want {
			t.Errorf("%s: ReadOnly %v, want %v", a.ID, a.ReadOnly(), want)
		}
		if a.ReadOnly() {
			reads++
			if !strings.HasSuffix(a.ID, ".read") {
				t.Errorf("%s is low risk but is not named as a read", a.ID)
			}
		}
		if strings.HasSuffix(a.ID, ".read") && !a.ReadOnly() {
			t.Errorf("%s is named as a read but is not ReadOnly", a.ID)
		}
	}
	if reads == 0 {
		t.Fatal("no read action in the catalog")
	}
}

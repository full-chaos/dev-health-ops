package home

import (
	"strings"
	"testing"
)

func TestLimitingFactorClaimHedgesOnce(t *testing.T) {
	for name, top := range map[string]Signal{
		"metric signal":    {Title: "Blocked Work appears up", ClaimSubject: "Blocked Work"},
		"risk signal":      {Title: "Compounding risk appears high for Delivery", ClaimSubject: "Compounding risk for Delivery"},
		"no prior period":  {Title: "Cycle Time has no prior period to compare", ClaimSubject: "Cycle Time"},
		"no claim subject": {Title: "WIP saturation has been elevated for a week"},
	} {
		claim := BuildLimitingFactor([]Signal{top}).Claim
		if got := strings.Count(claim, "appears"); got != 1 {
			t.Errorf("%s: claim %q holds %d hedge words, want 1", name, claim, got)
		}
		if !strings.HasSuffix(claim, " appears to be the current limiting factor.") {
			t.Errorf("%s: claim %q", name, claim)
		}
	}
	if got, want := BuildLimitingFactor([]Signal{{Title: "Blocked Work appears up", ClaimSubject: "Blocked Work"}}).Claim, "Blocked Work appears to be the current limiting factor."; got != want {
		t.Errorf("claim = %q, want %q", got, want)
	}
}

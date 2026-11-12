package policycatalog

import (
	"slices"
	"testing"
)

// acrReadPair are the two ACR read actions. go-api's principal access read takes one listing, for acr.context.read
// (acp ruling Q2 on CHAOS-13561), and acr guards its context and evidence routes with it. That is the listing of both
// only while every role holds both or neither: a role that holds one of them without the other makes the listing per
// action. If this census goes red, stop and tell acp; do not widen it.
var acrReadPair = [2]string{"acr.context.read", "acr.evidence.read"}

// acrReadPairSplit lists the roles that hold exactly one of the pair.
func acrReadPairSplit(roles []Role) []string {
	var split []string
	for _, r := range roles {
		if slices.Contains(r.Actions, acrReadPair[0]) != slices.Contains(r.Actions, acrReadPair[1]) {
			split = append(split, r.ID)
		}
	}
	return split
}

func TestEveryRoleHoldsBothACRReadsOrNeither(t *testing.T) {
	roles := Default().Roles()
	if len(roles) == 0 {
		t.Fatal("the catalog has no roles")
	}
	if split := acrReadPairSplit(roles); len(split) != 0 {
		t.Fatalf("roles holding one of %v without the other: %v (the access read's listing would be per action; tell acp)", acrReadPair, split)
	}
	holders := 0
	for _, r := range roles {
		if Default().Allows(r.ID, acrReadPair[0]) {
			holders++
		}
	}
	if holders == 0 {
		t.Fatalf("no role holds %s: the census checks nothing", acrReadPair[0])
	}
}

func TestTheACRReadPairCensusHasTeeth(t *testing.T) {
	planted := []Role{
		{ID: "both", Actions: []string{"acr.context.read", "acr.evidence.read"}},
		{ID: "neither", Actions: []string{"metrics.read"}},
		{ID: "context_only", Actions: []string{"acr.context.read"}},
		{ID: "evidence_only", Actions: []string{"acr.evidence.read", "metrics.read"}},
	}
	if got := acrReadPairSplit(planted); !slices.Equal(got, []string{"context_only", "evidence_only"}) {
		t.Fatalf("split %v, want [context_only evidence_only]", got)
	}
}

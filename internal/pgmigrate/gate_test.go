package pgmigrate

import "testing"

func gateFixture() BuildHeads {
	baseline := Baseline{Heads: []string{"0066", "0100"}}
	chain := []ChainFile{{Revision: "0101"}, {Revision: "0102"}, {Revision: "0103"}}
	known := map[string]bool{"0050": true, "0066": true, "0100": true, "0101": true, "0102": true, "0103": true}
	return NewBuildHeads(baseline, chain, known)
}

func TestCompareHeads(t *testing.T) {
	build := gateFixture()
	for name, test := range map[string]struct {
		recorded []string
		want     HeadVerdict
	}{
		"at the chain head":               {[]string{"0066", "0103"}, HeadsEqual},
		"one chain revision behind":       {[]string{"0066", "0102"}, HeadsBehind},
		"at the baseline, chain pending":  {[]string{"0066", "0100"}, HeadsBehind},
		"the cutover head is missing":     {[]string{"0103"}, HeadsBehind},
		"below the baseline":              {[]string{"0050"}, HeadsBehind},
		"a revision the build lacks":      {[]string{"0066", "0104"}, HeadsAhead},
		"unknown wins over a missing one": {[]string{"0104"}, HeadsAhead},
	} {
		if got := build.CompareHeads(test.recorded); got != test.want {
			t.Errorf("%s: CompareHeads(%v) = %v, want %v", name, test.recorded, got, test.want)
		}
	}
}

func TestBuildHeadsLoadedFromTheEmbeddedChainAreEqualAtTheirOwnHeads(t *testing.T) {
	build, err := LoadBuildHeads()
	if err != nil {
		t.Fatal(err)
	}
	if got := build.CompareHeads(build.Heads()); got != HeadsEqual {
		t.Fatalf("the build's own heads %v compare %v, want equal", build.Heads(), got)
	}
	chain, err := LoadChain()
	if err != nil || len(chain) < 2 {
		t.Fatalf("chain: %v", err)
	}
	behind := []string{}
	for _, head := range build.Heads() {
		if head == chain[len(chain)-1].Revision {
			head = chain[len(chain)-2].Revision
		}
		behind = append(behind, head)
	}
	if got := build.CompareHeads(behind); got != HeadsBehind {
		t.Fatalf("one revision behind %v compares %v, want behind", behind, got)
	}
}

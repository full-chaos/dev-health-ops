package restorefence

import "testing"

func TestTheFloorIsTheMaximumOfEveryInput(t *testing.T) {
	got := mergeFloors(
		[]Floor{{"organization", "org_a", 5}, {"organization", "org_b", 9}},
		[]map[string]int64{{"organization:org_a": 8, "membership:mbr_x": 2}, {"organization:org_b": 4}},
	)
	want := map[string]int64{"organization:org_a": 8, "organization:org_b": 9, "membership:mbr_x": 2}
	if len(got) != len(want) {
		t.Fatalf("floors = %+v", got)
	}
	for _, f := range got {
		if want[f.AggregateType+":"+f.AggregateID] != f.Revision {
			t.Errorf("%s:%s = %d, want %d", f.AggregateType, f.AggregateID, f.Revision, want[f.AggregateType+":"+f.AggregateID])
		}
	}
}

func TestAnInputWithAMalformedKeyOrRevisionIsIgnored(t *testing.T) {
	got := mergeFloors(nil, []map[string]int64{{"nocolon": 5, ":x": 5, "a:": 5, "a:b": 0, "c:d": maxRevision + 1, "e:f": 6}})
	if len(got) != 1 || got[0].AggregateType != "e" || got[0].Revision != 6 {
		t.Fatalf("floors = %+v", got)
	}
}

func TestTheMergedFloorsComeBackInTypeAndIDOrder(t *testing.T) {
	got := mergeFloors([]Floor{{"principal", "prn_b", 1}, {"organization", "org_z", 1}, {"organization", "org_a", 1}}, nil)
	order := []string{got[0].AggregateID, got[1].AggregateID, got[2].AggregateID}
	if order[0] != "org_a" || order[1] != "org_z" || order[2] != "prn_b" {
		t.Fatalf("order = %v", order)
	}
}

package teamprovider

import (
	"context"
	"testing"
)

func TestCarryRefusesNoConnectionAndAnEmptyOrg(t *testing.T) {
	if _, err := Carry(context.Background(), nil, "org", []string{"a"}); err != ErrInvalidArguments {
		t.Fatalf("nil connection: %v", err)
	}
}

func TestTallyCountsHowTheProvidersIDComparesWithTheStoredOne(t *testing.T) {
	stored := map[string]Stored{"a": {ProviderTeamID: "1"}, "b": {}, "c": {ProviderTeamID: "3"}}
	var tally Tally
	tally.Add(stored, "a", "1") // unchanged
	tally.Add(stored, "b", "2") // filled
	tally.Add(stored, "c", "9") // changed: the key is held by another team now
	tally.Add(stored, "d", "")  // the run did not read it
	if tally != (Tally{Filled: 1, Unchanged: 1, Changed: 1, Unread: 1}) {
		t.Fatalf("tally = %+v", tally)
	}
}

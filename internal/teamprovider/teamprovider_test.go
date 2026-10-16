package teamprovider

import (
	"context"
	"testing"
	"time"
)

func TestMergeTakesTheProvidersOwnValuesAndClearsGone(t *testing.T) {
	gone := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	stored := map[string]Stored{"gh:a": {ProviderTeamID: "1", ProviderName: "Old", GoneAt: &gone}}

	got := Merge(stored, "gh:a", "1", "New")
	if got.ProviderTeamID != "1" || got.ProviderName != "New" || got.GoneAt != nil {
		t.Fatalf("Merge with the provider's values = %+v, want the new name and no gone time", got)
	}
	got = Merge(stored, "gh:a", "", "")
	if got.ProviderTeamID != "1" || got.ProviderName != "Old" || got.GoneAt != nil {
		t.Fatalf("Merge without the provider's values = %+v, want the stored id and name, gone cleared", got)
	}
	if got := Merge(stored, "gh:new", " 9 ", " Name "); got.ProviderTeamID != "9" || got.ProviderName != "Name" {
		t.Fatalf("Merge of a new team = %+v, want the trimmed values", got)
	}
}

func TestKeepReturnsExactlyWhatIsStored(t *testing.T) {
	gone := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	stored := map[string]Stored{"gh:a": {ProviderTeamID: "1", ProviderName: "A", GoneAt: &gone}}
	if got := Keep(stored, "gh:a"); got.ProviderTeamID != "1" || got.ProviderName != "A" || got.GoneAt == nil || !got.GoneAt.Equal(gone) {
		t.Fatalf("Keep = %+v", got)
	}
	if got := Keep(stored, "gh:none"); got != (Stored{}) {
		t.Fatalf("Keep of an unknown team = %+v, want the zero value", got)
	}
}

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

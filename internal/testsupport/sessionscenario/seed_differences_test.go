package sessionscenario

import (
	"strings"
	"testing"
)

// The leg that owns users seeds ONE thing differently from every other leg: olga's auth_provider_id. Nothing else of any
// user changes, and the list of differences names exactly that, once, with its ticket.
func TestOwnedLegSeedDiffersOnlyInOlgasProviderID(t *testing.T) {
	plain, owned := SeedUsers(false), SeedUsers(true)
	if len(plain) != len(owned) {
		t.Fatalf("the owned leg seeds %d users, the others %d", len(owned), len(plain))
	}
	changed := 0
	for index := range plain {
		a, b := plain[index], owned[index]
		if a == b {
			continue
		}
		changed++
		if a.ID != olga || a.ProviderID != "" || b.ProviderID != "300" {
			t.Errorf("unexpected difference: %+v against %+v", a, b)
		}
		a.ProviderID, b.ProviderID = "", ""
		if a != b {
			t.Errorf("olga differs in more than her provider id: %+v against %+v", a, b)
		}
	}
	if changed != 1 {
		t.Fatalf("%d users differ, want exactly olga", changed)
	}
}

func TestSeedDifferencesAreDeclaredOnceWithTheirTicket(t *testing.T) {
	if len(SeedDifferences) != 1 {
		t.Fatalf("%d seed differences, want exactly 1", len(SeedDifferences))
	}
	d := SeedDifferences[0]
	if d.User != olga || d.Column != "auth_provider_id" || d.Value != "300" || !strings.Contains(d.Reason, "CHAOS-11452") {
		t.Fatalf("the declared seed difference is %+v", d)
	}
}

// No compared users column changes: the corpus compares email, username, full name, provider, the three flags, the token
// version and the password hash prefix, never the provider id.
func TestTheComparedUsersColumnsAreTheSameOnEveryLeg(t *testing.T) {
	compared := func(u SeedUser) SeedUser { u.ProviderID = ""; return u }
	plain, owned := SeedUsers(false), SeedUsers(true)
	for index := range plain {
		if compared(plain[index]) != compared(owned[index]) {
			t.Errorf("%s: a compared column differs: %+v against %+v", plain[index].Email, plain[index], owned[index])
		}
	}
}

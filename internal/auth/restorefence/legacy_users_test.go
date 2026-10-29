package restorefence

import (
	"strings"
	"testing"
)

// Round1115 P2-b: a legacy user force-deleted at the source after the target is NOT deleted from the restored
// database (restrict and no-action foreign keys, and a cascade into sessions and tokens); it is tombstoned:
// is_active false and token_version bumped, so no restored token or password signs in.
func TestAForceDeletedLegacyUserIsTombstonedNotDeleted(t *testing.T) {
	var found bool
	for _, s := range sections {
		if !s.legacy || s.table != "users" || s.kind != kindTombstone {
			continue
		}
		found = true
		if s.name != "public.users.ids" || !s.manifestOnly || s.key.name != "id" {
			t.Errorf("tombstone section: name %q manifestOnly %v key %s", s.name, s.manifestOnly, s.key.name)
		}
		sql := s.applySQL(testSchema(t))
		for _, want := range []string{"UPDATE public.users t SET is_active = false, token_version = t.token_version + 1", "NOT EXISTS (SELECT 1 FROM m x WHERE x.id = t.id)"} {
			if !strings.Contains(sql, want) {
				t.Errorf("tombstone SQL lacks %q: %s", want, sql)
			}
		}
		if strings.Contains(strings.ToUpper(sql), "DELETE") {
			t.Error("a tombstone never deletes a user")
		}
		if s.idsSQL(testSchema(t)) == "" || s.action != ActionUserTombstoned {
			t.Error("the tombstoned ids are reported (ids only) as an operator action")
		}
	}
	if !found {
		t.Fatal("no legacy users section tombstones a restored user that the source deleted")
	}
}

func TestTheTombstoneActionLineCarriesIdsOnly(t *testing.T) {
	lines := ActionLines([]OperatorAction{{Kind: ActionUserTombstoned, Source: "public.users.ids", IDs: []string{"b", "a"}}})
	if len(lines) != 1 || !strings.Contains(lines[0], "operator-action user_tombstoned source=public.users.ids count=2 ids=a,b") {
		t.Fatalf("lines = %v", lines)
	}
}

// The counter section and the tombstone section of public.users are two sections of one table with distinct names.
func TestTheUsersTombstoneDoesNotReplaceTheCounterSection(t *testing.T) {
	counter, ok := sectionByName("public.users")
	if !ok || counter.kind != kindCounter {
		t.Fatal("public.users must stay the counter section")
	}
	if tomb, ok := sectionByName("public.users.ids"); !ok || tomb.kind != kindTombstone {
		t.Fatal("public.users.ids must be the tombstone section")
	}
}

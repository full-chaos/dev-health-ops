package restorefence

import (
	"strings"
	"testing"
)

// Round1122 P2-b (D6459): a link credential the source replaced (Issue deletes the old unconsumed row) must not stay
// usable in the restored database. The ids of the source's unconsumed link credentials are carried, and a restored
// unconsumed row absent from them is deleted. The consumed section stays.
func TestTheLinkCredentialPresenceSectionDeletesOnlyUnconsumedRowsTheSourceLacks(t *testing.T) {
	def, ok := sectionByName("auth.link_credentials.ids")
	if !ok {
		t.Fatal("auth.link_credentials.ids is not a section: a reissued link's old row stays usable after a restore")
	}
	if def.kind != kindPresence || def.legacy || def.key.name != "id" || def.suffix != ".ids" {
		t.Errorf("want an auth presence section keyed by id with the .ids suffix; got kind %v legacy %v key %s suffix %q", def.kind, def.legacy, def.key.name, def.suffix)
	}
	if def.manifestOnly {
		t.Error("D6459: a blind margin removes every unconsumed link credential (fail closed), so the section also runs without a manifest")
	}
	if !strings.Contains(def.revoked, "consumed_at IS NULL") {
		t.Errorf("the export carries unconsumed rows only; got %q", def.revoked)
	}
	sql := def.applySQL(testSchema(t))
	for _, want := range []string{`DELETE FROM "auth".link_credentials t`, "NOT EXISTS (SELECT 1 FROM m x WHERE x.id = t.id)", "t.consumed_at IS NULL"} {
		if !strings.Contains(sql, want) {
			t.Errorf("link credential presence SQL lacks %q: %s", want, sql)
		}
	}
	if consumed, ok := sectionByName("auth.link_credentials"); !ok || consumed.kind != kindRevoke {
		t.Error("the consumed section of link credentials must stay")
	}
}

func TestTheSectionOrderAppliesConsumptionBeforeTheLinkPresenceDelete(t *testing.T) {
	idx := map[string]int{}
	for i, s := range sections {
		idx[s.name] = i
	}
	if idx["auth.link_credentials"] >= idx["auth.link_credentials.ids"] {
		t.Error("the consumed section must run first, so a link consumed after T is kept consumed rather than deleted")
	}
	if idx["public.users"] >= idx["public.users.ids"] {
		t.Error("the users counter section runs before the tombstone")
	}
}

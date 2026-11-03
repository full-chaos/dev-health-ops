package restorefence

import (
	"strings"
	"testing"
)

// Round1115 P2-a: a legacy membership mirror row deleted after the target must stay deleted (the D6454 rule: a
// change that removes access survives the restore).
func TestTheLegacyMembershipMirrorIsAPresenceSection(t *testing.T) {
	def, ok := sectionByName("public.memberships")
	if !ok {
		t.Fatal("public.memberships is not a section: a removed membership's public mirror row reappears after a restore")
	}
	if def.kind != kindPresence || !def.legacy || def.key.name != "id" {
		t.Errorf("public.memberships must be a legacy presence section keyed by id; got kind %v legacy %v key %s", def.kind, def.legacy, def.key.name)
	}
	sql := def.applySQL(testSchema(t))
	for _, want := range []string{"DELETE FROM public.memberships", "NOT EXISTS (SELECT 1 FROM m x WHERE x.id = t.id)"} {
		if !strings.Contains(sql, want) {
			t.Errorf("membership presence SQL lacks %q: %s", want, sql)
		}
	}
}

// Without a manifest (the blind margin) nothing is known about which rows the source kept: a presence section that
// deletes every row absent from an empty manifest would remove every member. These sections run only with a manifest.
func TestMembershipPresenceNeverRunsWithoutAManifest(t *testing.T) {
	def, ok := sectionByName("public.memberships")
	if !ok {
		t.Fatal("public.memberships is not a section")
	}
	if def.runsWithoutManifest() {
		t.Error("a presence section of live members must not run on an empty manifest")
	}
}

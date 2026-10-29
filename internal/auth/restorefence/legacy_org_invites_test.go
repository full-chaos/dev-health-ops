package restorefence

import (
	"strings"
	"testing"
)

// Follow-up (round1128 P2): a legacy public.org_invites row that the source deleted or no longer holds pending must
// not stay pending after a restore; the legacy accept path still reads this table. Same rule as auth.invites.
func TestALegacyInviteTheSourceNoLongerHoldsPendingEndsRevoked(t *testing.T) {
	def, ok := sectionByName("public.org_invites.ids")
	if !ok {
		t.Fatal("public.org_invites.ids is not a section: a legacy invite the source deleted stays pending after a restore")
	}
	if def.kind != kindRetire || !def.legacy || def.key.name != "id" || def.suffix != ".ids" || def.manifestOnly {
		t.Errorf("want a legacy retire section keyed by id, run also without a manifest; got kind %v legacy %v key %s manifestOnly %v", def.kind, def.legacy, def.key.name, def.manifestOnly)
	}
	if def.revoked != "s.status = 'pending'" {
		t.Errorf("the export carries the source's pending ids only; got %q", def.revoked)
	}
	sql := def.applySQL(testSchema(t))
	for _, want := range []string{"UPDATE public.org_invites t SET status = 'revoked', updated_at = clock_timestamp()", "t.status = 'pending'", "NOT EXISTS (SELECT 1 FROM m x WHERE x.id = t.id)"} {
		if !strings.Contains(sql, want) {
			t.Errorf("legacy invite retire SQL lacks %q: %s", want, sql)
		}
	}
	if strings.Contains(sql, "'accepted'") || strings.Contains(sql, "DELETE") {
		t.Errorf("the retire never accepts or deletes an invite: %s", sql)
	}
	if existing, ok := sectionByName("public.org_invites"); !ok || existing.kind != kindRevoke {
		t.Error("the existing public.org_invites revoke section must stay")
	}
}

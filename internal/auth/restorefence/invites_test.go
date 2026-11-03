package restorefence

import (
	"strings"
	"testing"
)

// D6459 am.1: a restored invite that is pending but whose source state is accepted, revoked or absent must end revoked
// (never accepted: the membership created after T is not restored, so "accepted" would claim one that does not exist,
// and never pending). A blind margin has no source state: every pending invite is revoked (fail closed, admins re-invite).
func TestARestoredPendingInviteTheSourceNoLongerHoldsPendingEndsRevoked(t *testing.T) {
	def, ok := sectionByName("auth.invites.ids")
	if !ok {
		t.Fatal("auth.invites.ids is not a section: an invite revoked or accepted after T comes back pending and its token works")
	}
	if def.kind != kindRetire || def.legacy || def.key.name != "id" || def.suffix != ".ids" {
		t.Errorf("want an auth retire section keyed by id with the .ids suffix; got kind %v legacy %v key %s suffix %q", def.kind, def.legacy, def.key.name, def.suffix)
	}
	if def.manifestOnly {
		t.Error("a blind margin revokes every pending invite (fail closed), so the section also runs without a manifest")
	}
	if def.revoked != "s.status = 'pending'" {
		t.Errorf("the export carries the source's PENDING invite ids only; got %q", def.revoked)
	}
	sql := def.applySQL(testSchema(t))
	for _, want := range []string{`UPDATE "auth".invites t SET status = 'revoked', updated_at = clock_timestamp()`, "t.status = 'pending'", "NOT EXISTS (SELECT 1 FROM m x WHERE x.id = t.id)"} {
		if !strings.Contains(sql, want) {
			t.Errorf("invite retire SQL lacks %q: %s", want, sql)
		}
	}
	for _, bad := range []string{"'accepted'", "status = 'pending',", "DELETE"} {
		if strings.Contains(sql, bad) {
			t.Errorf("invite retire SQL must never move an invite to accepted or pending, or delete one (found %q): %s", bad, sql)
		}
	}
}

func TestTheInviteSectionIsOnNoExclusionList(t *testing.T) {
	if _, excl := excluded["auth.invites"]; excl {
		t.Error("auth.invites is a fence section now and must not stay on the exclusion list")
	}
}

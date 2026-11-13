package authschema

import "testing"

// retiredLinkTokensAckDigest is the -ack-grant-narrowing digest of the one narrowing this retirement makes on a
// database the live release migrated: the runtime role loses DELETE and the id SELECT on both link-token tables.
const retiredLinkTokensAckDigest = "26a7f6497fb8e33f"

func retiredLinkTokensLost() []GrantRecord {
	var lost []GrantRecord
	for _, table := range retiredLinkTokenTables {
		lost = append(lost,
			GrantRecord{Kind: "relation", Object: publicSchema + "." + table, Privilege: "DELETE"},
			GrantRecord{Kind: "column", Object: publicSchema + "." + table, Column: "id", Privilege: "SELECT"},
		)
	}
	return lost
}

func TestTheRetiredLinkTokensAckDigest(t *testing.T) {
	lost := retiredLinkTokensLost()
	before := make([]GrantRecord, 0, len(lost))
	for _, g := range lost {
		g.Object = g.Object[len(publicSchema+"."):]
		before = append(before, g)
	}
	sorted := narrowedGrants(before, nil)
	for i := range sorted {
		sorted[i].Object = publicSchema + "." + sorted[i].Object
	}
	if len(sorted) != len(lost) {
		t.Fatalf("narrowedGrants kept %d of %d records", len(sorted), len(lost))
	}
	if got := narrowingDigest(sorted); got != retiredLinkTokensAckDigest {
		t.Fatalf("digest of the retired link-token narrowing = %s, want %s", got, retiredLinkTokensAckDigest)
	}
}

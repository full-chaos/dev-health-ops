package postgres

import "testing"

func TestPostureManifestDigestIsStableAcrossCalls(t *testing.T) {
	first := PostureManifestDigest()
	second := PostureManifestDigest()
	if first == "" {
		t.Fatal("PostureManifestDigest() returned an empty digest")
	}
	if first != second {
		t.Fatalf("PostureManifestDigest() is not stable across calls: %s != %s", first, second)
	}
}

func TestPostureManifestDigestChangesWhenAFlagChanges(t *testing.T) {
	base := RolePosture{RequiredTables: []TablePrivilege{{TableName: "widgets", AllowInsert: true}}}
	changed := RolePosture{RequiredTables: []TablePrivilege{{TableName: "widgets", AllowInsert: true, AllowUpdate: true}}}
	if postureManifestDigest(base) == postureManifestDigest(changed) {
		t.Fatal("postureManifestDigest() did not change when AllowUpdate flipped")
	}
}

func TestPostureManifestDigestChangesWhenATableIsAdded(t *testing.T) {
	base := RolePosture{RequiredTables: []TablePrivilege{{TableName: "widgets"}}}
	widened := RolePosture{RequiredTables: []TablePrivilege{{TableName: "widgets"}, {TableName: "gadgets"}}}
	if postureManifestDigest(base) == postureManifestDigest(widened) {
		t.Fatal("postureManifestDigest() did not change when a table was added")
	}
}

// TestPostureManifestDigestDistinguishesTableAndColumnBoundaries proves
// writePostureDigestInput's delimiting is load-bearing: TableName "ab" with
// no column must never hash identically to TableName "a" ColumnName "b".
func TestPostureManifestDigestDistinguishesTableAndColumnBoundaries(t *testing.T) {
	viaTable := RolePosture{RequiredTables: []TablePrivilege{{TableName: "ab"}}}
	viaColumn := RolePosture{ColumnScoped: []ColumnPrivilege{{TableName: "a", ColumnName: "b", Privilege: "SELECT"}}}
	if postureManifestDigest(viaTable) == postureManifestDigest(viaColumn) {
		t.Fatal("postureManifestDigest() collided across a table/column boundary")
	}
}

func TestPostureManifestDigestIsOrderSensitiveAcrossRoles(t *testing.T) {
	posture := RolePosture{RequiredTables: []TablePrivilege{{TableName: "widgets"}}}
	// DomainPosture is always hashed first, then Queue, then Coordinator --
	// the same posture assigned to a different role position must not
	// collide, since CheckPostureManifestLockstep only ever compares the
	// combined digest.
	if postureManifestDigest(posture, RolePosture{}) == postureManifestDigest(RolePosture{}, posture) {
		t.Fatal("postureManifestDigest() is insensitive to which role position a posture occupies")
	}
}

package shadowvenue

import "testing"

// The class table is pinned: the embedded table hashes to ClassTableSHA256, a changed class changes the hash, and every
// row of the embedded table has its own route's class (20 external, 6 destructive, the 7 NO_OUTBOUND rows external).
func TestTheClassTableIsPinnedAndEveryEmbeddedRowMatchesIt(t *testing.T) {
	if got := classDigest(classes()); got != ClassTableSHA256 || len(classes()) != 158 {
		t.Fatalf("class table %s of %d routes, pinned %s", got, len(classes()), ClassTableSHA256)
	}
	changed := map[string]rowClass{}
	for k, c := range classes() {
		changed[k] = c
	}
	changed["POST /api/v1/billing/plans/pull-stripe"] = rowClass{ext: none}
	if classDigest(changed) == ClassTableSHA256 {
		t.Fatal("a changed class keeps the pinned hash")
	}
	for _, row := range mustRows(t, string(DefaultRows)) {
		if row.Control() {
			continue
		}
		if c, ok := classOf(row.Key()); !ok || c.ext != row.Ext || c.destructive != row.Destructive {
			t.Errorf("row %s %s: class %+v, row ext %s destructive %v", row.ID, row.Key(), c, row.Ext, row.Destructive)
		}
	}
	for key := range NoOutboundRows {
		if c, _ := classOf(key); c.ext == none {
			t.Errorf("NO_OUTBOUND %s has no outside service in the class table", key)
		}
	}
}

package shadowvenue

import "testing"

// adminUsersVenueRows are the /admin/users shadow rows (admin.users, mounted admin-or): plain rows in org A that
// write nothing. The GET rows read; PATCH, DELETE and the password route name a missing user; the empty POST bodies are
// refused by the handler's own validation after the guard, so the guard and the shadow see every caller.
var adminUsersVenueRows = []string{
	"GET /api/v1/admin/users",
	"GET /api/v1/admin/users/{user_id}",
	"POST /api/v1/admin/users",
	"PATCH /api/v1/admin/users/{user_id}",
	"POST /api/v1/admin/users/{user_id}/password",
	"DELETE /api/v1/admin/users/{user_id}",
}

func TestTheAdminUsersRowsAreVenueRows(t *testing.T) {
	byKey := map[string]Row{}
	for _, row := range mustRows(t, string(DefaultRows)) {
		if !row.Control() {
			byKey[row.Key()] = row
		}
	}
	for _, key := range adminUsersVenueRows {
		row, ok := byKey[key]
		if !ok {
			t.Errorf("%s is not in the row table", key)
			continue
		}
		if row.Org != "A" || row.Destructive || row.Ext != none {
			t.Errorf("%s: org %s destructive %v ext %s, want org A, not destructive, no outside service", key, row.Org, row.Destructive, row.Ext)
		}
		if c, ok := classOf(key); !ok || c != (rowClass{ext: none}) {
			t.Errorf("%s: pinned class %+v (present %v), want no outside service, not destructive", key, c, ok)
		}
	}
}

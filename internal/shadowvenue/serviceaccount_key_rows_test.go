package shadowvenue

import "testing"

// serviceAccountKeyVenueRows are the service account key shadow rows (admin.users, mounted WrapPlatform): plain rows in
// org A that write nothing. Each names a missing service account or credential; the guard and the shadow see every
// caller before auth-service is asked.
var serviceAccountKeyVenueRows = []string{
	"GET /api/v1/admin/service-accounts/{service_account_id}/keys",
	"POST /api/v1/admin/service-accounts/{service_account_id}/keys",
	"POST /api/v1/admin/service-accounts/{service_account_id}/keys/rotate",
	"POST /api/v1/admin/service-accounts/keys/{credential_id}/revoke",
}

func TestTheServiceAccountKeyRowsAreVenueRows(t *testing.T) {
	byKey := map[string]Row{}
	for _, row := range mustRows(t, string(DefaultRows)) {
		if !row.Control() {
			byKey[row.Key()] = row
		}
	}
	for _, key := range serviceAccountKeyVenueRows {
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

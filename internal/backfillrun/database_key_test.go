package backfillrun

import (
	"bytes"
	"testing"
)

// `dho backfill run` writes scheduled_sync_occurrences, which only the coordinator
// role (or the owner) may write: COORDINATOR_DATABASE_URI comes first, then
// MIGRATION_DATABASE_URI, then POSTGRES_URI (CHAOS-9029).
func TestResolveDatabaseKeyOrder(t *testing.T) {
	const (
		coordinator = "postgresql://coordinator_role:pw@db.invalid:5432/app"
		migration   = "postgresql://migration_role:pw@db.invalid:5432/app"
		domain      = "postgresql://domain_role:pw@db.invalid:5432/app"
	)
	for _, tc := range []struct {
		name   string
		env    map[string]string
		wantDS string
		wantOK bool
	}{
		{"coordinator beats migration and postgres", map[string]string{"COORDINATOR_DATABASE_URI": coordinator, "MIGRATION_DATABASE_URI": migration, "POSTGRES_URI": domain}, coordinator, true},
		{"coordinator beats postgres", map[string]string{"COORDINATOR_DATABASE_URI": coordinator, "POSTGRES_URI": domain}, coordinator, true},
		{"migration beats postgres", map[string]string{"MIGRATION_DATABASE_URI": migration, "POSTGRES_URI": domain}, migration, true},
		{"postgres is the last fallback", map[string]string{"POSTGRES_URI": domain}, domain, true},
		{"nothing set fails", map[string]string{}, "", false},
		{"empty coordinator value is not set", map[string]string{"COORDINATOR_DATABASE_URI": "", "POSTGRES_URI": domain}, domain, true},
	} {
		lookup := func(key string) (string, bool) { v, ok := tc.env[key]; return v, ok }
		var stderr bytes.Buffer
		got, _, ok := resolveDatabase(lookup, &stderr)
		if ok != tc.wantOK || (ok && got.Reveal() != tc.wantDS) {
			t.Errorf("%s: ok=%v dsn matches=%v, want ok=%v dsn=%s", tc.name, ok, got.Reveal() == tc.wantDS, tc.wantOK, tc.wantDS)
		}
	}
}

// The coordinator component form (DEV_HEALTH_PG_COORDINATOR_*) is accepted too, and a
// coordinator setting that is wrong stops the verb instead of falling back to a role
// without the grant.
func TestResolveDatabaseCoordinatorFormsAndErrors(t *testing.T) {
	form := map[string]string{"DEV_HEALTH_PG_COORDINATOR_HOST": "db.invalid", "DEV_HEALTH_PG_COORDINATOR_USER": "coordinator_role", "POSTGRES_URI": "postgresql://domain_role:pw@db.invalid:5432/app"}
	var stderr bytes.Buffer
	got, _, ok := resolveDatabase(func(k string) (string, bool) { v, ok := form[k]; return v, ok }, &stderr)
	if !ok || !bytes.Contains([]byte(got.Reveal()), []byte("coordinator_role")) {
		t.Errorf("component form not used: ok=%v", ok)
	}
	both := map[string]string{"COORDINATOR_DATABASE_URI": "postgresql://c:pw@db.invalid:5432/app", "DEV_HEALTH_PG_COORDINATOR_HOST": "db.invalid", "POSTGRES_URI": "postgresql://domain_role:pw@db.invalid:5432/app"}
	stderr.Reset()
	if _, _, ok := resolveDatabase(func(k string) (string, bool) { v, ok := both[k]; return v, ok }, &stderr); ok || stderr.Len() == 0 {
		t.Errorf("raw and component coordinator settings together must fail with a message: ok=%v", ok)
	}
}

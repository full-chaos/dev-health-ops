package postgres

import "testing"

// TestAPIPostureHasNoDuplicateTable pins the real defect CHAOS-6250 hit:
// apiPosture() declared "organizations" twice (once for the acr
// entitlement route, once for the new admin org CRUD), and
// riverstore.ValidateMigrationOptions -- exercised for real only inside
// internal/testsupport/venueoracle.Start's migrate() step, live-DB-gated --
// rejects a duplicate TableName as ErrMigrationConfiguration with no
// message naming which table. That silent-shaped failure is exactly why
// this table-only invariant gets a fast, no-DB unit test of its own: it
// should never again take a live venue-oracle run (containers, Python,
// migrations) to surface a typo-class bug. A second route area needing
// more privilege on an already-declared table widens that ONE entry in
// place; it never appends a second one.
func TestAPIPostureHasNoDuplicateTable(t *testing.T) {
	seen := map[string]bool{}
	for _, table := range apiPosture().RequiredTables {
		if seen[table.TableName] {
			t.Fatalf("apiPosture() declares %q more than once -- merge the entries, don't append a second", table.TableName)
		}
		seen[table.TableName] = true
	}
}

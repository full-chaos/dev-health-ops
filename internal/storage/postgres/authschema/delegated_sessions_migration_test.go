package authschema

import (
	"strings"
	"testing"
)

// delegatedSessionsMigrationName finds the migration by name, so a renumber at a
// final merge changes no cell.
const delegatedSessionsMigrationName = "delegated_sessions"

// The migration takes auth.principals before the ALTER on auth.sessions, in
// writer order: auth-migrate does not retry a deadlock (rulings on CHAOS-10987, D5).
func TestDelegatedSessionsMigrationTakesPrincipalsFirst(t *testing.T) {
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range migrations {
		if m.Name != delegatedSessionsMigrationName {
			continue
		}
		lock := strings.Index(m.SQL, "LOCK TABLE principals IN SHARE ROW EXCLUSIVE MODE;")
		alter := strings.Index(m.SQL, "ALTER TABLE sessions")
		if lock < 0 || alter < 0 || lock > alter {
			t.Fatalf("LOCK TABLE at %d, ALTER at %d: the lock must come first", lock, alter)
		}
		for _, line := range strings.Split(m.SQL[:lock], "\n") {
			if line != "" && !strings.HasPrefix(line, "--") {
				t.Fatalf("a statement runs before the LOCK TABLE: %q", line)
			}
		}
		return
	}
	t.Fatalf("the lineage lacks the migration %s", delegatedSessionsMigrationName)
}

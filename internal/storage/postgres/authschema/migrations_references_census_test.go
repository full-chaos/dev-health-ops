package authschema

import (
	"regexp"
	"strings"
	"testing"
)

// The auth migration login holds REFERENCES on the ops tables (PublicAnchorPrivilege), so it could create a foreign
// key from an auth table to one of them. No auth migration does today; one that does needs a ruling, and this census
// is where it is stopped. An unqualified REFERENCES target resolves in the auth schema (the migration's search_path);
// a qualified one, or one built by format(), names another schema.
var referencesAnotherSchema = regexp.MustCompile(`(?i)\breferences\s+(%|"?[a-z0-9_]+"?\s*\.)`)

func referencesOutsideSchema(sql string) []string {
	var lines []string
	for _, line := range strings.Split(sql, "\n") {
		if i := strings.Index(line, "--"); i >= 0 {
			line = line[:i]
		}
		if referencesAnotherSchema.MatchString(line) {
			lines = append(lines, strings.TrimSpace(line))
		}
	}
	return lines
}

func TestNoAuthMigrationReferencesAnotherSchema(t *testing.T) {
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range migrations {
		if found := referencesOutsideSchema(m.SQL); len(found) > 0 {
			t.Errorf("migration %04d_%s has a REFERENCES clause that references another schema: %q", m.Version, m.Name, found)
		}
	}
}

func TestTheReferencesCensusSeesWhatItIsMeantToSee(t *testing.T) {
	for _, bad := range []string{
		"ALTER TABLE t ADD FOREIGN KEY (a) REFERENCES public.users (id)",
		`a uuid REFERENCES "public"."users" (id)`,
		"EXECUTE format('ALTER TABLE x ADD FOREIGN KEY (a) REFERENCES %I.%I (id)', 'public', 'users')",
		"a int references other.t(id)",
	} {
		if len(referencesOutsideSchema(bad)) == 0 {
			t.Errorf("not seen: %s", bad)
		}
	}
	for _, good := range []string{
		"a uuid REFERENCES principals (id) ON DELETE RESTRICT",
		"a uuid REFERENCES principals(id)",
		"-- REFERENCES public.users (id) in a comment",
		"GRANT REFERENCES ON TABLE x TO y",
	} {
		if found := referencesOutsideSchema(good); len(found) != 0 {
			t.Errorf("seen in %q: %v", good, found)
		}
	}
}

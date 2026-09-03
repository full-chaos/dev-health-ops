package authschema

import (
	"errors"
	"regexp"
	"sort"
	"strings"
	"testing"
	"testing/fstest"
)

func TestEmbeddedLineageIsWellFormed(t *testing.T) {
	migrations, err := Migrations()
	if err != nil {
		t.Fatalf("Migrations: %v", err)
	}
	if len(migrations) == 0 {
		t.Fatal("no migrations are embedded")
	}
	for index, migration := range migrations {
		if migration.Version != index+1 {
			t.Fatalf("migration %d has version %d, want %d", index, migration.Version, index+1)
		}
		if migration.Name == "" || strings.TrimSpace(migration.SQL) == "" {
			t.Fatalf("migration %d is missing a name or body", migration.Version)
		}
	}
	head, err := HeadVersion()
	if err != nil {
		t.Fatalf("HeadVersion: %v", err)
	}
	if head != len(migrations) {
		t.Fatalf("HeadVersion = %d, want %d", head, len(migrations))
	}
}

// TestLoadMigrationsRejectsMalformedLineages is the positive control for the
// loader: a guard that only ever sees a well-formed lineage has never been
// shown to reject anything.
func TestLoadMigrationsRejectsMalformedLineages(t *testing.T) {
	good := "CREATE TABLE t (id int);"
	cases := []struct {
		name  string
		files fstest.MapFS
	}{
		{"no migrations at all", fstest.MapFS{"migrations/.keep": &fstest.MapFile{Data: []byte("x")}}},
		{"unnumbered filename", fstest.MapFS{"migrations/create_things.sql": &fstest.MapFile{Data: []byte(good)}}},
		{"wrong extension", fstest.MapFS{"migrations/0001_things.txt": &fstest.MapFile{Data: []byte(good)}}},
		{"uppercase in name", fstest.MapFS{"migrations/0001_Things.sql": &fstest.MapFile{Data: []byte(good)}}},
		{"empty body", fstest.MapFS{"migrations/0001_things.sql": &fstest.MapFile{Data: []byte("   \n")}}},
		{"version zero", fstest.MapFS{"migrations/0000_things.sql": &fstest.MapFile{Data: []byte(good)}}},
		{
			name: "gap in the sequence",
			files: fstest.MapFS{
				"migrations/0001_a.sql": &fstest.MapFile{Data: []byte(good)},
				"migrations/0003_c.sql": &fstest.MapFile{Data: []byte(good)},
			},
		},
		{
			name: "does not start at one",
			files: fstest.MapFS{
				"migrations/0002_b.sql": &fstest.MapFile{Data: []byte(good)},
				"migrations/0003_c.sql": &fstest.MapFile{Data: []byte(good)},
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := loadMigrations(testCase.files); !errors.Is(err, ErrInvalidLineage) {
				t.Fatalf("loadMigrations accepted %s (err = %v)", testCase.name, err)
			}
		})
	}

	// The accepting row: a well-formed two-migration lineage must load, or
	// every rejection above could be explained by the loader rejecting
	// everything.
	ok := fstest.MapFS{
		"migrations/0001_a.sql": &fstest.MapFile{Data: []byte(good)},
		"migrations/0002_b.sql": &fstest.MapFile{Data: []byte(good)},
	}
	loaded, err := loadMigrations(ok)
	if err != nil {
		t.Fatalf("loadMigrations rejected a well-formed lineage: %v", err)
	}
	if len(loaded) != 2 || loaded[0].Version != 1 || loaded[1].Version != 2 {
		t.Fatalf("loaded = %+v, want versions 1 and 2 in order", loaded)
	}
}

// The text-derived additive-only guard that used to live here is RETIRED,
// along with the dynamic-SQL refusal that was bolted onto it.
//
// It scanned migration SQL with a regexp and was walked through six times
// across two review rounds. The two that settle the argument: a `DO $$ BEGIN
// EXECUTE 'DROP ' || 'TABLE ...'; END $$;` block drops a table while
// containing no contiguous destructive token, and `ALTER COLUMN x DROP NOT
// NULL` followed by `ALTER COLUMN x TYPE text USING NULL::text` erases every
// value in a column while adding no keyword the pattern looked for. Both are
// legal SQL that means something the scanner could not compute, and no pattern
// over concatenated string fragments can be made to.
//
// The property is now measured where it is actually true or false: the lineage
// is applied ONE MIGRATION AT A TIME against a real PostgreSQL, the catalog is
// snapshotted around each step, and every step must only add -- nothing
// disappears, nothing loses NOT NULL, nothing changes type. See
// TestEveryMigrationStepIsAdditive in additive_integration_test.go, whose
// control replays five destructive migrations (including both evasions above)
// as real DDL and asserts each is caught.
//
// A shape read from the catalog cannot be evaded by how a statement was
// spelled, because it is measured AFTER the statement ran.

// secretRoot names the substrings that make a column name suspicious.
var secretRoot = regexp.MustCompile(`(?i)(password|secret|token|credential|private|key)`)

// approvedSecretColumns is the reviewed allowlist: every column whose name
// trips secretRoot but which demonstrably holds no secret.
//
// It is an explicit list rather than a cleverer pattern because the reviewed
// decision for each column is the point. A pattern that happened to admit them
// all would also silently admit the next column someone adds with a similar
// shape, which is precisely the thing this guard exists to stop.
var approvedSecretColumns = map[string]string{
	"password_hash":       "a one-way verifier, never the password",
	"password_algorithm":  "names the KDF, not the material",
	"password_updated_at": "a timestamp",
	"token_hash":          "a one-way digest; the token is returned once and never stored",
	"secret_hash":         "a one-way digest of a registered credential",
	"credential_class":    "a classification from contracts/auth/v1, not material",
	"public_key_jwk":      "the PUBLIC half; this is what /jwks serves",
	"idempotency_key":     "a dedup key for outbox delivery, not a credential",
	"role_key":            "a role identifier",
	"action_key":          "an action identifier",
	"hash_algorithm":      "names the hash, not the material",
}

// approvedSecretSuffixes are the shapes that are safe by construction.
var approvedSecretSuffixes = []string{"_hash", "_digest", "_ref", "_kind", "_prefix", "_id"}

// columnDefinition accepts ANY leading whitespace. The previous form required
// exactly four spaces, so a two-space or tab-indented `api_token` column was
// valid PostgreSQL that the secret-name rule never examined (codex round 1,
// P2) -- and because the lineage's other columns kept the examined count above
// zero, the vacuity guard did not fire either. Matching more lines is safe:
// extra candidates (a CONSTRAINT line, say) simply do not trip secretRoot.
var columnDefinition = regexp.MustCompile(`(?m)^\s+([a-z_][a-z0-9_]*)\s+`)

// The text-derived secret-column guard that used to live here is RETIRED.
//
// It scanned migration SQL with a regexp and was walked through four times
// across two review rounds: a quoted table name, an exactly-four-spaces
// indentation rule, a one-line `CREATE TABLE t (api_token text NOT NULL);`,
// and `CREATE UNLOGGED TABLE`. Each was a legal way to write a real column
// that the scanner did not see, and its `checked > 0` vacuity proxy could not
// notice, because other tables kept the global count positive.
//
// The property is now checked against the CATALOG of the database the
// migrations actually built -- see TestNoPlaintextCredentialColumnInTheApplied
// Schema in catalog_integration_test.go, with a control that replays all four
// evasions as real DDL and asserts every one is flagged. A column in
// information_schema is a column however it was written, so the evasion class
// is gone rather than enumerated one instance at a time.
//
// secretRoot, approvedSecretColumns and hasApprovedSuffix survive here because
// the RULE is still the right rule; only the thing it reads has changed.

func hasApprovedSuffix(column string) bool {
	for _, suffix := range approvedSecretSuffixes {
		if strings.HasSuffix(column, suffix) {
			return true
		}
	}
	return false
}

// splitStatements is a deliberately naive splitter: this lineage is our own
// SQL, it contains no semicolons inside string literals or dollar-quoted
// bodies, and a real parser would be a dependency and a maintenance surface
// for a test-only helper. If a future migration needs a function body, this
// helper must be revisited rather than worked around.
func splitStatements(sql string) []string {
	parts := strings.Split(sql, ";")
	statements := make([]string, 0, len(parts))
	for _, part := range parts {
		if strings.TrimSpace(stripComments(part)) != "" {
			statements = append(statements, stripComments(part))
		}
	}
	return statements
}

func stripComments(sql string) string {
	lines := strings.Split(sql, "\n")
	kept := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}

// TestEveryCreatedTableHasADeclaredPosture is the coverage claim this manifest
// would otherwise only assert. A table created by a migration but absent from
// RuntimePosture is invisible to the runtime — which may be correct, but must
// be a decision someone wrote down, not an omission.
func TestEveryCreatedTableHasADeclaredPosture(t *testing.T) {
	created := createdTables(t)
	declared := make(map[string]struct{})
	for _, entry := range RuntimePosture() {
		declared[entry.Table] = struct{}{}
	}
	// schema_migrations is auth-migrate's own bookkeeping and is deliberately
	// NOT reachable by the runtime: the runtime never reads or writes the
	// lineage, it only observes that the schema exists (CHAOS-4881's readiness
	// check queries pg_namespace, not this table).
	deliberatelyUnreachable := map[string]struct{}{versionTable: {}}

	var missing []string
	for table := range created {
		if _, ok := declared[table]; ok {
			continue
		}
		if _, ok := deliberatelyUnreachable[table]; ok {
			continue
		}
		missing = append(missing, table)
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf(
			"tables created by the lineage but absent from RuntimePosture: %v\n"+
				"Either grant the runtime what it needs, or record the table in deliberatelyUnreachable with a reason.",
			missing,
		)
	}

	// The inverse: a posture entry naming a table the lineage never creates
	// would grant nothing and silently under-privilege the runtime.
	var phantom []string
	for table := range declared {
		if _, ok := created[table]; !ok {
			phantom = append(phantom, table)
		}
	}
	sort.Strings(phantom)
	if len(phantom) > 0 {
		t.Errorf("RuntimePosture declares tables the lineage never creates: %v", phantom)
	}
}

// createTableStatement accepts the optional double quotes codex round 1 (P2)
// showed slipping past the bare-identifier form: `CREATE TABLE "probe" (...)`
// creates a real table that the previous pattern did not see, so the table
// escaped TestEveryCreatedTableHasADeclaredPosture entirely and no manifest
// decision was ever required for it.
var createTableStatement = regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"?([a-z_][a-z0-9_]*)"?`)

func createdTables(t *testing.T) map[string]struct{} {
	t.Helper()
	migrations, err := Migrations()
	if err != nil {
		t.Fatalf("Migrations: %v", err)
	}
	created := make(map[string]struct{})
	for _, migration := range migrations {
		for _, match := range createTableStatement.FindAllStringSubmatch(stripComments(migration.SQL), -1) {
			created[match[1]] = struct{}{}
		}
	}
	if len(created) == 0 {
		t.Fatal("no CREATE TABLE statements found; the extraction is broken")
	}
	return created
}

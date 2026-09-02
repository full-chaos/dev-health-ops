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

// destructiveStatement matches DDL and DML this lineage must not contain.
//
// CHAOS-4882's scope is "additive-only schema/migrations (no existing row
// moves, no writer changes)". That is a property of the SQL text, so it is
// checked against the SQL text rather than asserted in a PR body — the same
// reasoning that put the secret-column rule below into a test.
var destructiveStatement = regexp.MustCompile(
	`(?is)\b(DROP\s+(TABLE|SCHEMA|COLUMN|INDEX|CONSTRAINT|TYPE)|TRUNCATE|DELETE\s+FROM|UPDATE\s+\w+\s+SET|ALTER\s+TABLE\s+\S+\s+DROP)\b`,
)

func TestLineageIsAdditiveOnly(t *testing.T) {
	migrations, err := Migrations()
	if err != nil {
		t.Fatalf("Migrations: %v", err)
	}
	for _, migration := range migrations {
		for _, statement := range splitStatements(migration.SQL) {
			if match := destructiveStatement.FindString(statement); match != "" {
				t.Errorf(
					"migration %04d_%s contains a destructive statement (%q); Wave 1 is additive only:\n%s",
					migration.Version, migration.Name, strings.TrimSpace(match), strings.TrimSpace(statement),
				)
			}
		}
	}
}

// TestAdditiveGuardCatchesADestructiveStatement is the positive control for
// the guard above. Without it, a regexp that matched nothing at all would
// report every lineage as clean.
func TestAdditiveGuardCatchesADestructiveStatement(t *testing.T) {
	forbidden := []string{
		"DROP TABLE principals;",
		"drop table if exists sessions;",
		"TRUNCATE security_audit_events;",
		"DELETE FROM users WHERE id = 1;",
		"ALTER TABLE users DROP COLUMN email;",
		"UPDATE principals SET kind = 'user';",
	}
	for _, statement := range forbidden {
		if !destructiveStatement.MatchString(statement) {
			t.Errorf("the additive-only guard does not catch %q", statement)
		}
	}
	// And it must not fire on the statements this lineage legitimately uses.
	for _, statement := range []string{
		"CREATE TABLE principals (id uuid PRIMARY KEY);",
		"CREATE UNIQUE INDEX users_email_lower_key ON users (email_lower);",
		"CREATE INDEX sessions_live_idx ON sessions (principal_id) WHERE revoked_at IS NULL;",
	} {
		if destructiveStatement.MatchString(statement) {
			t.Errorf("the additive-only guard falsely rejects %q", statement)
		}
	}
}

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

var columnDefinition = regexp.MustCompile(`(?m)^\s{4}([a-z_][a-z0-9_]*)\s+`)

// TestNoPlaintextCredentialColumn enforces CHAOS-4882's acceptance criterion
// "No plaintext credential/secret column is added anywhere in the new schema".
//
// It reads the criterion as a property of column NAMES, which is what can be
// checked mechanically. A column called `secret_hash` holding a plaintext
// secret would pass; that is a writer's discipline and no schema test can
// reach it. What this does catch is the far likelier failure: someone adding
// `api_token` or `client_secret` because it was convenient.
func TestNoPlaintextCredentialColumn(t *testing.T) {
	migrations, err := Migrations()
	if err != nil {
		t.Fatalf("Migrations: %v", err)
	}
	checked := 0
	for _, migration := range migrations {
		for _, match := range columnDefinition.FindAllStringSubmatch(migration.SQL, -1) {
			column := match[1]
			if !secretRoot.MatchString(column) {
				continue
			}
			checked++
			if _, approved := approvedSecretColumns[column]; approved {
				continue
			}
			if hasApprovedSuffix(column) {
				continue
			}
			t.Errorf(
				"migration %04d_%s declares column %q, whose name suggests credential material.\n"+
					"Store a hash or a reference instead, or add it to approvedSecretColumns with the reason it is safe.",
				migration.Version, migration.Name, column,
			)
		}
	}
	// A guard that inspected zero candidate columns would pass silently while
	// covering nothing — the same vacuous-pass shape CHAOS-4881's body-bound
	// test had.
	if checked == 0 {
		t.Fatal("the secret-column guard examined no candidate columns; its extraction is broken, not the schema clean")
	}
	t.Logf("examined %d candidate column(s) against the secret-name rule", checked)
}

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

var createTableStatement = regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-z_][a-z0-9_]*)`)

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

package audit

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// guardedTables are the two tables whose writes must go through Commit.
//
// A write to either outside this package is the defect CHAOS-4885 exists to
// prevent: a state mutation whose event or audit row is written separately can
// commit without them, and no type can stop it, because a package that does
// not import audit is outside every call graph audit's types govern. That is
// not hypothetical -- this package's own first draft interpolated the schema
// as a bare string, in new code, an hour after the type meant to prevent it
// merged (recorded on CHAOS-4917).
var guardedTables = []string{"auth_outbox_events", "security_audit_events"}

// permittedWriters are the files allowed to INSERT into a guarded table.
//
// THE ALLOWLIST IS ITSELF A HAND-MAINTAINED LIST, which is the defect this
// package's neighbours have spent six review rounds removing. It is held to
// the same standard as the guard: every entry must still match a real write
// (see the second half of the test). An entry whose file stopped writing the
// table is deleted by a failing test rather than left to rot into permission
// nobody re-examined.
var permittedWriters = map[string]string{
	"internal/storage/postgres/authschema/capability_integration_test.go": "the grant probe: it must INSERT to prove the runtime role HOLDS Insert on both tables",
	"internal/storage/postgres/authschema/posture_integration_test.go":    "the posture control: its INSERT exercises the SEQUENCE grant as well as the table grant, so it must write rather than assert",
}

// guardPattern is the ONE definition of what counts as a write, shared by the
// tree walk and by TestGuardPatternRejectsKnownBypasses. Two copies of this
// regex would be two things that must agree and nothing making them.
func guardPattern(table string) *regexp.Regexp {
	return regexp.MustCompile(
		`(?is)INSERT(?:\s|/\*.*?\*/|--[^\n]*\n)+INTO(?:\s|/\*.*?\*/|--[^\n]*\n)+[^;]{0,120}` +
			regexp.QuoteMeta(table))
}

// TestNothingOutsideThisPackageWritesTheGuardedTables scans RAW SOURCE rather
// than the Go AST.
//
// IT COVERS INSERT ONLY, and that is a deliberate boundary rather than an
// oversight -- a guard whose name says "writes" while it checks one verb is
// the false-enforcement shape this lane has spent the day finding. DELETE and
// UPDATE are real runtime capabilities on auth_outbox_events (see
// authschema/grants.go:105), and PR 2's delivery worker will legitimately
// DELETE published rows. So the verb set widens when that lands, and it widens
// HERE, in the guard -- not by adding the reaper to permittedWriters, which
// would be permanent permission granted for a temporary reason.
//
// Also uncovered, from codex round 1: CopyFrom with a pgx.Identifier, a table
// name built by concatenation, and SQL in a non-Go file. Each is a real way to
// write these tables that this scan does not see.
//
// The table name travels inside a SQL string literal, so an AST walk keyed on
// call expressions sees `tx.Exec(ctx, someString)` and learns nothing. The
// string is what carries the table name and the string is what must be read.
func TestNothingOutsideThisPackageWritesTheGuardedTables(t *testing.T) {
	root := repoRoot(t)
	// One pattern per table, whitespace-insensitive, because these statements
	// are formatted across several lines in every real writer.
	patterns := make(map[string]*regexp.Regexp, len(guardedTables))
	for _, table := range guardedTables {
		// Comments and line comments are token separators in PostgreSQL, so
		// `INSERT /* x */ INTO` is valid SQL that `INSERT\s+INTO` does not
		// match. Round 2 found that bypass; TestGuardPatternRejectsKnownBypasses
		// keeps it from coming back.
		patterns[table] = guardPattern(table)
	}

	matched := map[string]bool{}
	var offenders []string

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", ".venv", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		// THIS package is what the rule is about -- "nothing OUTSIDE
		// internal/audit" -- so its own files are permitted by the rule rather
		// than by an allowlist entry. Enumerating them was wrong twice over:
		// audit.go needed permission to contain the mechanism, and widening the
		// pattern for comments made this very test file match its own bypass
		// fixtures. A rule stated as "outside X" should skip X, not list it.
		//
		// The bound, since it is real: a write added INSIDE this package is not
		// caught here. That is in-package discipline, the same residue as the
		// zero-value ValidatedIdentifier -- the guard is over the boundary.
		if rel, err := filepath.Rel(root, path); err == nil &&
			strings.HasPrefix(rel, filepath.Join("internal", "audit")+string(filepath.Separator)) {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		for table, pattern := range patterns {
			if !pattern.Match(body) {
				continue
			}
			if _, permitted := permittedWriters[rel]; permitted {
				matched[rel] = true
				continue
			}
			offenders = append(offenders, rel+" writes "+table)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking the tree: %v", err)
	}

	for _, offender := range offenders {
		t.Errorf("%s — every write to a guarded table must go through audit.Commit, "+
			"so the state mutation, the outbox event and the audit row commit together. "+
			"If this write is legitimate, add it to permittedWriters with the reason.", offender)
	}

	// THE ACCEPTING HALF. Without it a guard that matched nothing — a broken
	// regex, a wrong root, a walk that skipped the tree — passes silently and
	// looks identical to a clean repository.
	for rel, why := range permittedWriters {
		if !matched[rel] {
			t.Errorf("permittedWriters names %s (%s) but nothing there matched a guarded "+
				"INSERT. Either the write moved and this entry is stale permission nobody "+
				"re-examined, or the scan is broken and this test proves nothing.", rel, why)
		}
	}
}

// repoRoot walks up from the test's working directory to the module root.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the working directory")
		}
		dir = parent
	}
}

// TestGuardPatternRejectsKnownBypasses is the permanent red fixture for the
// syntax round 2 slipped past the guard.
//
// The guard's value is entirely in what it MATCHES, and a pattern that misses
// valid SQL is worse than no guard: it reports clean and licenses the belief
// that nothing outside this package writes these tables. Each string below is
// SQL PostgreSQL accepts, so each must match.
func TestGuardPatternRejectsKnownBypasses(t *testing.T) {
	pattern := guardPattern("auth_outbox_events")
	for _, c := range []struct{ name, sql string }{
		{"plain", `INSERT INTO auth.auth_outbox_events (a) VALUES (1)`},
		{"block comment between the tokens", `INSERT /* guard bypass */ INTO auth.auth_outbox_events (a) VALUES (1)`},
		{"line comment between the tokens", "INSERT --x\n INTO auth.auth_outbox_events (a) VALUES (1)"},
		{"newline and tabs", "INSERT\n\t\tINTO auth.auth_outbox_events (a) VALUES (1)"},
		{"comment after INTO", `INSERT INTO /* x */ auth.auth_outbox_events (a) VALUES (1)`},
		{"lowercase", `insert into auth.auth_outbox_events (a) values (1)`},
	} {
		t.Run(c.name, func(t *testing.T) {
			if !pattern.MatchString(c.sql) {
				t.Errorf("the guard does not match valid SQL, so a writer using this form "+
					"would pass unnoticed: %s", c.sql)
			}
		})
	}
	// The accepting row: the pattern must not match text that is merely ABOUT
	// the table, or every comment mentioning it becomes a false offender.
	for _, benign := range []string{
		`// auth_outbox_events is written only through Commit`,
		`SELECT count(*) FROM auth.auth_outbox_events`,
	} {
		if pattern.MatchString(benign) {
			t.Errorf("the guard matched text that writes nothing: %s", benign)
		}
	}
}

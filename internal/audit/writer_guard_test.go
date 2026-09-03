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
	"internal/audit/audit.go": "the two intended inserts; this is the mechanism",
	"internal/storage/postgres/authschema/capability_integration_test.go": "the grant probe: it must INSERT to prove the runtime role HOLDS Insert on both tables",
	"internal/storage/postgres/authschema/posture_integration_test.go":    "the posture control: its INSERT exercises the SEQUENCE grant as well as the table grant, so it must write rather than assert",
}

// TestNothingOutsideThisPackageWritesTheGuardedTables scans RAW SOURCE rather
// than the Go AST.
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
		patterns[table] = regexp.MustCompile(`(?is)INSERT\s+INTO\s+[^;]{0,120}` + regexp.QuoteMeta(table))
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

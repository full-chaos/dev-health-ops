package audit

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
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
	"internal/storage/postgres/authschema/capability_integration_test.go:insertFixtures":                         "the grant probe: it must write to prove the runtime role HOLDS the privilege",
	"internal/storage/postgres/authschema/posture_integration_test.go:TestRuntimeRolePostureAgainstLivePostgres": "the posture control: its INSERT exercises the SEQUENCE grant as well as the table grant",

	// The two halves of the mechanism itself. They are permitted for the
	// reason the package exists, and they are listed rather than exempted by
	// file so that a THIRD write appearing in audit.go still has to argue for
	// itself.
	"internal/audit/audit.go:insertOutboxEvent": "the mechanism: this IS the outbox insert Commit performs inside the transaction",
	"internal/audit/audit.go:insertAuditEvent":  "the mechanism: this IS the audit-row insert Commit performs inside the transaction",

	// The reaper deletes, which is the one verb that can destroy evidence, so
	// its permission is the narrowest and the reason is the specific invariant
	// it upholds -- not "it is the reaper".
	"internal/audit/reaper.go:Reap": "reclaims ALREADY-PUBLISHED events only; published_at IS NOT NULL is the invariant, proven by TestReapNeverDeletesAnUnpublishedEvent",

	// The reaper's tests need rows in states Commit cannot produce -- already
	// published, and published three days ago. One seeding helper does that
	// write, so the permission is one function wide rather than one file wide,
	// and a new write elsewhere in that test file is still caught.
	"internal/audit/reaper_integration_test.go:seedOutboxEvent": "seeds already-published rows the reaper is meant to reclaim; Commit cannot create a published row",

	// The index measurement needs a backlog big enough for the planner to have
	// a real choice, which Commit cannot produce at any reasonable speed.
	"internal/audit/reaper_index_integration_test.go:seedManyOutboxEvents": "bulk-loads the published backlog 0006's index is measured against",
	// explainReap does NOT write: its DELETE runs under EXPLAIN inside a
	// transaction that is rolled back. It is listed anyway because the guard
	// cannot see that and must not assume it -- a permit with a stated reason
	// is the right outcome for a write the scanner cannot prove is inert.
	"internal/audit/reaper_index_integration_test.go:explainReap": "plans the reaper's own DELETE under EXPLAIN in a rolled-back transaction; nothing is written",
}

// guardPattern is the ONE definition of what counts as a write, shared by the
// tree walk and by TestGuardPatternRejectsKnownBypasses. Two copies of this
// regex would be two things that must agree and nothing making them.
// enclosingFunc returns the name of the function containing byte offset off,
// or "" if the offset is at file scope.
//
// WHY BOTH AST AND RAW TEXT. Neither tool can do this alone, and PR 1 hit the
// half of it that bites: an AST walk keyed on call expressions sees
// tx.Exec(ctx, someString) and learns nothing, because the table name lives
// inside a string. Raw text finds the string and knows nothing about which
// function it is in. So the text scan finds the write and the AST answers where
// it is -- each used for the thing it can actually see.
//
// That is what lets the reaper be allowlisted BY FUNCTION. A file-level
// exemption would let any future write in the same file inherit permission
// granted for one function's reason.
func enclosingFunc(fset *token.FileSet, file *ast.File, off int) string {
	name := ""
	ast.Inspect(file, func(n ast.Node) bool {
		decl, ok := n.(*ast.FuncDecl)
		if !ok {
			return true
		}
		start := fset.Position(decl.Pos()).Offset
		end := fset.Position(decl.End()).Offset
		if off >= start && off < end {
			name = decl.Name.Name
			if decl.Recv != nil && len(decl.Recv.List) > 0 {
				name = receiverName(decl.Recv.List[0].Type) + "." + name
			}
		}
		return true
	})
	return name
}

func receiverName(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.StarExpr:
		return receiverName(t.X)
	case *ast.Ident:
		return t.Name
	}
	return "?"
}

// guardedVerbs are the statements that CHANGE a guarded table.
//
// PR 1 covered INSERT only and said so. DELETE and UPDATE are real runtime
// capabilities on auth_outbox_events (authschema/grants.go), and PR 2 adds a
// reaper that deletes published rows -- so the verb set widens HERE, in the
// guard, rather than by allowlisting the reaper's file. A file-level exemption
// would be permanent permission granted for one function's reason.
var guardedVerbs = []string{"INSERT", "DELETE", "UPDATE"}

// sep matches the separators PostgreSQL allows between tokens: whitespace,
// block comments (which nest) and line comments.
const sep = `(?:\s|/\*.*?\*/|--[^\n]*\n)+`

// guardPattern is the ONE definition of what counts as a write to table,
// shared by the tree walk and by its fixtures.
//
// INSERT names its target with INTO, DELETE with FROM, and UPDATE names it
// directly -- so the shapes differ and a single "verb then table" pattern would
// match `SELECT ... FROM outbox WHERE id IN (DELETE ...)` and other text that
// changes nothing.
func guardPattern(table string) *regexp.Regexp {
	t := regexp.QuoteMeta(table)
	return regexp.MustCompile(`(?is)` +
		`(?:INSERT` + sep + `INTO` + sep + `[^;]{0,120}` + t +
		`|DELETE` + sep + `FROM` + sep + `[^;]{0,120}` + t +
		`|UPDATE` + sep + `[^;]{0,120}` + t + sep + `SET)`)
}

// TestNothingOutsideThisPackageWritesTheGuardedTables scans RAW SOURCE rather
// than the Go AST.
//
// IT COVERS INSERT, DELETE AND UPDATE. PR 1 covered INSERT and said so; the
// reaper in PR 2 deletes published rows, so the verb set widened HERE as
// planned rather than by exempting the reaper's file.
//
// The allowlist is keyed file:FUNCTION, which is what makes that distinction
// real. A file-level exemption would let any later write in the same file
// inherit permission granted for one function's reason -- and the fixture
// files already on the list are large test files with many functions.
//
// Also uncovered, from codex round 1: CopyFrom with a pgx.Identifier, a table
// name built by concatenation, and SQL in a non-Go file. Each is a real way to
// write these tables that this scan does not see.
//
// The table name travels inside a SQL string literal, so an AST walk keyed on
// call expressions sees `tx.Exec(ctx, someString)` and learns nothing. The
// string is what carries the table name and the string is what must be read.
// writeVerbPattern finds a write verb regardless of its target, so a write can
// be noticed even when the guard cannot tell WHICH table it hits.
var writeVerbPattern = regexp.MustCompile(`(?is)(?:INSERT` + sep + `INTO|DELETE` + sep + `FROM|UPDATE)` + sep)

// targetWindow is how far past the verb the target is looked for. The real
// statements put it immediately after; 200 bytes covers a line break and a
// comment without reaching the next statement.
const targetWindow = 200

// isForUpdate reports whether the verb at off is the UPDATE of a FOR UPDATE
// locking clause rather than an UPDATE statement. RE2 has no lookbehind, so
// this is done by hand on the bytes before the match.
func isForUpdate(body []byte, off int) bool {
	if !bytes.EqualFold(body[off:min(off+6, len(body))], []byte("UPDATE")) {
		return false
	}
	i := off - 1
	for i >= 0 && (body[i] == ' ' || body[i] == '\t' || body[i] == '\n' || body[i] == '\r') {
		i--
	}
	if i < 2 {
		return false
	}
	return bytes.EqualFold(body[i-2:i+1], []byte("FOR"))
}

func mentionsGuardedTable(b []byte) bool {
	lower := bytes.ToLower(b)
	for _, table := range guardedTables {
		if bytes.Contains(lower, []byte(strings.ToLower(table))) {
			return true
		}
	}
	return false
}

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

	fset := token.NewFileSet()
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
		// The guard's OWN fixtures are strings written to look exactly like
		// writes -- that is what they are for -- so this ONE file is data, not
		// a writer. Everything else in internal/audit is scanned like any other
		// package.
		//
		// This used to skip the whole package, which was wrong once the package
		// acquired a second writer. A blanket skip is a FILE-level exemption
		// wearing a different hat: it let any future write anywhere in
		// internal/audit inherit permission granted for the mechanism's sake.
		// The reaper is the case that made it matter -- a DELETE must not
		// inherit an INSERT's justification just by sharing a directory. So the
		// in-package writers are now enumerated by function like everyone else,
		// and audit.go's helpers carry their own reasons below.
		if rel, err := filepath.Rel(root, path); err == nil &&
			rel == filepath.Join("internal", "audit", "writer_guard_test.go") {
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
		var parsed *ast.File
		for table, pattern := range patterns {
			for _, loc := range pattern.FindAllIndex(body, -1) {
				if parsed == nil {
					parsed, err = parser.ParseFile(fset, path, body, 0)
					if err != nil {
						return err
					}
				}
				key := rel + ":" + enclosingFunc(fset, parsed, loc[0])
				if _, permitted := permittedWriters[key]; permitted {
					matched[key] = true
					continue
				}
				offenders = append(offenders, key+" writes "+table)
			}
		}

		// FAIL CLOSED ON AN UNRESOLVED TARGET.
		//
		// Everything above can only see a table NAME sitting near the verb. A
		// writer that hoists the name into a variable --
		//   table := authschema.Quote(schema) + ".auth_outbox_events"
		//   ... "DELETE FROM " + table + " AS e"
		// -- is invisible to it. That is not a contrived evasion: the reaper was
		// written that way because the expression is used twice in one
		// statement, and it slipped the guard on the first run. Ordinary
		// idiomatic Go is a worse bypass than a clever one, because nobody
		// needs to intend it.
		//
		// So a write whose target this scan CANNOT resolve is reported rather
		// than passed over. The file-level condition is what keeps that honest:
		// repo-wide, "flag every dynamic write" would fire on every DELETE in
		// joboutbox and elsewhere, none of which concern these tables. Inside a
		// file that names a guarded table at all, silence is the wrong default.
		if mentionsGuardedTable(body) {
			seen := map[string]bool{}
			for _, loc := range writeVerbPattern.FindAllIndex(body, -1) {
				// FOR UPDATE is a locking clause, not a write. The
				// table-specific pattern never had to care because it required
				// UPDATE <table> SET; a bare verb does, and the reaper's own
				// FOR UPDATE SKIP LOCKED was the case that proved it -- the
				// first version of this check reported Reap twice, once for the
				// DELETE and once for the lock.
				if isForUpdate(body, loc[0]) {
					continue
				}
				window := body[loc[1]:min(loc[1]+targetWindow, len(body))]
				if mentionsGuardedTable(window) || !bytes.Contains(window, []byte("`+")) {
					continue
				}
				if parsed == nil {
					parsed, err = parser.ParseFile(fset, path, body, 0)
					if err != nil {
						return err
					}
				}
				key := rel + ":" + enclosingFunc(fset, parsed, loc[0])
				if _, permitted := permittedWriters[key]; permitted {
					matched[key] = true
					continue
				}
				if seen[key] {
					continue
				}
				seen[key] = true
				offenders = append(offenders,
					key+" writes a table this guard cannot resolve (the name is built from a variable)")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking the tree: %v", err)
	}

	for _, offender := range offenders {
		t.Errorf("%s — every INSERT, DELETE or UPDATE of a guarded table must go through audit.Commit or an allowlisted function, "+
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

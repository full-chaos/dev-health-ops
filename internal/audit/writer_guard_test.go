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
		// UPDATE accepts an OPTIONAL ALIAS between the table and SET. Round 1
		// found that `UPDATE auth.auth_outbox_events AS e SET published_at = now()`
		// slipped through, because this arm required SET immediately after the
		// table name. Both `AS e` and a bare `e` are legal there.
		`|UPDATE` + sep + `[^;]{0,120}` + t + sep +
		`(?:(?:AS` + sep + `)?[A-Za-z_][A-Za-z0-9_$]*` + sep + `)?SET)`)
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

// fallThroughShapes are statement shapes the TABLE pattern does not resolve, so
// their safety rests ENTIRELY on the fail-closed branch.
//
// Both tests that care about them range over THIS list: the offset test records
// that each falls through, and the verdict table asserts that each is
// nonetheless caught. Sharing the list is what makes those two facts one fact.
// Previously the two tests each named the quoted-identifier case independently
// and merely happened to agree, so a shape added to the fall-through list would
// have acquired no verdict assertion -- a predicate without a fixture, which is
// the defect lane-auth-contracts found in their own harness and then found the
// consequence of in mine.
//
// Adding an entry here is a deliberate edit that forces both tests to cover it.
var fallThroughShapes = []struct {
	name      string
	guarded   string
	unguarded string
}{
	{
		// The UPDATE arm needs whitespace after the table name; a quoted
		// identifier puts a closing quote there instead.
		name:      "update quoted table",
		guarded:   `UPDATE "auth"."auth_outbox_events" AS e SET published_at = now()`,
		unguarded: `UPDATE "auth"."other_table" AS e SET published_at = now()`,
	},
}

// guardFinding is one write the guard objects to, located by byte offset so a
// caller can name the enclosing function.
type guardFinding struct {
	offset     int
	reason     string
	unresolved bool // came from the fail-closed pass rather than a table match
}

// findGuardedWrites IS the guard's decision. The repository walk and the
// statement-shape table below both call it, so neither can drift from the
// other.
//
// It was extracted because of a gap lane-auth-contracts found: the offset test
// RECORDS that a quoted aliased UPDATE falls through to the fail-closed branch,
// and nothing asserted that fail-closed then CATCHES it. I had measured that
// with a throwaway probe, and the probe was gone. The repository walk cannot
// cover it either -- it scans real files and never synthesises a statement, and
// no real file contains that shape -- so weakening the fail-closed branch would
// have left an unguarded write to a guarded table with every test green.
//
// Sharing the function rather than restating the logic is the point: a shape
// table that reimplemented these two passes would be testing a copy, which is
// this package's oldest recurring defect.
func findGuardedWrites(body []byte, patterns map[string]*regexp.Regexp) []guardFinding {
	var out []guardFinding
	covered := map[int]bool{}
	for table, pattern := range patterns {
		for _, loc := range pattern.FindAllIndex(body, -1) {
			covered[loc[0]] = true
			out = append(out, guardFinding{offset: loc[0], reason: "writes " + table})
		}
	}
	if !mentionsGuardedTable(body) {
		return out
	}
	for _, loc := range writeVerbPattern.FindAllIndex(body, -1) {
		if isForUpdate(body, loc[0]) {
			continue
		}
		if covered[loc[0]] {
			continue
		}
		window := body[loc[1]:min(loc[1]+targetWindow, len(body))]
		if !mentionsGuardedTable(window) && !bytes.Contains(window, []byte("`+")) {
			continue
		}
		out = append(out, guardFinding{
			offset: loc[0],
			reason: "writes a guarded table in a form this guard cannot resolve " +
				"(an unrecognised statement shape, or a table name built from a variable)",
			unresolved: true,
		})
	}
	return out
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
		findings := findGuardedWrites(body, patterns)
		if len(findings) == 0 {
			return nil
		}
		parsed, err := parser.ParseFile(fset, path, body, 0)
		if err != nil {
			return err
		}
		reported := map[string]bool{}
		for _, f := range findings {
			key := rel + ":" + enclosingFunc(fset, parsed, f.offset)
			if _, permitted := permittedWriters[key]; permitted {
				matched[key] = true
				continue
			}
			if f.unresolved {
				if reported[key] {
					continue
				}
				reported[key] = true
			}
			offenders = append(offenders, key+" "+f.reason)
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

// TestVerbAndTablePatternsStartAtTheSameOffset turns a stated assumption into a
// measurement.
//
// The fail-closed branch exempts a write when coveredByTablePattern[loc[0]] is
// set, where loc[0] is the VERB pattern's match offset and the map was filled
// from the TABLE pattern's match offsets. That is only sound if the two
// patterns start at the same byte for the same write. I believed they did --
// every alternation of both begins at the verb keyword -- and said so as a
// least-sure line rather than proving it. If it were ever false the failure
// would be a SILENT EXEMPTION, which is precisely the defect round 1 found.
func TestVerbAndTablePatternsStartAtTheSameOffset(t *testing.T) {
	const tbl = "auth_outbox_events"
	table := guardPattern(tbl)

	cases := []struct{ name, sql string }{
		{"insert unaliased", `INSERT INTO auth.auth_outbox_events (kid) VALUES (1)`},
		{"insert comment between tokens", "INSERT /* c */ INTO auth.auth_outbox_events (kid) VALUES (1)"},
		{"insert multi-line", "INSERT\n\tINTO\n\tauth.auth_outbox_events (kid)\n\tVALUES (1)"},
		{"insert quoted table", `INSERT INTO "auth"."auth_outbox_events" (kid) VALUES (1)`},
		{"delete unaliased", `DELETE FROM auth.auth_outbox_events WHERE id = 1`},
		{"delete aliased", `DELETE FROM auth.auth_outbox_events AS e WHERE e.id = 1`},
		{"delete multi-line", "DELETE\n\tFROM\n\tauth.auth_outbox_events\n\tWHERE id = 1"},
		{"update unaliased", `UPDATE auth.auth_outbox_events SET published_at = now()`},
		{"update aliased with AS", `UPDATE auth.auth_outbox_events AS e SET published_at = now()`},
		{"update aliased bare", `UPDATE auth.auth_outbox_events e SET published_at = now()`},
		{"update quoted table", `UPDATE "auth"."auth_outbox_events" AS e SET published_at = now()`},
		{"update multi-line", "UPDATE\n\tauth.auth_outbox_events\n\tAS e\n\tSET published_at = now()"},
	}

	verbsSeen := map[string]int{}
	var tableMisses []string
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			body := []byte(c.sql)
			tl := table.FindIndex(body)
			vl := writeVerbPattern.FindIndex(body)
			if vl == nil {
				t.Fatalf("the VERB pattern does not match %q at all", c.sql)
			}
			if tl == nil {
				// NOT A PASS AND NOT A FAILURE: the table pattern does not
				// resolve this shape, so there is no offset pair to compare and
				// the write is caught by the FAIL-CLOSED branch instead. That is
				// the design -- proximity no longer exempts, so a write the
				// table pattern misses is still reported. Measured: a probe
				// containing this exact statement is flagged as "a guarded table
				// in a form this guard cannot resolve", and the same statement
				// against a non-guarded table is not flagged.
				//
				// It is recorded rather than skipped because the SET of shapes
				// relying on fail-closed is a fact about coverage, and it is
				// asserted against a literal below.
				tableMisses = append(tableMisses, c.name)
				return
			}
			if tl[0] != vl[0] {
				t.Errorf("OFFSETS DIVERGE: table pattern starts at %d, verb pattern at %d, for:\n  %s\n"+
					"coveredByTablePattern is keyed on the verb offset and filled from the table "+
					"offset, so a write like this would be exempted while never having been matched",
					tl[0], vl[0], c.sql)
			}
			verbsSeen[strings.ToUpper(strings.Fields(c.sql)[0])]++
		})
	}

	// EVERY ALTERNATION EXERCISED, or the agreement above is about a subset.
	for _, verb := range []string{"INSERT", "DELETE", "UPDATE"} {
		if verbsSeen[verb] == 0 {
			t.Errorf("no case exercised the %s alternation; the offset agreement is unproven for it", verb)
		}
	}

	// WHICH SHAPES RELY ON FAIL-CLOSED RATHER THAN ON THE TABLE PATTERN.
	//
	// A literal, because this is a coverage fact and deriving it from the run
	// would make the assertion agree with whatever happened. If the table
	// pattern is later widened to resolve a shape, this fails and the list is
	// edited deliberately; if a NEW shape starts falling through, it fails too.
	//
	// update quoted table is here because the table arm requires whitespace
	// after the table name and a quoted identifier puts a closing quote there.
	// The write is still caught -- see the note above -- with a less specific
	// message.
	wantTableMisses := make([]string, 0, len(fallThroughShapes))
	for _, f := range fallThroughShapes {
		wantTableMisses = append(wantTableMisses, f.name)
	}
	if strings.Join(tableMisses, ",") != strings.Join(wantTableMisses, ",") {
		t.Errorf("shapes falling through to fail-closed are %v, expected exactly %v. "+
			"If the table pattern changed on purpose, change this list on purpose",
			tableMisses, wantTableMisses)
	}

	// NEGATIVE CONTROL, and it is also the reason the guard keys on OFFSET
	// rather than on ordinal position. With a non-guarded write first, the verb
	// pattern's FIRST match and the table pattern's FIRST match are different
	// writes, so their offsets MUST differ. If this ever agreed, the check above
	// would be satisfied by construction and would prove nothing.
	decoy := []byte(`UPDATE other_table SET x = 1;
	INSERT INTO auth.auth_outbox_events (kid) VALUES (1)`)
	dt := table.FindIndex(decoy)
	dv := writeVerbPattern.FindIndex(decoy)
	if dt == nil || dv == nil {
		t.Fatal("the decoy matched neither pattern; the negative control is not exercising anything")
	}
	if dt[0] == dv[0] {
		t.Errorf("the negative control did NOT diverge (both at %d), so the offset check above "+
			"cannot distinguish agreement from coincidence", dt[0])
	} else {
		t.Logf("negative control diverges as required: table pattern at %d, verb pattern at %d "+
			"-- pairing by ordinal would be wrong here, which is why the guard pairs by offset",
			dt[0], dv[0])
	}
}

// TestTheGuardsVerdictOnStatementShapes pins the CONSEQUENCE, not just the
// classification.
//
// lane-auth-contracts found the gap: TestVerbAndTablePatternsStartAtTheSameOffset
// RECORDS that a quoted aliased UPDATE falls through to the fail-closed branch,
// and nothing asserted that fail-closed then CATCHES it. I had measured exactly
// that with a throwaway probe -- flagged against a guarded table, not flagged
// against a non-guarded one -- and the probe no longer existed, so the property
// that shape's safety rests on was verified once by an artefact nothing would
// miss.
//
// The repository walk cannot cover it. It scans real files and never
// synthesises a statement, and no file in the tree contains that shape, so the
// walk will never meet it. Weaken the fail-closed branch -- restore a proximity
// exemption, narrow the window, make the covered lookup forgiving -- and the
// quoted aliased UPDATE becomes an unguarded write to a guarded table while
// every existing test stays green.
//
// Every shape appears TWICE, against a guarded table and a non-guarded one,
// because a guard that flags everything satisfies the first row alone.
func TestTheGuardsVerdictOnStatementShapes(t *testing.T) {
	patterns := make(map[string]*regexp.Regexp, len(guardedTables))
	for _, table := range guardedTables {
		patterns[table] = guardPattern(table)
	}

	// The SQL is wrapped in the Go it would really appear in, so the input has
	// the shape the walk actually sees rather than a bare statement.
	inGo := func(sql string) []byte {
		return []byte("package p\n\nfunc w() {\n\t_, _ = tx.Exec(ctx, `" + sql + "`)\n}\n")
	}

	cases := []struct {
		name        string
		sql         string
		wantFlagged bool
	}{
		{"aliased UPDATE, guarded", `UPDATE auth.auth_outbox_events AS e SET published_at = now()`, true},
		{"aliased UPDATE, not guarded", `UPDATE auth.other_table AS e SET published_at = now()`, false},
		{"plain UPDATE, guarded", `UPDATE auth.security_audit_events SET outcome = 'x'`, true},
		{"plain UPDATE, not guarded", `UPDATE auth.other_table SET outcome = 'x'`, false},
		{"INSERT, guarded", `INSERT INTO auth.auth_outbox_events (kid) VALUES (1)`, true},
		{"INSERT, not guarded", `INSERT INTO auth.other_table (kid) VALUES (1)`, false},
		{"DELETE, guarded", `DELETE FROM auth.auth_outbox_events WHERE id = 1`, true},
		{"DELETE, not guarded", `DELETE FROM auth.other_table WHERE id = 1`, false},
		{"SELECT only, guarded table named", `SELECT id FROM auth.auth_outbox_events WHERE id = 1`, false},
		{"SELECT FOR UPDATE, guarded table named", `SELECT id FROM auth.auth_outbox_events FOR UPDATE SKIP LOCKED`, false},
	}

	// EVERY FALL-THROUGH SHAPE GETS BOTH ROWS, taken from the shared list rather
	// than restated, so a shape recorded as relying on fail-closed cannot exist
	// without an assertion that fail-closed catches it.
	for _, f := range fallThroughShapes {
		cases = append(cases,
			struct {
				name        string
				sql         string
				wantFlagged bool
			}{f.name + " (fall-through), guarded", f.guarded, true},
			struct {
				name        string
				sql         string
				wantFlagged bool
			}{f.name + " (fall-through), not guarded", f.unguarded, false},
		)
	}
	if len(fallThroughShapes) == 0 {
		t.Fatal("no fall-through shapes declared; if the table pattern now resolves everything, " +
			"say so deliberately rather than leaving this table asserting nothing about them")
	}

	var flagged, clean int
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := len(findGuardedWrites(inGo(c.sql), patterns)) > 0
			if got != c.wantFlagged {
				t.Errorf("guard flagged=%v, want %v, for:\n  %s", got, c.wantFlagged, c.sql)
			}
			if c.wantFlagged {
				flagged++
			} else {
				clean++
			}
		})
	}

	// Both verdicts must be represented, or the table proves one direction.
	if flagged == 0 {
		t.Error("no case expected a flag; this table cannot detect a guard that stopped guarding")
	}
	if clean == 0 {
		t.Error("no case expected no flag; this table cannot detect a guard that flags everything")
	}
}

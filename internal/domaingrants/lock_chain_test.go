package domaingrants

import (
	"os"
	"path/filepath"
	"testing"
)

// Coverage for the unparsed-LOCK REPORTING CHAIN.
//
// Clause mutation of the re-derived lock model killed 19 of 25, and five of the
// six survivors were one cluster: `parseLockStatements` reporting a refusal was
// tested, but every link AFTER it was not. ParseStatement propagating the refusal,
// the analyzer recording it, and the gate surfacing it could each be deleted with
// every test still green -- so "an unparsed LOCK fails the gate" was a claim about
// four functions of which one was exercised.
//
// The real tree contains no unparsed LOCK (both production strings parse), so the
// whole-repo gate cannot reach this path at all. These tests build the state
// deliberately, ending with an end-to-end run over a temporary module.

// TestParseStatementPropagatesUnparsedLocks covers the link from the parser to
// StatementResult. Killing this required no fixture beyond a bad statement, and
// its absence meant the refusal could be computed and then dropped one frame later.
func TestParseStatementPropagatesUnparsedLocks(t *testing.T) {
	t.Parallel()

	// A shape the parser refuses outright.
	result := ParseStatement("LOCK TABLE")
	if len(result.UnparsedLocks) == 0 {
		t.Error("ParseStatement dropped the parser's refusal: a LOCK that could not be read must " +
			"reach StatementResult, or nothing downstream can report it")
	}

	// A mode word the parser does not know is rejected by the PARSER.
	result = ParseStatement("LOCK TABLE public.t IN NONSENSE MODE")
	if len(result.UnparsedLocks) == 0 {
		t.Error("a lock mode built from unknown words must be recorded as unparsed")
	}

	// A DIFFERENT refusal path, and the one mutation testing found untested: the
	// statement parses cleanly -- every mode word is one the parser accepts -- but
	// the resulting COMBINATION is not a real PostgreSQL mode. `IN UPDATE MODE`
	// uses only legal mode words, so it reaches lockRequirementForMode, which
	// refuses it. Without this case the parser's word check masked the refusal
	// path entirely, and deleting that recording changed nothing.
	result = ParseStatement("LOCK TABLE public.t IN UPDATE MODE")
	if len(result.UnparsedLocks) == 0 {
		t.Error("a syntactically parseable but semantically unrecognised mode must be recorded as " +
			"unparsed; returning a guessed privilege set is how the previous version passed " +
			"silently for a role that happened to hold UPDATE")
	}
	if _, recorded := result.LockRequirements["t"]; recorded {
		t.Error("an unrecognised mode must produce NO requirement at all -- a guess that happens " +
			"to be satisfied is indistinguishable from knowledge")
	}
}

// TestParseLockStatementAcceptsNowaitWithoutModeClause covers `LOCK TABLE t
// NOWAIT`. The grammar test only exercised NOWAIT after an explicit mode, which
// is a different branch: with no IN clause the parser must still accept NOWAIT
// and keep the defaulted mode.
func TestParseLockStatementAcceptsNowaitWithoutModeClause(t *testing.T) {
	t.Parallel()

	statements, unparsed := parseLockStatements("LOCK TABLE public.t NOWAIT")
	if len(unparsed) != 0 {
		t.Fatalf("`LOCK TABLE t NOWAIT` must parse, got unparsed %v", unparsed)
	}
	if len(statements) != 1 {
		t.Fatalf("expected one statement, got %d", len(statements))
	}
	if statements[0].Mode != lockDefaultMode {
		t.Errorf("mode = %q, want the default %q -- NOWAIT is not a mode clause",
			statements[0].Mode, lockDefaultMode)
	}
}

// TestParseLockStatementIgnoresLockAwayFromStatementStart covers the anchoring.
// The earlier cases (`SKIP LOCKED`, `lock_key`) were rejected by the word-boundary
// check alone, so removing the statement-start anchor changed nothing and the
// mutation survived. A whole-word LOCK inside a string literal is the case that
// actually needs the anchor -- and it matters because an unparsed LOCK now FAILS
// the gate, so a false positive here is a false gate failure.
func TestParseLockStatementIgnoresLockAwayFromStatementStart(t *testing.T) {
	t.Parallel()

	for _, sql := range []string{
		"SELECT 'LOCK TABLE public.t' AS example FROM public.other",
		"INSERT INTO public.audit (note) VALUES ('LOCK public.a, public.b')",
	} {
		statements, unparsed := parseLockStatements(sql)
		if len(statements) != 0 || len(unparsed) != 0 {
			t.Errorf("%q: LOCK appears mid-statement (inside a literal), so it must not be read as a "+
				"LOCK statement -- a false positive here is a false GATE FAILURE. got %d parsed, %v unparsed",
				sql, len(statements), unparsed)
		}
	}

	// Control: the same text AT a statement start must still be read, so the
	// anchor is not simply rejecting everything.
	if statements, _ := parseLockStatements("BEGIN; LOCK TABLE public.t IN SHARE MODE"); len(statements) != 1 {
		t.Errorf("a LOCK following `;` is statement-initial and must parse, got %d", len(statements))
	}
}

// TestUnparsedLockReachesTheGateEndToEnd is the link the other tests cannot
// reach: analyzer recording, and incompleteFor surfacing it. Both survived
// mutation because no test drove a real derivation containing an unparsed LOCK --
// and the production tree has none, so the whole-repo gate never exercises it.
func TestUnparsedLockReachesTheGateEndToEnd(t *testing.T) {
	dir := t.TempDir()
	write := func(rel, content string) {
		full := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module fixture\n\ngo 1.25\n")
	write("pgxpool/pgxpool.go", `package pgxpool

type Pool struct{}

func (p *Pool) Exec(ctx interface{}, sql string, args ...interface{}) error { return nil }
`)
	// A LOCK with a mode this analyzer does not recognise, executed on the domain
	// pool. The seed name is what puts it in the domain surface.
	write("main.go", `package main

import "fixture/pgxpool"

type pools struct {
	Domain *pgxpool.Pool
}

func run(p *pools) {
	domainPool := p.Domain
	_ = domainPool.Exec(nil, "LOCK TABLE public.t IN NONSENSE MODE")
}

func main() { run(nil) }
`)

	derived, err := DeriveForRole(dir, RoleDomain)
	if err != nil {
		t.Fatalf("DeriveForRole: %v", err)
	}
	if len(derived.UnparsedLocks) == 0 {
		t.Fatal("the analyzer did not record the unparsed LOCK: the parser's refusal was computed " +
			"and then dropped, so nothing downstream can report it")
	}

	var selectOnly PrivilegeSet
	selectOnly.add(PrivSelect)
	report, err := CompareRoles([]RoleInput{
		{Role: RoleDomain, Derived: derived,
			Truth: truthWith(map[string]PrivilegeSet{"t": selectOnly})},
		{Role: RoleCoordinator,
			Derived: &DerivedSurface{Role: RoleCoordinator, Tables: map[string]*TableSurface{}},
			Truth:   truthWith(map[string]PrivilegeSet{"t": selectOnly})},
	})
	if err != nil {
		t.Fatal(err)
	}

	surfaced := false
	for _, surface := range report.Incomplete {
		if len(surface.UnparsedLocks) > 0 {
			surfaced = true
		}
	}
	if !surfaced {
		t.Error("the unparsed LOCK did not reach IncompleteRoleSurface, so the gate cannot see it")
	}
	// It must reach the advisory report under its own category. The report is the
	// tool's only output now, so a refusal that is computed and not printed is
	// indistinguishable from a statement that parsed cleanly.
	var reported string
	for _, line := range AdvisoryReport(report) {
		if line.Category == CategoryUnparsedLock {
			reported += line.Text + "\n"
		}
	}
	if reported == "" {
		t.Error("an unparsed LOCK must be REPORTED: its target may never enter the derived surface " +
			"at all, so an absence is not a safe default and silence here is a lie by omission")
	}
}

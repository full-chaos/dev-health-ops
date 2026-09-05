package daily

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// TestGovernanceArtifactSQLUsesFINALWherePythonDoes guards the #2229 round-3
// fix: these reads must stay `FINAL`, because argMax is nondeterministic when
// the version column ties.
//
// It carries NO build tag deliberately. The regression is a one-line edit, and
// a guard skipped whenever Docker is absent does not defend a one-line edit.
//
// # THIS IS THE THIRD VERSION, AND THE FIRST TWO BOTH FAILED SILENTLY
//
// v1 embedded a COPY of the scan subquery: reverting the loader would leave the
// test reading FINAL and passing. It could not fail.
//
// v2 asserted against the `governanceArtifactsSQL` const and was
// "mutation-proved" -- but the mutation chosen was deleting the fragment from
// the const, which is exactly the case v2 caught. Proving a guard catches the
// bypass its author thought of is not proof the guard works. The confirmation
// pass found three it did not catch:
//
//   - nothing tied the const to EXECUTION, so a `strings.Replace` at the call
//     site, or an inline query, passed untouched;
//   - the negative control rejected `--` comments only, so a `/* ... */` block
//     comment could satisfy a fragment while the real clause dropped FINAL;
//   - the allowlist check was a single `Contains` against a string that occurs
//     more than once, so changing ONE of the two joins passed.
//
// v3 closes all three. The execution link is the important one: it makes the
// other two moot for any bypass that changes what the loader actually runs.
func TestGovernanceArtifactSQLUsesFINALWherePythonDoes(t *testing.T) {
	// --- 1. EXECUTION LINK -------------------------------------------------
	// Assert the loader really passes THIS const to the driver. Without this
	// the whole file is a statement about a string nobody proved is used.
	executed := captureGovernanceQuery(t)
	if executed != governanceArtifactsSQL {
		t.Fatalf("LoadGovernanceArtifacts did not execute governanceArtifactsSQL verbatim.\n"+
			"Something between the const and conn.Query is rewriting it (a strings.Replace, a\n"+
			"concatenation, or an inline literal). Every assertion below inspects the const, so\n"+
			"they would all pass while production ran different SQL.\nexecuted len=%d, const len=%d",
			len(executed), len(governanceArtifactsSQL))
	}

	// --- 2. STRIP COMMENTS BEFORE INSPECTING -------------------------------
	// A fragment appearing only inside a comment must not satisfy anything.
	// Both SQL comment syntaxes, because v2 handled exactly one of them.
	sql := stripSQLComments(executed)

	// Positive/negative control on the stripper itself: it must remove a
	// planted comment and must NOT remove real SQL. Without this the stripper
	// could return "" and every Contains below would fail confusingly, or
	// return the input unchanged and silently restore the v2 hole.
	if got := stripSQLComments("SELECT a /* FROM x FINAL */ FROM y -- FROM z FINAL\n"); strings.Contains(got, "FINAL") {
		t.Fatalf("stripSQLComments left comment text behind: %q", got)
	}
	if got := stripSQLComments("SELECT a FROM y"); !strings.Contains(got, "FROM y") {
		t.Fatalf("stripSQLComments destroyed real SQL: %q", got)
	}

	// --- 3. THE FINAL READS ------------------------------------------------
	for _, required := range []struct {
		fragment string
		why      string
	}{
		{
			"FROM ci_pipeline_runs FINAL",
			"Python reads this table with FINAL (audit/ai_governance/loaders.py:248). " +
				"argMax is unspecified on a tied version: measured at 60/300/180/120/80 " +
				"disagreeing keys over five identical runs on 400 tied keys",
		},
		{
			"FROM security_alerts FINAL",
			"Python reads this table with FINAL (audit/ai_governance/loaders.py:255)",
		},
		{
			"FROM git_pull_requests FINAL",
			"this dedup is ADDED -- Python joins the table raw -- and its only " +
				"justification is determinism, which argMax does not provide",
		},
	} {
		if !strings.Contains(sql, required.fragment) {
			t.Errorf("the executed query no longer contains %q (outside comments).\n%s",
				required.fragment, required.why)
		}
	}

	// --- 4. BOTH ALLOWLIST JOINS, COUNTED ----------------------------------
	// Python uses argMax for the exact AND wildcard joins (loaders.py:276,:288),
	// so both must stay argMax. v2 used a single Contains, which one surviving
	// occurrence satisfied.
	const allowlistDedup = "argMax(status, computed_at)"
	if got := strings.Count(sql, allowlistDedup); got != 2 {
		t.Errorf("expected exactly 2 occurrences of %q (the exact and wildcard "+
			"ai_tool_allowlist joins), found %d. Python uses argMax for BOTH "+
			"(loaders.py:276,:288); switching either to FINAL diverges from Python "+
			"rather than matching it, and switching only one is the bypass this "+
			"count exists to catch", allowlistDedup, got)
	}
}

// captureGovernanceQuery runs LoadGovernanceArtifacts against a connection that
// records the query and refuses, returning the exact string the loader passed.
func captureGovernanceQuery(t *testing.T) string {
	t.Helper()
	capture := &queryCapturingRows{}
	start := time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	_, err := LoadGovernanceArtifacts(
		context.Background(), capture,
		"00000000-0000-4000-8000-0000000000a0",
		start, start.Add(24*time.Hour-time.Microsecond),
	)
	if err == nil {
		t.Fatal("the capturing connection must make the loader fail; it returned nil error, " +
			"which means the query was never issued and the capture is empty")
	}
	if capture.query == "" {
		t.Fatalf("no query was captured (loader returned %v before querying) -- the execution "+
			"link is not being exercised, so this test proves nothing", err)
	}
	return capture.query
}

// queryCapturingRows implements the loader's narrow repositoryRows capability,
// recording the query and refusing so nothing else has to be faked.
type queryCapturingRows struct{ query string }

func (c *queryCapturingRows) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	c.query = query
	return nil, errors.New("query captured; not executed")
}

var (
	sqlBlockComment = regexp.MustCompile(`(?s)/\*.*?\*/`)
	sqlLineComment  = regexp.MustCompile(`--[^\n]*`)
)

// stripSQLComments removes both SQL comment forms so a fragment that appears
// only in a comment cannot satisfy a fragment assertion.
//
// v2 checked `--` only, and the confirmation pass showed
// `FROM ci_pipeline_runs /* FROM ci_pipeline_runs FINAL */` would pass while
// the real clause had no FINAL.
func stripSQLComments(query string) string {
	return sqlLineComment.ReplaceAllString(sqlBlockComment.ReplaceAllString(query, " "), "")
}

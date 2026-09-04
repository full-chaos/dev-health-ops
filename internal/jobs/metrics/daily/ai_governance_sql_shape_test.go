package daily

import (
	"strings"
	"testing"
)

// TestGovernanceArtifactSQLUsesFINALWherePythonDoes is the cheap half of the
// #2229 round-3 guard, and deliberately carries NO build tag: it runs in every
// unit pass, needs no container, and reddens the moment someone turns these
// reads back into argMax.
//
// # WHY A STATIC ASSERTION AND NOT ONLY A BEHAVIOURAL ONE
//
// The behavioural proof lives in the integration suite
// (TestGovernanceScanDedupIsDeterministicAndMatchesFINAL) and is worth having,
// but it costs a ClickHouse container and only runs where one is available.
// The regression it defends against is a one-line edit. A guard that is
// skipped whenever Docker is absent does not defend a one-line edit.
//
// It asserts against `governanceArtifactsSQL`, the const the loader actually
// executes -- NOT a copy. That distinction is the whole point: an earlier
// version of the sibling integration test embedded its own argMax version of
// the scan subquery, so when the loader moved to FINAL the test carried on
// exercising SQL production no longer ran, and would have passed forever.
//
// # THE MEASUREMENT BEHIND IT
//
// On ClickHouse 26.7.6.57, 400 keys each holding two rows at an identical
// last_synced across 40 unmerged parts with merges stopped, the same query
// returned 60, 300, 180, 120 and 80 disagreeing keys on five consecutive runs.
// argMax returned a mix; FINAL returned the last-inserted value every time.
// They converge only after a merge, and pre-merge is the normal state during
// an active sync -- which is when this job runs.
func TestGovernanceArtifactSQLUsesFINALWherePythonDoes(t *testing.T) {
	for _, required := range []struct {
		fragment string
		why      string
	}{
		{
			"FROM ci_pipeline_runs FINAL",
			"Python reads this table with FINAL (audit/ai_governance/loaders.py:248). " +
				"argMax is unspecified on a tied version, so a same-millisecond " +
				"success/pending pair can suppress MISSING_SECURITY_SCAN nondeterministically",
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
		if !strings.Contains(governanceArtifactsSQL, required.fragment) {
			t.Errorf("governanceArtifactsSQL no longer contains %q.\n%s", required.fragment, required.why)
		}
	}

	// The allowlist joins must STAY argMax. Python uses argMax there
	// (loaders.py:276 and :288), so "fixing" them to FINAL would be a
	// divergence dressed as consistency. The tie Python inherits there is
	// recorded in RISK-NOTES, not corrected here.
	if !strings.Contains(governanceArtifactsSQL, "argMax(status, computed_at)") {
		t.Error("the ai_tool_allowlist joins must stay argMax(status, computed_at): Python uses " +
			"argMax there (loaders.py:276,:288), so FINAL would diverge from Python rather than " +
			"match it. Do not sweep these into FINAL for consistency")
	}

	// Negative control: the fragments above must not be satisfiable by a
	// comment. If the query ever grows a `--` line mentioning one of them, this
	// test would pass on the comment alone -- exactly the class of false
	// negative that has bitten this lane three times today.
	for _, line := range strings.Split(governanceArtifactsSQL, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "--") {
			continue
		}
		if strings.Contains(trimmed, "FINAL") && strings.Contains(trimmed, "FROM ") {
			t.Errorf("a COMMENT in governanceArtifactsSQL contains a `FROM ... FINAL` phrase (%q). "+
				"The assertions above match anywhere in the string, so a comment like this can "+
				"satisfy them while the real read says argMax. Reword the comment", trimmed)
		}
	}
}

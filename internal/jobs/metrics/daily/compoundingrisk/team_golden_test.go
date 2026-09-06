package compoundingrisk

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

const teamGoldenOrgID = "org-compounding-team-golden"

// teamGoldenRepoID mirrors the Python generator's `_repo(suffix)` -- a
// deterministic UUID keyed by a two-digit suffix, so the fixture's repo_ids
// are stable and readable in a diff.
func teamGoldenRepoID(suffix string) string {
	return goldenRepoStem + suffix
}

// teamGoldenCases mirrors generate_daily_compounding_risk_team_python_golden.py's
// CASES list -- same order (repo_id suffix ascending, load-bearing: see that
// generator's module docstring and BuildTeamRows' own doc comment on why
// summation order must match), same team assignments, same values. A team of
// "" means "present in repo_inputs but unresolved" (excluded from
// repoToTeam), matching CompoundingRiskTeamExecutor's own shape for a repo
// teamresolve could not attribute.
func teamGoldenCases() []struct {
	repoIDSuffix string
	team         string
	inputs       Inputs
} {
	return []struct {
		repoIDSuffix string
		team         string
		inputs       Inputs
	}{
		{"01", "team-alpha", Inputs{ptr(0.10), ptr(0.04), ptr(10.0), ptr(0.30), ptr(0.20), ptr(3.0)}},
		{"02", "team-alpha", Inputs{ptr(0.20), ptr(0.06), ptr(14.0), ptr(0.40), ptr(0.30), ptr(5.0)}},
		{"03", "", Inputs{ptr(0.99), ptr(0.99), ptr(99.0), ptr(0.99), ptr(0.99), ptr(9.0)}},
		{"04", "team-beta", Inputs{ptr(0.05), ptr(0.02), ptr(8.0), ptr(0.15), ptr(0.10), ptr(2.0)}},
		{"05", "team-beta", Inputs{nil, ptr(0.03), ptr(9.0), ptr(0.18), ptr(0.12), ptr(4.0)}},
		{"06", "team-beta", Inputs{ptr(0.11), ptr(0.04), ptr(11.0), ptr(0.22), ptr(0.14), ptr(6.0)}},
		{"07", "team-gamma", Inputs{ptr(0.08), ptr(0.03), ptr(6.0), nil, nil, nil}},
		{"08", "team-gamma", Inputs{ptr(0.09), ptr(0.05), ptr(7.0), nil, nil, nil}},
		{"09", "team-solo", Inputs{ptr(0.17), ptr(0.07), ptr(13.0), ptr(0.28), ptr(0.19), ptr(1.0)}},
	}
}

func loadTeamGolden(t *testing.T) goldenDocument {
	t.Helper()
	path := filepath.Join(
		repositoryRoot(t), "tests", "fixtures", "daily_compounding_risk_team_python_golden.json",
	)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document goldenDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return document
}

// TestBuildTeamRowsMatchesFrozenPythonGolden is the differential oracle for
// CHAOS-5084's team scope: every team row BuildTeamRows produces, compared
// field-for-field against the frozen output of the REAL Python
// _build_team_rows. Comparison is EXACT for the same reason
// TestComputeMatchesFrozenPythonGolden's is: a tolerance would hide the FMA
// contraction and mean-summation-order classes this port exists to avoid,
// both of which can move a team across a severity boundary.
func TestBuildTeamRowsMatchesFrozenPythonGolden(t *testing.T) {
	golden := loadTeamGolden(t)
	cases := teamGoldenCases()

	repoInputs := make([]RepoInputs, 0, len(cases))
	repoToTeam := make(map[string]string, len(cases))
	for _, testCase := range cases {
		repoID := teamGoldenRepoID(testCase.repoIDSuffix)
		repoInputs = append(repoInputs, RepoInputs{RepoID: repoID, Inputs: testCase.inputs})
		if testCase.team != "" {
			repoToTeam[repoID] = testCase.team
		}
	}

	records := BuildTeamRows(
		goldenDay(), teamGoldenOrgID, repoInputs, repoToTeam, goldenStamp(),
		DefaultWeights, DefaultThresholds, DefaultReferences,
	)

	// Positive control: the corpus has 4 distinct teams (alpha, beta, gamma,
	// solo) plus one deliberately unresolved repo. If this count is wrong the
	// comparison below could pass by coincidence (e.g. a phantom team or a
	// dropped team both under-produce, and comparing against a
	// shorter-than-intended golden would not catch the shape being wrong for
	// the wrong reason).
	const wantTeams = 4
	if len(records) != wantTeams {
		t.Fatalf("BuildTeamRows returned %d team rows, want %d (team-alpha, team-beta, "+
			"team-gamma, team-solo) -- got scope_ids %v", len(records), wantTeams, scopeIDs(records))
	}
	if len(golden.Records) != len(records) {
		t.Fatalf(
			"frozen golden has %d records but BuildTeamRows produced %d -- the two "+
				"corpora have drifted; regenerate with\n"+
				"    python tests/fixtures/generate_daily_compounding_risk_team_python_golden.py",
			len(golden.Records), len(records),
		)
	}

	for index, record := range records {
		live := render(record)
		want := golden.Records[index]
		if !reflect.DeepEqual(live, want) {
			t.Errorf("team row %d (scope_id=%s):\n got %+v\nwant %+v", index, record.ScopeID, live, want)
		}
	}
}

func scopeIDs(records []Record) []string {
	ids := make([]string, len(records))
	for i, r := range records {
		ids[i] = r.ScopeID
	}
	return ids
}

// TestBuildTeamRowsExcludesUnresolvedRepoWithoutAffectingOthers isolates the
// negative-control repo (suffix "03" in the golden corpus, present in
// repoInputs but absent from repoToTeam) from the golden comparison above --
// its own assertion, so a future corpus edit that accidentally resolves it
// (making the "unresolved" case stop testing what it claims to) fails loudly
// here rather than silently passing the aggregate golden comparison.
func TestBuildTeamRowsExcludesUnresolvedRepoWithoutAffectingOthers(t *testing.T) {
	unresolved := RepoInputs{RepoID: "repo-unresolved", Inputs: Inputs{
		ReworkChurn: ptr(0.99), ComplexityDelta: ptr(0.99), ReviewLatencyP90H: ptr(99.0),
		SingleOwnerRatio: ptr(0.99), OwnershipGini: ptr(0.99),
	}}
	member := RepoInputs{RepoID: "repo-member", Inputs: Inputs{
		ReworkChurn: ptr(0.10), ComplexityDelta: ptr(0.04), ReviewLatencyP90H: ptr(10.0),
		SingleOwnerRatio: ptr(0.30), OwnershipGini: ptr(0.20),
	}}
	repoToTeam := map[string]string{"repo-member": "solo-team"} // "repo-unresolved" deliberately absent

	records := BuildTeamRows(
		goldenDay(), teamGoldenOrgID, []RepoInputs{unresolved, member}, repoToTeam, goldenStamp(),
		DefaultWeights, DefaultThresholds, DefaultReferences,
	)
	if len(records) != 1 {
		t.Fatalf("got %d team rows, want exactly 1 (the unresolved repo must not become its own team): %v",
			len(records), scopeIDs(records))
	}
	if records[0].ScopeID != "solo-team" {
		t.Fatalf("team row scope_id = %q, want %q", records[0].ScopeID, "solo-team")
	}
	// The team's mean must equal the SOLE resolved member's own values --
	// proof the unresolved repo's 0.99s never entered the mean.
	if records[0].ReworkChurn == nil || *records[0].ReworkChurn != 0.10 {
		t.Fatalf("team rework_churn = %v, want 0.10 (the unresolved repo's 0.99 must not have "+
			"contributed to this team's mean)", records[0].ReworkChurn)
	}
}

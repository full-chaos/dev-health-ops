package complexity

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func intp(n int) *int { return &n }

func TestMissingCount(t *testing.T) {
	cases := []struct {
		name                 string
		totalFiles, nonEmpty int
		want                 int
	}{
		{"nothing missing", 10, 10, 0},
		{"some missing", 10, 4, 6},
		{"non_empty exceeds total (should not clamp negative)", 4, 10, 0},
		{"both zero", 0, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := MissingCount(tc.totalFiles, tc.nonEmpty); got != tc.want {
				t.Errorf("MissingCount(%d,%d) = %d, want %d", tc.totalFiles, tc.nonEmpty, got, tc.want)
			}
		})
	}
}

func TestRemainingAfter(t *testing.T) {
	cases := []struct {
		name      string
		remaining *int
		consumed  int
		want      *int
	}{
		{"unbounded stays unbounded regardless of consumed", nil, 500, nil},
		{"budget partially consumed", intp(100), 30, intp(70)},
		{"budget exactly exhausted", intp(30), 30, intp(0)},
		{"consumed exceeds budget clamps to zero, not negative", intp(10), 50, intp(0)},
		{"zero consumed leaves budget unchanged", intp(10), 0, intp(10)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := RemainingAfter(tc.remaining, tc.consumed)
			if !intPtrEqual(got, tc.want) {
				t.Errorf("RemainingAfter(%v,%d) = %v, want %v", derefOrNil(tc.remaining), tc.consumed, derefOrNil(got), derefOrNil(tc.want))
			}
		})
	}
}

func TestShouldAttemptBlameFallback(t *testing.T) {
	cases := []struct {
		name      string
		missing   int
		remaining *int
		want      bool
	}{
		{"nothing missing never attempts, even with unlimited budget", 0, nil, false},
		{"missing files, unbounded budget attempts", 5, nil, true},
		{"missing files, positive remaining attempts", 5, intp(1), true},
		{"missing files, but budget is exactly exhausted -- refuses", 5, intp(0), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ShouldAttemptBlameFallback(tc.missing, tc.remaining); got != tc.want {
				t.Errorf("ShouldAttemptBlameFallback(%d,%v) = %v, want %v", tc.missing, derefOrNil(tc.remaining), got, tc.want)
			}
		})
	}
}

func TestBlameUnusable(t *testing.T) {
	cases := []struct {
		name                 string
		filteredMissingPaths int
		hasBlameLineText     bool
		want                 bool
	}{
		{"nothing left to recover is not a failure", 0, false, false},
		{"paths to recover, blame usable -- not unusable", 3, true, false},
		{"paths to recover, blame NOT usable -- unusable", 3, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := BlameUnusable(tc.filteredMissingPaths, tc.hasBlameLineText); got != tc.want {
				t.Errorf("BlameUnusable(%d,%v) = %v, want %v", tc.filteredMissingPaths, tc.hasBlameLineText, got, tc.want)
			}
		})
	}
}

func TestEmptyGitFilesBlameUnusable(t *testing.T) {
	cases := []struct {
		name             string
		totalFiles       int
		hasBlameLineText bool
		want             bool
	}{
		{"a genuinely empty repo is not a blame failure", 0, false, false},
		{"files exist, blame usable", 5, true, false},
		{"files exist, blame NOT usable -- unusable", 5, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := EmptyGitFilesBlameUnusable(tc.totalFiles, tc.hasBlameLineText); got != tc.want {
				t.Errorf("EmptyGitFilesBlameUnusable(%d,%v) = %v, want %v", tc.totalFiles, tc.hasBlameLineText, got, tc.want)
			}
		})
	}
}

// TestPlanNonEmptyBranchMaxFilesCapsTheWholeRepoNotJustGitFiles is the
// scenario the review flagged this oracle for by name: max_files must cap
// the TOTAL file count across BOTH phases, not just the git_files read. A
// plan that reset the budget for the blame phase (instead of threading the
// already-reduced `remaining` through) would silently let a repo exceed
// max_files whenever files were split across git_files and git_blame.
func TestPlanNonEmptyBranchMaxFilesCapsTheWholeRepoNotJustGitFiles(t *testing.T) {
	// max_files=10, git_files returns all 10 -- nothing left for blame, even
	// though 5 files are still missing.
	plan := PlanNonEmptyBranch(intp(10), 15, 10, 10)
	if plan.AttemptBlameFallback {
		t.Fatalf("budget exhausted by git_files alone must refuse the blame phase, got plan=%+v", plan)
	}
	if !intPtrEqual(plan.BlameLimit, intp(0)) {
		t.Fatalf("BlameLimit should be the exhausted budget (0), got %v", derefOrNil(plan.BlameLimit))
	}

	// Same shape, but git_files only partially filled the budget -- the
	// blame phase gets exactly what's left, not a fresh max_files.
	plan2 := PlanNonEmptyBranch(intp(10), 15, 10, 6)
	if !plan2.AttemptBlameFallback {
		t.Fatalf("leftover budget with real missing files must attempt blame, got plan=%+v", plan2)
	}
	if !intPtrEqual(plan2.BlameLimit, intp(4)) {
		t.Fatalf("BlameLimit should be the leftover budget (10-6=4), got %v", derefOrNil(plan2.BlameLimit))
	}
}

func TestPlanEmptyBranch(t *testing.T) {
	if p := PlanEmptyBranch(intp(5), true); !p.AttemptBlameFallback || p.NonEmptyBranch {
		t.Errorf("PlanEmptyBranch(5,true) = %+v, want AttemptBlameFallback=true, NonEmptyBranch=false", p)
	}
	if p := PlanEmptyBranch(nil, false); p.AttemptBlameFallback {
		t.Errorf("PlanEmptyBranch(nil,false) = %+v, want AttemptBlameFallback=false", p)
	}
}

func intPtrEqual(a, b *int) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func derefOrNil(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}

// TestBlameMaxFilesArithmeticMatchesLivePython re-derives missing/remaining/
// attempt_blame from the REAL job_complexity_db.py expressions (via the
// oracle script) and compares them to PlanNonEmptyBranch, so a transcription
// slip in fileselect.go shows up as a live mismatch rather than being
// checked only against this file's own restatement of the formula. Skipped
// unless the live-oracle gate is set, matching this package's other oracle
// tests.
func TestBlameMaxFilesArithmeticMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE") == "" {
		t.Skip("live Python oracle runs only through the uncached live-oracle gate")
	}

	type caseIn struct {
		MaxFiles         *int `json:"max_files"`
		TotalFiles       int  `json:"total_files"`
		NonEmpty         int  `json:"non_empty"`
		GitFilesReturned int  `json:"git_files_returned"`
	}
	cases := []caseIn{
		{nil, 100, 40, 40},
		{intp(50), 100, 40, 40},
		{intp(40), 100, 40, 40},
		{intp(10), 15, 10, 10},
		{intp(10), 15, 10, 6},
		{intp(0), 5, 0, 0},
		{intp(1000000), 3, 3, 3},
	}
	payload, err := json.Marshal(cases)
	if err != nil {
		t.Fatalf("marshal cases: %v", err)
	}

	python := os.Getenv("DEV_HEALTH_PYTHON")
	if python == "" {
		python = "python3"
	}
	script := filepath.Join("testdata", "python_blame_maxfiles_oracle.py")
	cmd := exec.Command(python, script)
	cmd.Stdin = bytes.NewReader(payload)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("oracle failed: %v\n%s", err, output)
	}

	var got []struct {
		Missing      int  `json:"missing"`
		Remaining    *int `json:"remaining"`
		AttemptBlame bool `json:"attempt_blame"`
	}
	if err := json.Unmarshal(output, &got); err != nil {
		t.Fatalf("parse oracle output: %v\n%s", err, output)
	}
	if len(got) != len(cases) {
		t.Fatalf("oracle returned %d results for %d cases", len(got), len(cases))
	}

	for i, c := range cases {
		plan := PlanNonEmptyBranch(c.MaxFiles, c.TotalFiles, c.NonEmpty, c.GitFilesReturned)
		missing := MissingCount(c.TotalFiles, c.NonEmpty)
		want := got[i]
		if missing != want.Missing {
			t.Errorf("case %d: missing = %d, python %d", i, missing, want.Missing)
		}
		if !intPtrEqual(plan.BlameLimit, want.Remaining) {
			t.Errorf("case %d: remaining = %v, python %v", i, derefOrNil(plan.BlameLimit), derefOrNil(want.Remaining))
		}
		if plan.AttemptBlameFallback != want.AttemptBlame {
			t.Errorf("case %d: attempt_blame = %v, python %v", i, plan.AttemptBlameFallback, want.AttemptBlame)
		}
	}
}

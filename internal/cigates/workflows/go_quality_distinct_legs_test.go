package workflows_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A matrix padded with duplicate legs must not reach the leg count: the leg selector refuses a list with a repeated leg name, and the
// queue-tested check counts DISTINCT successful job names, not jobs.

// paddedLegs is eleven entries with six distinct names: static, test 1 and 2 twice each, race 2, 3 and 4 twice each, race 4 a third time.
var paddedLegNames = []string{
	"static", "test-1-of-4", "test-1-of-4", "test-2-of-4", "test-2-of-4",
	"race-2-of-5", "race-2-of-5", "race-3-of-5", "race-3-of-5", "race-4-of-5", "race-4-of-5",
}

func paddedLegsJSON() string {
	var entries []string
	for _, n := range paddedLegNames {
		switch {
		case n == "static":
			entries = append(entries, `{"name":"static","args":"static"}`)
		case strings.HasPrefix(n, "test-"):
			entries = append(entries, `{"name":"`+n+`","args":"test `+n[5:6]+` 4"}`)
		default:
			entries = append(entries, `{"name":"`+n+`","args":"race `+n[5:6]+` 5","heavy":true,"pool":true}`)
		}
	}
	return "[" + strings.Join(entries, ",") + "]"
}

func TestTheLegSelectorRefusesADuplicateLegName(t *testing.T) {
	root := repoRoot(t)
	f := filepath.Join(t.TempDir(), "legs.json")
	if err := os.WriteFile(f, []byte(paddedLegsJSON()), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"merge_group", "false"}, {"push", "false"}, {"pull_request", "true"}, {"pull_request", "false"}} {
		out, errs, code := runLegsScript(t, root, append(args, f)...)
		if code != 3 || strings.Contains(out, "static") {
			t.Errorf("%v: exit %d, stdout %q: a list with a repeated leg name must fail the plan job with exit 3", args, code, out)
		}
		if !strings.Contains(errs, "duplicate") || !strings.Contains(errs, "test-1-of-4") {
			t.Errorf("%v: stderr %q must name the duplicate leg (test-1-of-4)", args, errs)
		}
	}
}

func TestTheQueueHelperCountsDistinctLegNamesNotJobs(t *testing.T) {
	good := qualityRunsJSON(qualitySha, "merge_group", qualityRepo, "success")
	var padded []qualityLeg
	for _, n := range paddedLegNames {
		padded = append(padded, qualityLeg{name: n, job: "success", step: "success"})
	}
	if len(padded) != 11 {
		t.Fatalf("the padded list has %d entries, want 11", len(padded))
	}
	got, _ := runQualityHelper(t, qualitySha, good, qualityJobsJSON(padded), false, "")
	if got {
		t.Error("eleven successful jobs with six distinct names were accepted as a fully run queue run")
	}
	got, _ = runQualityHelper(t, qualitySha, good, qualityJobsJSON(qualityLegs(nil)), false, filepath.Join(t.TempDir(), "run-id"))
	if !got {
		t.Error("eleven successful jobs with eleven distinct names were refused")
	}
}

func TestTheShippedLegListHasElevenDistinctLegs(t *testing.T) {
	seen := map[string]bool{}
	legs := goQualityLegList(t, repoRoot(t))
	for _, e := range legs {
		n, _ := e["name"].(string)
		if n == "" || seen[n] {
			t.Errorf("ci/go_quality_legs.json: leg name %q is empty or repeated", n)
		}
		seen[n] = true
	}
	if len(legs) != 11 || len(seen) != 11 {
		t.Errorf("ci/go_quality_legs.json has %d entries and %d distinct names, want 11 and 11", len(legs), len(seen))
	}
}

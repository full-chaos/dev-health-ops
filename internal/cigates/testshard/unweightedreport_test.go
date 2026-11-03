package testshard_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// CHAOS-11682: on merge_group the unweighted-slow guard only reports (a package the entry never touched must not eject it); on every
// other event it still fails, so the author of a pull request can add the row. An unusable log stays a failure in every mode.

const reportOnlyEnv = "DEV_HEALTH_GO_UNWEIGHTED_REPORT_ONLY"

func reportEnv(summary string) []string {
	return []string{reportOnlyEnv + "=1", "GITHUB_STEP_SUMMARY=" + summary}
}

func TestMergeGroupReportsAnUnweightedSlowPackageAndKeepsTheLegGreen(t *testing.T) {
	t.Parallel()
	pkg, key := unweightedPackage(t, 1, shardCount)
	summary := filepath.Join(t.TempDir(), "summary.md")
	log := coverLog(t, 1, shardCount, map[string]string{pkg: "47.600s"})
	errs, err := unweightedGuard(t, reportEnv(summary), 1, log)
	if err != nil {
		t.Fatalf("report mode failed on an unweighted package at 47.6 s: %v\n%s", err, errs)
	}
	for _, want := range []string{"::warning::", "unweighted-slow-package: " + pkg + " took 47.6s", "\n" + key + "\t48\n"} {
		if !strings.Contains(errs, want) {
			t.Errorf("the report lacks %q: %s", want, errs)
		}
	}
	b, rerr := os.ReadFile(summary)
	if rerr != nil || !strings.Contains(string(b), key) {
		t.Errorf("the step summary does not name %q: err=%v %q", key, rerr, b)
	}
	if errs, err := unweightedGuard(t, []string{reportOnlyEnv + "="}, 1, log); err == nil || !strings.Contains(errs, "\n"+key+"\t48\n") {
		t.Errorf("with the variable empty (pull_request, push, local) the guard must still fail with the row: err=%v\n%s", err, errs)
	}
}

func TestReportModeStillFailsWhenTheLogCannotProveCoverage(t *testing.T) {
	t.Parallel()
	full := coverLog(t, 1, shardCount, nil)
	lines := strings.SplitAfter(full, "\n")
	for _, c := range []struct{ name, log, want string }{
		{"a zero-byte log", "", "holds no package summary line"},
		{"a truncated log", strings.Join(lines[:len(lines)/2], ""), "no summary line for"},
		{"foreign packages only", "ok  \tgithub.com/x/y\t3s\n", "no summary line for"},
	} {
		errs, err := unweightedGuard(t, reportEnv(filepath.Join(t.TempDir(), "s.md")), 1, c.log)
		if err == nil || !strings.Contains(errs, c.want) {
			t.Errorf("%s in report mode: err=%v, want a failure with %q\n%s", c.name, err, c.want, errs)
		}
	}
	if errs, err := unweightedGuardAt(t, reportEnv(""), 1, shardCount, filepath.Join(t.TempDir(), "absent.log")); err == nil || !strings.Contains(errs, "readable file") {
		t.Errorf("a missing log in report mode: err=%v\n%s", err, errs)
	}
	zero := writeLog(t, "")
	if _, err := unweightedGuardAt(t, append(reportEnv(""), "GO_TEST_WEIGHTS="+zero), 1, shardCount, writeLog(t, full)); err == nil {
		t.Error("an empty weights file passed in report mode")
	}
}

func TestReportModeNamesANestedModulePackageWithoutFailing(t *testing.T) {
	t.Parallel()
	out, errs, err := checkGo(t, nil, "test-shard-plan", "1", fmt.Sprint(shardCount))
	if err != nil {
		t.Fatalf("test-shard-plan: %v\n%s", err, errs)
	}
	nested := ""
	for _, l := range strings.Split(out, "\n") {
		if f := strings.Split(l, "\t"); len(f) == 3 && f[0] != "." {
			nested = f[1]
			break
		}
	}
	if nested == "" {
		t.Skip("shard 1 holds no nested-module package")
	}
	errs, err = unweightedGuard(t, reportEnv(filepath.Join(t.TempDir(), "s.md")), 1, coverLog(t, 1, shardCount, map[string]string{nested: "45.000s"}))
	if err != nil {
		t.Fatalf("report mode failed on a nested-module package: %v\n%s", err, errs)
	}
	if !strings.Contains(errs, "::warning::") || !strings.Contains(errs, "nested-module-slow-package: "+nested) {
		t.Errorf("the report lacks the warning or the package: %s", errs)
	}
}

// The workflow sets the variable on merge_group only: report for merge_group, fail for pull_request, push, schedule and dispatch.
func TestOnlyMergeGroupSetsTheUnweightedReportOnlyEnvironment(t *testing.T) {
	t.Parallel()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), ".github", "workflows", "go-quality.yml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)
	const want = reportOnlyEnv + ": ${{ github.event_name == 'merge_group' && '1' || '' }}"
	if strings.Count(text, reportOnlyEnv) != 1 || !strings.Contains(text, want) {
		t.Fatalf("go-quality.yml must set %q exactly once", want)
	}
	i, j := strings.Index(text, want), strings.Index(text, "bash ci/check_go.sh ci-leg ${{ matrix.args }}")
	if j < 0 || j < i || strings.Contains(text[i:j], "      - name:") {
		t.Errorf("the environment is not on the Run Go quality leg step")
	}
	m := regexp.MustCompile(`github\.event_name == '([a-z_]+)' && '1' \|\| ''`).FindStringSubmatch(want)
	if m == nil {
		t.Fatal("cannot read the event out of the expression")
	}
	for event, report := range map[string]bool{"merge_group": true, "pull_request": false, "push": false, "schedule": false, "workflow_dispatch": false} {
		if got := event == m[1]; got != report {
			t.Errorf("event %s: report-only=%v, want %v", event, got, report)
		}
	}
}

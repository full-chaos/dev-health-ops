package testshard_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-10498: a plain test leg fails when one of its packages ran 30 s or more and has no row in ci/go_test_weights.tsv (a
// package with no row weighs 1, so it unbalances the shards unseen), and the failure prints the row to paste.

// unweightedPackage is a package of the shard's root-module plan with no weights row, and its key.
func unweightedPackage(t *testing.T, shard, count int) (string, string) {
	t.Helper()
	mod, _ := goList(t)
	weighted := map[string]bool{}
	for _, r := range weightRows(t) {
		weighted[r.pkg] = true
	}
	for _, pkg := range shardPlan(t, shard, count) {
		if key := keyOf(mod, pkg); !weighted[key] {
			return pkg, key
		}
	}
	t.Fatalf("shard %d/%d holds no package without a weights row", shard, count)
	return "", ""
}

// weightedPackage is a package of the shard's root-module plan that has a weights row.
func weightedPackage(t *testing.T, shard, count int) string {
	t.Helper()
	mod, _ := goList(t)
	weighted := map[string]bool{}
	for _, r := range weightRows(t) {
		weighted[r.pkg] = true
	}
	for _, pkg := range shardPlan(t, shard, count) {
		if weighted[keyOf(mod, pkg)] {
			return pkg
		}
	}
	t.Fatalf("shard %d/%d holds no weighted package", shard, count)
	return ""
}

func unweightedGuard(t *testing.T, env []string, shard int, log string) (string, error) {
	t.Helper()
	return unweightedGuardAt(t, env, shard, shardCount, writeLog(t, log))
}

func unweightedGuardAt(t *testing.T, env []string, shard, count int, path string) (string, error) {
	t.Helper()
	_, errs, err := checkGo(t, env, "test-shard-unweighted-guard", fmt.Sprint(shard), fmt.Sprint(count), path)
	return errs, err
}

// coverLog is the go test log of a whole shard: a fast "ok" line for every package of its plan (all modules), except the packages in
// times, whose third field is the given text ("30.000s", "(cached)"). The guard needs a summary line per package to prove coverage.
func fullPlan(t *testing.T, shard, count int) []string {
	t.Helper()
	out, errs, err := checkGo(t, nil, "test-shard-plan", fmt.Sprint(shard), fmt.Sprint(count))
	if err != nil {
		t.Fatalf("test-shard-plan %d %d: %v\n%s", shard, count, err, errs)
	}
	var pkgs []string
	for _, l := range strings.Split(out, "\n") {
		if f := strings.Split(l, "\t"); len(f) == 3 {
			pkgs = append(pkgs, f[1])
		}
	}
	return pkgs
}

func coverLog(t *testing.T, shard, count int, times map[string]string) string {
	t.Helper()
	var b strings.Builder
	for _, pkg := range fullPlan(t, shard, count) {
		tm := "0.010s"
		if v, ok := times[pkg]; ok {
			tm = v
		}
		fmt.Fprintf(&b, "ok  \t%s\t%s\n", pkg, tm)
	}
	return b.String()
}

func TestAnUnweightedPackageOf30SecondsFailsTheShardAndPrintsItsRow(t *testing.T) {
	t.Parallel()
	pkg, key := unweightedPackage(t, 1, shardCount)
	errs, err := unweightedGuard(t, nil, 1, coverLog(t, 1, shardCount, map[string]string{pkg: "30.000s"}))
	if err == nil {
		t.Fatalf("an unweighted package at 30 s passed the guard")
	}
	for _, want := range []string{"unweighted-slow-package: " + pkg + " took 30s", "ci/go_test_weights.tsv", "\n" + key + "\t30\n"} {
		if !strings.Contains(errs, want) {
			t.Errorf("the failure lacks %q: %s", want, errs)
		}
	}
	if errs, err := unweightedGuard(t, nil, 1, coverLog(t, 1, shardCount, nil)+"FAIL\t"+pkg+"\t44.6s\n"); err == nil || !strings.Contains(errs, "\n"+key+"\t45\n") {
		t.Errorf("a FAIL line of an unweighted package at 44.6 s: err=%v, want the row %q\n%s", err, key+"\t45", errs)
	}
}

func TestTheUnweightedGuardPassesBelowTheThresholdAWeightedACachedAndAForeignPackage(t *testing.T) {
	t.Parallel()
	pkg, _ := unweightedPackage(t, 1, shardCount)
	other, _ := unweightedPackage(t, 2, shardCount)
	for name, log := range map[string]string{
		"29.999 s":                       coverLog(t, 1, shardCount, map[string]string{pkg: "29.999s"}),
		"a weighted package at 900 s":    coverLog(t, 1, shardCount, map[string]string{weightedPackage(t, 1, shardCount): "900.000s"}),
		"cached":                         coverLog(t, 1, shardCount, map[string]string{pkg: "(cached)"}),
		"a package of another shard":     coverLog(t, 1, shardCount, nil) + "ok  \t" + other + "\t300s\n",
		"a package outside every module": coverLog(t, 1, shardCount, nil) + "ok  \tgithub.com/x/y\t300s\n",
		"a test's own output":            coverLog(t, 1, shardCount, nil) + "--- PASS: TestX (31.00s)\n    ok  \t" + pkg + "\t31s\n",
	} {
		if errs, err := unweightedGuard(t, nil, 1, log); err != nil {
			t.Errorf("%s: the guard failed: %v\n%s", name, err, errs)
		}
	}
}

func TestTheUnweightedThresholdIsReadFromTheEnvironment(t *testing.T) {
	t.Parallel()
	pkg, _ := unweightedPackage(t, 1, shardCount)
	if _, err := unweightedGuard(t, []string{"DEV_HEALTH_GO_TEST_UNWEIGHTED_SLOW_SECONDS=5"}, 1, coverLog(t, 1, shardCount, map[string]string{pkg: "6s"})); err == nil {
		t.Error("DEV_HEALTH_GO_TEST_UNWEIGHTED_SLOW_SECONDS=5: a 6 s unweighted package passed")
	}
}

func TestTheUnweightedGuardRefusesUnusableWeights(t *testing.T) {
	t.Parallel()
	empty := filepath.Join(t.TempDir(), "w.tsv")
	if err := os.WriteFile(empty, []byte("# no rows\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if errs, err := unweightedGuard(t, []string{"GO_TEST_WEIGHTS=" + empty}, 1, ""); err == nil || !strings.Contains(errs, "holds no row") {
		t.Errorf("an empty weights file: err=%v\n%s", err, errs)
	}
}

// The guard fails closed: whenever the log cannot prove that every package of the shard was measured, it refuses, with a message that
// says why. None of these logs holds a slow unweighted package, so a pass would be a false green.
func TestTheUnweightedGuardFailsClosedWhenItCannotProveCoverage(t *testing.T) {
	t.Parallel()
	pkgs := fullPlan(t, 1, shardCount)
	full := coverLog(t, 1, shardCount, nil)
	lines := strings.SplitAfter(full, "\n")
	for _, c := range []struct{ name, log, want string }{
		{"a zero-byte log", "", "holds no package summary line"},
		{"a log of build chatter only", "go: downloading x v1\n--- PASS: TestX (0.01s)\nPASS\n", "holds no package summary line"},
		{"a log of foreign packages only", "ok  \tgithub.com/x/y\t3s\n", "no summary line for " + fmt.Sprint(len(pkgs)) + " of"},
		{"a truncated log", strings.Join(lines[:len(lines)/2], ""), "no summary line for"},
		{"a log that lacks one package", strings.Join(append(append([]string{}, lines[:len(lines)-2]...), lines[len(lines)-1]), ""), "no summary line for 1 of " + fmt.Sprint(len(pkgs))},
		{"a build-failed package", strings.ReplaceAll(full, "ok  \t"+pkgs[0]+"\t0.010s", "FAIL\t"+pkgs[0]+" [build failed]"), "no summary line for 1 of"},
	} {
		errs, err := unweightedGuard(t, nil, 1, c.log)
		if err == nil {
			t.Errorf("%s: the guard passed", c.name)
			continue
		}
		if !strings.Contains(errs, c.want) {
			t.Errorf("%s: the failure lacks %q: %s", c.name, c.want, errs)
		}
	}
	if errs, err := unweightedGuard(t, nil, 1, full); err != nil {
		t.Errorf("the complete log failed: %v\n%s", err, errs)
	}
	dir := t.TempDir()
	for name, path := range map[string]string{"a missing path": filepath.Join(dir, "absent.log"), "a directory": dir} {
		if errs, err := unweightedGuardAt(t, nil, 1, shardCount, path); err == nil || !strings.Contains(errs, "readable file") {
			t.Errorf("%s: err=%v, want the readable-file refusal\n%s", name, err, errs)
		}
	}
	unreadable := writeLog(t, full)
	if err := os.Chmod(unreadable, 0); err != nil {
		t.Fatal(err)
	}
	if f, err := os.Open(unreadable); err == nil {
		f.Close()
	} else if errs, err := unweightedGuardAt(t, nil, 1, shardCount, unreadable); err == nil || !strings.Contains(errs, "readable file") {
		t.Errorf("an unreadable log: err=%v\n%s", err, errs)
	}
}

// Executed through the real test leg: the leg's own go test output (the stand-in prints it) with an unweighted package of the
// shard at 31 s fails `ci-leg test`, before the wall-time guard; the same output at 29 s passes.
func TestATestLegFailsOnAnUnweightedSlowPackageOfItsOwnLog(t *testing.T) {
	t.Parallel()
	pkg, key := unweightedPackage(t, 350, 350)
	for _, c := range []struct {
		seconds string
		fail    bool
	}{{"31.2s", true}, {"29.0s", false}} {
		out := writeLog(t, coverLog(t, 350, 350, map[string]string{pkg: c.seconds}))
		r := runCheckGo(t, []string{"FAKE_GO_TEST_STDOUT=" + out}, false, "ci-leg", "test", "350", "350")
		if c.fail && (r.Err == nil || !strings.Contains(r.Stderr, "\n"+key+"\t31\n")) {
			t.Errorf("%s: the leg did not fail with the row %q: err=%v\n%s", c.seconds, key+"\t31", r.Err, r.Stderr)
		}
		if !c.fail && r.Err != nil {
			t.Errorf("%s: the leg failed: %v\n%s", c.seconds, r.Err, r.Stderr)
		}
	}
}

// A leg whose go test run failed still names the unweighted slow package it saw and prints the row to paste; the leg stays red,
// and the partial log does not add a coverage complaint.
func TestAFailedGoTestRunStillPrintsTheUnweightedRow(t *testing.T) {
	t.Parallel()
	pkg, key := unweightedPackage(t, 1, shardCount)
	out := writeLog(t, "FAIL\t"+pkg+"\t31.2s\n")
	r := runCheckGo(t, []string{"FAKE_GO_TEST_STDOUT=" + out}, true, "ci-leg", "test", "1", fmt.Sprint(shardCount))
	if r.Err == nil {
		t.Fatalf("the leg passed although go test failed")
	}
	if !strings.Contains(r.Stderr, "\n"+key+"\t31\n") {
		t.Errorf("the failed leg did not print the row %q:\n%s", key+"\t31", r.Stderr)
	}
	if strings.Contains(r.Stderr, "no summary line") {
		t.Errorf("a failed run's partial log drew a coverage complaint:\n%s", r.Stderr)
	}
	bad := writeLog(t, "FAIL\t"+pkg+"\t5s\n")
	if r := runCheckGo(t, []string{"FAKE_GO_TEST_STDOUT=" + bad}, true, "ci-leg", "test", "1", fmt.Sprint(shardCount)); r.Err == nil || strings.Contains(r.Stderr, "unweighted-slow-package") {
		t.Errorf("a failed run with a fast package: err=%v\n%s", r.Err, r.Stderr)
	}
}

// Weights rows are keyed by root-module package, and the weights-table test refuses any other key, so a nested module's package
// cannot be given a row: the guard names the package and says so instead of printing a row the table would reject.
func TestASlowNestedModulePackageIsNamedWithoutAnUnacceptableRow(t *testing.T) {
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
	errs, err = unweightedGuard(t, nil, 1, coverLog(t, 1, shardCount, map[string]string{nested: "30.000s"}))
	if err == nil {
		t.Fatalf("a nested-module package at 30 s passed the guard")
	}
	for _, want := range []string{"nested-module-slow-package: " + nested + " took 30s", "no row can be added", "root-module"} {
		if !strings.Contains(errs, want) {
			t.Errorf("the failure lacks %q: %s", want, errs)
		}
	}
	if strings.Contains(errs, "unweighted-slow-package") || strings.Contains(errs, "\t30\n") {
		t.Errorf("the failure prints a row the weights table would reject: %s", errs)
	}
}

// Each package the main test legs measured at 20 s or more (main run 39606646375) has a row: the guard is green on main.
func TestThePackagesMeasuredHeavyOnMainHaveRows(t *testing.T) {
	t.Parallel()
	weighted := map[string]bool{}
	for _, r := range weightRows(t) {
		weighted[r.pkg] = true
	}
	for _, key := range []string{"internal/cigates/outboundtransport", "internal/cigates/raceunweighted", "internal/cigates/bigboy",
		"internal/syncdispatchruntime", "internal/testsupport/venueoracle/goldenhygiene", "internal/queryapiservice",
		"internal/auth/contracts", "internal/cigates/affected",
		"internal/storage/clickhouse", "internal/cigates/docstools", "internal/platform/tracing"} {
		if !weighted[key] {
			t.Errorf("%s ran >= 20 s in a main test leg and has no row in ci/go_test_weights.tsv", key)
		}
	}
}

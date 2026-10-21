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
	_, errs, err := checkGo(t, env, "test-shard-unweighted-guard", fmt.Sprint(shard), fmt.Sprint(shardCount), writeLog(t, log))
	return errs, err
}

func TestAnUnweightedPackageOf30SecondsFailsTheShardAndPrintsItsRow(t *testing.T) {
	t.Parallel()
	pkg, key := unweightedPackage(t, 1, shardCount)
	errs, err := unweightedGuard(t, nil, 1, "ok  \t"+pkg+"\t30.000s\n")
	if err == nil {
		t.Fatalf("an unweighted package at 30 s passed the guard")
	}
	for _, want := range []string{"unweighted-slow-package: " + pkg + " took 30s", "ci/go_test_weights.tsv", "\n" + key + "\t30\n"} {
		if !strings.Contains(errs, want) {
			t.Errorf("the failure lacks %q: %s", want, errs)
		}
	}
	if errs, err := unweightedGuard(t, nil, 1, "FAIL\t"+pkg+"\t44.6s\n"); err == nil || !strings.Contains(errs, "\n"+key+"\t45\n") {
		t.Errorf("a FAIL line of an unweighted package at 44.6 s: err=%v, want the row %q\n%s", err, key+"\t45", errs)
	}
}

func TestTheUnweightedGuardPassesBelowTheThresholdAWeightedACachedAndAForeignPackage(t *testing.T) {
	t.Parallel()
	pkg, _ := unweightedPackage(t, 1, shardCount)
	other, _ := unweightedPackage(t, 2, shardCount)
	for name, log := range map[string]string{
		"29.999 s":                       "ok  \t" + pkg + "\t29.999s\n",
		"a weighted package at 900 s":    "ok  \t" + weightedPackage(t, 1, shardCount) + "\t900.000s\n",
		"cached":                         "ok  \t" + pkg + "\t(cached)\n",
		"a package of another shard":     "ok  \t" + other + "\t300s\n",
		"a package outside every module": "ok  \tgithub.com/x/y\t300s\n",
		"a test's own output":            "--- PASS: TestX (31.00s)\n    ok  \t" + pkg + "\t31s\n",
	} {
		if errs, err := unweightedGuard(t, nil, 1, log); err != nil {
			t.Errorf("%s: the guard failed: %v\n%s", name, err, errs)
		}
	}
}

func TestTheUnweightedThresholdIsReadFromTheEnvironment(t *testing.T) {
	t.Parallel()
	pkg, _ := unweightedPackage(t, 1, shardCount)
	if _, err := unweightedGuard(t, []string{"DEV_HEALTH_GO_TEST_UNWEIGHTED_SLOW_SECONDS=5"}, 1, "ok  \t"+pkg+"\t6s\n"); err == nil {
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

// Executed through the real test leg: the leg's own go test output (the stand-in prints it) with an unweighted package of the
// shard at 31 s fails `ci-leg test`, before the wall-time guard; the same output at 29 s passes.
func TestATestLegFailsOnAnUnweightedSlowPackageOfItsOwnLog(t *testing.T) {
	t.Parallel()
	pkg, key := unweightedPackage(t, 350, 350)
	for _, c := range []struct {
		seconds string
		fail    bool
	}{{"31.2s", true}, {"29.0s", false}} {
		out := writeLog(t, "ok  \t"+pkg+"\t"+c.seconds+"\n")
		r := runCheckGo(t, []string{"FAKE_GO_TEST_STDOUT=" + out}, false, "ci-leg", "test", "350", "350")
		if c.fail && (r.Err == nil || !strings.Contains(r.Stderr, "\n"+key+"\t31\n")) {
			t.Errorf("%s: the leg did not fail with the row %q: err=%v\n%s", c.seconds, key+"\t31", r.Err, r.Stderr)
		}
		if !c.fail && r.Err != nil {
			t.Errorf("%s: the leg failed: %v\n%s", c.seconds, r.Err, r.Stderr)
		}
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
		"internal/auth/contracts", "internal/relevancedecider"} {
		if !weighted[key] {
			t.Errorf("%s ran >= 20 s in a main test leg and has no row in ci/go_test_weights.tsv", key)
		}
	}
}

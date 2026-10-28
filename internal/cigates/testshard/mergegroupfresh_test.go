package testshard_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// CHAOS-11745: the hosted legs restore a Go build cache that holds test results, and a test that reads a file through a child
// process (helm, bash, go, git) can read "ok (cached)" after that file changed. On merge_group every go test of the test and race
// legs carries -count=1 in its argv, and the leg's cache receipt, read from the go test -json events of its stages, fails when any
// package reads "ok (cached)". A pull request keeps its cached results and the receipt only reports them.

// mergeGroupLegs are go-quality legs (ci/go_quality_legs.json) the stand-in runs: test 1/3 holds the heavy package that runs
// alone, and every race leg.
var mergeGroupLegs = [][]string{
	{"test", "1", "3"}, {"race", "1", "4"}, {"race", "2", "4"}, {"race", "3", "4"}, {"race", "4", "4"},
}

// otherShardCensus is a one-row race-excluded census whose package is in test shard 2/3, so test shard 1/3 runs no census row:
// the stand-in prints no go test -json event of a real row, and the census receipts stay out of this check.
func otherShardCensus(t *testing.T) string {
	t.Helper()
	mod, _ := goList(t)
	plan := shardPlan(t, 2, 3)
	if len(plan) == 0 {
		t.Fatal("test shard 2/3 plans no root-module package")
	}
	return writeLog(t, keyOf(mod, plan[0])+"\tTestNotInShardOne\n")
}

// summaryEvents turns a plain go test log into the -json form: each "ok<TAB>pkg<TAB>..." line is the output event of its package.
func summaryEvents(t *testing.T, log string) string {
	t.Helper()
	var b strings.Builder
	for _, line := range strings.Split(strings.TrimSuffix(log, "\n"), "\n") {
		f := strings.Split(line, "\t")
		if len(f) < 2 {
			t.Fatalf("not a summary line: %q", line)
		}
		b.WriteString(outputEvent(t, strings.TrimSpace(f[1]), "", line+"\n"))
	}
	return b.String()
}

func outputEvent(t *testing.T, pkg, test, output string) string {
	t.Helper()
	e := map[string]string{"Time": "2026-01-01T00:00:00Z", "Action": "output", "Package": pkg, "Output": output}
	if test != "" {
		e["Test"] = test
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return string(b) + "\n"
}

func hasArg(call, arg string) bool {
	for _, f := range strings.Fields(call) {
		if f == arg {
			return true
		}
	}
	return false
}

type legCalls struct {
	leg   string
	run   legRun
	calls []string
}

func runLegs(t *testing.T, mergeGroup bool) []legCalls {
	t.Helper()
	out := make([]legCalls, len(mergeGroupLegs))
	t.Run("legs", func(t *testing.T) {
		for i, leg := range mergeGroupLegs {
			t.Run(strings.Join(leg, "-"), func(t *testing.T) {
				t.Parallel()
				env := freshEnv(mergeGroup)
				if leg[0] == "test" {
					env = append(env, "GO_RACE_EXCLUDED_TESTS="+otherShardCensus(t))
				}
				r := runCheckGo(t, env, false, append([]string{"ci-leg"}, leg...)...)
				out[i] = legCalls{strings.Join(leg, " "), r, append(append([]string{}, r.Calls...), r.ListCalls...)}
			})
		}
	})
	return out
}

func goTestHeaders(stdout string) []string {
	var h []string
	for _, l := range strings.Split(stdout, "\n") {
		if strings.HasPrefix(l, "go test") {
			h = append(h, l)
		}
	}
	return h
}

func TestOnMergeGroupEveryGoTestOfTheTestAndRaceLegsCarriesCountOne(t *testing.T) {
	t.Parallel()
	seen := map[string]bool{}
	for _, l := range runLegs(t, true) {
		if l.run.Err != nil {
			t.Errorf("ci-leg %s: %v\n%s", l.leg, l.run.Err, l.run.Stderr)
			continue
		}
		if len(l.calls) == 0 {
			t.Errorf("ci-leg %s: the stand-in recorded no go test call", l.leg)
		}
		for _, c := range l.calls {
			if !hasArg(c, "-count=1") {
				t.Errorf("ci-leg %s on merge_group: a go test without -count=1 in its argv: %s", l.leg, c)
			}
			switch {
			case hasArg(c, "-list"):
				seen["providersync list"] = true
			case hasArg(c, "./internal/providersync"):
				seen["providersync race slice"] = true
			case hasArg(c, "-tags=integration"):
				seen["multi-replica gate"] = true
			case strings.Contains(c, "/internal/gqlgenguard"):
				seen["heavy package alone"] = true
			case hasArg(c, "-race") && strings.Contains(c, "/internal/cigates/testshard"):
				seen["race solo"] = true
			case hasArg(c, "-race"):
				seen["race packages"] = true
			default:
				seen["test packages"] = true
			}
		}
		headers := goTestHeaders(l.run.Stdout)
		if len(headers) == 0 {
			t.Errorf("ci-leg %s printed no go test line", l.leg)
		}
		for _, h := range headers {
			if !strings.Contains(h, "-count=1") {
				t.Errorf("ci-leg %s on merge_group: the go test line does not show -count=1: %s", l.leg, h)
			}
		}
	}
	for _, kind := range []string{"providersync list", "providersync race slice", "multi-replica gate", "heavy package alone", "race solo", "race packages", "test packages"} {
		if !seen[kind] {
			t.Errorf("no leg ran a %s call: the check covers less than the legs run", kind)
		}
	}
}

func TestOnAPullRequestTheLegsKeepTheirArgvAndTheirLogLines(t *testing.T) {
	t.Parallel()
	for _, l := range runLegs(t, false) {
		if l.run.Err != nil {
			t.Errorf("ci-leg %s: %v\n%s", l.leg, l.run.Err, l.run.Stderr)
			continue
		}
		for _, c := range l.calls {
			// The integration-tagged multi-replica worker gate carries -count=1 on every event.
			if hasArg(c, "-count=1") && !hasArg(c, "-tags=integration") {
				t.Errorf("ci-leg %s on a pull request: a go test with -count=1 (cached results must stay allowed): %s", l.leg, c)
			}
		}
		for _, h := range goTestHeaders(l.run.Stdout) {
			if strings.Contains(h, "-count=1") {
				t.Errorf("ci-leg %s on a pull request: the go test line shows -count=1: %s", l.leg, h)
			}
		}
	}
}

func TestOnMergeGroupACachedPackageOutsideTheSecurityListFailsTheReceipt(t *testing.T) {
	t.Parallel()
	mod, _ := goList(t)
	log := "ok  \t" + mod + "/internal/queryapi/home\t(cached)\nok  \t" + mod + "/internal/auth/org\t0.020s\n" +
		"ok  \t" + mod + "/internal/clientip\t0.004s\n"
	_, errs, err := receipt(t, true, log)
	if err == nil {
		t.Fatal("a cached package outside ci/security_fresh_packages.txt passed the merge_group receipt")
	}
	for _, want := range []string{"internal/queryapi/home", "NOT executed"} {
		if !strings.Contains(errs, want) {
			t.Errorf("the failure lacks %q: %s", want, errs)
		}
	}
	for _, ran := range []string{"internal/auth/org", "internal/clientip"} {
		if strings.Contains(errs, ran) {
			t.Errorf("the failure names %s, which ran: %s", ran, errs)
		}
	}
}

func TestOnMergeGroupTheReceiptPassesWhenNoPackageIsCached(t *testing.T) {
	t.Parallel()
	mod, _ := goList(t)
	log := "ok  \t" + mod + "/internal/queryapi/home\t0.031s\nok  \t" + mod + "/internal/auth/org\t0.020s\n" +
		"ok  \t" + mod + "/internal/apiservice/ssovenue\t0.002s [no tests to run]\n"
	out, errs, err := receipt(t, true, log)
	if err != nil {
		t.Fatalf("a leg with no cached package failed the receipt: %v\n%s", err, errs)
	}
	for _, want := range []string{
		"cache-receipt: 0 package(s) reported \"ok (cached)\": cached = NOT executed by this leg\n",
		"security-fresh: 1 package(s) of ci/security_fresh_packages.txt executed with -count=1 on merge_group, 0 cached\n",
		"merge-group-fresh: every go test of this leg ran with -count=1, 0 package(s) cached\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the receipt lacks %q: %s", want, out)
		}
	}
}

// In the -json stream a line a test prints carries its Test: a test whose output reads like a cached summary is not a package
// served from the cache.
func TestATestsOwnOutputLineIsNotReadAsACachedPackage(t *testing.T) {
	t.Parallel()
	mod, _ := goList(t)
	events := outputEvent(t, mod+"/internal/cigates/testshard", "TestAChildGoTest", "ok  \t"+mod+"/internal/clientip\t(cached)\n") +
		summaryEvents(t, "ok  \t"+mod+"/internal/cigates/testshard\t0.500s\n")
	out, errs, err := checkGo(t, freshEnv(true), "security-fresh-receipt", writeLog(t, events))
	if err != nil {
		t.Fatalf("a test's own output line failed the merge_group receipt: %v\n%s", err, errs)
	}
	if !strings.Contains(out, "cache-receipt: 0 package(s) reported \"ok (cached)\"") {
		t.Errorf("a test's own output line was counted as a cached package: %s", out)
	}
}

func TestOnAPullRequestACachedPackageOfAnyListIsReportedOnly(t *testing.T) {
	t.Parallel()
	mod, _ := goList(t)
	log := "ok  \t" + mod + "/internal/queryapi/home\t(cached)\nok  \t" + mod + "/internal/auth/org\t(cached)\n" +
		"ok  \t" + mod + "/internal/clientip\t0.004s\n"
	out, errs, err := receipt(t, false, log)
	if err != nil {
		t.Fatalf("a pull request failed on cached packages: %v\n%s", err, errs)
	}
	if !strings.Contains(out, "cache-receipt: 2 package(s) reported \"ok (cached)\": cached = NOT executed by this leg\n") {
		t.Errorf("the receipt does not count the cached packages: %s", out)
	}
}

// Through the real test leg: the leg's own go test -json events (the stand-in prints them) with one cached package fail the leg
// on merge_group and pass it on a pull request, where the receipt counts it.
func TestACachedPackageInTheTestLegsOwnEventsFailsOnlyTheMergeGroupLeg(t *testing.T) {
	t.Parallel()
	plan := shardPlan(t, 350, 350)
	if len(plan) == 0 {
		t.Fatal("test shard 350/350 plans no root-module package")
	}
	pkg := plan[0]
	out := writeLog(t, summaryEvents(t, coverLog(t, 350, 350, map[string]string{pkg: "(cached)"})))
	mg := runCheckGo(t, append(freshEnv(true), "FAKE_GO_TEST_STDOUT="+out), false, "ci-leg", "test", "350", "350")
	if mg.Err == nil || !strings.Contains(mg.Stderr, pkg) || !strings.Contains(mg.Stderr, "NOT executed") {
		t.Errorf("merge_group: the leg did not fail naming the cached %s: err=%v\n%s", pkg, mg.Err, mg.Stderr)
	}
	pr := runCheckGo(t, append(freshEnv(false), "FAKE_GO_TEST_STDOUT="+out), false, "ci-leg", "test", "350", "350")
	if pr.Err != nil {
		t.Errorf("pull request: a cached package failed the leg: %v\n%s", pr.Err, pr.Stderr)
	}
	if !strings.Contains(pr.Stdout, "cache-receipt: 1 package(s) reported \"ok (cached)\"") {
		t.Errorf("pull request: the receipt does not count the cached package: %s", pr.Stdout)
	}
}

// The race leg's providersync slice writes its own go test -json file: its cached summary reaches the receipt too.
func TestACachedProvidersyncRaceSliceFailsOnlyTheMergeGroupRaceLeg(t *testing.T) {
	t.Parallel()
	mod, _ := goList(t)
	ps := mod + "/internal/providersync"
	out := writeLog(t, outputEvent(t, ps, "", fmt.Sprintf("ok  \t%s\t(cached)\n", ps)))
	mg := runCheckGo(t, append(freshEnv(true), "FAKE_GO_PROVIDERSYNC_STDOUT="+out), false, "ci-leg", "race", "1", "4")
	if mg.Err == nil || !strings.Contains(mg.Stderr, ps) || !strings.Contains(mg.Stderr, "NOT executed") {
		t.Errorf("merge_group: the race leg did not fail naming the cached providersync slice: err=%v\n%s", mg.Err, mg.Stderr)
	}
	pr := runCheckGo(t, append(freshEnv(false), "FAKE_GO_PROVIDERSYNC_STDOUT="+out), false, "ci-leg", "race", "1", "4")
	if pr.Err != nil {
		t.Errorf("pull request: a cached providersync slice failed the race leg: %v\n%s", pr.Err, pr.Stderr)
	}
	if !strings.Contains(pr.Stdout, "cache-receipt: 1 package(s) reported \"ok (cached)\"") {
		t.Errorf("pull request: the race receipt does not count the providersync slice: %s", pr.Stdout)
	}
}

// mergeGroupLegFunctions hold every go test of the test and race legs (a branch the stand-in run may not reach is read here):
// go_test_leg runs every stage of check_test_shard and check_race_shard (checkgo's leg-site guard pins that they hold no go test
// call of their own).
var mergeGroupLegFunctions = []string{
	"go_test_leg", "go_test_json_run", "check_multi_replica_workers", "check_providersync_race_shard",
}

// legCallsWithoutCountOne lists the go test lines of the leg functions that name no -count=1, and the number of lines seen.
func legCallsWithoutCountOne(t *testing.T, script string) (missing []string, seen int) {
	t.Helper()
	for _, name := range mergeGroupLegFunctions {
		for _, line := range strings.Split(functionBody(t, script, name), "\n") {
			if !goTestWithoutTimeout.MatchString(line) || strings.HasPrefix(strings.TrimSpace(line), "#") {
				continue
			}
			seen++
			if !strings.Contains(line, " -count=1 ") && !strings.Contains(line, "${MERGE_GROUP_FRESH:+-count=1} ") {
				missing = append(missing, name+": "+strings.TrimSpace(line))
			}
		}
	}
	return missing, seen
}

func TestEveryGoTestLineOfTheTestAndRaceLegsNamesCountOne(t *testing.T) {
	t.Parallel()
	script := readCheckGo(t)
	missing, seen := legCallsWithoutCountOne(t, script)
	if seen != 5 {
		t.Fatalf("saw %d go test line(s) in %v, want 5: the scan is stale", seen, mergeGroupLegFunctions)
	}
	if len(missing) != 0 {
		t.Errorf("go test line(s) with no -count=1 on merge_group: %v", missing)
	}
	planted := strings.Replace(script, "-trimpath ${MERGE_GROUP_FRESH:+-count=1} -json \"$@\"", "-trimpath -json \"$@\"", 1)
	if planted == script {
		t.Fatal("the go_test_leg call to plant is not in ci/check_go.sh")
	}
	if missing, _ := legCallsWithoutCountOne(t, planted); len(missing) != 1 {
		t.Errorf("a go_test_leg call that lost -count=1 was not caught: %v", missing)
	}
}

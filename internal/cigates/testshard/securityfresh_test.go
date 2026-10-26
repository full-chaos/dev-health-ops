package testshard_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// CHAOS-11448: on merge_group the go-quality test and race legs run the security packages of ci/security_fresh_packages.txt with
// -count=1, so their tests always execute; every other package may still read "ok (cached)", and the leg says so in its receipt.

const securityFreshList = "ci/security_fresh_packages.txt"

// acpRequiredSecurityTrees is acp's required set (2026-10-26): the list may add to it, never drop one.
var acpRequiredSecurityTrees = []string{
	"./internal/auth/...", "./internal/api/session/...", "./internal/api/sessionissue/...", "./internal/api/sso/...",
	"./internal/api/oauthprovider/...", "./internal/api/authmail/...", "./internal/apiservice/authflowvenue/...",
	"./internal/apiservice/ssovenue/...", "./internal/storage/postgres/authschema/...", "./internal/httpguard/...",
	"./internal/token/...", "./internal/edgetokenmint/...", "./internal/mintcli/edgetoken/...", "./internal/queryapi/authctx/...",
	"./internal/queryapi/principal/...", "./internal/platform/secrets/...", "./cmd/auth-service/...", "./cmd/auth-migrate/...",
}

// acpAddedSecurityTrees is the GO of 2026-10-26 (CHAOS-11448): internal/entitlements is deliberately not here.
var acpAddedSecurityTrees = []string{
	"./contracts/auth/v1/...", "./internal/audit/...", "./internal/api/credentials/...", "./internal/egressguard/...",
	"./internal/envelopemint/...", "./internal/clicredentials/...", "./internal/projectmembership/...", "./internal/api/policy/...",
}

func listLines(text string) []string {
	var out []string
	for _, l := range strings.Split(text, "\n") {
		if i := strings.Index(l, "#"); i >= 0 {
			l = l[:i]
		}
		if l = strings.Join(strings.Fields(l), ""); l != "" {
			out = append(out, l)
		}
	}
	return out
}

func readSecurityList(t *testing.T) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), securityFreshList))
	if err != nil {
		t.Fatalf("%s: %v", securityFreshList, err)
	}
	return listLines(string(b))
}

// missingPaths names every pattern whose directory does not exist under root, or that is not a ./path/... pattern.
func missingPaths(root string, lines []string) []string {
	var bad []string
	for _, l := range lines {
		dir, ok := strings.CutSuffix(l, "/...")
		if !ok || !strings.HasPrefix(dir, "./") {
			bad = append(bad, l)
			continue
		}
		if st, err := os.Stat(filepath.Join(root, dir)); err != nil || !st.IsDir() {
			bad = append(bad, l)
		}
	}
	return bad
}

// uncovered names every wanted pattern that no line of the list covers (the same pattern, or a parent tree).
func uncovered(lines, want []string) []string {
	var gaps []string
	for _, w := range want {
		wd := strings.TrimSuffix(w, "/...")
		found := false
		for _, l := range lines {
			ld := strings.TrimSuffix(l, "/...")
			if wd == ld || strings.HasPrefix(wd, ld+"/") {
				found = true
			}
		}
		if !found {
			gaps = append(gaps, w)
		}
	}
	return gaps
}

var authPackagesBlock = regexp.MustCompile(`(?m)^AUTH_PACKAGES=\(\n((?:  \S+\n)+)\)`)

func authPackages(t *testing.T, source string) []string {
	t.Helper()
	m := authPackagesBlock.FindStringSubmatch(source)
	if m == nil {
		t.Fatal("ci/check_go.sh: no AUTH_PACKAGES=( ... ) block")
	}
	return strings.Fields(m[1])
}

func TestEverySecurityFreshPathExists(t *testing.T) {
	t.Parallel()
	if bad := missingPaths(repoRoot(t), readSecurityList(t)); len(bad) != 0 {
		t.Errorf("%s lists path(s) that do not exist or are not ./dir/... patterns: %v", securityFreshList, bad)
	}
}

func TestAListedPathThatDoesNotExistIsReported(t *testing.T) {
	t.Parallel()
	lines := append(readSecurityList(t), "./internal/auth/nosuchpackage/...", "internal/auth/...")
	bad := missingPaths(repoRoot(t), lines)
	if len(bad) != 2 {
		t.Errorf("want the two planted lines, got %v", bad)
	}
}

func TestTheSecurityListKeepsEveryAcpTree(t *testing.T) {
	t.Parallel()
	lines := readSecurityList(t)
	for _, want := range [][]string{acpRequiredSecurityTrees, acpAddedSecurityTrees} {
		if gaps := uncovered(lines, want); len(gaps) != 0 {
			t.Errorf("%s dropped %v", securityFreshList, gaps)
		}
	}
	for _, l := range lines {
		if strings.HasPrefix(l, "./internal/entitlements") {
			t.Errorf("acp ruled internal/entitlements out of the list, but it holds %s", l)
		}
	}
}

func TestTheSecurityListIsASupersetOfAuthPackages(t *testing.T) {
	t.Parallel()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if gaps := uncovered(readSecurityList(t), authPackages(t, string(b))); len(gaps) != 0 {
		t.Errorf("AUTH_PACKAGES member(s) not in %s: %v", securityFreshList, gaps)
	}
}

func TestAnAuthPackagesMemberMissingFromTheListIsReported(t *testing.T) {
	t.Parallel()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	planted := strings.Replace(string(b), "  ./internal/token/...\n)", "  ./internal/token/...\n  ./internal/plantedauthpkg/...\n)", 1)
	if planted == string(b) {
		t.Fatal("the AUTH_PACKAGES tail to plant after is not in ci/check_go.sh")
	}
	gaps := uncovered(readSecurityList(t), authPackages(t, planted))
	if len(gaps) != 1 || gaps[0] != "./internal/plantedauthpkg/..." {
		t.Errorf("want exactly the planted member, got %v", gaps)
	}
}

func freshEnv(on bool) []string {
	if on {
		return []string{"DEV_HEALTH_GO_FRESH_SECURITY=1"}
	}
	return []string{"DEV_HEALTH_GO_FRESH_SECURITY="}
}

func receipt(t *testing.T, on bool, log string) (string, string, error) {
	t.Helper()
	return checkGo(t, freshEnv(on), "security-fresh-receipt", writeLog(t, log))
}

func TestACachedSecurityPackageFailsTheMergeGroupLeg(t *testing.T) {
	t.Parallel()
	mod, _ := goList(t)
	log := "ok  \t" + mod + "/internal/auth/org\t(cached)\nok  \t" + mod + "/internal/auth/session\t0.011s\n"
	_, errs, err := receipt(t, true, log)
	if err == nil {
		t.Fatal("a cached security package passed the merge_group guard")
	}
	for _, want := range []string{"internal/auth/org", "NOT executed"} {
		if !strings.Contains(errs, want) {
			t.Errorf("the failure lacks %q: %s", want, errs)
		}
	}
	if strings.Contains(errs, "internal/auth/session") {
		t.Errorf("the failure names a package that ran: %s", errs)
	}
}

func TestACachedOtherPackageIsAllowedAndTheReceiptSaysItWasNotExecuted(t *testing.T) {
	t.Parallel()
	mod, _ := goList(t)
	log := "ok  \t" + mod + "/internal/queryapi/home\t(cached)\nok  \t" + mod + "/internal/auth/org\t0.020s\n" +
		"ok  \t" + mod + "/internal/apiservice/ssovenue\t(cached) [no tests to run]\n"
	out, errs, err := receipt(t, true, log)
	if err != nil {
		t.Fatalf("a cached non-security package failed the guard: %v\n%s", err, errs)
	}
	for _, want := range []string{
		"cache-receipt: 2 package(s) reported \"ok (cached)\": cached = NOT executed by this leg\n",
		"security-fresh: 1 package(s) of ci/security_fresh_packages.txt executed with -count=1 on merge_group, 0 cached\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the receipt lacks %q: %s", want, out)
		}
	}
}

func TestOffMergeGroupTheSecurityGuardIsOffAndTheReceiptStillSaysCachedIsNotExecuted(t *testing.T) {
	t.Parallel()
	mod, _ := goList(t)
	out, errs, err := receipt(t, false, "ok  \t"+mod+"/internal/auth/org\t(cached)\n")
	if err != nil {
		t.Fatalf("a pull request or push run failed the guard: %v\n%s", err, errs)
	}
	for _, want := range []string{"cache-receipt: 1 package(s) reported \"ok (cached)\": cached = NOT executed by this leg\n", "security-fresh: off (event is not merge_group)\n"} {
		if !strings.Contains(out, want) {
			t.Errorf("the receipt lacks %q: %s", want, out)
		}
	}
}

func TestTheSplitSendsSecurityPackagesToTheFreshCallOnly(t *testing.T) {
	t.Parallel()
	mod, _ := goList(t)
	args := []string{mod + "/internal/auth/org", mod + "/internal/queryapi/home", mod + "/internal/httpguard", mod + "/internal/entitlements"}
	out, errs, err := checkGo(t, freshEnv(true), append([]string{"security-fresh-split"}, args...)...)
	if err != nil {
		t.Fatalf("security-fresh-split: %v\n%s", err, errs)
	}
	for _, want := range []string{"sec " + args[0], "plain " + args[1], "sec " + args[2], "plain " + args[3]} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("the split lacks %q: %s", want, out)
		}
	}
	out, _, err = checkGo(t, freshEnv(false), append([]string{"security-fresh-split"}, args[0])...)
	if err != nil || !strings.HasSuffix(out, "\nplain "+args[0]+"\n") || strings.Contains(out, "sec ") {
		t.Errorf("off merge_group every package is plain: %q (%v)", out, err)
	}
}

func TestNoRaceSoloPackageIsASecurityPackage(t *testing.T) {
	t.Parallel()
	mod, _ := goList(t)
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "go_race_solo.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	var solo []string
	for _, l := range strings.Split(string(b), "\n") {
		if f := strings.Split(l, "\t"); l != "" && !strings.HasPrefix(l, "#") {
			solo = append(solo, mod+"/"+f[0])
		}
	}
	out, errs, err := checkGo(t, freshEnv(true), append([]string{"security-fresh-split"}, solo...)...)
	if err != nil {
		t.Fatalf("security-fresh-split: %v\n%s", err, errs)
	}
	if strings.Contains(out, "sec ") {
		t.Errorf("a race solo package is in the security set, but the solo call carries no -count=1: %s", out)
	}
}

func TestTheFreshCallsCarryCountOneAndThePlainCallsDoNot(t *testing.T) {
	t.Parallel()
	source := readCheckGo(t)
	for _, want := range []string{
		`go test -mod=readonly -trimpath -count=1 -timeout 10m ${rest_sec}`,
		`go test -mod=readonly -trimpath -race -count=1 -timeout "${DEV_HEALTH_GO_RACE_TEST_TIMEOUT:-10m}" "${pkgs_sec[@]}"`,
		`go test -mod=readonly -trimpath -timeout 10m ${rest} 2>&1`,
		`go test -mod=readonly -trimpath -race -timeout "${DEV_HEALTH_GO_RACE_TEST_TIMEOUT:-10m}" "${pkgs[@]}"`,
	} {
		if !strings.Contains(source, want) {
			t.Errorf("ci/check_go.sh lacks the call %q", want)
		}
	}
}

func TestOnlyTheMergeGroupGoQualityLegStepSetsTheFreshSecurityEnvironment(t *testing.T) {
	t.Parallel()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), ".github", "workflows", "go-quality.yml"))
	if err != nil {
		t.Fatal(err)
	}
	const want = "DEV_HEALTH_GO_FRESH_SECURITY: ${{ github.event_name == 'merge_group' && '1' || '' }}"
	text := string(b)
	if strings.Count(text, "DEV_HEALTH_GO_FRESH_SECURITY") != 1 || !strings.Contains(text, want) {
		t.Fatalf("go-quality.yml must set %q exactly once", want)
	}
	i, j := strings.Index(text, want), strings.Index(text, "bash ci/check_go.sh ci-leg ${{ matrix.args }}")
	if j < 0 || j < i || strings.Contains(text[i:j], "      - name:") {
		t.Errorf("the environment is not on the Run Go quality leg step")
	}
}

package testshard_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
)

// CHAOS-13693: the auth race and race-integration legs cut a split package's test names as the go-quality test legs do
// (CHAOS-13668): by their own tests tables (ci/auth_race_split_weights.tsv, ci/auth_race_integration_split_weights.tsv) through
// the names mode of ci/go_race_shard.awk, so adding or removing an unmeasured test moves no other test. The auth plan is
// unchanged (a part weighs ceil(W / parts)); a stale, short, stray or missing table fails the auth leg loudly, and no race or
// test-leg plan reads the auth tables. A change to any auth split or weight table selects the auth legs.

type authSplitKind struct {
	kind, splits, tests, env string
	legs                     int
	listFlags                []string // the flags of the leg's go test -list (check_go.sh auth_leg_flags without -timeout and -v)
}

var authSplitKinds = []authSplitKind{
	{"race", "ci/auth_race_splits.tsv", "ci/auth_race_split_weights.tsv", "AUTH_RACE_SPLIT_WEIGHTS", 3, []string{"-count=1", "-race"}},
	{"race-integration", "ci/auth_race_integration_splits.tsv", "ci/auth_race_integration_split_weights.tsv", "AUTH_RACE_INTEGRATION_SPLIT_WEIGHTS", 4,
		[]string{"-count=1", "-race", "-tags=integration"}},
}

// The five tables that decide which names an auth leg runs: each must select the auth legs (auth_path_relevant).
var authLegTables = []string{"ci/auth_race_splits.tsv", "ci/auth_race_split_weights.tsv", "ci/auth_race_integration_weights.tsv",
	"ci/auth_race_integration_splits.tsv", "ci/auth_race_integration_split_weights.tsv"}

// authSplitRows reads a package<TAB>parts split table (repository-relative path).
func authSplitRows(t *testing.T, rel string) map[string]int {
	t.Helper()
	rows := map[string]int{}
	for _, line := range strings.Split(readRepoFile(t, rel), "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		var key string
		var n int
		if c, err := fmt.Sscanf(line, "%s\t%d", &key, &n); c != 2 || err != nil || n < 2 || strings.Count(line, "\t") != 1 {
			t.Fatalf("%s: row is not <package><TAB><parts >= 2>: %q", rel, line)
		}
		rows[key] = n
	}
	return rows
}

// lastLines: the last two non-empty lines of a stderr (the die line of check_go.sh and the line before it).
func lastLines(s string) string {
	var keep []string
	for _, l := range strings.Split(s, "\n") {
		if strings.TrimSpace(l) != "" {
			keep = append(keep, l)
		}
	}
	return strings.Join(keep[max(len(keep)-2, 0):], " | ")
}

func readRepoFile(t *testing.T, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), rel))
	if err != nil {
		t.Fatalf("%s: %v", rel, err)
	}
	return string(b)
}

func TestTheAuthSplitWeightsTablesAreClean(t *testing.T) {
	t.Parallel()
	for _, k := range authSplitKinds {
		splits := authSplitRows(t, k.splits)
		rows := splitWeightRowsOf(t, k.tests)
		for key := range rows {
			if _, ok := splits[key]; !ok {
				t.Errorf("%s has rows of %s, which %s does not split", k.tests, key, k.splits)
			}
		}
		for key, parts := range splits {
			if n := len(rows[key]); n < parts {
				t.Errorf("%s: split package %s has %d test row(s), fewer than its %d parts: a part could be empty", k.tests, key, n, parts)
			}
		}
	}
}

// authTree runs SCRIPT-relative check_go.sh calls: root is the tree whose ci/check_go.sh runs (the real one, or a planted mutant).
type authTree struct{ root string }

func (a authTree) checkGo(t *testing.T, env []string, args ...string) (string, string, error) {
	t.Helper()
	cmd := exec.Command("bash", append([]string{filepath.Join(a.root, "ci", "check_go.sh")}, args...)...)
	cmd.Dir = a.root
	cmd.Env = append(append(os.Environ(), reportOnlyEnv+"="), env...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	return out.String(), errb.String(), err
}

// authLegNames runs `auth KIND-leg all LEGS` (AUTH_LEG_LIST=1) and returns, per split package import path, name -> legs it runs
// in, and the whole-package lines of every leg in order (the plan without the split names).
func (a authTree) authLegNames(t *testing.T, env []string, k authSplitKind) (map[string]map[string][]int, string, error) {
	t.Helper()
	out, errs, err := a.checkGo(t, append(append([]string{}, env...), "AUTH_LEG_LIST=1"), "auth", k.kind+"-leg", "all", fmt.Sprint(k.legs))
	if err != nil {
		return nil, "", fmt.Errorf("auth %s-leg all %d: %v: %s", k.kind, k.legs, err, lastLines(errs))
	}
	header := regexp.MustCompile(`^==> auth ` + regexp.QuoteMeta(k.kind) + `-leg ([0-9]+) ` + fmt.Sprint(k.legs) + `$`)
	names := map[string]map[string][]int{}
	var plan strings.Builder
	leg := 0
	for _, l := range strings.Split(out, "\n") {
		if m := header.FindStringSubmatch(l); m != nil {
			leg++
			plan.WriteString(l + "\n")
			continue
		}
		f := strings.Fields(strings.TrimPrefix(l, "AUTH-LEG-RUN "+k.kind+" "))
		if !strings.HasPrefix(l, "AUTH-LEG-RUN ") || len(f) != 2 || leg == 0 {
			continue
		}
		if f[1] == "*" {
			plan.WriteString(l + "\n")
			continue
		}
		if names[f[0]] == nil {
			names[f[0]] = map[string][]int{}
		}
		names[f[0]][f[1]] = append(names[f[0]][f[1]], leg)
	}
	if leg != k.legs {
		return nil, "", fmt.Errorf("auth %s-leg all %d printed %d leg header(s)", k.kind, k.legs, leg)
	}
	return names, plan.String(), nil
}

// cutDefect: the legs of PKG run every listed name exactly once, no other name, and each part of the Go mirror cut in one leg.
func cutDefect(pkg string, got map[string][]int, rows map[string]int, parts int, listed []string) string {
	want := cutMirror(rows, parts, listed)
	partLeg := map[int]int{}
	for _, n := range listed {
		legs := got[n]
		if len(legs) != 1 {
			return fmt.Sprintf("%s %s runs in %d legs, want exactly 1", pkg, n, len(legs))
		}
		p := want[n]
		if l, ok := partLeg[p]; ok && l != legs[0] {
			return fmt.Sprintf("%s %s runs in leg %d, but part %d of the Go mirror cut runs in leg %d", pkg, n, legs[0], p, l)
		}
		partLeg[p] = legs[0]
	}
	for n := range got {
		if _, ok := want[n]; !ok {
			return fmt.Sprintf("%s %s runs but go test -list does not list it", pkg, n)
		}
	}
	if len(partLeg) != parts {
		return fmt.Sprintf("%s: %d of its %d parts hold a name", pkg, len(partLeg), parts)
	}
	return ""
}

var testName = regexp.MustCompile(`^(Test|Example|Fuzz)\S*$`)

// C1 over the real tree: every name the kind's own go test -list reports for a split package runs in exactly one leg, and the
// legs cut the names as the Go mirror cuts them by the kind's real table.
func TestEveryAuthSplitLegRunsItsGoMirrorCut(t *testing.T) {
	t.Parallel()
	mod, _ := goList(t)
	for _, k := range authSplitKinds {
		t.Run(k.kind, func(t *testing.T) {
			t.Parallel()
			got, _, err := authTree{repoRoot(t)}.authLegNames(t, nil, k)
			if err != nil {
				t.Fatal(err)
			}
			rows := splitWeightRowsOf(t, k.tests)
			for key, parts := range authSplitRows(t, k.splits) {
				pkg := mod + "/" + key
				out, errs, err := run(t, []string{"GOWORK=off"}, "go", append(append([]string{"test", "-mod=readonly", "-trimpath"}, k.listFlags...), "-list", ".*", pkg)...)
				if err != nil {
					t.Fatalf("go test -list %s: %v\n%s", pkg, err, errs)
				}
				var listed []string
				for _, l := range strings.Split(out, "\n") {
					if testName.MatchString(l) {
						listed = append(listed, l)
					}
				}
				for n := range rows[key] {
					if !strings.Contains("\n"+out, "\n"+n+"\n") {
						t.Errorf("%s: the row of %s %s names a test go test -list no longer reports", k.tests, key, n)
					}
				}
				if d := cutDefect(pkg, got[pkg], rows[key], parts, listed); d != "" {
					t.Error(d)
				}
			}
		})
	}
}

// A stand-in go: a `go test ... -list` call records its arguments and prints FAKE_AUTH_LIST; every other call is the real go.
const authListGo = `#!/usr/bin/env bash
case " $* " in *" test "*" -list "*)
  printf '%%s\n' "$*" >> "${FAKE_AUTH_LIST_LOG}"
  cat "${FAKE_AUTH_LIST}"
  printf 'ok  \tfake\t0.01s\n'
  exit 0 ;;
esac
exec "%s" "$@"
`

// authFixture: the stand-in go on PATH, the listing it prints and a tests table, as env for an auth leg run.
func authFixture(t *testing.T, k authSplitKind, names []string, rows map[string]map[string]int) ([]string, string) {
	t.Helper()
	dir := t.TempDir()
	real, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "go"), []byte(fmt.Sprintf(authListGo, real)), 0o755); err != nil {
		t.Fatal(err)
	}
	list := filepath.Join(dir, "list.txt")
	if err := os.WriteFile(list, []byte(strings.Join(names, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	log := filepath.Join(dir, "list.log")
	return []string{"PATH=" + dir + string(os.PathListSeparator) + os.Getenv("PATH"), "FAKE_AUTH_LIST=" + list, "FAKE_AUTH_LIST_LOG=" + log,
		k.env + "=" + writeTestRows(t, dir, rows)}, log
}

// authSplitKey: the kind's one split package (the cells below place a fixture listing on it).
func authSplitKey(t *testing.T, k authSplitKind) (string, int) {
	t.Helper()
	splits := authSplitRows(t, k.splits)
	if len(splits) != 1 {
		t.Fatalf("%s splits %d packages; the fixture cells place one listing on one package", k.splits, len(splits))
	}
	for key, parts := range splits {
		return key, parts
	}
	return "", 0
}

// Fixture rows per kind: different rows, so a kind that reads the other kind's table cuts differently.
func authFixtureRows(k authSplitKind, key string) map[string]map[string]int {
	if k.kind == "race" {
		return map[string]map[string]int{key: {"TestHeavy": 100, "TestMid": 50}}
	}
	return map[string]map[string]int{key: {"TestB": 100, "TestP": 60, "TestR": 50}}
}

var (
	authModOnce sync.Once
	authMod     string
)

func authModule(t *testing.T) string {
	authModOnce.Do(func() { authMod, _ = goList(t) })
	return authMod
}

// R1 (red on b729bbd45: round robin over the sorted names moves every later name): over a fixture listing of the kind's split
// package, adding or removing an unmeasured test leaves every other test in its leg; the legs cut as the Go mirror cuts by the
// kind's own table; the listing carries the kind's flags; the plan lines do not depend on the rows.
func cellAuthLegsMoveNoOtherName(t *testing.T, tree authTree) string {
	type result struct {
		what, plan, calls string
		names             []string
		got               map[string][]int
	}
	for _, k := range authSplitKinds {
		key, parts := authSplitKey(t, k)
		pkg := authModule(t) + "/" + key
		rows := authFixtureRows(k, key)
		var res []result
		for _, v := range []struct {
			what  string
			names []string
		}{
			{"base", r1Names},
			{"add TestA (sorts first)", append([]string{"TestA"}, r1Names...)},
			{"add TestE (sorts in the middle)", append(append([]string{}, r1Names...), "TestE")},
			{"add TestZ (sorts last)", append(append([]string{}, r1Names...), "TestZ")},
			{"remove TestD", []string{"TestB", "TestF", "TestH", "TestHeavy", "TestMid", "TestP", "TestR"}},
		} {
			sort.Strings(v.names)
			env, log := authFixture(t, k, v.names, rows)
			got, plan, err := tree.authLegNames(t, env, k)
			if err != nil {
				return fmt.Sprintf("%s %s: %v", k.kind, v.what, err)
			}
			calls, err := os.ReadFile(log)
			if err != nil || len(calls) == 0 {
				return fmt.Sprintf("%s %s: no go test -list call reached the stand-in go (%v)", k.kind, v.what, err)
			}
			res = append(res, result{v.what, plan, strings.TrimSpace(string(calls)), v.names, got[pkg]})
		}
		// First the ticket's rule: no listed name changes leg when one unmeasured name comes or goes, and the plan stays.
		for _, r := range res[1:] {
			if r.plan != res[0].plan {
				return fmt.Sprintf("%s %s: the leg plan (whole packages, leg headers) changed with the listing", k.kind, r.what)
			}
			for n, legs := range r.got {
				if b, ok := res[0].got[n]; ok && b[0] != legs[0] {
					return fmt.Sprintf("%s %s: %s moved from leg %d to leg %d", k.kind, r.what, n, b[0], legs[0])
				}
			}
		}
		// Then the cut itself: exactly once, as the Go mirror cuts by the kind's own table, listed under the kind's flags.
		for _, r := range res {
			if d := cutDefect(pkg, r.got, rows[key], parts, r.names); d != "" {
				return fmt.Sprintf("%s %s: %s", k.kind, r.what, d)
			}
			for _, c := range strings.Split(r.calls, "\n") {
				for _, f := range k.listFlags {
					if !strings.Contains(" "+c+" ", " "+f+" ") {
						return fmt.Sprintf("%s %s: the listing %q lacks the leg's flag %s", k.kind, r.what, c, f)
					}
				}
			}
		}
	}
	return ""
}

// R2/R3/R4/C6: a stale row, fewer rows than parts, a row of a package the kind does not split, a malformed table and a missing
// table each fail the kind's legs with a message that names the fault.
func cellAuthTableFaultsFailTheLeg(t *testing.T, tree authTree) string {
	for _, k := range authSplitKinds {
		key, _ := authSplitKey(t, k)
		for _, c := range []struct {
			what string
			rows map[string]map[string]int
			raw  string // a table body in place of rows ("-" = no file)
			want []string
		}{
			{"a stale row", map[string]map[string]int{key: {"TestHeavy": 100, "TestMid": 50, "TestGoneCHAOS13693": 30}}, "", []string{"TestGoneCHAOS13693", "no longer reports"}},
			{"fewer rows than parts", map[string]map[string]int{key: {"TestHeavy": 100}}, "", []string{"fewer than its"}},
			{"a row of a package the kind does not split", map[string]map[string]int{key: {"TestHeavy": 100, "TestMid": 50}, "internal/auth/strayCHAOS13693": {"TestB": 5}}, "", []string{"strayCHAOS13693", "is no split package"}},
			{"a malformed table", nil, "not a row\n", []string{"bad test row"}},
			{"a missing table", nil, "-", []string{"legs need", "no-such-table.tsv"}},
		} {
			env, _ := authFixture(t, k, r1Names, c.rows)
			if c.raw != "" {
				p := filepath.Join(t.TempDir(), "no-such-table.tsv")
				if c.raw != "-" {
					p = filepath.Join(t.TempDir(), "broken.tsv")
					if err := os.WriteFile(p, []byte(c.raw), 0o644); err != nil {
						t.Fatal(err)
					}
				}
				env = append(env, k.env+"="+p)
			}
			out, errs, err := tree.checkGo(t, append(env, "AUTH_LEG_LIST=1"), "auth", k.kind+"-leg", "all", fmt.Sprint(k.legs))
			if err == nil {
				return fmt.Sprintf("%s: %s passed the legs:\n%s", k.kind, c.what, out)
			}
			for _, w := range c.want {
				if !strings.Contains(errs, w) {
					return fmt.Sprintf("%s: %s failed without naming %q: %s", k.kind, c.what, w, lastLines(errs))
				}
			}
		}
	}
	return ""
}

// R5: the auth path filter selects every auth split and weight table (paths-selftest prints each as relevant), and every
// ci/auth_race* table of the tree is one of them.
func cellAuthTablesSelectTheAuthLegs(t *testing.T, tree authTree) string {
	out, errs, err := tree.checkGo(t, nil, "auth", "paths-selftest")
	if err != nil {
		return fmt.Sprintf("auth paths-selftest: %v: %s", err, lastLines(errs))
	}
	for _, p := range authLegTables {
		if !strings.Contains(out, "auth path filter: "+p+" -> relevant\n") {
			return fmt.Sprintf("auth paths-selftest does not select the auth legs for %s", p)
		}
	}
	return ""
}

func TestAddingOneUnmeasuredTestMovesNoOtherAuthLegName(t *testing.T) {
	t.Parallel()
	if d := cellAuthLegsMoveNoOtherName(t, authTree{repoRoot(t)}); d != "" {
		t.Error(d)
	}
}

func TestAStaleShortStrayOrMissingAuthSplitTableFailsTheAuthLeg(t *testing.T) {
	t.Parallel()
	if d := cellAuthTableFaultsFailTheLeg(t, authTree{repoRoot(t)}); d != "" {
		t.Error(d)
	}
}

func TestEveryAuthSplitAndWeightTableSelectsTheAuthLegs(t *testing.T) {
	t.Parallel()
	if d := cellAuthTablesSelectTheAuthLegs(t, authTree{repoRoot(t)}); d != "" {
		t.Error(d)
	}
	entries, err := os.ReadDir(filepath.Join(repoRoot(t), "ci"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "auth_race") && !strings.Contains(strings.Join(authLegTables, " "), "ci/"+e.Name()) {
			t.Errorf("ci/%s is an auth race table the auth path filter cell does not name: add it to authLegTables and auth_path_relevant", e.Name())
		}
	}
	for _, p := range authLegTables {
		if _, err := os.Stat(filepath.Join(repoRoot(t), p)); err != nil {
			t.Errorf("%s: %v", p, err)
		}
	}
}

// C6/C2: no race or test-leg plan reads the auth tables: with both broken, race-shard-plan 1/6, test-shard-plan 1/10,
// test-shard-split-list 1/10 and the auth test leg listing print byte for byte the same (stdout and stderr).
func TestTheRaceAndTestLegPlansDoNotReadTheAuthSplitWeights(t *testing.T) {
	t.Parallel()
	broken := filepath.Join(t.TempDir(), "auth_split_weights.tsv")
	if err := os.WriteFile(broken, []byte("not a row\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	bad := []string{"AUTH_RACE_SPLIT_WEIGHTS=" + broken, "AUTH_RACE_INTEGRATION_SPLIT_WEIGHTS=" + broken}
	for _, c := range []struct {
		env  []string
		args []string
	}{
		{nil, []string{"race-shard-plan", "1", "6"}},
		{nil, []string{"test-shard-plan", "1", fmt.Sprint(shardCount)}},
		{nil, []string{"test-shard-split-list", "1", fmt.Sprint(shardCount)}},
		{[]string{"AUTH_LEG_LIST=1"}, []string{"auth", "test-leg", "1", "1"}},
	} {
		out, errs, err := checkGo(t, c.env, c.args...)
		if err != nil {
			t.Fatalf("%v: %v\n%s", c.args, err, errs)
		}
		bout, berrs, err := checkGo(t, append(append([]string{}, c.env...), bad...), c.args...)
		if err != nil || bout != out || berrs != errs {
			t.Errorf("%v reads an auth split weights table: a broken table changes its output (err %v)", c.args, err)
		}
	}
}

// mutantTree: a tree whose ci/check_go.sh is SRC with FROM replaced by TO; every other entry links to the real tree (the auth
// listing reads only the module and ci/).
func mutantTree(t *testing.T, src, from, to string) authTree {
	t.Helper()
	root := repoRoot(t)
	dir := t.TempDir()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() == "ci" {
			continue
		}
		if err := os.Symlink(filepath.Join(root, e.Name()), filepath.Join(dir, e.Name())); err != nil {
			t.Fatal(err)
		}
	}
	ci, err := os.ReadDir(filepath.Join(root, "ci"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "ci"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, e := range ci {
		if e.Name() == "check_go.sh" {
			continue
		}
		if err := os.Symlink(filepath.Join(root, "ci", e.Name()), filepath.Join(dir, "ci", e.Name())); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "ci", "check_go.sh"), []byte(strings.Replace(src, from, to, 1)), 0o755); err != nil {
		t.Fatal(err)
	}
	return authTree{dir}
}

// One planted mutant per rule of the auth wiring in ci/check_go.sh; each fails its cell (the cells hold with the real script: the
// three tests above).
func TestTheAuthSplitWiringPlantedMutantsAreCaught(t *testing.T) {
	t.Parallel()
	src := readRepoFile(t, "ci/check_go.sh")
	// Control: the tree copy with the real script passes every cell, so a mutant fails by its rule, not by the copy.
	control := mutantTree(t, src, "", "")
	for _, cell := range []func(*testing.T, authTree) string{cellAuthLegsMoveNoOtherName, cellAuthTableFaultsFailTheLeg, cellAuthTablesSelectTheAuthLegs} {
		if d := cell(t, control); d != "" {
			t.Fatalf("the tree copy with the real ci/check_go.sh fails a cell: %s", d)
		}
	}
	call := `    names="$(test_split_cut_names "${tests}" . "${pkg}" "${pkg#"${modpath}/"}" "${part}" "${parts}" "${flags[@]}")" || exit 2` + "\n"
	for _, c := range []struct {
		rule, from, to string
		cell           func(*testing.T, authTree) string
	}{
		{"an auth part's names are cut by the table, not round robin", call,
			`    names="$(test_split_listed_names . "${pkg}" "${parts}" "${flags[@]}" | awk -v p="${part}" -v np="${parts}" '(NR - 1) % np == p - 1')" || exit 2` + "\n",
			cellAuthLegsMoveNoOtherName},
		{"race-integration reads its own table", `    race-integration) printf '%s' "${AUTH_RACE_INTEGRATION_SPLIT_WEIGHTS}" ;;`,
			`    race-integration) printf '%s' "${AUTH_RACE_SPLIT_WEIGHTS}" ;;`, cellAuthLegsMoveNoOtherName},
		{"the listing keeps the leg's flags", call, strings.Replace(call, ` "${flags[@]}")"`, `)"`, 1), cellAuthLegsMoveNoOtherName},
		{"the auth plan refuses a row of a package the kind does not split", `-v splits="${splits}" -v tests="${tests}" -v defw=1`,
			`-v splits="${splits}" -v defw=1`, cellAuthTableFaultsFailTheLeg},
		{"a missing table fails the leg by name", `&& { [ -z "${tests}" ] || [ -f "${tests}" ]; }; }`, `; }`, cellAuthTableFaultsFailTheLeg},
		{"a table-only change selects the auth legs", `    ci/auth_race_splits.tsv|ci/auth_race_split_weights.tsv|`, `    ci/auth_race_splits.tsv|`,
			cellAuthTablesSelectTheAuthLegs},
	} {
		t.Run(c.rule, func(t *testing.T) {
			t.Parallel()
			if n := strings.Count(src, c.from); n != 1 {
				t.Fatalf("ci/check_go.sh holds %q %d times, not once: update this planted mutant", c.from, n)
			}
			d := c.cell(t, mutantTree(t, src, c.from, c.to))
			if d == "" {
				t.Errorf("the mutant passed its cell")
			}
			t.Logf("planted defect caught: %s", d)
		})
	}
}

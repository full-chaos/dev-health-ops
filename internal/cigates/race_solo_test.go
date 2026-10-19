package cigates_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// CHAOS-10289: internal/fixturescli runs alone in a race leg. ci/go_race_solo.tsv leaves it out of the weight-balanced
// split of ci/go_race_shard.awk, and ci/check_go.sh runs it last, in its own `go test -race`, in the leg that is lightest
// after the split. These guards drive the real awk and the real check_go.sh (a stand-in `go test` records its calls).

func soloExec(t *testing.T, dir string, env []string, stdin string, name string, args ...string) (string, string, error) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	return out.String(), errb.String(), err
}

func soloRows(t *testing.T, path string) map[string]string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	rows := map[string]string{}
	for _, l := range strings.Split(string(b), "\n") {
		if strings.TrimSpace(l) == "" || strings.HasPrefix(l, "#") {
			continue
		}
		k, v, _ := strings.Cut(l, "\t")
		rows[k] = v
	}
	return rows
}

// raceModulePackages is `go list ./...` of the root module without internal/providersync, the set the race legs split.
func raceModulePackages(t *testing.T, root string) (string, []string) {
	t.Helper()
	env := []string{"GOWORK=off", "GOFLAGS=-mod=readonly"}
	mod, errs, err := soloExec(t, root, env, "", "go", "list", "-m")
	if err != nil {
		t.Fatalf("go list -m: %v\n%s", err, errs)
	}
	mod = strings.TrimSpace(mod)
	out, errs, err := soloExec(t, root, env, "", "go", "list", "./...")
	if err != nil {
		t.Fatalf("go list ./...: %v\n%s", err, errs)
	}
	var pkgs []string
	for _, p := range strings.Fields(out) {
		if p != mod+"/internal/providersync" {
			pkgs = append(pkgs, p)
		}
	}
	return mod, pkgs
}

func shardAwk(t *testing.T, root, mod string, pkgs []string, shard, count int, weights, solo, part string) []string {
	t.Helper()
	args := []string{"-v", "mod=" + mod, "-v", fmt.Sprint("shard=", shard), "-v", fmt.Sprint("count=", count), "-v", "weights=" + weights}
	if solo != "" {
		args = append(args, "-v", "solo="+solo)
	}
	if part != "" {
		args = append(args, "-v", "part="+part)
	}
	args = append(args, "-f", filepath.Join(root, "ci", "go_race_shard.awk"))
	out, errs, err := soloExec(t, root, nil, strings.Join(pkgs, "\n")+"\n", "awk", args...)
	if err != nil {
		t.Fatalf("awk: %v\n%s", err, errs)
	}
	return strings.Fields(out)
}

func TestFixturescliIsPinnedToRunAloneInARaceLeg(t *testing.T) {
	root := repoRoot(t)
	solo := filepath.Join(root, "ci", "go_race_solo.tsv")
	rows := soloRows(t, solo)
	reason, ok := rows["internal/fixturescli"]
	if !ok || strings.TrimSpace(reason) == "" {
		t.Fatalf("ci/go_race_solo.tsv has no internal/fixturescli row with a reason: %v", rows)
	}
	weights := soloRows(t, filepath.Join(root, "ci", "go_race_weights.tsv"))
	mod, pkgs := raceModulePackages(t, root)
	target := mod + "/internal/fixturescli"
	if !contains(pkgs, target) {
		t.Fatalf("%s is not a package of the module", target)
	}
	for row := range rows {
		if _, ok := weights[row]; !ok {
			t.Errorf("solo row %s has no row in ci/go_race_weights.tsv: a solo package keeps its measured weight", row)
		}
		if !contains(pkgs, mod+"/"+row) {
			t.Errorf("solo row %s names no package of the module", row)
		}
	}
	for count := 1; count <= 4; count++ {
		var shared, alone []string
		for k := 1; k <= count; k++ {
			shared = append(shared, shardAwk(t, root, mod, pkgs, k, count, filepath.Join(root, "ci", "go_race_weights.tsv"), solo, "")...)
			alone = append(alone, shardAwk(t, root, mod, pkgs, k, count, filepath.Join(root, "ci", "go_race_weights.tsv"), solo, "solo")...)
		}
		if !reflect.DeepEqual(alone, []string{target}) {
			t.Fatalf("count %d: the solo parts are %v, want only %s", count, alone, target)
		}
		if contains(shared, target) {
			t.Fatalf("count %d: %s is still in a shared split", count, target)
		}
		if !sameMultiset(append(append([]string{}, shared...), alone...), pkgs) {
			t.Fatalf("count %d: shared plus solo is not a partition of the package list", count)
		}
	}
}

func TestRaceShardWithoutASoloFileSplitsEveryPackageAsBefore(t *testing.T) {
	root := repoRoot(t)
	weights := filepath.Join(root, "ci", "go_race_weights.tsv")
	mod, pkgs := raceModulePackages(t, root)
	missing := filepath.Join(t.TempDir(), "none.tsv")
	for _, count := range []int{2, 3} {
		for k := 1; k <= count; k++ {
			with := shardAwk(t, root, mod, pkgs, k, count, weights, missing, "")
			without := shardAwk(t, root, mod, pkgs, k, count, weights, "", "")
			if !reflect.DeepEqual(with, without) {
				t.Fatalf("count %d shard %d: a missing solo file changes the split", count, k)
			}
			if got := shardAwk(t, root, mod, pkgs, k, count, weights, missing, "solo"); len(got) != 0 {
				t.Fatalf("count %d shard %d: solo part %v with no solo rows", count, k, got)
			}
		}
	}
}

func TestSoloSplitIsAPartitionWhateverTheRowsAndWeightsSay(t *testing.T) {
	root := repoRoot(t)
	dir := t.TempDir()
	var pkgs []string
	for i := 0; i < 23; i++ {
		pkgs = append(pkgs, fmt.Sprintf("m/p%d", i))
	}
	solo := filepath.Join(dir, "solo.tsv")
	weights := filepath.Join(dir, "w.tsv")
	if err := os.WriteFile(solo, []byte("# c\np3\tr\np4\tr\np22\tr\nnot-a-package\tr\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(weights, []byte("p3\t100000\np4\t0\np5\t-7\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, count := range []int{1, 2, 4, 7} {
		var shared, alone []string
		for k := 1; k <= count; k++ {
			shared = append(shared, shardAwk(t, root, "m", pkgs, k, count, weights, solo, "")...)
			alone = append(alone, shardAwk(t, root, "m", pkgs, k, count, weights, solo, "solo")...)
		}
		sort.Strings(alone)
		if !reflect.DeepEqual(alone, []string{"m/p22", "m/p3", "m/p4"}) {
			t.Fatalf("count %d: solo parts %v", count, alone)
		}
		if !sameMultiset(append(append([]string{}, shared...), alone...), pkgs) {
			t.Fatalf("count %d: not a partition", count)
		}
	}
}

// soloStandInGo records every `go test` of check_go.sh and answers `go test -list` with the providersync weights file's
// test names; every other go subcommand is the real go.
const soloStandInGo = `#!/usr/bin/env bash
if [ "$1" != "test" ]; then exec "%s" "$@"; fi
case " $* " in *" -list "*)
  cat "${FAKE_GO_LISTING}"
  exit 0 ;;
esac
printf '%%s\n' "$*" >> "${FAKE_GO_LOG}"
exit 0
`

func TestRaceLegRunsItsSoloPackageLastAndInItsOwnGoTest(t *testing.T) {
	root := repoRoot(t)
	realGo, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	mod, _ := raceModulePackages(t, root)
	target := mod + "/internal/fixturescli"
	weights, err := os.ReadFile(filepath.Join(root, "ci", "go_providersync_race_weights.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	var listing []string
	for _, l := range strings.Split(string(weights), "\n") {
		if l != "" && !strings.HasPrefix(l, "#") {
			listing = append(listing, strings.SplitN(l, "\t", 2)[0])
		}
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(fmt.Sprintf(soloStandInGo, realGo)), 0o755); err != nil {
		t.Fatal(err)
	}
	listPath := filepath.Join(dir, "listing.txt")
	if err := os.WriteFile(listPath, []byte(strings.Join(listing, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	owners := 0
	for shard := 1; shard <= 3; shard++ {
		log := filepath.Join(dir, fmt.Sprintf("calls-%d.log", shard))
		if err := os.WriteFile(log, nil, 0o644); err != nil {
			t.Fatal(err)
		}
		env := []string{"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"), "FAKE_GO_LOG=" + log, "FAKE_GO_LISTING=" + listPath, "TMPDIR=" + dir}
		stdout, errs, err := soloExec(t, root, env, "", "bash", "ci/check_go.sh", "ci-leg", "race", fmt.Sprint(shard), "3")
		if err != nil {
			t.Fatalf("race leg %d/3 failed: %v\n%s\n%s", shard, err, stdout, errs)
		}
		b, _ := os.ReadFile(log)
		calls := strings.Split(strings.TrimSpace(string(b)), "\n")
		var carrying []int
		for i, c := range calls {
			if contains(strings.Fields(c), target) {
				carrying = append(carrying, i)
			}
		}
		if len(carrying) == 0 {
			continue
		}
		owners++
		if len(carrying) != 1 {
			t.Fatalf("shard %d: %s runs %d times in one leg: %v", shard, target, len(carrying), calls)
		}
		soloAt := carrying[0]
		lastShared, provider := -1, -1
		for i, c := range calls {
			f := strings.Fields(c)
			if contains(f, mod+"/cmd/dho") && !contains(f, target) {
				lastShared = i
			}
			if strings.HasSuffix(c, "./internal/providersync") {
				provider = i
			}
		}
		if lastShared < 0 || provider < 0 || soloAt < lastShared || soloAt < provider {
			t.Fatalf("shard %d: %s must run after the leg's other packages and its providersync slice: %v", shard, target, calls)
		}
		var pkgsOfCall []string
		for _, w := range strings.Fields(calls[soloAt]) {
			if strings.HasPrefix(w, mod) {
				pkgsOfCall = append(pkgsOfCall, w)
			}
		}
		if !reflect.DeepEqual(pkgsOfCall, []string{target}) {
			t.Fatalf("shard %d: the solo go test names %v, want only %s", shard, pkgsOfCall, target)
		}
		if !strings.Contains(" "+calls[soloAt]+" ", " -race ") {
			t.Fatalf("shard %d: the solo go test is not a -race run: %s", shard, calls[soloAt])
		}
		if !strings.Contains(stdout, "solo, alone") {
			t.Fatalf("shard %d: the leg log does not say it ran a solo package: %s", shard, stdout)
		}
	}
	if owners != 1 {
		t.Fatalf("exactly one race leg of three runs the solo package, got %d", owners)
	}
}

func TestRaceSoloCensusNamesEachDefect(t *testing.T) {
	root := repoRoot(t)
	census := func(text string, absent bool) (string, error) {
		path := filepath.Join(t.TempDir(), "solo.tsv")
		if !absent {
			if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		_, errs, err := soloExec(t, root, []string{"GO_RACE_SOLO=" + path}, "", "bash", "ci/check_go.sh", "race-weights-rows")
		return errs, err
	}
	green, err := os.ReadFile(filepath.Join(root, "ci", "go_race_solo.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	if errs, err := census(string(green), false); err != nil {
		t.Fatalf("the committed solo file fails the census: %v\n%s", err, errs)
	}
	for _, c := range []struct{ name, text, want string }{
		{"no weights row", "internal/zz-no-weights-row\treason\n", "no row in ci/go_race_weights.tsv"},
		{"listed twice", "internal/fixturescli\ta\ninternal/fixturescli\tb\n", "twice"},
		{"unsorted", "internal/storage/clickhouse\ta\ninternal/fixturescli\tb\n", "not sorted"},
		{"providersync", "internal/providersync\ta\n", "cannot be solo"},
	} {
		errs, err := census(c.text, false)
		if err == nil || !strings.Contains(errs, c.want) {
			t.Errorf("%s: want a failure containing %q, got err=%v\n%s", c.name, c.want, err, errs)
		}
	}
	errs, err := census("", true)
	if err == nil || !strings.Contains(errs, "race solo file missing") {
		t.Errorf("a missing solo file must fail the census, got err=%v\n%s", err, errs)
	}
}

func sameMultiset(a, b []string) bool {
	x := append([]string{}, a...)
	y := append([]string{}, b...)
	sort.Strings(x)
	sort.Strings(y)
	return reflect.DeepEqual(x, y)
}

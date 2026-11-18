package testshard_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// CHAOS-13958: three heavy packages in one race batch ran 1.2-2.0x their time alone (race leg 13: 881, 1037, 1280 s). The race
// plan now keeps the heavy packages of ci/go_race_apart.tsv one per leg and off the legs of its listed serial parts. The same
// plan with no apart rows is the red: it stacks the three on one leg.

type raceApartLeg struct {
	pkgs  []string // package keys of the leg's batch and solo packages (root module, providersync left out)
	parts []string // "key#part" of the leg's race parts
}

func raceApartRows(t *testing.T) (heavy map[string]bool, parts map[string]bool) {
	t.Helper()
	heavy = map[string]bool{}
	for k := range raceApartHeavySeconds(t) {
		heavy[k] = true
	}
	_, parts = raceApartTable(t)
	return heavy, parts
}

// raceApartHeavySeconds: the heavy rows of ci/go_race_apart.tsv, package key -> its median seconds.
func raceApartHeavySeconds(t *testing.T) map[string]int {
	t.Helper()
	heavy, _ := raceApartTable(t)
	return heavy
}

func raceApartTable(t *testing.T) (heavy map[string]int, parts map[string]bool) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "go_race_apart.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	heavy, parts = map[string]int{}, map[string]bool{}
	for _, l := range strings.Split(string(b), "\n") {
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		f := strings.Split(l, "\t")
		if len(f) != 3 {
			t.Fatalf("ci/go_race_apart.tsv: bad row %q", l)
		}
		switch f[0] {
		case "heavy":
			v, err := strconv.Atoi(f[2])
			if err != nil {
				t.Fatalf("ci/go_race_apart.tsv: bad seconds in %q", l)
			}
			heavy[f[1]] = v
		case "part":
			parts[f[1]+"#"+f[2]] = true
		default:
			t.Fatalf("ci/go_race_apart.tsv: bad kind in %q", l)
		}
	}
	if len(heavy) == 0 || len(parts) == 0 {
		t.Fatal("ci/go_race_apart.tsv holds no heavy or no part row")
	}
	return heavy, parts
}

// raceApartPlan plans every race leg of count with env and returns each leg's rows and the plans' stderr.
func raceApartPlan(t *testing.T, env []string, count int) ([]raceApartLeg, string) {
	t.Helper()
	var legs []raceApartLeg
	var warns strings.Builder
	for k := 1; k <= count; k++ {
		out, errs, err := checkGo(t, env, "race-shard-plan", strconv.Itoa(k), strconv.Itoa(count))
		if err != nil {
			t.Fatalf("race-shard-plan %d %d %v: %v\n%s", k, count, env, err, errs)
		}
		warns.WriteString(errs)
		var leg raceApartLeg
		for _, row := range strings.Split(out, "\n") {
			f := strings.Split(row, "\t")
			if len(f) < 3 || f[0] != "." || f[2] == "internal/providersync" {
				continue
			}
			if len(f) == 5 {
				leg.parts = append(leg.parts, f[2]+"#"+f[3])
			} else {
				leg.pkgs = append(leg.pkgs, f[2])
			}
		}
		legs = append(legs, leg)
	}
	return legs, warns.String()
}

// raceApartDefects: the heavy rule's breaks in a plan (two heavy packages on a leg, a heavy package beside a listed part).
func raceApartDefects(legs []raceApartLeg, heavy, parts map[string]bool) []string {
	var d []string
	for i, leg := range legs {
		var h, p []string
		for _, k := range leg.pkgs {
			if heavy[k] {
				h = append(h, k)
			}
		}
		for _, k := range leg.parts {
			if parts[k] {
				p = append(p, k)
			}
		}
		if len(h) > 1 {
			d = append(d, "leg "+strconv.Itoa(i+1)+" holds heavy "+strings.Join(h, ", "))
		}
		if len(h) > 0 && len(p) > 0 {
			d = append(d, "leg "+strconv.Itoa(i+1)+" holds heavy "+strings.Join(h, ", ")+" beside part "+strings.Join(p, ", "))
		}
	}
	return d
}

func TestTheRacePlanKeepsTheHeavyPackagesApartAndOffTheListedParts(t *testing.T) {
	t.Parallel()
	_, races, _ := legCounts(t)
	heavy, parts := raceApartRows(t)
	legs, warns := raceApartPlan(t, nil, races)
	for _, d := range raceApartDefects(legs, heavy, parts) {
		t.Error(d)
	}
	if strings.Contains(warns, "WARN race plan:") {
		t.Errorf("the race plan of %d legs warns:\n%s", races, warns)
	}
	seen := map[string]int{}
	for _, leg := range legs {
		for _, k := range leg.pkgs {
			seen[k]++
		}
		for _, k := range leg.parts {
			seen[k]++
		}
	}
	for k := range heavy {
		if seen[k] != 1 {
			t.Errorf("heavy row %s: planned %d time(s), want once (a row of no race package, or a split/solo one)", k, seen[k])
		}
	}
	for k := range parts {
		if seen[k] != 1 {
			t.Errorf("part row %s: planned %d time(s), want once", k, seen[k])
		}
	}
}

// The red of the apart rule: the same plan without apart rows puts two heavy packages on one leg. It runs at 13 race legs
// (oldPlanLegs: the leg count of the plans that stacked them on main, not the shipped count); with more legs the plan spreads
// them by itself.
func TestTheRacePlanWithoutApartRowsStacksHeavyPackages(t *testing.T) {
	t.Parallel()
	const oldPlanLegs = 13
	heavy, parts := raceApartRows(t)
	none := filepath.Join(t.TempDir(), "apart.tsv")
	if err := os.WriteFile(none, []byte("# no rows\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	legs, _ := raceApartPlan(t, []string{"GO_RACE_APART=" + none}, oldPlanLegs)
	d := raceApartDefects(legs, heavy, parts)
	if len(d) == 0 {
		t.Errorf("the %d-leg race plan without apart rows keeps every heavy package apart by itself: the red is gone", oldPlanLegs)
	}
	t.Logf("%d legs, no apart rows: %v", oldPlanLegs, d)
}

// raceApartAwk runs ci/go_race_shard.awk (rserial, apart) on packages m/<name> with the given rows.
func raceApartAwk(t *testing.T, shard, count int, pkgs []string, weights, splits, apart string, extra ...string) (string, string, error) {
	t.Helper()
	dir := t.TempDir()
	write := func(name, text string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	cmd := exec.Command("awk", "-v", "mod=m", "-v", "shard="+strconv.Itoa(shard), "-v", "count="+strconv.Itoa(count),
		"-v", "weights="+write("w.tsv", weights), "-v", "splits="+write("s.tsv", splits), "-v", "rserial=1", "-v", "rk=1.5",
		"-v", "apart="+write("a.tsv", apart))
	// extra: "solo=<rows>" or "pin=<rows>": a table written to a file and passed as that variable.
	for _, e := range extra {
		k, rows, _ := strings.Cut(e, "=")
		cmd.Args = append(cmd.Args, "-v", k+"="+write(k+".tsv", rows))
	}
	cmd.Args = append(cmd.Args, "-f", filepath.Join(repoRoot(t), "ci", "go_race_shard.awk"))
	in := ""
	for _, p := range pkgs {
		in += "m/" + p + "\n"
	}
	cmd.Stdin = strings.NewReader(in)
	var out, errb strings.Builder
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	return out.String(), errb.String(), err
}

func TestTheRacePlanRefusesABadApartRowAndWarnsWhenNoLegIsFree(t *testing.T) {
	t.Parallel()
	pkgs := []string{"a", "b", "c", "s"}
	weights := "a\t500\nb\t400\nc\t300\ns\t90\n"
	splits := "s\t3\n"
	for _, c := range []struct{ name, apart, want string }{
		{"kind", "big\ta\t500\n", "bad apart row"},
		{"no value", "heavy\ta\n", "bad apart row"},
		{"zero", "heavy\ta\t0\n", "bad apart row"},
		{"repeat", "heavy\ta\t500\nheavy\ta\t500\n", "repeats heavy a"},
		{"split heavy", "heavy\ts\t90\n", "heavy s is a split package"},
		{"part of no split", "part\ta\t1\n", "part 1 of a is no race split part"},
		{"part past the count", "part\ts\t4\n", "part 4 of s is no race split part"},
		{"repeat part", "part\ts\t2\npart\ts\t2\n", "repeats part 2 of s"},
		{"repeat part padded", "part\ts\t02\npart\ts\t2\n", "repeats part 2 of s"},
		{"repeat part padded twice", "part\ts\t2\npart\ts\t002\n", "repeats part 2 of s"},
		{"blank package", "heavy\t\t500\n", "bad apart row"},
		{"space package", "heavy\t \t500\n", "bad apart row"},
		{"package with a space", "heavy\ta b\t500\n", "bad apart row"},
		{"no weights row", "heavy\tzz\t500\n", "heavy zz has no race weights row"},
		{"solo heavy not planned", "heavy\tq\t500\n", "heavy q is solo or pinned"},
		{"pinned heavy not planned", "heavy\tr\t500\n", "heavy r is solo or pinned"},
	} {
		// q is solo and r pinned, and neither is in this run's packages: the row is refused when the file is read.
		_, errs, err := raceApartAwk(t, 1, 2, pkgs, weights+"q\t50\nr\t50\n", splits, c.apart, "solo=q\tx\n", "pin=r\tx\n")
		if err == nil || !strings.Contains(errs, c.want) {
			t.Errorf("%s: want a refusal %q, got err %v\n%s", c.name, c.want, err, errs)
		}
	}
	// Three heavy packages on two legs: one leg takes two, and that leg says so.
	var warns []string
	place := map[string]int{}
	for k := 1; k <= 2; k++ {
		out, errs, err := raceApartAwk(t, k, 2, pkgs, weights, splits, "heavy\ta\t500\nheavy\tb\t400\nheavy\tc\t300\n")
		if err != nil {
			t.Fatalf("leg %d: %v\n%s", k, err, errs)
		}
		for _, l := range strings.Split(strings.TrimSpace(errs), "\n") {
			if l != "" {
				warns = append(warns, l)
			}
		}
		for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
			place[strings.Split(l, "\t")[0]]++
		}
	}
	if len(warns) != 1 || !strings.HasPrefix(warns[0], "WARN race plan: m/c heavy, no leg without a heavy package or a listed part, on leg ") {
		t.Errorf("want one WARN for m/c, got %q", warns)
	}
	// A part written 01 is part 1: the heavy package keeps off its leg as it does for 1 (part 1 is on the leg the heavy
	// package would take first).
	for _, num := range []string{"1", "01"} {
		var partLeg, heavyLeg int
		for k := 1; k <= 3; k++ {
			out, errs, err := raceApartAwk(t, k, 3, pkgs, weights, splits, "heavy\ta\t500\npart\ts\t"+num+"\n")
			if err != nil {
				t.Fatalf("part %s, leg %d: %v\n%s", num, k, err, errs)
			}
			for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
				f := strings.Split(l, "\t")
				if f[0] == "m/a" {
					heavyLeg = k
				}
				if f[0] == "m/s" && len(f) == 3 && f[1] == "1" {
					partLeg = k
				}
			}
		}
		if partLeg == 0 || heavyLeg == 0 || partLeg == heavyLeg {
			t.Errorf("part row %q: heavy m/a on leg %d, part 1 of m/s on leg %d: want two legs", num, heavyLeg, partLeg)
		}
	}
	var got []string
	for p, n := range place {
		got = append(got, p+"="+strconv.Itoa(n))
	}
	sort.Strings(got)
	if strings.Join(got, " ") != "m/a=1 m/b=1 m/c=1 m/s=3" {
		t.Errorf("the plan of two legs is no partition: %v", got)
	}
}

// CHAOS-13833: the shipped race legs (ci/go_quality_legs.json count) together run every root-module race package exactly once (a
// split package: each of its parts once), none of ci/race_excluded_packages.tsv, and the providersync slice on every leg.
func TestTheShippedRaceLegsCoverEveryRacePackageOnce(t *testing.T) {
	t.Parallel()
	_, races, _ := legCounts(t)
	mod, pkgs := raceshardGoList(t)
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "race_excluded_packages.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	excluded := map[string]bool{}
	for _, l := range strings.Split(string(b), "\n") {
		if f := strings.Split(l, "\t"); l != "" && !strings.HasPrefix(l, "#") && f[0] != "" {
			excluded[f[0]] = true
		}
	}
	b, err = os.ReadFile(filepath.Join(repoRoot(t), "ci", "go_race_splits.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	parts := map[string]int{}
	for _, l := range strings.Split(string(b), "\n") {
		if f := strings.Split(l, "\t"); len(f) == 2 && !strings.HasPrefix(l, "#") {
			n, err := strconv.Atoi(f[1])
			if err != nil {
				t.Fatalf("ci/go_race_splits.tsv: bad row %q", l)
			}
			parts[f[0]] = n
		}
	}
	want := map[string]int{}
	for _, p := range pkgs {
		k := keyOf(mod, p)
		if excluded[k] || k == "internal/providersync" {
			continue
		}
		if n := parts[k]; n > 1 {
			for i := 1; i <= n; i++ {
				want[k+"#"+strconv.Itoa(i)] = 1
			}
			continue
		}
		want[k] = 1
	}
	got := map[string]int{}
	slices := 0
	for k := 1; k <= races; k++ {
		out, errs, err := checkGo(t, nil, "race-shard-plan", strconv.Itoa(k), strconv.Itoa(races))
		if err != nil {
			t.Fatalf("race-shard-plan %d %d: %v\n%s", k, races, err, errs)
		}
		if strings.Contains(errs, "WARN race plan:") {
			t.Errorf("race leg %d/%d warns:\n%s", k, races, errs)
		}
		slice := 0
		for _, row := range strings.Split(out, "\n") {
			f := strings.Split(row, "\t")
			if len(f) < 3 {
				continue
			}
			if f[0] != "." {
				got[f[0]+" "+f[2]]++ // a nested module's package: counted for duplicates only
				continue
			}
			switch {
			case f[2] == "internal/providersync":
				slice++
			case len(f) == 5:
				got[f[2]+"#"+f[3]]++
			default:
				got[f[2]]++
			}
		}
		if slice != 1 {
			t.Errorf("race leg %d/%d lists the providersync slice %d time(s), want once", k, races, slice)
		}
		slices += slice
	}
	for k, n := range got {
		if n != 1 {
			t.Errorf("%s: in %d race legs, want exactly 1", k, n)
		}
		if !strings.Contains(k, " ") && want[k] == 0 {
			t.Errorf("%s: planned, but it is no race package (race-excluded, or not in go list)", k)
		}
	}
	for k := range want {
		if got[k] == 0 {
			t.Errorf("%s: in no race leg", k)
		}
	}
	t.Logf("%d race legs: %d root items (packages and parts) each once, providersync on %d legs", races, len(want), slices)
}

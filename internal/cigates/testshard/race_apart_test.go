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
// plan now keeps the heavy packages of ci/go_race_apart.tsv one per leg and off the legs of its listed serial parts, at a batch
// overlap of 1.5 (GO_RACE_BATCH_OVERLAP). The old plan (overlap 3.5, no apart rows, the old checkgo and affected rows) is the red.

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

func TestTheOldRacePlanStacksTheHeavyTrioOnOneLeg(t *testing.T) {
	t.Parallel()
	_, races, _ := legCounts(t)
	heavy, parts := raceApartRows(t)
	dir := t.TempDir()
	src := filepath.Join(repoRoot(t), "ci", "go_race_weights.d")
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	old := map[string]string{"internal__cigates__checkgo.tsv": "internal/cigates/checkgo\t985\n", "internal__cigates__affected.tsv": "internal/cigates/affected\t947\n"}
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if row, ok := old[e.Name()]; ok {
			b = []byte(row)
		}
		if err := os.WriteFile(filepath.Join(dir, e.Name()), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	none := filepath.Join(t.TempDir(), "apart.tsv")
	if err := os.WriteFile(none, []byte("# no rows\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	legs, _ := raceApartPlan(t, []string{"GO_RACE_BATCH_OVERLAP=3.5", "GO_RACE_APART=" + none, "GO_RACE_WEIGHTS_DIR=" + dir}, races)
	// The trio of race leg 13: the heavy rows of 600 s or more.
	var trio []string
	for k, v := range raceApartHeavySeconds(t) {
		if v >= 600 {
			trio = append(trio, k)
		}
	}
	sort.Strings(trio)
	if len(trio) != 3 {
		t.Fatalf("ci/go_race_apart.tsv holds %d heavy rows of 600 s or more, want the 3 of race leg 13: %v", len(trio), trio)
	}
	stacked := false
	for _, leg := range legs {
		n := 0
		for _, k := range leg.pkgs {
			for _, x := range trio {
				if k == x {
					n++
				}
			}
		}
		stacked = stacked || n == len(trio)
	}
	if !stacked {
		t.Errorf("the old race plan does not put %v on one leg: the red is gone", trio)
	}
	if len(raceApartDefects(legs, heavy, parts)) == 0 {
		t.Error("the heavy rule finds no break in the old race plan")
	}
}

// raceApartAwk runs ci/go_race_shard.awk (rserial, apart) on packages m/<name> with the given rows.
func raceApartAwk(t *testing.T, shard, count int, pkgs []string, weights, splits, apart string) (string, string, error) {
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
		"-v", "apart="+write("a.tsv", apart), "-f", filepath.Join(repoRoot(t), "ci", "go_race_shard.awk"))
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
	} {
		_, errs, err := raceApartAwk(t, 1, 2, pkgs, weights, splits, c.apart)
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
	var got []string
	for p, n := range place {
		got = append(got, p+"="+strconv.Itoa(n))
	}
	sort.Strings(got)
	if strings.Join(got, " ") != "m/a=1 m/b=1 m/c=1 m/s=3" {
		t.Errorf("the plan of two legs is no partition: %v", got)
	}
}

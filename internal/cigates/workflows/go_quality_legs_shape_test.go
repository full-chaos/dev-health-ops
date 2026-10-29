package workflows_test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The leg file is validated as a whole before any event filter: one static leg with args "static", test-K-of-N with args "test K N"
// for K = 1..N, race-K-of-M (heavy) with args "race K M" for K = 1..M, and no other name. The queue-tested check then takes the
// EXPECTED job names from that validated list: a run counts only when exactly those legs ran green.

type legEntry struct {
	Name  string `json:"name"`
	Args  string `json:"args"`
	Heavy bool   `json:"heavy,omitempty"`
	Pool  bool   `json:"pool,omitempty"`
}

func goodLegs() []legEntry {
	l := []legEntry{{Name: "static", Args: "static"}}
	for k := 1; k <= 4; k++ {
		l = append(l, legEntry{Name: fmt.Sprintf("test-%d-of-4", k), Args: fmt.Sprintf("test %d 4", k)})
	}
	for k := 1; k <= 5; k++ {
		l = append(l, legEntry{Name: fmt.Sprintf("race-%d-of-5", k), Args: fmt.Sprintf("race %d 5", k), Heavy: true, Pool: true})
	}
	return l
}

func legsJSON(t *testing.T, l []legEntry) string {
	t.Helper()
	b, err := json.Marshal(l)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func shapeCells(t *testing.T) []struct {
	name string
	legs []legEntry
	want string
} {
	t.Helper()
	mut := func(f func(l []legEntry) []legEntry) []legEntry { return f(goodLegs()) }
	padded := []legEntry{{Name: "static", Args: "static"}}
	for i := 1; i <= 9; i++ {
		padded = append(padded, legEntry{Name: fmt.Sprintf("padding-%d", i), Args: "static"})
	}
	return []struct {
		name string
		legs []legEntry
		want string
	}{
		{"the padded matrix of the review: static plus nine uniquely named all-static legs", padded, "unknown leg name padding-1"},
		{"wrong args on a correct name", mut(func(l []legEntry) []legEntry { l[2].Args = "test 1 4"; return l }), "leg test-2-of-4 has args"},
		{"wrong race args on a correct name", mut(func(l []legEntry) []legEntry { l[6].Args = "race 1 5"; return l }), "leg race-2-of-5 has args"},
		{"a gap in the test legs", mut(func(l []legEntry) []legEntry { return append(l[:3], l[4:]...) }), "test legs are not K = 1..N complete"},
		{"a gap in the race legs", mut(func(l []legEntry) []legEntry { return append(l[:7], l[8:]...) }), "race legs are not K = 1..M complete"},
		{"mixed test shard counts", mut(func(l []legEntry) []legEntry {
			l[4] = legEntry{Name: "test-4-of-3", Args: "test 4 3"}
			return l
		}), "test legs name more than one shard count"},
		{"mixed race leg counts", mut(func(l []legEntry) []legEntry {
			l[9] = legEntry{Name: "race-5-of-4", Args: "race 5 4", Heavy: true, Pool: true}
			return l
		}), "race legs name more than one leg count"},
		{"a race leg without heavy", mut(func(l []legEntry) []legEntry { l[5].Heavy = false; return l }), "race leg race-1-of-5 must be heavy"},
		{"an extra unknown name", append(goodLegs(), legEntry{Name: "extra", Args: "static"}), "unknown leg name extra"},
		{"static with other args", mut(func(l []legEntry) []legEntry { l[0].Args = "test 1 4"; return l }), "the static leg must have args"},
		{"no test legs", mut(func(l []legEntry) []legEntry { return append(l[:1], l[5:]...) }), "no test leg"},
		{"no race legs", mut(func(l []legEntry) []legEntry { return l[:5] }), "no race leg"},
		{"no static leg", mut(func(l []legEntry) []legEntry { return l[1:] }), "no static leg"},
		{"static twice", mut(func(l []legEntry) []legEntry { return append(l, legEntry{Name: "static", Args: "static"}) }), "duplicate"},
	}
}

func TestTheLegSelectorValidatesTheWholeLegFileBeforeFiltering(t *testing.T) {
	root := repoRoot(t)
	for _, c := range shapeCells(t) {
		f := filepath.Join(t.TempDir(), "legs.json")
		if err := os.WriteFile(f, []byte(legsJSON(t, c.legs)), 0o644); err != nil {
			t.Fatal(err)
		}
		for _, args := range [][]string{{"merge_group", "false"}, {"push", "false"}, {"pull_request", "true"}, {"pull_request", "false"}} {
			out, errs, code := runLegsScript(t, root, append(append([]string{}, args...), f)...)
			if code != 3 || strings.Contains(out, "static") {
				t.Errorf("%s %v: exit %d, stdout %q: a malformed leg list must fail the plan job with exit 3", c.name, args, code, out)
			}
			if !strings.Contains(errs, c.want) {
				t.Errorf("%s %v: stderr %q must name the reason %q", c.name, args, errs, c.want)
			}
		}
	}
}

func TestTheLegSelectorAcceptsTheWellFormedLegFile(t *testing.T) {
	root := repoRoot(t)
	f := filepath.Join(t.TempDir(), "legs.json")
	if err := os.WriteFile(f, []byte(legsJSON(t, goodLegs())), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"merge_group", "false"}, {"push", "false"}, {"pull_request", "true"}, {"pull_request", "false"}} {
		if _, errs, code := runLegsScript(t, root, append(append([]string{}, args...), f)...); code != 0 {
			t.Errorf("%v: exit %d, stderr %q: a well-formed leg list must pass", args, code, errs)
		}
	}
}

// runQualityHelperNames runs the queue helper in its expected-names form: the sixth argument is @<file> with one expected job name per line.
func runQualityHelperNames(t *testing.T, jobs, namesFile string) bool {
	t.Helper()
	dir := t.TempDir()
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write("gh", queueGhStub)
	write("runs.json", qualityRunsJSON(qualitySha, "merge_group", qualityRepo, "success"))
	write("jobs-21.json", jobs)
	arg := "@" + filepath.Join(dir, "no-such-names-file")
	if namesFile != "" {
		write("names.txt", namesFile)
		arg = "@" + filepath.Join(dir, "names.txt")
	}
	cmd := exec.Command("bash", filepath.Join(rootOf(t), "ci", "queue_tested_sha.sh"), qualityRepo, qualitySha, "go-quality.yml", qualityLegPrefix, qualityLegStep, arg)
	cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "STUB_DIR="+dir)
	return cmd.Run() == nil
}

func expectedNamesText(names []string) string {
	var b strings.Builder
	for _, n := range names {
		b.WriteString(qualityLegPrefix + n + ")\n")
	}
	return b.String()
}

func TestTheQueueHelperTakesTheExpectedLegNamesNotACount(t *testing.T) {
	all := expectedNamesText(qualityLegNames)
	ok := func(names []string) string {
		var legs []qualityLeg
		for _, n := range names {
			legs = append(legs, qualityLeg{name: n, job: "success", step: "success"})
		}
		return qualityJobsJSON(legs)
	}
	padded := []string{"static"}
	for i := 1; i <= 9; i++ {
		padded = append(padded, fmt.Sprintf("padding-%d", i))
	}
	for _, c := range []struct {
		name string
		jobs string
		want bool
	}{
		{"every expected leg ran green", ok(qualityLegNames), true},
		{"the padded matrix of the review (ten distinct successful names, wrong ones)", ok(padded), false},
		{"one expected leg is missing", ok(qualityLegNames[:9]), false},
		{"an extra prefixed job beside every expected leg", ok(append(append([]string{}, qualityLegNames...), "padding-1")), false},
		{"one expected leg is a duplicate of another (ten jobs, nine names)", ok(append(append([]string{}, qualityLegNames[:9]...), "static")), false},
		{"one expected leg failed", qualityJobsJSON(qualityLegs(func(i int, l *qualityLeg) {
			if i == 3 {
				l.job = "failure"
			}
		})), false},
		{"one expected leg skipped its gate step", qualityJobsJSON(qualityLegs(func(i int, l *qualityLeg) {
			if i == 7 {
				l.step = "skipped"
			}
		})), false},
	} {
		if got := runQualityHelperNames(t, c.jobs, all); got != c.want {
			t.Errorf("%s: helper accepted = %v, want %v", c.name, got, c.want)
		}
	}
	if runQualityHelperNames(t, ok(qualityLegNames), "") {
		t.Error("a missing expected-names file must be refused")
	}
	if runQualityHelperNames(t, ok(qualityLegNames), "\n\n") {
		t.Error("an empty expected-names file must be refused")
	}
}

func TestTheLegSelectorRefusesALegWithoutAStringName(t *testing.T) {
	root := repoRoot(t)
	base := strings.TrimSuffix(legsJSON(t, goodLegs()), "]")
	for _, extra := range []string{`{"args":"static"}`, `{"name":7,"args":"static"}`, `{"name":null,"args":"static"}`} {
		f := filepath.Join(t.TempDir(), "legs.json")
		if err := os.WriteFile(f, []byte(base+","+extra+"]"), 0o644); err != nil {
			t.Fatal(err)
		}
		out, errs, code := runLegsScript(t, root, "merge_group", "false", f)
		if code != 3 || strings.Contains(out, "static") || !strings.Contains(errs, "has no string name") {
			t.Errorf("%s: exit %d, stdout %q, stderr %q: a leg without a string name must fail with exit 3", extra, code, out, errs)
		}
	}
}

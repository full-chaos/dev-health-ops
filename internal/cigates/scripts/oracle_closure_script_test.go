//go:build !race

package scripts_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The dependency closure file of a venue package (ci/venue_oracle_closure.d/<package>.txt) and the per-leg slices of a
// changed-file selection: a missing, empty, malformed or unreadable file is refused, never read as "nothing to run".

func closureTree(t *testing.T, tests ...string) *otree {
	t.Helper()
	if len(tests) == 0 {
		tests = []string{"TestVenueOracleX"}
	}
	return scratchOTree(t, []pkgTests{{"internal/apiservice", tests}})
}

func (o *otree) listRC(changedFile string) cmdResult {
	o.t.Helper()
	o.write("changed.txt", changedFile+"\n")
	return o.venueOracles(nil, "--changed", filepath.Join(o.dir, "changed.txt"), "--list").res
}

func wantFail(t *testing.T, r cmdResult, needle string, ctx ...any) {
	t.Helper()
	if r.code == 0 || !strings.Contains(r.stderr, needle) {
		t.Fatalf("%v: want a failure containing %q, got exit %d: %s", ctx, needle, r.code, tail(r.stderr, 300))
	}
}

func TestOracleClosureFiles(t *testing.T) {
	t.Parallel()
	t.Run("a missing closure directory fails loudly in the real layout", func(t *testing.T) {
		t.Parallel()
		o := closureTree(t)
		o.write("changed.txt", "internal/auth/signedtoken/x.go\n")
		list := func() cmdResult {
			return o.venueOracles(nil, "--changed", filepath.Join(o.dir, "changed.txt"), "--list").res
		}
		if r := list(); r.code != 0 {
			t.Fatalf("control: a scratch tree without the generator keeps the package and row rules: %s", r.stderr)
		}
		o.mkdir("internal/testsupport/venuechanged")
		wantFail(t, list(), "ci/venue_oracle_closure.d is missing")
		o.mkdir("ci/venue_oracle_closure.d")
		wantFail(t, list(), "internal__apiservice.txt is missing")
	})
	t.Run("the selection never reads an unreadable or empty registry as nothing to run", func(t *testing.T) {
		t.Parallel()
		o := closureTree(t)
		if err := os.RemoveAll(filepath.Join(o.dir, "ci", "venue_oracle_registry.d")); err != nil {
			t.Fatal(err)
		}
		wantFail(t, o.listRC("internal/auth/signedtoken/x.go"), "could not be read")
		l := scratchOTree(t, []pkgTests{{"internal/apiservice", []string{"TestVenueOracleX"}}}, "TestVenueOracleX")
		wantFail(t, l.listRC("internal/auth/signedtoken/x.go"), "holds no run row")
	})
	t.Run("a closure file that is empty or malformed is refused", func(t *testing.T) {
		t.Parallel()
		o := closureTree(t)
		o.mkdir("ci/venue_oracle_closure.d")
		for _, c := range []struct{ body, msg string }{
			{"", "is empty"}, {"\n", "has a line that is not a package directory"},
			{"internal/a\x01b\n", "has a line that is not a package directory"}, {"~\n", "has a line that is not a package directory"},
		} {
			o.write("ci/venue_oracle_closure.d/internal__apiservice.txt", c.body)
			wantFail(t, o.listRC("internal/auth/signedtoken/x.go"), c.msg, c.body)
		}
		o.write("ci/venue_oracle_closure.d/internal__apiservice.txt", "internal/auth/signedtoken\n~internal/auth/passwordpolicy/data\n")
		r := o.venueOracles(nil, "--changed", filepath.Join(o.dir, "changed.txt"), "--list").res
		if r.code != 0 || !strings.Contains(r.stdout, "TestVenueOracleX") {
			t.Fatalf("a good closure file: exit %d: %s", r.code, tail(r.stderr, 300))
		}
	})
	t.Run("a closure line with a space or a comma selects", func(t *testing.T) {
		t.Parallel()
		o := closureTree(t)
		o.write("ci/venue_oracle_closure.d/internal__apiservice.txt", "internal/dep_test\n~internal/dep_test/a,b\n~internal/dep_test/assets/a b\n")
		want := []selectedRow{{"internal/apiservice", "TestVenueOracleX"}}
		for _, changed := range []string{"internal/dep_test/x.go", "internal/dep_test/a,b/value.txt", "internal/dep_test/assets/a b/value.txt"} {
			if got := o.selected(false, changed); !sameSelected(got, want) {
				t.Errorf("%s selects %v, want %v", changed, got, want)
			}
		}
		if got := o.selected(false, "internal/dep_test/assets/ab/value.txt"); len(got) != 0 {
			t.Errorf("a near miss selects %v", got)
		}
	})
	t.Run("a closure path that is a directory is refused", func(t *testing.T) {
		t.Parallel()
		o := closureTree(t)
		o.mkdir("ci/venue_oracle_closure.d/internal__apiservice.txt")
		wantFail(t, o.listRC("internal/auth/signedtoken/x.go"), "not a readable regular file")
	})
	t.Run("a change inside the venue package still validates its closure file", func(t *testing.T) {
		t.Parallel()
		o := closureTree(t)
		o.mkdir("ci/venue_oracle_closure.d")
		wantFail(t, o.listRC("internal/apiservice/x.go"), "internal__apiservice.txt is missing")
		o.write("ci/venue_oracle_closure.d/internal__apiservice.txt", "internal/a\n")
		r := o.venueOracles(nil, "--changed", filepath.Join(o.dir, "changed.txt"), "--list").res
		if r.code != 0 || !strings.Contains(r.stdout, "TestVenueOracleX") {
			t.Fatalf("exit %d: %s", r.code, tail(r.stderr, 300))
		}
	})
	t.Run("a closure line that can never match is refused", func(t *testing.T) {
		t.Parallel()
		o := closureTree(t)
		o.mkdir("ci/venue_oracle_closure.d")
		for _, body := range []string{"~internal/dep/data/\n", "internal/dep/\n", "~/abs\n", "internal//dep\n", "~internal/./dep\n", "../up\n"} {
			o.write("ci/venue_oracle_closure.d/internal__apiservice.txt", body)
			wantFail(t, o.listRC("internal/auth/signedtoken/x.go"), "not a clean relative directory", body)
		}
	})
}

func (o *otree) legRows(shard, count int) []string {
	o.t.Helper()
	r := o.venueOracles(nil, "--changed", filepath.Join(o.dir, "changed.txt"), "--list", strconv.Itoa(shard), strconv.Itoa(count))
	if r.res.code != 0 {
		o.t.Fatalf("leg %d/%d: %s", shard, count, r.res.stderr)
	}
	var rows []string
	for _, l := range strings.Split(r.res.stdout, "\n") {
		if strings.HasPrefix(l, "SELECT\t") {
			rows = append(rows, l)
		}
	}
	return rows
}

func TestOracleChangedLegs(t *testing.T) {
	t.Parallel()
	t.Run("the legs of a change set partition its selection", func(t *testing.T) {
		t.Parallel()
		o := scratchOTree(t, []pkgTests{
			{"internal/a", []string{"TestVenueOracleA1", "TestVenueOracleA2"}}, {"internal/b", []string{"TestVenueOracleB"}},
			{"internal/c", []string{"TestVenueOracleC"}}, {"internal/d", []string{"TestVenueOracleD"}},
		})
		o.write("changed.txt", "internal/a/x.go\ninternal/b/x.go\ninternal/c/x.go\n")
		whole := parseSelected(o.venueOracles(nil, "--changed", filepath.Join(o.dir, "changed.txt"), "--list").res.stdout)
		if len(whole) != 4 {
			t.Fatalf("the whole selection is %v", whole)
		}
		for _, count := range []int{1, 2, 3, 7} {
			seen := map[string]int{}
			total := 0
			for shard := 1; shard <= count; shard++ {
				for _, row := range o.legRows(shard, count) {
					seen[row]++
					total++
				}
			}
			if total != len(whole) || len(seen) != len(whole) {
				t.Errorf("count %d: legs hold %d rows (%d distinct), the selection has %d", count, total, len(seen), len(whole))
			}
		}
		empty := false
		for shard := 1; shard <= 7; shard++ {
			if len(o.legRows(shard, 7)) == 0 {
				empty = true
			}
		}
		if !empty {
			t.Error("more legs than rows must leave some legs empty")
		}
	})
	t.Run("a leg with an empty slice is a green no-op", func(t *testing.T) {
		t.Parallel()
		o := scratchOTree(t, []pkgTests{{"internal/a", []string{"TestVenueOracleA"}}})
		o.write("changed.txt", "internal/a/x.go\n")
		empty := 0
		for s := 1; s <= 3; s++ {
			if len(o.legRows(s, 3)) == 0 {
				empty = s
				break
			}
		}
		if empty == 0 {
			t.Fatal("one row cannot fill three legs")
		}
		r := o.venueOracles(nil, "--changed", filepath.Join(o.dir, "changed.txt"), strconv.Itoa(empty), "3")
		if r.res.code != 0 || !strings.Contains(r.res.stdout, "nothing to run") {
			t.Fatalf("exit %d: %s%s", r.res.code, tail(r.res.stdout, 300), tail(r.res.stderr, 300))
		}
	})
	t.Run("a leg outside one to count is refused with a change set", func(t *testing.T) {
		t.Parallel()
		o := scratchOTree(t, []pkgTests{{"internal/a", []string{"TestVenueOracleA"}}})
		o.write("changed.txt", "internal/a/x.go\n")
		for _, args := range [][]string{{"0", "3"}, {"4", "3"}, {"x", "3"}} {
			r := o.venueOracles(nil, append([]string{"--changed", filepath.Join(o.dir, "changed.txt"), "--list"}, args...)...)
			if r.res.code == 0 {
				t.Errorf("%v accepted", args)
			}
		}
	})
	t.Run("the run of a leg runs only its slice", func(t *testing.T) {
		t.Parallel()
		o := scratchOTree(t, []pkgTests{{"internal/a", []string{"TestVenueOracleA"}}, {"internal/b", []string{"TestVenueOracleB"}}})
		o.write("changed.txt", "internal/a/x.go\ninternal/b/x.go\n")
		var ran [2][]string
		for shard := 1; shard <= 2; shard++ {
			r := o.venueOracles(nil, "--changed", filepath.Join(o.dir, "changed.txt"), strconv.Itoa(shard), "2")
			if r.res.code != 0 {
				t.Fatalf("leg %d: %s", shard, tail(r.res.stderr, 300))
			}
			for _, l := range strings.Split(r.res.stdout, "\n") {
				if strings.Contains(l, "  RUN  ") {
					ran[shard-1] = append(ran[shard-1], l)
				}
			}
		}
		if len(ran[0]) != 1 || len(ran[1]) != 1 || ran[0][0] == ran[1][0] {
			t.Errorf("legs ran %v", ran)
		}
	})
	t.Run("the changed job matrix holds the whole registry within its timeout", func(t *testing.T) {
		t.Parallel()
		job := oracleWorkflowJob(t, "venue-oracles-changed.yml", "venue-oracles-changed")
		shards, _ := nested(job, "strategy", "matrix", "shard").([]any)
		for i, s := range shards {
			if n, _ := s.(int); n != i+1 {
				t.Fatalf("matrix shard list %v is not 1..N", shards)
			}
		}
		if fmt.Sprint(nested(job, "env", "VENUE_LEGS")) != strconv.Itoa(len(shards)) || nested(job, "env", "VENUE_LEG") != "${{ matrix.shard }}" {
			t.Fatalf("env %v does not match %d legs", job["env"], len(shards))
		}
		o := realOTree(t)
		_, legs := oraclePlan(t, o, len(shards))
		if len(legs) != len(shards) {
			t.Fatalf("%d planned legs, %d matrix legs", len(legs), len(shards))
		}
		limit := job["timeout-minutes"].(int)*60 - 600
		for k, l := range legs {
			if l[1] > limit {
				t.Errorf("leg %d plans %ds of tests, over the %ds the job timeout leaves", k, l[1], limit)
			}
		}
	})
	t.Run("the leg receipt counts the selected rows not the registry", func(t *testing.T) {
		t.Parallel()
		o := scratchOTree(t, []pkgTests{{"internal/a", []string{"TestVenueOracleA"}}, {"internal/b", []string{"TestVenueOracleB"}}})
		o.write("changed.txt", "internal/a/x.go\n")
		r := o.venueOracles(nil, "--changed", filepath.Join(o.dir, "changed.txt"), "--list", "1", "1")
		if r.res.code != 0 || !strings.Contains(r.res.stderr, "takes 1 of 1 selected row(s)") {
			t.Fatalf("exit %d: %s", r.res.code, r.res.stderr)
		}
	})
}

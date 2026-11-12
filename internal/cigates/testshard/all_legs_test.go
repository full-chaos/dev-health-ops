package testshard_test

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// The test-leg plan verbs take SHARD `all` (CHAOS-13520): `VERB all COUNT` prints, for each leg 1..COUNT, a
// "==> VERB K COUNT" line and then what `VERB K COUNT` prints after the module lines every call opens with, from one
// listing of each module and split package. The leg guards read that one output instead of one plan per leg.

// allLegs runs `VERB all COUNT` and returns what precedes the first leg header and, per leg, the lines after its header.
// It fails unless the headers are exactly legs 1..COUNT, in order, each once.
func allLegs(t *testing.T, env []string, verb string, count int) (string, map[int]string) {
	t.Helper()
	out, errs, err := checkGo(t, env, verb, "all", strconv.Itoa(count))
	if err != nil {
		t.Fatalf("%s all %d: %v\n%s", verb, count, err, errs)
	}
	header := regexp.MustCompile(`(?m)^==> ` + regexp.QuoteMeta(verb) + ` ([0-9]+) ` + strconv.Itoa(count) + `\n`)
	loc := header.FindAllStringSubmatchIndex(out, -1)
	parts := map[int]string{}
	for i, m := range loc {
		k, _ := strconv.Atoi(out[m[2]:m[3]])
		if k != i+1 {
			t.Fatalf("%s all %d: header %d names leg %d, want legs 1..%d in order, each once", verb, count, i+1, k, count)
		}
		end := len(out)
		if i+1 < len(loc) {
			end = loc[i+1][0]
		}
		parts[k] = out[m[1]:end]
	}
	if len(loc) != count {
		t.Fatalf("%s all %d prints %d leg header(s), want %d", verb, count, len(loc), count)
	}
	return out[:loc[0][0]], parts
}

var allLegVerbs = []string{"test-shard-plan", "test-shard-split-list", "race-excluded-shard-plan"}

// TestTheAllLegsFormsPrintEachLegsOwnOutput: for two real legs, the leg holding the most split-package parts and the
// other leg holding the most race-excluded rows, each verb's own `VERB K COUNT` output equals the all-legs form's
// opening lines plus its part for leg K, byte for byte, and stderr is equal.
func TestTheAllLegsFormsPrintEachLegsOwnOutput(t *testing.T) {
	t.Parallel()
	prefix, parts := map[string]string{}, map[string]map[int]string{}
	for _, verb := range allLegVerbs {
		prefix[verb], parts[verb] = allLegs(t, nil, verb, shardCount)
	}
	most := func(verb string, keep func(string) bool, skip int) int {
		best, n := 0, -1
		for k := 1; k <= shardCount; k++ {
			c := 0
			for _, l := range strings.Split(parts[verb][k], "\n") {
				if keep(l) {
					c++
				}
			}
			if k != skip && c > n {
				best, n = k, c
			}
		}
		return best
	}
	splitLeg := most("test-shard-plan", func(l string) bool { return strings.Count(l, "\t") == 4 }, 0)
	raceLeg := most("race-excluded-shard-plan", func(l string) bool { return strings.Count(l, "\t") == 1 }, splitLeg)
	type res struct{ out, errs string }
	got := map[string]res{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, verb := range allLegVerbs {
		for _, k := range []int{splitLeg, raceLeg} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				out, errs, err := checkGo(t, nil, verb, strconv.Itoa(k), strconv.Itoa(shardCount))
				if err != nil {
					errs = fmt.Sprintf("exit: %v\n%s", err, errs)
				}
				mu.Lock()
				defer mu.Unlock()
				got[fmt.Sprintf("%s %d", verb, k)] = res{out, errs}
			}()
		}
	}
	wg.Wait()
	for _, verb := range allLegVerbs {
		for _, k := range []int{splitLeg, raceLeg} {
			r := got[fmt.Sprintf("%s %d", verb, k)]
			if want := prefix[verb] + parts[verb][k]; r.out != want {
				a, b := strings.Split(r.out, "\n"), strings.Split(want, "\n")
				i := 0
				for i < len(a) && i < len(b) && a[i] == b[i] {
					i++
				}
				g, e := "<end>", "<end>"
				if i < len(a) {
					g = a[i]
				}
				if i < len(b) {
					e = b[i]
				}
				t.Errorf("%s %d %d differs from its part of %s all %d at line %d: %q, all-legs form %q", verb, k, shardCount, verb, shardCount, i+1, g, e)
			}
			if r.errs != "" {
				t.Errorf("%s %d %d: stderr %q", verb, k, shardCount, r.errs)
			}
		}
	}
}

// TestTheAllLegsFormNeedsACount: `VERB all` without a positive COUNT is refused, as `VERB K` is.
func TestTheAllLegsFormNeedsACount(t *testing.T) {
	t.Parallel()
	for _, verb := range allLegVerbs {
		for _, count := range []string{"0", "x"} {
			if _, errs, err := checkGo(t, nil, verb, "all", count); err == nil || !strings.Contains(errs, "test COUNT must be a positive integer") {
				t.Errorf("%s all %s: want a refusal naming the count, got err=%v stderr=%s", verb, count, err, errs)
			}
		}
	}
}

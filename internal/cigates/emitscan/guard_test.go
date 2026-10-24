package emitscan_test

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates/emitscan"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

// pinKey identifies a pinned finding by site, not by line: rule, file, enclosing function and detail.
type pinKey struct{ rule, file, fn, detail string }

type pin struct {
	key   pinKey
	count int
}

func readPins(t *testing.T) []pin {
	t.Helper()
	handle, err := os.Open("pins.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	var pins []pin
	scanner := bufio.NewScanner(handle)
	first := true
	for scanner.Scan() {
		if first {
			first = false
			continue
		}
		fields := strings.Split(scanner.Text(), "\t")
		if len(fields) != 6 || strings.TrimSpace(fields[5]) == "" {
			t.Fatalf("pins.tsv: a row needs rule, file, func, detail, count and a reason: %q", scanner.Text())
		}
		count, err := strconv.Atoi(fields[4])
		if err != nil || count < 1 {
			t.Fatalf("pins.tsv: bad count in the row for %s", fields[1])
		}
		pins = append(pins, pin{key: pinKey{fields[0], fields[1], fields[2], fields[3]}, count: count})
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return pins
}

// TestEmitPathsMatchTheCensus is the gate: every finding of the scan over the
// emit paths is pinned exactly (rule, file, count, reason), a pin without a
// finding is stale, and a file the scan cannot read is a failure.
func TestEmitPathsMatchTheCensus(t *testing.T) {
	findings, err := emitscan.Scan(repoRoot(t), []string{"internal", "cmd"})
	if err != nil {
		t.Fatal(err)
	}
	pinned := map[pinKey]int{}
	for _, p := range readPins(t) {
		if _, dup := pinned[p.key]; dup {
			t.Fatalf("pins.tsv: %s %s %s is pinned twice", p.key.rule, p.key.file, p.key.fn)
		}
		pinned[p.key] = p.count
	}
	found := map[pinKey][]emitscan.Finding{}
	for _, finding := range findings {
		detail := finding.Detail
		if detail == "" {
			detail = "-"
		}
		key := pinKey{finding.Rule, finding.File, finding.Func, detail}
		found[key] = append(found[key], finding)
	}
	var keys []pinKey
	for key := range found {
		keys = append(keys, key)
	}
	for key := range pinned {
		if _, ok := found[key]; !ok {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].file != keys[j].file {
			return keys[i].file < keys[j].file
		}
		if keys[i].fn != keys[j].fn {
			return keys[i].fn < keys[j].fn
		}
		return keys[i].rule < keys[j].rule
	})
	seen := map[pinKey]bool{}
	for _, key := range keys {
		if seen[key] {
			continue
		}
		seen[key] = true
		have := found[key]
		want := pinned[key]
		if len(have) == want {
			continue
		}
		if len(have) == 0 {
			t.Errorf("%s %s %s %s: stale pin (no finding), remove the row", key.rule, key.file, key.fn, key.detail)
			continue
		}
		var where []string
		for _, finding := range have {
			where = append(where, fmt.Sprintf("%d", finding.Line))
		}
		t.Errorf("%s %s %s %s: %d finding(s) at line %s, %d pinned", key.rule, key.file, key.fn, key.detail, len(have), strings.Join(where, ","), want)
	}
}

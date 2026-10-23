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

type pin struct {
	rule, file string
	count      int
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
		if len(fields) != 4 || strings.TrimSpace(fields[3]) == "" {
			t.Fatalf("pins.tsv: a row needs rule, file, count and a reason: %q", scanner.Text())
		}
		count, err := strconv.Atoi(fields[2])
		if err != nil || count < 1 {
			t.Fatalf("pins.tsv: bad count in the row for %s", fields[1])
		}
		pins = append(pins, pin{rule: fields[0], file: fields[1], count: count})
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
	pinned := map[[2]string]int{}
	for _, p := range readPins(t) {
		key := [2]string{p.rule, p.file}
		if _, dup := pinned[key]; dup {
			t.Fatalf("pins.tsv: %s %s is pinned twice", p.rule, p.file)
		}
		pinned[key] = p.count
	}
	found := map[[2]string][]emitscan.Finding{}
	for _, finding := range findings {
		key := [2]string{finding.Rule, finding.File}
		found[key] = append(found[key], finding)
	}
	var keys [][2]string
	for key := range found {
		keys = append(keys, key)
	}
	for key := range pinned {
		if _, ok := found[key]; !ok {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i][1] != keys[j][1] {
			return keys[i][1] < keys[j][1]
		}
		return keys[i][0] < keys[j][0]
	})
	seen := map[[2]string]bool{}
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
			t.Errorf("%s %s: stale pin (no finding), remove the row", key[0], key[1])
			continue
		}
		var where []string
		for _, finding := range have {
			where = append(where, fmt.Sprintf("%d", finding.Line))
		}
		t.Errorf("%s %s: %d finding(s) at line %s, %d pinned", key[0], key[1], len(have), strings.Join(where, ","), want)
	}
}

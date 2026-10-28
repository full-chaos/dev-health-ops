package pgmigrate

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func runHistoryVerb(t *testing.T, run func(context.Context, cli.Env) int, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), cli.Env{Args: args, Stdout: &stdout, Stderr: &stderr})
	return code, stdout.String(), stderr.String()
}

// The embedded walk holds one entry per revision, starts at the base, and its
// heads are exactly the baseline's heads.
func TestHistoryHoldsTheBaselineHeads(t *testing.T) {
	entries, err := LoadHistory()
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	var heads []string
	for _, entry := range entries {
		if seen[entry.Revision] {
			t.Errorf("revision %s is listed twice", entry.Revision)
		}
		seen[entry.Revision] = true
		if entry.RealHead {
			heads = append(heads, entry.Revision)
		}
		if entry.Down == "" {
			t.Errorf("revision %s has no down revision text", entry.Revision)
		}
	}
	if entries[len(entries)-1].Down != "<base>" {
		t.Errorf("the walk ends at %q, want the base", entries[len(entries)-1].Down)
	}
	// The walk is the frozen Alembic history: it ends at FrozenHead. The archived Go-only revisions and the chain add
	// the rest, up to the build's heads.
	if want := []string{cutoverRevision, FrozenHead}; strings.Join(sortedCopy(heads), ",") != strings.Join(want, ",") {
		t.Errorf("the walk's heads are %v, want %v", heads, want)
	}
	chain, err := LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	var withChain []string
	for _, entry := range WithChain(entries, baseline, chain) {
		if entry.RealHead {
			withChain = append(withChain, entry.Revision)
		}
	}
	if want := Heads(baseline, chain); strings.Join(sortedCopy(withChain), ",") != strings.Join(want, ",") {
		t.Errorf("the walk with the archive and the chain has heads %v, the build reaches %v", withChain, want)
	}
}

func sortedCopy(values []string) []string {
	out := append([]string(nil), values...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

// A chain revision that is an Alembic revision is already in the walk: nothing is added.
// A chain revision with no Alembic script heads the application branch: it prints
// ahead of the branch, the branch's top is no longer a head, and the next one
// descends from it.
func TestHistoryWithAChain(t *testing.T) {
	entries, err := LoadHistory()
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	archived := ArchivedChain()
	if got := WithChain(entries, baseline, ChainThrough(archived, FrozenHead)); len(got) != len(entries)+len(ChainAbove(archived, FrozenHead)) {
		t.Errorf("the walk with the archive has %d entries, want %d: the Alembic revisions are added once, the Go-only ones each", len(got), len(entries)+len(ChainAbove(archived, FrozenHead)))
	}
	top := ""
	for _, entry := range WithChain(entries, baseline, nil) {
		if entry.RealHead && entry.Revision != "0066" {
			top = entry.Revision
		}
	}
	if top != applicationHead(baseline) {
		t.Errorf("the walk with the archive ends at %s, want the baseline %s", top, applicationHead(baseline))
	}
	chain := append([]ChainFile(nil),
		ChainFile{Revision: "9001", Name: "9001_add_widget_table.sql"},
		ChainFile{Revision: "9002", Name: "9002_widget_index.sql"})
	var out bytes.Buffer
	if err := WriteHistory(&out, WithChain(entries, baseline, chain)); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(out.String(), "\n")
	var found []string
	for index, line := range lines {
		if strings.Contains(line, " -> 9002") {
			found = lines[index : index+3]
			break
		}
	}
	if len(found) == 0 {
		t.Fatalf("no line for the new head:\n%s", out.String())
	}
	if found[0] != "9001 -> 9002 (head), Widget index." || found[1] != top+" -> 9001, Add widget table." {
		t.Errorf("chain lines = %q", found[:2])
	}
	if !strings.Contains(found[2], " -> "+top) || strings.Contains(found[2], "(head)") {
		t.Errorf("the old application head line = %q, want no head mark", found[2])
	}
	if !strings.HasPrefix(lines[0], "0065 -> 0066 (river_cutover) (head), ") {
		t.Errorf("the cutover head line changed: %q", lines[0])
	}
}

func TestHistoryRefusesVerboseAndPositionals(t *testing.T) {
	for _, args := range [][]string{{"--verbose"}, {"-v"}, {"extra"}} {
		code, stdout, _ := runHistoryVerb(t, history, args...)
		if code != cli.ExitUsage || stdout != "" {
			t.Errorf("history %v: exit %d stdout %q, want exit 2 and nothing on stdout", args, code, stdout)
		}
	}
	code, stdout, _ := runHistoryVerb(t, history)
	if code != cli.ExitOK || !strings.HasSuffix(stdout, "<base> -> 0001, Initial consolidated schema migration.\n") {
		t.Errorf("history: exit %d, stdout ends %q", code, stdout[max(0, len(stdout)-80):])
	}
}

// The squash changes no line of `history`: the archived revisions print as the chain printed them before
// (testdata/history_before_squash.txt is the output of the build before the squash, af8d28ed).
func TestHistoryOutputIsUnchangedByTheSquash(t *testing.T) {
	want, err := os.ReadFile("testdata/history_before_squash.txt")
	if err != nil {
		t.Fatal(err)
	}
	entries, err := LoadHistory()
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := WriteHistory(&out, WithChain(entries, baseline, chain)); err != nil {
		t.Fatal(err)
	}
	if out.String() != string(want) {
		t.Errorf("history changed:\n%s\nwant:\n%s", out.String(), want)
	}
}

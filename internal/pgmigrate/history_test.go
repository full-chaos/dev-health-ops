package pgmigrate

import (
	"bytes"
	"context"
	"encoding/json"
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
	if strings.Join(sortedCopy(heads), ",") != strings.Join(sortedCopy(baseline.Heads), ",") {
		t.Errorf("the walk's heads are %v, the baseline's %v", heads, baseline.Heads)
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

// A revision after the baseline heads the application branch: it prints ahead of
// the branch, the old application head is no longer a head, and the next one
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
	application := applicationHead(baseline)
	chain := []ChainFile{
		{Revision: "0141", Name: "0141_add_widget_table.sql"},
		{Revision: "0142", Name: "0142_widget_index.sql"},
	}
	var out bytes.Buffer
	if err := WriteHistory(&out, WithChain(entries, baseline, chain)); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(out.String(), "\n")
	var top []string
	for index, line := range lines {
		if strings.Contains(line, " -> 0142") {
			top = lines[index : index+3]
			break
		}
	}
	if len(top) == 0 {
		t.Fatalf("no line for the new head:\n%s", out.String())
	}
	if top[0] != "0141 -> 0142 (head), Widget index." || top[1] != application+" -> 0141, Add widget table." {
		t.Errorf("chain lines = %q", top[:2])
	}
	if !strings.HasPrefix(top[2], "0139 -> "+application+" (application_schema), ") || strings.Contains(top[2], "(head)") {
		t.Errorf("the old application head line = %q, want no head mark", top[2])
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

// downgrade refuses whatever the target: nothing is read, nothing is written,
// and the refusal names the forward-only design and both ways back.
func TestDowngradeRefuses(t *testing.T) {
	for _, target := range []string{"-1", "base", "0139", "0066@base"} {
		code, stdout, stderr := runHistoryVerb(t, downgrade, target)
		if code != cli.ExitRefused || stdout != "" {
			t.Errorf("downgrade %s: exit %d stdout %q, want exit 3 and nothing on stdout", target, code, stdout)
		}
		var body struct {
			Error struct{ Code, Detail string }
		}
		if err := json.Unmarshal([]byte(stderr), &body); err != nil {
			t.Fatalf("downgrade %s: stderr %q is not the error line: %v", target, stderr, err)
		}
		if body.Error.Code != "forward_only" || !strings.Contains(body.Error.Detail, "forward-only") ||
			!strings.Contains(body.Error.Detail, "restore the database from a backup") || !strings.Contains(body.Error.Detail, "alembic downgrade") {
			t.Errorf("downgrade %s: error %+v does not name the design and both recoveries", target, body.Error)
		}
	}
	for _, args := range [][]string{{}, {"a", "b"}, {"--nope", "0139"}} {
		if code, _, _ := runHistoryVerb(t, downgrade, args...); code != cli.ExitUsage {
			t.Errorf("downgrade %v: exit %d, want 2", args, code)
		}
	}
}

package pgmigrate

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func aliasNames(resolve ResolveDSN) []string {
	var names []string
	for _, alias := range Aliases(resolve) {
		names = append(names, alias.Name)
	}
	return names
}

// applicationRevisions is the baseline's application head and the revision below it.
func applicationRevisions(t *testing.T) (head, below string, entries []HistoryEntry) {
	t.Helper()
	baseline, err := LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	entries, err = LoadHistory()
	if err != nil {
		t.Fatal(err)
	}
	head = applicationHead(baseline)
	for _, entry := range entries {
		if entry.Revision == head {
			below = entry.Down
		}
	}
	if below == "" || below == "<base>" {
		t.Fatalf("no revision below the application head %s", head)
	}
	return head, below, entries
}

// _database_has_revision: any current head that is the target or descends from it.
func TestHasRevisionWalksTheGraph(t *testing.T) {
	head, below, entries := applicationRevisions(t)
	for _, tc := range []struct {
		name    string
		current []string
		target  string
		want    bool
	}{
		{"the head itself", []string{head}, head, true},
		{"a descendant of the target", []string{head}, "0075", true},
		{"an ancestor is not enough", []string{below}, head, false},
		{"the cutover branch", []string{"0066"}, "0066", true},
		{"the cutover branch does not reach the application head", []string{"0066"}, head, false},
		{"a branch point is an ancestor of both", []string{"0066"}, "0065", true},
		{"no heads", nil, head, false},
		{"below the minimum", []string{"0070"}, "0075", false},
		{"either head suffices", []string{"0066", head}, head, true},
		{"an unknown revision ends its branch", []string{"9999"}, head, false},
	} {
		if got := HasRevision(entries, tc.current, tc.target); got != tc.want {
			t.Errorf("%s: HasRevision(%v, %s) = %v, want %v", tc.name, tc.current, tc.target, got, tc.want)
		}
	}
}

// _required_postgres_revisions: the application head, plus 0066 with the cutover.
func TestRequiredRevisions(t *testing.T) {
	head, _, _ := applicationRevisions(t)
	baseline, err := LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	if got := RequiredRevisions(baseline, nil, false); !reflect.DeepEqual(got, []string{head}) {
		t.Errorf("without the cutover: %v", got)
	}
	if got := RequiredRevisions(baseline, nil, true); !reflect.DeepEqual(got, []string{head, "0066"}) {
		t.Errorf("with the cutover: %v", got)
	}
	chain := []ChainFile{{Revision: "9001", Name: "9001_x.sql"}}
	if got := RequiredRevisions(baseline, chain, true); !reflect.DeepEqual(got, []string{"9001", "0066"}) {
		t.Errorf("with a chain revision: %v", got)
	}
}

func TestAliasVerbsRefuseWhatPythonRefuses(t *testing.T) {
	resolve := ResolveDSN(nil)
	names := map[string]cli.Command{}
	for _, alias := range Aliases(resolve) {
		names[alias.Name] = alias
	}
	for _, want := range []string{"current", "heads", "history", "downgrade", "status"} {
		if _, ok := names[want]; !ok {
			t.Errorf("no %s alias (have %v)", want, aliasNames(resolve))
		}
	}
	for _, args := range [][]string{{"--nope"}, {"extra"}} {
		var stderr strings.Builder
		if code := names["status"].Run(context.Background(), cli.Env{Args: args, Stdout: &stderr, Stderr: &stderr}); code != cli.ExitUsage {
			t.Errorf("status %v: exit %d, want 2", args, code)
		}
	}
}

package pgmigrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func downgradeFixture(t *testing.T) (Baseline, []ChainFile, []DownFile, DownRange, map[string]bool) {
	t.Helper()
	baseline, err := LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	down, err := LoadDownChain()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := LoadHistory()
	if err != nil {
		t.Fatal(err)
	}
	return baseline, chain, down, NewDownRange(baseline, chain, down), KnownRevisions(entries, baseline, chain)
}

// Every chain revision has down SQL, one file each and named alike: a revision added
// to sql/ without its sql/down file fails here, so the ported range cannot silently
// stop short of the head.
func TestDownChainCoversEveryChainRevision(t *testing.T) {
	_, chain, down, r, _ := downgradeFixture(t)
	if len(chain) == 0 {
		t.Fatal("the chain is empty: the check measures nothing")
	}
	if len(down) != len(chain) {
		t.Fatalf("%d down files for %d chain files", len(down), len(chain))
	}
	for index, file := range chain {
		if down[index].Name != file.Name {
			t.Errorf("sql/down/%s does not pair with sql/%s", down[index].Name, file.Name)
		}
		if strings.TrimSpace(down[index].SQL) == "" || !regexp.MustCompile(`(?im)^\s*(CREATE|DROP|ALTER|UPDATE|DELETE)\b`).MatchString(down[index].SQL) {
			t.Errorf("sql/down/%s holds no statement", down[index].Name)
		}
	}
	if len(r.Covered) != len(chain) || r.Floor != "0138" || r.Covered[0] != "0139" {
		t.Errorf("range %+v, want floor 0138 and every chain revision from 0139", r)
	}
}

// PlanDowngrade and ParseDowngradeTarget enumerate every cell: each refusal code and
// each accepted shape is pinned with its exact steps.
func TestPlanDowngradeCells(t *testing.T) {
	_, _, _, r, known := downgradeFixture(t)
	top := r.Covered[len(r.Covered)-1]
	steps := func(from, to string) []DownStep {
		order := append([]string{r.Floor}, r.Covered...)
		var out []DownStep
		var start, stop int
		for index, revision := range order {
			if revision == from {
				start = index
			}
			if revision == to {
				stop = index
			}
		}
		for position := start; position > stop; position-- {
			out = append(out, DownStep{Revision: order[position], Previous: order[position-1]})
		}
		return out
	}
	graph := known
	cases := []struct {
		name     string
		recorded []string
		arg      string
		steps    []DownStep
		code     string
	}{
		{"explicit, prod-like two heads, leaves 0066", []string{"0066", top}, "0144", steps(top, "0144"), ""},
		{"explicit to the floor", []string{"0066", top}, "0138", steps(top, "0138"), ""},
		{"explicit equal to current", []string{"0066", top}, top, nil, ""},
		{"explicit, single head", []string{"0141"}, "0139", steps("0141", "0139"), ""},
		{"-1, single head", []string{top}, "-1", steps(top, "0147"), ""},
		{"-3, single head", []string{top}, "-3", steps(top, "0145"), ""},
		{"-10 reaches exactly the floor", []string{top}, "-10", steps(top, "0138"), ""},
		{"-11 needs the floor revision itself", []string{top}, "-11", nil, "below_baseline_floor"},
		{"-1 with 0066 recorded: Python reverts 0066 first", []string{"0066", top}, "-1", nil, "below_baseline_floor"},
		{"-2 with 0066 recorded", []string{"0066", top}, "-2", nil, "below_baseline_floor"},
		{"-1 at the floor", []string{"0066", "0138"}, "-1", nil, "below_baseline_floor"},
		{"explicit above current", []string{"0140"}, "0143", nil, "target_not_below_current"},
		{"explicit, recorded below the floor", []string{"0066", "0100"}, "0139", nil, "below_baseline_floor"},
		{"nothing recorded", nil, "0139", nil, "no_revision_recorded"},
		{"a revision this build does not know", []string{"9999"}, "0139", nil, "ahead_of_build"},
		{"pre-database: 0066", nil, "0066", nil, "below_baseline_floor"},
		{"pre-database: 0137", nil, "0137", nil, "below_baseline_floor"},
		{"pre-database: unknown id", nil, "0999", nil, "unknown_revision"},
		{"pre-database: word", nil, "abc", nil, "unknown_revision"},
		{"pre-database: partial id", nil, "014", nil, "unknown_revision"},
		{"pre-database: base", nil, "base", nil, "unsupported_target"},
		{"pre-database: head", nil, "head", nil, "unsupported_target"},
		{"pre-database: heads", nil, "heads", nil, "unsupported_target"},
		{"pre-database: +1", nil, "+1", nil, "unsupported_target"},
		{"pre-database: rev@-1", nil, "0145@-1", nil, "unsupported_target"},
		{"pre-database: branch@base", nil, "application_schema@base", nil, "unsupported_target"},
		{"pre-database: -0", nil, "-0", nil, "unsupported_target"},
		{"pre-database: -1.5", nil, "-1.5", nil, "unsupported_target"},
	}
	for _, tc := range cases {
		target, refusal := ParseDowngradeTarget(tc.arg, graph)
		if refusal == nil {
			refusal = r.CheckTarget(target)
		}
		preDB := strings.HasPrefix(tc.name, "pre-database")
		if preDB {
			if refusal == nil || refusal.Code != tc.code || !refusal.PreDatabase {
				t.Errorf("%s: got %+v, want a pre-database refusal %s", tc.name, refusal, tc.code)
			}
			continue
		}
		if refusal != nil {
			t.Errorf("%s: refused before the database: %+v", tc.name, refusal)
			continue
		}
		got, refusal := PlanDowngrade(tc.recorded, target, r, known)
		switch {
		case tc.code != "":
			if refusal == nil || refusal.Code != tc.code || refusal.PreDatabase {
				t.Errorf("%s: got steps %v refusal %+v, want %s", tc.name, got, refusal, tc.code)
			}
		case refusal != nil:
			t.Errorf("%s: refused: %+v", tc.name, refusal)
		case !reflect.DeepEqual(got, tc.steps):
			t.Errorf("%s: steps %v, want %v", tc.name, got, tc.steps)
		}
	}
}

// The refusals that need no database happen before a DSN is resolved: a resolver that
// fails the test proves it, with the exit codes and error codes the verb prints.
func TestDowngradeVerbRefusesBeforeAnyDatabase(t *testing.T) {
	resolve := func(_ secrets.LookupEnv, _ io.Writer) (secrets.Value, string, bool) {
		t.Fatal("the DSN was resolved")
		return secrets.Value{}, "", false
	}
	var run func(context.Context, cli.Env) int
	for _, child := range Command(resolve).Children {
		if child.Name == "downgrade" {
			run = child.Run
		}
	}
	for arg, code := range map[string]string{"base": "unsupported_target", "0066": "below_baseline_floor", "0100": "below_baseline_floor", "abc": "unknown_revision", "0999": "unknown_revision", "heads": "unsupported_target"} {
		code2, stdout, stderr := runHistoryVerb(t, run, arg)
		if code2 != cli.ExitRefused || stdout != "" {
			t.Errorf("downgrade %s: exit %d stdout %q", arg, code2, stdout)
		}
		var body struct {
			Error struct{ Code, Detail string }
		}
		if err := json.Unmarshal([]byte(lastLine(stderr)), &body); err != nil || body.Error.Code != code {
			t.Errorf("downgrade %s: stderr %q, want code %s", arg, stderr, code)
		}
	}
	for _, args := range [][]string{{}, {"a", "b"}, {"--nope", "0139"}} {
		if code, _, _ := runHistoryVerb(t, run, args...); code != cli.ExitUsage {
			t.Errorf("downgrade %v: exit %d, want 2", args, code)
		}
	}
}

// Every combination of facts has exactly one label, and the labels are the claims the
// read-back supports: no label says more than the facts do.
func TestOutcomeLabelOverEveryExitPath(t *testing.T) {
	boom := errors.New("boom")
	for name, tc := range map[string]struct {
		facts runFacts
		want  string
	}{
		"read-back failed, no error":        {runFacts{ReadFailed: true}, OutcomeUnknown},
		"read-back failed beats a refusal":  {runFacts{ReadFailed: true, Refused: true, Err: boom}, OutcomeUnknown},
		"read-back failed after an error":   {runFacts{ReadFailed: true, Err: boom, Changed: true}, OutcomeUnknown},
		"refused":                           {runFacts{Refused: true, Err: boom}, OutcomeRefused},
		"succeeded, nothing to do":          {runFacts{}, OutcomeNoop},
		"succeeded, state moved":            {runFacts{Changed: true}, OutcomeCommitted},
		"failed, state unchanged":           {runFacts{Err: boom}, OutcomeRolledBack},
		"failed, state moved (chain steps)": {runFacts{Err: boom, Changed: true}, OutcomePartial},
	} {
		if got := outcomeOf(tc.facts); got != tc.want {
			t.Errorf("%s: %q, want %q", name, got, tc.want)
		}
	}
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{BelowHeadError{}, true}, {AheadOfBuildError{}, true}, {ForeignDatabaseError{}, true}, {SchemaMismatchError{}, true},
		{DowngradeRefusal{Code: "x"}, true}, {fmt.Errorf("wrapped: %w", BelowHeadError{}), true}, {boom, false}, {nil, false},
	} {
		if got := isRefusal(tc.err); got != tc.want {
			t.Errorf("isRefusal(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
	// the verb's pre-database refusal logs its outcome line without a database.
	_, _, stderr := runHistoryVerb(t, downgradeChildForTest(t), "base")
	if !strings.Contains(stderr, `"msg":"migrate outcome","direction":"down","from":null,"requested":"base","observed":null,"outcome":"refused"`) {
		t.Errorf("the pre-database refusal logged %q", stderr)
	}
}

func downgradeChildForTest(t *testing.T) func(context.Context, cli.Env) int {
	t.Helper()
	for _, child := range Command(nil).Children {
		if child.Name == "downgrade" {
			return child.Run
		}
	}
	t.Fatal("no downgrade verb")
	return nil
}

// lastLine is the error document: stderr is log lines, then the error.
func lastLine(text string) string {
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	return lines[len(lines)-1]
}

package pgmigrate_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func TestHeadsReplaceTheApplicationHeadWithTheLastChainRevision(t *testing.T) {
	baseline := pgmigrate.Baseline{Heads: []string{"0138", "0066"}}
	chain := []pgmigrate.ChainFile{{Revision: "0139"}, {Revision: "0140"}}
	got := pgmigrate.Heads(baseline, chain)
	if strings.Join(got, ",") != "0066,0140" {
		t.Fatalf("heads = %v, want the cutover head and the last chain revision", got)
	}
	if got := pgmigrate.Heads(baseline, nil); strings.Join(got, ",") != "0066,0138" {
		t.Fatalf("heads without a chain = %v", got)
	}
	// The label belongs to its script: a chain revision has none.
	var out bytes.Buffer
	if err := pgmigrate.WriteHeads(&out, baseline, chain); err != nil {
		t.Fatal(err)
	}
	if want := "0066 (river_cutover) (head)\n0140 (head)\n"; out.String() != want {
		t.Fatalf("heads printed %q, want %q", out.String(), want)
	}
}

func TestCurrentMarksAHeadAndOnlyAHead(t *testing.T) {
	baseline := pgmigrate.Baseline{Heads: []string{"0138", "0066"}}
	chain := []pgmigrate.ChainFile{{Revision: "0139"}}
	var out bytes.Buffer
	if err := pgmigrate.WriteCurrent(&out, []string{"0066", "0138", "0139"}, baseline, chain); err != nil {
		t.Fatal(err)
	}
	// 0138 is the application head the chain replaced: no longer a head.
	if want := "0066 (head)\n0138\n0139 (head)\n"; out.String() != want {
		t.Fatalf("current printed %q, want %q", out.String(), want)
	}
}

func TestRevisionVerbsRefuseVerboseAndArguments(t *testing.T) {
	resolve := pgmigrate.ResolveDSN(func(secrets.LookupEnv, io.Writer) (secrets.Value, string, bool) {
		t.Fatal("a refused invocation must not resolve the DSN")
		return secrets.Value{}, "", false
	})
	for _, verb := range []string{"current", "heads"} {
		var run func(context.Context, cli.Env) int
		for _, child := range pgmigrate.Command(resolve).Children {
			if child.Name == verb {
				run = child.Run
			}
		}
		for name, args := range map[string][]string{"long": {"--verbose"}, "short": {"-v"}, "positional": {"x"}} {
			var stderr bytes.Buffer
			code := run(context.Background(), cli.Env{Args: args, Lookup: func(string) (string, bool) { return "", false }, Stdout: &stderr, Stderr: &stderr})
			if code != cli.ExitUsage {
				t.Errorf("%s %s: exit %d, stderr %s", verb, name, code, stderr.String())
			}
		}
	}
}

// TestHeadsLabelTheApplicationHeadWhateverItsNumber: `alembic heads` shows
// application_schema on the application head of any number (0067 declares the
// label for the branch), not on one fixed revision.
func TestHeadsLabelTheApplicationHeadWhateverItsNumber(t *testing.T) {
	for _, application := range []string{"0138", "0139", "0140", "0200"} {
		var out bytes.Buffer
		if err := pgmigrate.WriteHeads(&out, pgmigrate.Baseline{Heads: []string{application, "0066"}}, nil); err != nil {
			t.Fatal(err)
		}
		want := "0066 (river_cutover) (head)\n" + application + " (application_schema) (head)\n"
		if out.String() != want {
			t.Errorf("heads with application head %s printed %q, want %q", application, out.String(), want)
		}
	}
}

// TestBaselineHoldsOnlyTheCutoverAndApplicationHeads pins what the label rule
// assumes: the checked-in baseline has exactly the cutover head and one
// application head. A third head fails here, and the rule must then follow the
// revision graph instead.
func TestBaselineHoldsOnlyTheCutoverAndApplicationHeads(t *testing.T) {
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	if len(baseline.Heads) != 2 || (baseline.Heads[0] != "0066" && baseline.Heads[1] != "0066") {
		t.Fatalf("baseline heads %v, want the cutover head 0066 and one application head", baseline.Heads)
	}
}

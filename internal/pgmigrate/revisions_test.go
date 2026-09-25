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
	chain := []pgmigrate.ChainFile{{Revision: "0140"}, {Revision: "0141"}}
	got := pgmigrate.Heads(baseline, chain)
	if strings.Join(got, ",") != "0066,0141" {
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
	if want := "0066 (river_cutover) (head)\n0141 (head)\n"; out.String() != want {
		t.Fatalf("heads printed %q, want %q", out.String(), want)
	}
}

func TestCurrentMarksAHeadAndOnlyAHead(t *testing.T) {
	baseline := pgmigrate.Baseline{Heads: []string{"0138", "0066"}}
	chain := []pgmigrate.ChainFile{{Revision: "0140"}}
	var out bytes.Buffer
	if err := pgmigrate.WriteCurrent(&out, []string{"0066", "0139", "0140"}, baseline, chain); err != nil {
		t.Fatal(err)
	}
	// 0138 is the application head the chain replaced: no longer a head.
	if want := "0066 (head)\n0139\n0140 (head)\n"; out.String() != want {
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

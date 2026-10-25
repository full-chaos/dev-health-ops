package devhealth_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/rivermigrate"
)

// renderedStreakIndexArgs is the args of the streak-index hook container, rendered with the flag on.
func renderedStreakIndexArgs(t *testing.T, sets ...string) (args []string, env map[string]string) {
	t.Helper()
	jobs, secrets, refusal := renderJobs(t, append([]string{"web.enabled=false", "migrations.hook.streakIndex.enabled=true"}, sets...)...)
	if refusal != "" {
		t.Fatalf("render refused: %s", refusal)
	}
	job := jobs["t-dev-health-streak-index"]
	if job == nil {
		t.Fatal("the hook did not render")
	}
	c := container(t, job, "streak-index")
	return stringsOf(c["args"]), podEnvironment(t, c, secrets)
}

// runDho runs args through the real `dho migrate` command tree, the way the image's entrypoint would.
func runDho(ctx context.Context, args []string, lookup func(string) (string, bool), stdout, stderr io.Writer) int {
	node := rivermigrate.Command()
	rest := args[1:]
	for len(rest) > 0 {
		var next *cli.Command
		for i := range node.Children {
			if node.Children[i].Name == rest[0] {
				next = &node.Children[i]
			}
		}
		if next == nil || next.Kind != cli.Group {
			break
		}
		node, rest = *next, rest[1:]
	}
	for _, child := range node.Children {
		if len(rest) > 0 && child.Name == rest[0] {
			return child.Run(ctx, cli.Env{Args: rest[1:], Lookup: lookup, Stdout: stdout, Stderr: stderr})
		}
	}
	return cli.ExitUsage
}

// The rendered hook's arguments are accepted by the verb's own parser: with no database configured it gets as far as
// the DSN lookup and fails there, never at an argument error.
func TestRenderedStreakIndexHookArgumentsPassTheVerbParser(t *testing.T) {
	args, _ := renderedStreakIndexArgs(t)
	if len(args) < 4 || args[0] != "migrate" || args[1] != "postgres" || args[2] != "build-index" {
		t.Fatalf("args = %v, want migrate postgres build-index ...", args)
	}
	var stdout, stderr bytes.Buffer
	code := runDho(context.Background(), args, func(string) (string, bool) { return "", false }, &stdout, &stderr)
	if code == cli.ExitUsage || strings.Contains(stderr.String(), "argument error") {
		t.Fatalf("the rendered args %v were refused: exit %d: %s", args, code, stderr.String())
	}
}

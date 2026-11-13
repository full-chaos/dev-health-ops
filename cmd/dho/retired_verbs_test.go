package main

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// retiredVerbGroups are command groups the tree no longer has. Each must answer
// as an unknown command, never run or print its own help.
var retiredVerbGroups = []string{"service-credentials"}

func TestRetiredVerbGroupsAreUnknownCommands(t *testing.T) {
	for _, group := range retiredVerbGroups {
		for _, verb := range []string{"", "create", "list", "rotate", "revoke"} {
			path := []string{group}
			if verb != "" {
				path = append(path, verb)
			}
			out, code := helpWith(t, commands(), path, nil, nil, false)
			want := `dho: unknown command "` + group + `"`
			if code != cli.ExitUsage || !strings.Contains(out, want) {
				t.Errorf("dho %s: exit %d, output %q; want exit %d and %s", strings.Join(path, " "), code, out, cli.ExitUsage, want)
			}
		}
	}
}

func TestRetiredVerbGroupsAreNotInTheTree(t *testing.T) {
	var walk func(level []cli.Command, prefix []string)
	walk = func(level []cli.Command, prefix []string) {
		for _, command := range level {
			path := append(append([]string{}, prefix...), command.Name)
			for _, group := range retiredVerbGroups {
				if command.Name == group {
					t.Errorf("the tree has %q", strings.Join(path, " "))
				}
			}
			walk(command.Children, path)
		}
	}
	walk(commands(), nil)
}

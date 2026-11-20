package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func groupPaths(level []cli.Command, prefix []string, out *[][]string) {
	for _, command := range level {
		path := append(append([]string{}, prefix...), command.Name)
		if command.Kind == cli.Group {
			*out = append(*out, path)
		}
		groupPaths(command.Children, path, out)
	}
}

func runGroup(args []string) (string, string, int) {
	var stdout, stderr bytes.Buffer
	code := cli.Execute(context.Background(), "dho", commands(), cli.Env{Args: args, Stdout: &stdout, Stderr: &stderr})
	return stdout.String(), stderr.String(), code
}

// A lone help word answers rc 0 on stdout; help followed by any other word is
// a usage error (rc 2, usage on stderr, nothing on stdout) for every group.
func TestEveryGroupHelpIsLoneWordOnly(t *testing.T) {
	var groups [][]string
	groupPaths(commands(), nil, &groups)
	if len(groups) == 0 {
		t.Fatal("no groups found in the dho tree")
	}
	for _, group := range groups {
		name := strings.Join(group, " ")
		for _, help := range []string{"--help", "-h"} {
			stdout, _, code := runGroup(append(append([]string{}, group...), help))
			if code != cli.ExitOK || !strings.Contains(stdout, "usage:") {
				t.Errorf("dho %s %s: rc %d stdout %q, want rc 0 and usage", name, help, code, stdout)
			}
			for _, extra := range []string{"x", "--help"} {
				stdout, stderr, code := runGroup(append(append([]string{}, group...), help, extra))
				if code != cli.ExitUsage || stdout != "" || !strings.Contains(stderr, "usage:") {
					t.Errorf("dho %s %s %s: rc %d stdout %q stderr %q, want rc 2, empty stdout, usage on stderr", name, help, extra, code, stdout, stderr)
				}
			}
		}
	}
}

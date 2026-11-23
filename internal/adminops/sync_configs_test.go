package adminops

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// `admin sync-configs artifact-excludes set|show` refuse a bad command line before they connect, and never echo the
// typed configuration id.
func TestArtifactExcludesVerbsRefuseBadArgumentsBeforeConnecting(t *testing.T) {
	noDatabase := func(string) (string, bool) { return "", false }
	const id = "0b5e0f3a-1111-4111-8111-111111111111"
	cases := []struct {
		name string
		run  func(context.Context, cli.Env) int
		args []string
	}{
		{"set needs --config-id", runArtifactExcludesSet, []string{"--prefix", "ci-x-", "--reason", "r"}},
		{"set needs a UUID", runArtifactExcludesSet, []string{"--config-id", "not-a-uuid", "--prefix", "ci-x-", "--reason", "r"}},
		{"set needs --prefix or --clear", runArtifactExcludesSet, []string{"--config-id", id, "--reason", "r"}},
		{"set refuses --prefix with --clear", runArtifactExcludesSet, []string{"--config-id", id, "--prefix", "ci-x-", "--clear", "--reason", "r"}},
		{"set needs --reason", runArtifactExcludesSet, []string{"--config-id", id, "--prefix", "ci-x-"}},
		{"set refuses a blank --reason", runArtifactExcludesSet, []string{"--config-id", id, "--clear", "--reason", " "}},
		{"set refuses a positional", runArtifactExcludesSet, []string{"--config-id", id, "--clear", "--reason", "r", "x"}},
		{"show needs --config-id", runArtifactExcludesShow, nil},
		{"show needs a UUID", runArtifactExcludesShow, []string{"--config-id", "x"}},
	}
	for _, c := range cases {
		var stdout, stderr bytes.Buffer
		code := c.run(context.Background(), cli.Env{Args: c.args, Stdout: &stdout, Stderr: &stderr, Lookup: noDatabase})
		if code != cli.ExitUsage {
			t.Errorf("%s: exit %d, want %d (stderr %q)", c.name, code, cli.ExitUsage, stderr.String())
		}
		if stdout.Len() != 0 {
			t.Errorf("%s: stdout = %q", c.name, stdout.String())
		}
		if strings.Contains(stderr.String(), id) {
			t.Errorf("%s: stderr echoes the configuration id", c.name)
		}
	}
}

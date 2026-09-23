package workersctl

import (
	"bytes"
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// TestDhoWorkersRunsTheSameVerbTree pins the fold (spec S2): every argv the
// dev-health-workerctl binary took reaches the same code through
// `dho workers <argv>`, with the same stdout, stderr and exit code. The
// lookup is empty, so each verb stops at the first check it reaches before
// a database: usage errors (exit 2) or the missing-DSN configuration error
// (exit 1). The dho side runs through cli.Execute on a tree holding only
// this vertical, exactly as cmd/dho registers it.
func TestDhoWorkersRunsTheSameVerbTree(t *testing.T) {
	cases := [][]string{
		{},
		{"--version"},
		{"status"},
		{"status", "extra"},
		{"jobs", "list"},
		{"metrics", "daily-start", "--org", "not-a-uuid"},
		{"workgraph", "trigger"},
		{"investment", "trigger"},
		{"external-recompute", "replay"},
		{"queues", "status"},
		{"contracts", "check"},
		{"routes", "apply", "--kind", "metrics.daily"},
		{"job-routes", "list"},
		{"providersync", "retire-linear-pseudo-projects"},
		{"sync-dispatch-outbox", "close-backlog"},
		{"streams", "status"},
		{"unknown-verb"},
		{"workers", "status"},
	}
	empty := func(string) (string, bool) { return "", false }
	tree := []cli.Command{Command()}
	sawUsage, sawConfig := false, false
	for _, argv := range cases {
		var directOut, directErr, dhoOut, dhoErr bytes.Buffer
		directCode := execute(context.Background(), argv, empty, &directOut, &directErr)
		dhoCode := cli.Execute(context.Background(), "dho", tree, cli.Env{
			Args: append([]string{"workers"}, argv...), Lookup: empty, Stdout: &dhoOut, Stderr: &dhoErr,
		})
		if dhoCode != directCode || dhoOut.String() != directOut.String() || dhoErr.String() != directErr.String() {
			t.Errorf("argv %q:\n direct code=%d out=%q err=%q\n dho    code=%d out=%q err=%q",
				argv, directCode, directOut.String(), directErr.String(), dhoCode, dhoOut.String(), dhoErr.String())
		}
		switch directCode {
		case cli.ExitUsage:
			sawUsage = true
		case cli.ExitFailure:
			sawConfig = true
		}
	}
	if !sawUsage || !sawConfig {
		t.Fatalf("the table reached usage=%v config=%v; it must reach both, or it compares nothing", sawUsage, sawConfig)
	}
}

// TestDhoWorkersVersionNamesTheBinary pins what `dho workers --version`
// reports: the binary is dho, so the service is "dho" (it was
// "dev-health-workerctl" while that binary existed).
func TestDhoWorkersVersionNamesTheBinary(t *testing.T) {
	var stdout bytes.Buffer
	code := cli.Execute(context.Background(), "dho", []cli.Command{Command()}, cli.Env{
		Args: []string{"workers", "--version"}, Stdout: &stdout,
	})
	if code != cli.ExitOK || !bytes.Contains(stdout.Bytes(), []byte(`"service":"dho"`)) {
		t.Fatalf("code=%d stdout=%s", code, stdout.String())
	}
}

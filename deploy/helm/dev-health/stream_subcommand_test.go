package devhealth_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// A stream group runs `dho stream-runner`. Without subcommand the dho image
// would get --profile=... as its command and exit 2 (unknown command), so the
// render refuses such a group -- for a missing and for a wrong subcommand --
// and still renders a correct one.
func TestStreamGroupsRequireTheStreamRunnerVerb(t *testing.T) {
	for name, extra := range map[string]string{
		"missing subcommand": "",
		"wrong subcommand":   "        subcommand: api\n",
	} {
		t.Run(name, func(t *testing.T) {
			output, err := renderWithStreamGroup(t, extra)
			if err == nil {
				t.Fatal("a stream group without `subcommand: stream-runner` rendered")
			}
			if !strings.Contains(output, "needs subcommand: stream-runner") {
				t.Fatalf("render failed for another reason: %s", output)
			}
		})
	}
	output, err := renderWithStreamGroup(t, "        subcommand: stream-runner\n")
	if err != nil {
		t.Fatalf("a correct stream group did not render: %v\n%s", err, output)
	}
	if !strings.Contains(output, "- \"stream-runner\"\n            - \"--profile=ingest\"") {
		t.Fatalf("the verb is not the first argument:\n%s", output)
	}
}

func renderWithStreamGroup(t *testing.T, extra string) (string, error) {
	t.Helper()
	values := filepath.Join(t.TempDir(), "values.yaml")
	body := "goWorkers:\n  groups:\n    - name: stream-ingest\n      image: ghcr.io/full-chaos/dev-health-go-dho:latest\n" +
		strings.ReplaceAll(extra, "        ", "      ") +
		"      runtimeProfile: ingest\n      replicas: 1\n      terminationGracePeriodSeconds: 60\n      autoscaling: {enabled: false}\n"
	if err := os.WriteFile(values, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command("helm", "template", "stream-subcommand-test", ".", "-f", values).CombinedOutput()
	return string(output), err
}

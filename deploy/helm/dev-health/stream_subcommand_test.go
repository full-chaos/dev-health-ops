package devhealth_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/streamrunnerservice"
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
	return renderStreamGroup(t, "ghcr.io/full-chaos/dev-health-go-dho:latest", "ingest", extra)
}

func renderStreamGroup(t *testing.T, image, profile, extra string) (string, error) {
	t.Helper()
	values := filepath.Join(t.TempDir(), "values.yaml")
	body := "goWorkers:\n  groups:\n    - name: stream-" + profile + "\n      image: " + image + "\n" +
		strings.ReplaceAll(extra, "        ", "      ") +
		"      runtimeProfile: " + profile + "\n      replicas: 1\n      terminationGracePeriodSeconds: 60\n      autoscaling: {enabled: false}\n"
	if err := os.WriteFile(values, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command("helm", "template", "stream-subcommand-test", ".", "-f", values).CombinedOutput()
	return string(output), err
}

// dho stream-runner exits 1 on a profile it does not serve, so the values
// schema refuses one at render time; every served profile still renders.
func TestStreamGroupsRefuseAProfileDhoDoesNotServe(t *testing.T) {
	output, err := renderStreamGroup(t, "ghcr.io/full-chaos/dev-health-go-dho:latest", "bogus", "        subcommand: stream-runner\n")
	if err == nil {
		t.Fatal("a stream group with runtimeProfile: bogus rendered")
	}
	if !strings.Contains(output, "runtimeProfile") {
		t.Fatalf("render failed for another reason: %s", output)
	}
	for _, profile := range streamrunnerservice.Profiles() {
		if output, err := renderStreamGroup(t, "ghcr.io/full-chaos/dev-health-go-dho:latest", profile, "        subcommand: stream-runner\n"); err != nil {
			t.Fatalf("profile %q did not render: %v\n%s", profile, err, output)
		}
	}
}

// The schema's profile list is a copy of the one dho serves; it must not drift.
func TestValuesSchemaPinsTheStreamRunnerProfiles(t *testing.T) {
	raw, err := os.ReadFile("values.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties struct {
			GoWorkers struct {
				Properties struct {
					Groups struct {
						Items struct {
							Properties struct {
								RuntimeProfile struct {
									Enum []string `json:"enum"`
								} `json:"runtimeProfile"`
							} `json:"properties"`
						} `json:"items"`
					} `json:"groups"`
				} `json:"properties"`
			} `json:"goWorkers"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	got := schema.Properties.GoWorkers.Properties.Groups.Items.Properties.RuntimeProfile.Enum
	if want := streamrunnerservice.Profiles(); !slices.Equal(got, want) {
		t.Fatalf("values.schema.json runtimeProfile enum = %v, dho stream-runner serves %v", got, want)
	}
}

// The retired stream-runner image has no dho entrypoint and exits 2 on the
// stream-runner verb, so a stream group that names it is refused.
func TestStreamGroupsRefuseTheRetiredStreamRunnerImage(t *testing.T) {
	output, err := renderStreamGroup(t, "ghcr.io/full-chaos/dev-health-go-stream-runner:latest", "external", "        subcommand: stream-runner\n")
	if err == nil {
		t.Fatal("a stream group on the retired dev-health-go-stream-runner image rendered")
	}
	if !strings.Contains(output, "retired dev-health-go-stream-runner image") {
		t.Fatalf("render failed for another reason: %s", output)
	}
}

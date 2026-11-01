package httpapi_test

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// chartWebMaxReplicas reads web.autoscaling.maxReplicas from the chart's values.
func chartWebMaxReplicas(t *testing.T) int {
	t.Helper()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "deploy", "helm", "dev-health", "values.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var values struct {
		Web struct {
			Autoscaling struct {
				MaxReplicas int `yaml:"maxReplicas"`
			} `yaml:"autoscaling"`
		} `yaml:"web"`
	}
	if err := yaml.Unmarshal(raw, &values); err != nil {
		t.Fatal(err)
	}
	if values.Web.Autoscaling.MaxReplicas < 1 {
		t.Fatalf("web.autoscaling.maxReplicas is %d: the chart has no web maximum to size the validate limit against", values.Web.Autoscaling.MaxReplicas)
	}
	return values.Web.Autoscaling.MaxReplicas
}

// A ratio, not a clock (D6418): the checks one web process makes in a window, plus one for a window edge, times the most
// processes the chart can run. The test fails if the count, the window, the web interval or the chart maximum moves.
func TestValidateLimitCoversTheWebCheckers(t *testing.T) {
	perProcess := int(httpapi.ValidateLimitWindow/httpapi.ValidateWebCheckInterval) + 1
	processes := chartWebMaxReplicas(t)
	if need := processes * perProcess; httpapi.ValidateLimitCount < need {
		t.Fatalf("validate limit %d per %s, but %d web processes check every %s: %d checks per process per window, %d needed",
			httpapi.ValidateLimitCount, httpapi.ValidateLimitWindow, processes, httpapi.ValidateWebCheckInterval, perProcess, need)
	}
}

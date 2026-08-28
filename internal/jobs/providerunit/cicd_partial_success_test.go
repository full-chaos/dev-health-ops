package providerunit

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// observeCicdPartialSuccess direct unit test (CHAOS-4394, codex review round
// 1, P2). It must count ONLY a genuine github cicd/tests partial success --
// mirroring TestAllArtifactsUnreadableIsCountedOnlyForItsOwnCategory's
// structure for the identical reason: proving the branch does not fire for
// every completion the handler ever sees.
func TestObserveCicdPartialSuccessFiresOnlyForItsOwnCase(t *testing.T) {
	t.Parallel()
	claim := providersync.Claim{}
	claim.Provider, claim.Dataset = "github", "cicd"
	claim.ID = "22222222-2222-4222-8222-222222222222"
	watermark := time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC)
	incomplete := []providersync.GitHubTestsIncomplete{
		{Component: "report_member", Cause: "artifact_unavailable", Count: 1},
	}

	cases := []struct {
		name      string
		claim     providersync.Claim
		watermark *time.Time
		payload   map[string]any
		want      bool
	}{
		{
			name: "genuine partial success", claim: claim, watermark: &watermark,
			payload: map[string]any{"repo": "acme/api", "incomplete": incomplete},
			want:    true,
		},
		{
			name: "tests dataset counts too", claim: func() providersync.Claim {
				c := claim
				c.Dataset = "tests"
				return c
			}(), watermark: &watermark,
			payload: map[string]any{"repo": "acme/api", "incomplete": incomplete},
			want:    true,
		},
		{
			name:  "nil watermark means the unit did not actually advance",
			claim: claim, watermark: nil,
			payload: map[string]any{"repo": "acme/api", "incomplete": incomplete},
			want:    false,
		},
		{
			name:  "no incomplete evidence means a clean success, not partial",
			claim: claim, watermark: &watermark,
			payload: map[string]any{"repo": "acme/api", "incomplete": []providersync.GitHubTestsIncomplete{}},
			want:    false,
		},
		{
			name:  "incomplete key absent entirely",
			claim: claim, watermark: &watermark,
			payload: map[string]any{"repo": "acme/api"},
			want:    false,
		},
		{
			name: "a different provider must not count", claim: func() providersync.Claim {
				c := claim
				c.Provider = "gitlab"
				return c
			}(), watermark: &watermark,
			payload: map[string]any{"repo": "acme/api", "incomplete": incomplete},
			want:    false,
		},
		{
			name: "a different dataset must not count", claim: func() providersync.Claim {
				c := claim
				c.Dataset = "files"
				return c
			}(), watermark: &watermark,
			payload: map[string]any{"repo": "acme/api", "incomplete": incomplete},
			want:    false,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			metrics := providerfoundation.NewMetrics()
			handler := &Handler{ProviderMetrics: metrics}
			handler.observeCicdPartialSuccess(testCase.claim, testCase.watermark, testCase.payload)
			var output bytes.Buffer
			if err := metrics.WritePrometheus(&output); err != nil {
				t.Fatal(err)
			}
			line := `dev_health_cicd_partial_success_total{reason="artifact_unavailable"} 1`
			if got := strings.Contains(output.String(), line); got != testCase.want {
				t.Fatalf("counter recorded=%v, want %v:\n%s", got, testCase.want, output.String())
			}
		})
	}

	// A deployment without the shared registry, or a payload whose
	// "incomplete" key survived a JSON round-trip as the generic []any shape
	// rather than the live typed slice, must not panic.
	(&Handler{}).observeCicdPartialSuccess(claim, &watermark, map[string]any{"incomplete": incomplete})
	metrics := providerfoundation.NewMetrics()
	(&Handler{ProviderMetrics: metrics}).observeCicdPartialSuccess(
		claim, &watermark, map[string]any{"incomplete": []any{"not-the-right-shape"}},
	)
	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "dev_health_cicd_partial_success_total{") {
		t.Fatalf("a malformed/decoded incomplete shape must fail closed, not count:\n%s", output.String())
	}
}

// The reason label must collapse a mix of distinct causes rather than
// growing an unbounded combination label, and must classify a single cause
// as itself -- exercised here through the same public entry point
// observeCicdPartialSuccess uses, not just GitHubTestsCicdPartialSuccessReason
// directly, so a future refactor of the wiring cannot silently drop the
// collapse.
func TestObserveCicdPartialSuccessCollapsesMixedCauses(t *testing.T) {
	t.Parallel()
	claim := providersync.Claim{}
	claim.Provider, claim.Dataset = "github", "cicd"
	claim.ID = "33333333-3333-4333-8333-333333333333"
	watermark := time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC)
	mixed := []providersync.GitHubTestsIncomplete{
		{Component: "report_member", Cause: "artifact_oversized", Count: 1},
		{Component: "report_member", Cause: "artifact_unavailable", Count: 1},
	}

	metrics := providerfoundation.NewMetrics()
	handler := &Handler{ProviderMetrics: metrics}
	handler.observeCicdPartialSuccess(claim, &watermark, map[string]any{"repo": "acme/api", "incomplete": mixed})

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	line := `dev_health_cicd_partial_success_total{reason="mixed"} 1`
	if !strings.Contains(output.String(), line) {
		t.Fatalf("mixed causes were not collapsed to reason=mixed:\n%s", output.String())
	}
}

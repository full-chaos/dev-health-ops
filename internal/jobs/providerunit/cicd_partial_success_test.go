package providerunit

import (
	"bytes"
	"encoding/json"
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

// RED before the codex review round 2, P1 fix: the real production caller
// never reaches observeCicdPartialSuccess with the live typed
// []providersync.GitHubTestsIncomplete slice. loadChunkedFinalResult loads
// the final chunk through PostgresRepository.LoadPreparedChunk, which
// json.Unmarshal's the JSONB sidecar into map[string]any, so "incomplete"
// arrives as []any (each element a map[string]any) every single time. A bare
// `payload["incomplete"].([]providersync.GitHubTestsIncomplete)` type
// assertion always misses that shape and returns ok=false, silently
// disabling the counter in production while every test built on the live
// typed slice (like the cases above) kept passing. This constructs the
// payload the SAME way production does -- marshal real JSON, unmarshal into
// map[string]any -- to prove the fix actually decodes it.
func TestObserveCicdPartialSuccessDecodesTheJSONRoundTrippedProductionShape(t *testing.T) {
	t.Parallel()
	claim := providersync.Claim{}
	claim.Provider, claim.Dataset = "github", "cicd"
	claim.ID = "44444444-4444-4444-8444-444444444444"
	watermark := time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC)

	live := map[string]any{
		"repo": "acme/api",
		"incomplete": []providersync.GitHubTestsIncomplete{
			{Component: "report_member", Cause: "artifact_unavailable", Count: 1},
		},
	}
	encoded, err := json.Marshal(live)
	if err != nil {
		t.Fatal(err)
	}
	// This is the SAME operation loadPreparedChunkRow performs on the stored
	// JSONB payload -- decode into a generic map, not the typed shape.
	var productionShaped map[string]any
	if err := json.Unmarshal(encoded, &productionShaped); err != nil {
		t.Fatal(err)
	}
	if _, isLiveShape := productionShaped["incomplete"].([]providersync.GitHubTestsIncomplete); isLiveShape {
		t.Fatal("premise broken: json.Unmarshal into map[string]any must not produce the live typed slice")
	}

	metrics := providerfoundation.NewMetrics()
	handler := &Handler{ProviderMetrics: metrics}
	handler.observeCicdPartialSuccess(claim, &watermark, productionShaped)

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	line := `dev_health_cicd_partial_success_total{reason="artifact_unavailable"} 1`
	if !strings.Contains(output.String(), line) {
		t.Fatalf("the JSON-round-tripped production shape was not counted:\nwant line: %s\ngot:\n%s", line, output.String())
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

// TestObserveCicdPartialSuccessRecordsPerCauseOverflow pins the CHAOS-4592
// gate round 5, P3 fix. RecordCicdPartialSuccess names only ONE dominant
// reason for the whole unit -- indistinguishable from a single skip of that
// cause -- so a unit whose bounded SkippedArtifacts sample overflowed for
// one or more causes had no metric of its own, only the durable
// SkippedArtifactCauseOverflow field and the (round 2) log line. This
// constructs a production-shaped payload (JSON round-tripped, the same
// discipline TestObserveCicdPartialSuccessDecodesTheJSONRoundTrippedProductionShape
// pins for "incomplete") carrying skipped_artifact_cause_overflow for two
// causes, and asserts both get their own counter series, sorted.
func TestObserveCicdPartialSuccessRecordsPerCauseOverflow(t *testing.T) {
	t.Parallel()
	claim := providersync.Claim{}
	claim.Provider, claim.Dataset = "github", "cicd"
	claim.ID = "55555555-5555-4555-8555-555555555555"
	watermark := time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC)

	live := map[string]any{
		"repo": "acme/api",
		"incomplete": []providersync.GitHubTestsIncomplete{
			{Component: "report_member", Cause: "malformed", Count: 9},
		},
		"skipped_artifact_cause_overflow": map[string]bool{
			"malformed": true, "unreadable_archive": false,
		},
	}
	encoded, err := json.Marshal(live)
	if err != nil {
		t.Fatal(err)
	}
	var productionShaped map[string]any
	if err := json.Unmarshal(encoded, &productionShaped); err != nil {
		t.Fatal(err)
	}

	metrics := providerfoundation.NewMetrics()
	handler := &Handler{ProviderMetrics: metrics}
	handler.observeCicdPartialSuccess(claim, &watermark, productionShaped)

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	want := `dev_health_provider_skipped_artifact_cause_overflow_total{provider="github",dataset="cicd",cause="malformed"} 1`
	if !strings.Contains(rendered, want) {
		t.Fatalf("missing %q in:\n%s", want, rendered)
	}
	// unreadable_archive was recorded as overflowed=false -- must NOT get a
	// series at all, not a zero-valued one.
	if strings.Contains(rendered, `cause="unreadable_archive"`) {
		t.Fatalf("a false (not-overflowed) cause got its own series:\n%s", rendered)
	}
}

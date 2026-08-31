package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestWritePostureTelemetryEmitsBothMetricsForEveryRole pins the exposition
// format checkExecutedGrantPosture's caller relies on: every one of the
// three runtime roles must appear under both metric names, even a role
// with zero grants and zero gaps recorded, so a scrape (or a deploy log
// grep) never has to guess whether a missing role line means "zero" or
// "never measured".
func TestWritePostureTelemetryEmitsBothMetricsForEveryRole(t *testing.T) {
	t.Parallel()

	result := postureGateResult{
		GrantsApplied:  map[string]int{"domain": 42, "queue": 1, "coordinator": 12},
		PostureMissing: map[string]int{"domain": 0, "queue": 0, "coordinator": 3},
		PostureExcess:  map[string]int{"domain": 0, "queue": 0, "coordinator": 1},
	}
	var buf bytes.Buffer
	writePostureTelemetry(&buf, result)
	output := buf.String()

	for _, want := range []string{
		"# TYPE dev_health_runtime_grants_applied_total counter",
		"# TYPE dev_health_runtime_posture_missing gauge",
		"# TYPE dev_health_runtime_posture_excess_grants gauge",
		`dev_health_runtime_grants_applied_total{role="domain"} 42`,
		`dev_health_runtime_grants_applied_total{role="queue"} 1`,
		`dev_health_runtime_grants_applied_total{role="coordinator"} 12`,
		`dev_health_runtime_posture_missing{role="domain"} 0`,
		`dev_health_runtime_posture_missing{role="queue"} 0`,
		`dev_health_runtime_posture_missing{role="coordinator"} 3`,
		`dev_health_runtime_posture_excess_grants{role="domain"} 0`,
		`dev_health_runtime_posture_excess_grants{role="queue"} 0`,
		`dev_health_runtime_posture_excess_grants{role="coordinator"} 1`,
	} {
		if !strings.Contains(output, want) {
			t.Errorf("telemetry output missing %q; got:\n%s", want, output)
		}
	}
}

// TestWritePostureTelemetryDefaultsUnrecordedRolesToZero guards against a
// caller that only populated a subset of the three roles' maps -- Go's zero
// value for a missing map key is 0, and that must render as an explicit
// "0" line rather than being silently omitted, or a scrape would read a
// bug (a role never checked) as a clean posture (a role checked and
// found complete).
func TestWritePostureTelemetryDefaultsUnrecordedRolesToZero(t *testing.T) {
	t.Parallel()

	result := postureGateResult{
		GrantsApplied:  map[string]int{},
		PostureMissing: map[string]int{},
		PostureExcess:  map[string]int{},
	}
	var buf bytes.Buffer
	writePostureTelemetry(&buf, result)
	output := buf.String()

	for _, role := range postureGateRoles {
		for _, metric := range []string{
			"dev_health_runtime_grants_applied_total",
			"dev_health_runtime_posture_missing",
			"dev_health_runtime_posture_excess_grants",
		} {
			want := metric + `{role="` + role + `"} 0`
			if !strings.Contains(output, want) {
				t.Errorf("expected zero-value line %q for unrecorded role, got:\n%s", want, output)
			}
		}
	}
}

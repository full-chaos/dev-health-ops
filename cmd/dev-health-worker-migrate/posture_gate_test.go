package main

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
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

// TestLogPostureCheckExcessOnlyDoesNotClaimConfirmed pins the CHAOS-4675
// round-1 codex finding (P3): a correct declared grant set plus a stray
// table-wide grant on a column-scoped table (missing=0, excess=1) must NOT
// log "runtime grant posture confirmed" -- the prior code did, immediately
// followed by the contradicting excess-gap line for the same role. Also
// asserts the excess line still fires and the function reports the role
// as not-OK, so go-worker-migrate's exit-code-1 behavior (unchanged by
// this fix) still has a true signal behind it.
func TestLogPostureCheckExcessOnlyDoesNotClaimConfirmed(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	excess := []postgresstore.PostureGap{
		{TableName: "integration_credentials", Excess: []string{"SELECT"}},
	}

	ok := logPostureCheck(logger, "coordinator", 3, nil, excess)
	output := buf.String()

	if ok {
		t.Error("logPostureCheck reported OK=true for an excess-only gap; want false")
	}
	if strings.Contains(output, "runtime grant posture confirmed") {
		t.Errorf("excess-only posture check logged a false \"confirmed\" line; got:\n%s", output)
	}
	if !strings.Contains(output, "runtime grant posture excess") {
		t.Errorf("excess-only posture check did not log the excess gap; got:\n%s", output)
	}
}

// TestLogPostureCheckMissingOnlyLogsGapNotConfirmed guards the companion
// case: a missing-only gap (excess empty) must still log "gap", not
// "confirmed", and must not spuriously log an "excess" line.
func TestLogPostureCheckMissingOnlyLogsGapNotConfirmed(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	missing := []postgresstore.PostureGap{
		{TableName: "work_items", ColumnName: "status", Missing: []string{"SELECT"}},
	}

	ok := logPostureCheck(logger, "domain", 5, missing, nil)
	output := buf.String()

	if ok {
		t.Error("logPostureCheck reported OK=true for a missing-only gap; want false")
	}
	if strings.Contains(output, "runtime grant posture confirmed") {
		t.Errorf("missing-only posture check logged a false \"confirmed\" line; got:\n%s", output)
	}
	if !strings.Contains(output, "runtime grant posture gap") {
		t.Errorf("missing-only posture check did not log the missing gap; got:\n%s", output)
	}
	if strings.Contains(output, "runtime grant posture excess") {
		t.Errorf("missing-only posture check spuriously logged an excess line; got:\n%s", output)
	}
}

// TestLogPostureCheckCleanLogsConfirmed guards the true-positive path: no
// missing, no excess, still logs "confirmed" and reports OK=true.
func TestLogPostureCheckCleanLogsConfirmed(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	ok := logPostureCheck(logger, "queue", 4, nil, nil)
	output := buf.String()

	if !ok {
		t.Error("logPostureCheck reported OK=false for a clean posture; want true")
	}
	if !strings.Contains(output, "runtime grant posture confirmed") {
		t.Errorf("clean posture check did not log \"confirmed\"; got:\n%s", output)
	}
}

// TestPostureFailureKindNamesTheGapKind pins the round-2 fix to main.go's
// stderr message: it must name which kind(s) of gap fired instead of
// always saying "missing privileges" (the round-1 finding's caller-side
// half -- codex noted main.go:159 said "missing privileges" for either
// gap kind).
func TestPostureFailureKindNamesTheGapKind(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		missing map[string]int
		excess  map[string]int
		want    string
	}{
		{
			name:    "missing only",
			missing: map[string]int{"domain": 1},
			excess:  map[string]int{},
			want:    "missing privileges",
		},
		{
			name:    "excess only",
			missing: map[string]int{"coordinator": 0},
			excess:  map[string]int{"coordinator": 1},
			want:    "excess privileges",
		},
		{
			name:    "both",
			missing: map[string]int{"domain": 1},
			excess:  map[string]int{"coordinator": 1},
			want:    "missing and excess privileges",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := postureGateResult{PostureMissing: tc.missing, PostureExcess: tc.excess}
			if got := postureFailureKind(result); got != tc.want {
				t.Errorf("postureFailureKind() = %q, want %q", got, tc.want)
			}
		})
	}
}

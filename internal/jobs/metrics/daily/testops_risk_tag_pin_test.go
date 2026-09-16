package daily

import (
	"math"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/finite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// TestComputeReleaseConfidenceFactorsJSONFamilyIsExactlyPinned is the
// red-first proof that factorsJSON's family argument inside
// computeReleaseConfidence is the literal string "testops_release_
// confidence": corrupting it changes no test-visible OUTPUT (the rendered
// factors_json is byte-identical either way), only which family a
// boundary trip is filed under -- only an exact-match finite.Count catches
// that.
func TestComputeReleaseConfidenceFactorsJSONFamilyIsExactlyPinned(t *testing.T) {
	repoID := uuid.New()
	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	nan := math.NaN()

	pipe := &testops.PipelineMetric{RepoID: repoID, OrgID: "org", SuccessRate: nan}
	test := &testops.TestMetric{RepoID: repoID, OrgID: "org"}

	const family = "testops_release_confidence"
	const field = "pipeline_success_rate"
	before := finite.Count(family, field, finite.ReasonNaN)

	row := computeReleaseConfidence(repoID, day, pipe, test, nil, day)
	if row == nil {
		t.Fatal("computeReleaseConfidence returned nil")
	}

	after := finite.Count(family, field, finite.ReasonNaN)
	if after != before+1 {
		t.Errorf("finite.Count(%s,%s,nan) = %d, want %d -- computeReleaseConfidence's factorsJSON family string must be exactly %q", family, field, after, before+1, family)
	}
}

// TestComputeQualityDragFactorsJSONFamilyIsExactlyPinned is
// TestComputeReleaseConfidenceFactorsJSONFamilyIsExactlyPinned's sibling
// for computeQualityDrag's own factorsJSON family string.
func TestComputeQualityDragFactorsJSONFamilyIsExactlyPinned(t *testing.T) {
	repoID := uuid.New()
	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	nan := math.NaN()

	pipe := &testops.PipelineMetric{RepoID: repoID, OrgID: "org", MedianDurationSeconds: &nan}
	test := &testops.TestMetric{RepoID: repoID, OrgID: "org"}

	const family = "testops_quality_drag"
	const field = "median_duration_seconds"
	before := finite.Count(family, field, finite.ReasonNaN)

	row := computeQualityDrag(repoID, day, pipe, test, day)
	if row == nil {
		t.Fatal("computeQualityDrag returned nil")
	}

	after := finite.Count(family, field, finite.ReasonNaN)
	if after != before+1 {
		t.Errorf("finite.Count(%s,%s,nan) = %d, want %d -- computeQualityDrag's factorsJSON family string must be exactly %q", family, field, after, before+1, family)
	}
}

// TestComputeQualityDragNumericFieldTagsAreExactlyPinned is the red-first
// proof that each of nullablePyRound's field-tag arguments in
// computeQualityDrag -- "drag_hours", "failure_rework_hours",
// "flake_investigation_hours", "queue_wait_hours",
// "retry_overhead_hours" -- is the literal string it looks like. Every one
// of these fields already goes nil correctly on a non-finite input
// (TestComputeQualityDragNonFiniteDurationNullsNumericFieldsNotRawNaN),
// but nulling the field does not depend on the tag string at all --
// NullIfNonFinite nils it the same way whatever the tag says. Only an
// exact-match finite.Count per tag catches a corrupted string.
func TestComputeQualityDragNumericFieldTagsAreExactlyPinned(t *testing.T) {
	repoID := uuid.New()
	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	nan := math.NaN()

	pipe := &testops.PipelineMetric{
		RepoID: repoID, OrgID: "org",
		MedianDurationSeconds: &nan,
		AvgQueueSeconds:       &nan,
		FailureCount:          1,
		PipelinesCount:        1,
	}
	test := &testops.TestMetric{RepoID: repoID, OrgID: "org", FlakeRate: nan, TotalCases: 1}

	const family = "testops_quality_drag"
	tags := []string{
		"drag_hours",
		"failure_rework_hours",
		"flake_investigation_hours",
		"queue_wait_hours",
		"retry_overhead_hours",
	}
	before := make(map[string]uint64, len(tags))
	for _, tag := range tags {
		before[tag] = finite.Count(family, tag, finite.ReasonNaN)
	}

	row := computeQualityDrag(repoID, day, pipe, test, day)
	if row == nil {
		t.Fatal("computeQualityDrag returned nil")
	}
	if row.DragHours != nil || row.FailureReworkHours != nil || row.FlakeInvestigationHours != nil ||
		row.QueueWaitHours != nil || row.RetryOverheadHours != nil {
		t.Fatalf("expected every quality-drag numeric field nil for an all-NaN-driving input row, got %+v", row)
	}

	for _, tag := range tags {
		after := finite.Count(family, tag, finite.ReasonNaN)
		if after != before[tag]+1 {
			t.Errorf("finite.Count(%s,%s,nan) = %d, want %d -- this field's tag string must be pinned exactly", family, tag, after, before[tag]+1)
		}
	}
}

// TestComputeQualityDragNonFiniteFlakeRateNullsOnlyFlakeInvestigationHours
// is the red-first proof for the closest-to-live gap in this file: an
// implementation that bypasses nullablePyRound for FlakeInvestigationHours
// (assigning the raw computed value directly instead) survives both this
// file's numeric non-finite coverage test (which never drives FlakeRate
// itself non-finite) and the reflected-field JSON sweeps (which only
// assert FactorsJSON stays valid -- FactorsJSON's own "flake_rate" field
// goes through the SAME finite boundary independently of
// FlakeInvestigationHours, so it stays null-safe even when the row field
// bypasses its own guard). FlakeRate is the only input to
// flakeInvestigationHours besides TotalCases (an int, always finite), so
// this isolates the one field.
func TestComputeQualityDragNonFiniteFlakeRateNullsOnlyFlakeInvestigationHours(t *testing.T) {
	repoID := uuid.New()
	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	nan := math.NaN()

	pipe := &testops.PipelineMetric{RepoID: repoID, OrgID: "org"}
	test := &testops.TestMetric{RepoID: repoID, OrgID: "org", FlakeRate: nan, TotalCases: 100}

	row := computeQualityDrag(repoID, day, pipe, test, day)
	if row == nil {
		t.Fatal("computeQualityDrag returned nil")
	}
	if row.FlakeInvestigationHours != nil {
		t.Errorf("FlakeInvestigationHours = %v, want nil (flake_rate was NaN) -- a bypass of the finite guard for this one field must not reach the wire", *row.FlakeInvestigationHours)
	}
	if row.QueueWaitHours == nil {
		t.Error("QueueWaitHours = nil, want a finite value -- its own inputs (pipelinesCount, avgQueue) were never non-finite here")
	}
	if row.FailureReworkHours == nil {
		t.Error("FailureReworkHours = nil, want a finite value -- its own inputs (failureCount, medianDur) were never non-finite here")
	}
	if row.RetryOverheadHours == nil {
		t.Error("RetryOverheadHours = nil, want a finite value -- its own inputs (rerunRate, pipelinesCount, medianDur) were never non-finite here")
	}
	if row.DragHours != nil {
		t.Errorf("DragHours = %v, want nil -- DragHours sums FlakeInvestigationHours, so a NaN flake rate must null the total too", *row.DragHours)
	}
}

// TestComputePipelineStabilityMedianRecoveryTimeTagIsExactlyPinned pins
// nullablePyRound's field tag for computePipelineStability's own boundary
// -- a distinct literal string from the testops_quality_drag family's
// tags above -- via an exact-match finite.Count, not just the nil check
// TestComputePipelineStabilityNonFiniteMedianDurationNullsRecoveryTime
// already runs.
func TestComputePipelineStabilityMedianRecoveryTimeTagIsExactlyPinned(t *testing.T) {
	repoID := uuid.New()
	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	nan := math.NaN()

	entries := []testops.PipelineMetric{
		{
			RepoID:                repoID,
			OrgID:                 "org",
			MedianDurationSeconds: &nan,
			FailureCount:          1,
			SuccessRate:           0.5,
		},
	}

	const family = "testops_pipeline_stability"
	const field = "median_recovery_time_seconds"
	before := finite.Count(family, field, finite.ReasonNaN)

	row := computePipelineStability(repoID, day, entries, day)
	if row == nil {
		t.Fatal("computePipelineStability returned nil")
	}
	if row.MedianRecoveryTimeSeconds != nil {
		t.Errorf("MedianRecoveryTimeSeconds = %v, want nil", *row.MedianRecoveryTimeSeconds)
	}

	after := finite.Count(family, field, finite.ReasonNaN)
	if after != before+1 {
		t.Errorf("finite.Count(%s,%s,nan) = %d, want %d -- the median-recovery-time tag string must be pinned exactly", family, field, after, before+1)
	}
}

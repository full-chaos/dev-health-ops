package synccoverage

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

func backfillPairFixture(sourceCount, datasetCount int) (syncConfig, effectiveScope, []string) {
	integrationID := uuid.MustParse("00000000-0000-4000-8000-0000000000aa")
	config := syncConfig{
		ID: uuid.MustParse("00000000-0000-4000-8000-0000000000bb"), OrgID: "org-acme",
		Provider: "github", Active: true, IntegrationID: &integrationID,
	}
	scope := effectiveScope{IntegrationID: &integrationID}
	sourceIDs := make([]string, 0, sourceCount)
	for index := range sourceCount {
		id := uuid.MustParse(fmt.Sprintf("00000000-0000-4000-8000-%012d", index+1))
		scope.Sources = append(scope.Sources, source{ID: id, Name: fmt.Sprintf("acme/repo-%02d", index)})
		sourceIDs = append(sourceIDs, id.String())
	}
	for index := range datasetCount {
		scope.DatasetKeys = append(scope.DatasetKeys, fmt.Sprintf("ds-%02d", index))
	}
	return config, scope, sourceIDs
}

func encodedPayload(t *testing.T, input payloadInput) ([]byte, error) {
	t.Helper()
	payload, err := buildPayload(input)
	if err != nil {
		return nil, err
	}
	encoded, err := marshalPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	return encoded, nil
}

// Repeated backfills over one range expand to more raw (pair, range) entries than maxBackfillPairs, yet they merge
// into one requested range per pair. The bound must count what the projection carries, so the build succeeds and
// equals the build from the single merged backfill.
func TestBackfillPairBoundCountsMergedRangesNotRawExpansion(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.November, 20, 3, 0, 0, 0, time.UTC)
	since := time.Date(2025, time.November, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC).Add(-time.Microsecond)

	t.Run("unresolved backfills over every pair", func(t *testing.T) {
		t.Parallel()
		config, scope, sourceIDs := backfillPairFixture(20, 15)
		jobs := maxBackfillPairs/(len(sourceIDs)*len(scope.DatasetKeys)) + 1
		repeated := make([]coverageInterval, 0, jobs)
		runIDs := make([]string, 0, jobs)
		for index := range jobs {
			runID := fmt.Sprintf("run-%05d", index)
			runIDs = append(runIDs, runID)
			repeated = append(repeated, coverageInterval{Since: since, Before: before, SourceIDs: sourceIDs, RunIDs: []string{runID}})
		}
		if raw := jobs * len(sourceIDs) * len(scope.DatasetKeys); raw <= maxBackfillPairs {
			t.Fatalf("fixture expands to %d raw pairs, want more than %d", raw, maxBackfillPairs)
		}
		merged := []coverageInterval{{Since: since, Before: before, SourceIDs: sourceIDs, RunIDs: runIDs}}
		assertSamePayload(t, config, scope, repeated, merged, now)
	})

	t.Run("resolved backfills of one pair", func(t *testing.T) {
		t.Parallel()
		config, scope, sourceIDs := backfillPairFixture(1, 1)
		repeated := make([]coverageInterval, 0, maxBackfillPairs+1)
		runIDs := make([]string, 0, 97)
		for index := range maxBackfillPairs + 1 {
			runID := fmt.Sprintf("run-%02d", index%97)
			if index < 97 {
				runIDs = append(runIDs, runID)
			}
			repeated = append(repeated, coverageInterval{Since: since, Before: before, SourceIDs: sourceIDs,
				DatasetKeys: scope.DatasetKeys, RunIDs: []string{runID}})
		}
		merged := []coverageInterval{{Since: since, Before: before, SourceIDs: sourceIDs,
			DatasetKeys: scope.DatasetKeys, RunIDs: runIDs}}
		assertSamePayload(t, config, scope, repeated, merged, now)
	})
}

func assertSamePayload(t *testing.T, config syncConfig, scope effectiveScope, repeated, merged []coverageInterval, now time.Time) {
	t.Helper()
	input := payloadInput{Config: config, Scope: scope, ActivePairs: map[string]struct{}{}, Now: now}
	input.Backfills = merged
	want, err := encodedPayload(t, input)
	if err != nil {
		t.Fatalf("merged backfill build: %v", err)
	}
	input.Backfills = repeated
	got, err := encodedPayload(t, input)
	if err != nil {
		t.Fatalf("repeated backfill build: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("repeated backfills changed the payload:\n got %.400s\nwant %.400s", got, want)
	}
}

// Disjoint backfill ranges that stay above maxBackfillPairs after merging still fail loudly: the bound is kept, not
// dropped.
func TestBackfillPairBoundStillRefusesDisjointRangesAboveTheLimit(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.November, 20, 3, 0, 0, 0, time.UTC)
	config, scope, sourceIDs := backfillPairFixture(1, 1)
	start := now.AddDate(-3, 0, 0)
	backfills := make([]coverageInterval, 0, maxBackfillPairs+1)
	for index := range maxBackfillPairs + 1 {
		since := start.Add(time.Duration(index) * time.Hour)
		backfills = append(backfills, coverageInterval{Since: since, Before: since.Add(30 * time.Minute),
			SourceIDs: sourceIDs, DatasetKeys: scope.DatasetKeys, RunIDs: []string{fmt.Sprintf("run-%05d", index)}})
	}
	_, err := buildPayload(payloadInput{Config: config, Scope: scope, Backfills: backfills,
		ActivePairs: map[string]struct{}{}, Now: now})
	var limit *LimitError
	if !errors.As(err, &limit) || limit.Bound != "backfill_pair" || limit.Max != maxBackfillPairs || limit.Count <= maxBackfillPairs {
		t.Fatalf("err = %v, want a backfill_pair LimitError above %d", err, maxBackfillPairs)
	}
}

package synccoverage

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

const backfillDifferentialSeedEnv = "SYNC_COVERAGE_BACKFILL_DIFF_SEED"

// TestBackfillGroupingDifferential pins "the payload is unchanged" against referenceBuildPayload (buildPayload before
// CHAOS-14233) on seeded random inputs under the old bound: each must build byte-equal.
func TestBackfillGroupingDifferential(t *testing.T) {
	t.Parallel()
	seed := backfillDifferentialSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	counts := runBackfillDifferential(t, seed, 2000, func(int) (payloadInput, bool) { return randomBackfillInput(rng), false })
	if counts.underLimit < 2000 {
		t.Fatalf("seed %d: coverage too thin: %d inputs under the old bound, want >= 2000", seed, counts.underLimit)
	}
}

// TestBackfillGroupingDifferentialAboveTheOldBound covers the inputs the old code refused: the new code must build the
// ones that merge under the bound, equal to the old code with the bound lifted, and still refuse the ones that do not.
func TestBackfillGroupingDifferentialAboveTheOldBound(t *testing.T) {
	t.Parallel()
	seed := backfillDifferentialSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x51ed270b27f4a7c1))
	now := time.Date(2026, time.November, 20, 3, 0, 0, 0, time.UTC)
	base := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	counts := runBackfillDifferential(t, seed, 16, func(index int) (payloadInput, bool) {
		switch {
		case index < 8:
			return chainedUnresolvedInput(rng, now, base), true
		case index < 10:
			return chainedResolvedInput(rng, now, base), true
		case index < 12:
			return crossTargetInput(rng, now, base, false), true
		case index < 13:
			return crossTargetInput(rng, now, base, true), true
		case index < 14:
			return distinctTargetsInput(rng, now, base), true
		default:
			return disjointResolvedInput(rng, now, base), false
		}
	})
	if counts.onlyNew < 14 || counts.bothRefuse < 2 {
		t.Fatalf("seed %d: only-new %d (want 14), both-refuse %d (want 2)", seed, counts.onlyNew, counts.bothRefuse)
	}
}

func backfillDifferentialSeed(t *testing.T) uint64 {
	t.Helper()
	value := os.Getenv(backfillDifferentialSeedEnv)
	if value == "" {
		return 14233
	}
	seed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		t.Fatalf("%s=%q: %v", backfillDifferentialSeedEnv, value, err)
	}
	return seed
}

type backfillDifferentialCounts struct{ underLimit, onlyNew, bothRefuse int }

func runBackfillDifferential(t *testing.T, seed uint64, inputs int, next func(int) (payloadInput, bool)) backfillDifferentialCounts {
	t.Helper()
	var counts backfillDifferentialCounts
	for index := range inputs {
		input, mustBuild := next(index)
		fail := func(format string, args ...any) {
			t.Helper()
			t.Fatalf("seed %d input %d: %s\n%s", seed, index, fmt.Sprintf(format, args...), describeBackfillInput(input))
		}
		old, oldErr := referenceBuildPayload(input, maxBackfillPairs)
		got, newErr := buildPayload(input)
		switch {
		case oldErr == nil:
			counts.underLimit++
			if newErr != nil {
				fail("old code builds, new code fails: %v", newErr)
			}
			assertPayloadBytesEqual(fail, got, old)
		case newErr == nil:
			counts.onlyNew++
			lifted, liftedErr := referenceBuildPayload(input, math.MaxInt)
			if liftedErr != nil {
				fail("only the new code builds, old code with the bound lifted fails: %v", liftedErr)
			}
			assertPayloadBytesEqual(fail, got, lifted)
		default:
			counts.bothRefuse++
			var oldLimit, newLimit *LimitError
			if !errors.As(oldErr, &oldLimit) || !errors.As(newErr, &newLimit) ||
				oldLimit.Bound != "backfill_pair" || newLimit.Bound != "backfill_pair" {
				fail("both refuse, want backfill_pair limits: old %v, new %v", oldErr, newErr)
			}
		}
		if mustBuild && newErr != nil {
			fail("input built to merge under the bound fails: %v", newErr)
		}
	}
	t.Logf("seed %d: %d inputs, %d under the old bound, %d only the new code builds, %d both refuse",
		seed, inputs, counts.underLimit, counts.onlyNew, counts.bothRefuse)
	return counts
}

func assertPayloadBytesEqual(fail func(string, ...any), got, want projectionPayload) {
	gotBytes, err := marshalPayload(got)
	if err != nil {
		fail("marshal new payload: %v", err)
	}
	wantBytes, err := marshalPayload(want)
	if err != nil {
		fail("marshal reference payload: %v", err)
	}
	if !bytes.Equal(gotBytes, wantBytes) {
		fail("payload differs:\n new %s\n ref %s", gotBytes, wantBytes)
	}
}

var differentialDatasets = []string{"commits", "prs", "cicd", "tests", "blame", "work-items"}

// randomBackfillInput draws a small scope with windows and backfills of every target shape (all sources, one pair,
// source or dataset subsets, duplicates, out-of-scope ids).
func randomBackfillInput(rng *rand.Rand) payloadInput {
	now := time.Date(2026, time.November, 20, 3, 0, 0, 0, time.UTC)
	base := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	sourceCount, datasetCount := 1+rng.IntN(5), 1+rng.IntN(4)
	config, scope, sourceIDs := differentialScope(rng, sourceCount, datasetCount)
	input := payloadInput{Config: config, Scope: scope, ActivePairs: map[string]struct{}{}, Now: now}
	outside := randomUUID(rng).String()
	pickSource := func() string {
		if rng.IntN(10) == 0 {
			return outside
		}
		return sourceIDs[rng.IntN(len(sourceIDs))]
	}
	pickDataset := func() string {
		if rng.IntN(10) == 0 {
			return "issues"
		}
		return scope.DatasetKeys[rng.IntN(len(scope.DatasetKeys))]
	}
	for range rng.IntN(7) {
		since := base.Add(time.Duration(rng.IntN(60*24)) * time.Hour)
		input.Windows = append(input.Windows, unitWindow{
			Since: since, Before: since.Add(time.Duration(1+rng.IntN(72)) * time.Hour),
			SourceID: pickSource(), DatasetKey: pickDataset(),
			Status: []string{"success", "failed", "planned"}[rng.IntN(3)], RunTime: since.Add(time.Hour),
		})
	}
	var previous *coverageInterval
	for range rng.IntN(26) {
		interval := randomRange(rng, base, previous)
		interval.RunIDs = randomRunIDs(rng)
		switch rng.IntN(6) {
		case 0, 1:
			interval.SourceIDs = append([]string(nil), sourceIDs...)
		case 2:
			interval.SourceIDs = []string{pickSource()}
			interval.DatasetKeys = []string{pickDataset()}
		case 3:
			interval.SourceIDs = []string{pickSource(), pickSource()}
		case 4:
			interval.DatasetKeys = []string{pickDataset(), pickDataset()}
		default:
			interval.SourceIDs = []string{pickSource(), pickSource(), pickSource()}
			interval.DatasetKeys = []string{pickDataset()}
		}
		input.Backfills = append(input.Backfills, interval)
		previous = &input.Backfills[len(input.Backfills)-1]
	}
	return input
}

// randomRange draws a backfill range that overlaps, repeats, abuts within the merge tolerance, abuts just outside it,
// is disjoint from, or is empty next to the previous one.
func randomRange(rng *rand.Rand, base time.Time, previous *coverageInterval) coverageInterval {
	if previous == nil || rng.IntN(5) == 0 {
		since := base.Add(time.Duration(rng.IntN(60)) * 24 * time.Hour)
		return coverageInterval{Since: since, Before: since.Add(time.Duration(1+rng.IntN(5))*24*time.Hour - time.Microsecond)}
	}
	length := time.Duration(1+rng.IntN(4)) * 24 * time.Hour
	switch rng.IntN(6) {
	case 0:
		return coverageInterval{Since: previous.Since, Before: previous.Before}
	case 1:
		return coverageInterval{Since: previous.Before.Add(intervalAdjacencyTolerance), Before: previous.Before.Add(length)}
	case 2:
		return coverageInterval{Since: previous.Before.Add(2 * intervalAdjacencyTolerance), Before: previous.Before.Add(length)}
	case 3:
		since := previous.Since.Add(previous.Before.Sub(previous.Since) / 2)
		return coverageInterval{Since: since, Before: since.Add(length)}
	case 4:
		since := previous.Before.Add(time.Duration(1+rng.IntN(5)) * 24 * time.Hour)
		return coverageInterval{Since: since, Before: since.Add(length)}
	default:
		return coverageInterval{Since: previous.Before, Before: previous.Before.Add(-time.Duration(rng.IntN(2)) * time.Hour)}
	}
}

func randomRunIDs(rng *rand.Rand) []string {
	count := rng.IntN(4)
	if count == 0 {
		return nil
	}
	runIDs := make([]string, 0, count)
	for range count {
		runIDs = append(runIDs, fmt.Sprintf("run-%d", rng.IntN(8)))
	}
	return runIDs
}

// chainedUnresolvedInput is the prod shape: unresolved backfills over every pair, day ranges that end one microsecond
// before the next day (load.go) and chain into a few merged ranges, raw expansion above maxBackfillPairs.
func chainedUnresolvedInput(rng *rand.Rand, now, base time.Time) payloadInput {
	config, scope, sourceIDs := differentialScope(rng, 20, 15)
	input := payloadInput{Config: config, Scope: scope, ActivePairs: map[string]struct{}{}, Now: now}
	days := maxBackfillPairs/(len(sourceIDs)*len(scope.DatasetKeys)) + 1 + rng.IntN(10)
	start := base.Add(time.Duration(rng.IntN(30)) * 24 * time.Hour)
	for index := range days + rng.IntN(20) {
		offset := index
		if index >= days {
			offset = rng.IntN(days)
		}
		day := start.Add(time.Duration(offset) * 24 * time.Hour)
		input.Backfills = append(input.Backfills, coverageInterval{
			Since: day, Before: day.Add(24*time.Hour - time.Microsecond),
			SourceIDs: append([]string(nil), sourceIDs...), RunIDs: []string{fmt.Sprintf("run-%d", index%13)},
		})
	}
	rng.Shuffle(len(input.Backfills), func(i, j int) {
		input.Backfills[i], input.Backfills[j] = input.Backfills[j], input.Backfills[i]
	})
	return input
}

// chainedResolvedInput repeats one pair's resolved backfills above maxBackfillPairs; they chain into one range.
func chainedResolvedInput(rng *rand.Rand, now, base time.Time) payloadInput {
	config, scope, sourceIDs := differentialScope(rng, 1, 1)
	input := payloadInput{Config: config, Scope: scope, ActivePairs: map[string]struct{}{}, Now: now}
	for index := range maxBackfillPairs + 1 + rng.IntN(50) {
		day := base.Add(time.Duration(index%40) * 24 * time.Hour)
		input.Backfills = append(input.Backfills, coverageInterval{
			Since: day, Before: day.Add(24*time.Hour - time.Microsecond), SourceIDs: sourceIDs,
			DatasetKeys: scope.DatasetKeys, RunIDs: []string{fmt.Sprintf("run-%d", index%7)},
		})
	}
	return input
}

// crossTargetInput is the loader shape where targets overlap: unresolved day ranges over every pair plus each pair's
// resolved runs on the same days, one of which bridges two ranges. Raw expansion and per-target sums exceed
// maxBackfillPairs; the per-pair merged ranges do not. With abut, each resolved run starts 1 us after its unresolved
// range ends (the merge tolerance), so only a merge across targets joins them.
func crossTargetInput(rng *rand.Rand, now, base time.Time, abut bool) payloadInput {
	config, scope, sourceIDs := differentialScope(rng, 90+rng.IntN(20), 10)
	input := payloadInput{Config: config, Scope: scope, ActivePairs: map[string]struct{}{}, Now: now}
	ranges := 12 + rng.IntN(6)
	day := func(offset int) time.Time { return base.Add(time.Duration(offset) * 24 * time.Hour) }
	for index := range ranges {
		input.Backfills = append(input.Backfills, coverageInterval{Since: day(3 * index), Before: day(3*index + 1).Add(-time.Microsecond),
			SourceIDs: append([]string(nil), sourceIDs...), RunIDs: []string{fmt.Sprintf("unresolved-%d", index%5)}})
	}
	for _, sourceID := range sourceIDs {
		for _, dataset := range scope.DatasetKeys {
			bridge := rng.IntN(ranges - 1)
			for index := range ranges {
				since, before := day(3*index), day(3*index+1).Add(-time.Microsecond)
				switch {
				case abut:
					since, before = day(3*index+1), day(3*index+2)
				case index == bridge:
					before = day(3*index + 4).Add(-time.Microsecond)
				}
				input.Backfills = append(input.Backfills, coverageInterval{Since: since, Before: before,
					SourceIDs: []string{sourceID}, DatasetKeys: []string{dataset}, RunIDs: []string{fmt.Sprintf("run-%d", index%7)}})
			}
		}
	}
	return input
}

// distinctTargetsInput gives one pair more backfills than maxBackfillPairs, each under its own target (an extra
// out-of-scope source); they all merge into one range.
func distinctTargetsInput(rng *rand.Rand, now, base time.Time) payloadInput {
	config, scope, sourceIDs := differentialScope(rng, 1, 1)
	input := payloadInput{Config: config, Scope: scope, ActivePairs: map[string]struct{}{}, Now: now}
	for index := range maxBackfillPairs + 1 + rng.IntN(20) {
		input.Backfills = append(input.Backfills, coverageInterval{Since: base, Before: base.Add(24*time.Hour - time.Microsecond),
			SourceIDs: []string{sourceIDs[0], randomUUID(rng).String()}, RunIDs: []string{fmt.Sprintf("run-%d", index%11)}})
	}
	return input
}

// disjointResolvedInput stays above maxBackfillPairs after merging, so both versions must refuse it.
func disjointResolvedInput(rng *rand.Rand, now, base time.Time) payloadInput {
	config, scope, sourceIDs := differentialScope(rng, 1, 1)
	input := payloadInput{Config: config, Scope: scope, ActivePairs: map[string]struct{}{}, Now: now}
	for index := range maxBackfillPairs + 1 {
		since := base.Add(time.Duration(index) * time.Hour)
		input.Backfills = append(input.Backfills, coverageInterval{
			Since: since, Before: since.Add(30 * time.Minute), SourceIDs: sourceIDs,
			DatasetKeys: scope.DatasetKeys, RunIDs: []string{"run-0"},
		})
	}
	return input
}

func differentialScope(rng *rand.Rand, sourceCount, datasetCount int) (syncConfig, effectiveScope, []string) {
	integrationID := randomUUID(rng)
	config := syncConfig{ID: randomUUID(rng), OrgID: "org-diff", Provider: "github", Active: true, IntegrationID: &integrationID}
	scope := effectiveScope{IntegrationID: &integrationID}
	sourceIDs := make([]string, 0, sourceCount)
	for index := range sourceCount {
		id := randomUUID(rng)
		scope.Sources = append(scope.Sources, source{ID: id, Name: fmt.Sprintf("acme/repo-%02d", index)})
		sourceIDs = append(sourceIDs, id.String())
	}
	for index := range datasetCount {
		key := fmt.Sprintf("ds-%02d", index)
		if index < len(differentialDatasets) && datasetCount <= len(differentialDatasets) {
			key = differentialDatasets[index]
		}
		scope.DatasetKeys = append(scope.DatasetKeys, key)
	}
	return config, scope, sourceIDs
}

func randomUUID(rng *rand.Rand) uuid.UUID {
	var id uuid.UUID
	for index := range id {
		id[index] = byte(rng.Uint32())
	}
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	return id
}

func describeBackfillInput(input payloadInput) string {
	var out strings.Builder
	fmt.Fprintf(&out, "sources %d datasets %v windows %d backfills %d\n", len(input.Scope.Sources), input.Scope.DatasetKeys,
		len(input.Windows), len(input.Backfills))
	for index, source := range input.Scope.Sources {
		fmt.Fprintf(&out, "  source[%d] %s\n", index, source.ID)
	}
	for index, window := range input.Windows {
		if index == 20 {
			fmt.Fprintf(&out, "  ... %d more windows\n", len(input.Windows)-index)
			break
		}
		fmt.Fprintf(&out, "  window %s..%s %s %s %s\n", window.Since.Format(time.RFC3339Nano), window.Before.Format(time.RFC3339Nano),
			window.SourceID, window.DatasetKey, window.Status)
	}
	for index, interval := range input.Backfills {
		if index == 40 {
			fmt.Fprintf(&out, "  ... %d more backfills\n", len(input.Backfills)-index)
			break
		}
		fmt.Fprintf(&out, "  backfill %s..%s sources %v datasets %v runs %v\n", interval.Since.Format(time.RFC3339Nano),
			interval.Before.Format(time.RFC3339Nano), interval.SourceIDs, interval.DatasetKeys, interval.RunIDs)
	}
	return out.String()
}

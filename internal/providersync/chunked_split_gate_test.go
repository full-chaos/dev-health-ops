package providersync

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

// THE GATE of the split (design S): main's split is the differential oracle. mainSplitCompleteRouteBatch below is main's splitCompleteRouteBatch
// copied verbatim (renamed); the MAIN arm is that split at the default 100-row cap plus the store builder under the same worst-case stamps as the
// product predicate. Assertions: never refused where the MAIN arm succeeds; chunk count never above the MAIN arm's; every row once and in order;
// the row cap; every chunk of two or more rows accepted by the store builder under the worst-case stamps. Seeded, deterministic, no containers.

func mainSplitCompleteRouteBatch(
	batch CompleteRouteBatch,
	comparison ShadowComparison,
	policy ChunkPolicy,
) ([]PreparedProviderChunk, error) {
	if policy.Validate() != nil || len(batch.Effects) == 0 {
		return nil, ErrInvalidConfiguration
	}
	ordered := append([]EffectBatch(nil), batch.Effects...)
	sortEffectBatches(ordered)
	// A prepared chunk is bounded by both the provider-page budget and the
	// sink effect-row budget. The smaller bound is intentional: a provider can
	// emit several normalized effects for one source item, so using only the
	// effect cap would allow one source page to grow without limit.
	maxRows := min(policy.MaxEffectRows, policy.MaxSourceItems)
	for maxRows >= 1 {
		count := 1
		for _, effect := range ordered {
			if rows := (len(effect.Rows) + maxRows - 1) / maxRows; rows > count {
				count = rows
			}
		}
		chunks := make([]PreparedProviderChunk, 0, count)
		valid := true
		for ordinal := 0; ordinal < count; ordinal++ {
			effects := make([]EffectBatch, 0, len(ordered))
			for _, effect := range ordered {
				// A trailing destination's rows fill the LAST sub-chunks of the emission, so they commit after every row
				// of the other destinations (effectSplitsLast).
				shift := 0
				if effectSplitsLast(effect.Destination) {
					shift = count - (len(effect.Rows)+maxRows-1)/maxRows
				}
				start := (ordinal - shift) * maxRows
				if ordinal < shift || start > len(effect.Rows) {
					start = len(effect.Rows)
				}
				end := start + maxRows
				if end > len(effect.Rows) {
					end = len(effect.Rows)
				}
				part, err := BuildEffectBatch(effect.Destination, effect.Recovery, effect.Rows[start:end])
				if err != nil {
					return nil, err
				}
				effects = append(effects, part)
			}
			payloadBytes := 0
			for _, effect := range effects {
				payloadBytes += effect.PayloadBytes
			}
			if payloadBytes > policy.MaxPreparedBytes {
				valid = false
				break
			}
			chunk := PreparedProviderChunk{
				SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion,
				Ordinal: ordinal, TotalChunks: count, Effects: effects,
				InventoryComplete: ordinal == count-1, PayloadBytes: payloadBytes,
			}
			if chunk.InventoryComplete {
				chunk.Result = cloneChunkResult(batch.Result)
				chunk.Watermark = batch.Watermark
				chunk.Evidence = batch.Evidence
				chunk.Comparison = comparison
				chunk.WorklogObservations = append([]JiraWorklogFetchObservation(nil), batch.WorklogObservations...)
			}
			encoded, encodeErr := encodedPreparedChunkPayload(chunk)
			if encodeErr != nil {
				return nil, encodeErr
			}
			chunk.PayloadBytes = len(encoded)
			if chunk.PayloadBytes > policy.MaxPreparedBytes {
				valid = false
				break
			}
			chunk.ManifestDigest = preparedChunkDigest(chunk)
			chunks = append(chunks, chunk)
		}
		if valid {
			return chunks, nil
		}
		if maxRows == 1 {
			break
		}
		maxRows /= 2
	}
	return nil, ErrChunkPolicyExceeded
}

var gateDestinations = []string{"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks", "test_suite_results",
	"test_case_results", "coverage_snapshots", testOpsRunReportCompletenessDestination}

type gateShape struct {
	name     string
	provider string
	dataset  string
	cases    int
	filled   bool
	pads     map[int]int
	randomUp int
	basePad  int
	verdict  int // padding bytes of the verdict row's reason (0 = a small row)
	policy   ChunkPolicy
}

func (shape gateShape) claim() Claim {
	provider, dataset := shape.provider, shape.dataset
	if provider == "" {
		provider, dataset = "github", "cicd"
	}
	return nativeTestClaim(provider, dataset)
}

func (shape gateShape) counts() map[string]int {
	counts := map[string]int{"test_case_results": shape.cases}
	if shape.filled {
		counts["ci_pipeline_runs"], counts["ci_job_runs"] = 1, min(shape.cases, 50)
		counts["ci_acceptance_checks"], counts["test_suite_results"] = min(shape.cases, 10), min(shape.cases, 20)
		counts["coverage_snapshots"], counts[testOpsRunReportCompletenessDestination] = min(shape.cases, 5), 1
	}
	return counts
}

func (shape gateShape) batch(t *testing.T) (CompleteRouteBatch, map[string][]json.RawMessage) {
	t.Helper()
	generator := rand.New(rand.NewSource(14411))
	counts := shape.counts()
	rows := map[string][]json.RawMessage{}
	effects := make([]EffectBatch, 0, 7)
	for _, destination := range gateDestinations {
		list := make([]json.RawMessage, 0, counts[destination])
		for index := 0; index < counts[destination]; index++ {
			pad := 0
			if destination == "test_case_results" {
				pad = 60
				if shape.basePad > 0 {
					pad = shape.basePad
				}
				if shape.randomUp > 0 {
					pad = 60 + generator.Intn(shape.randomUp)
				}
				if big, ok := shape.pads[index]; ok {
					pad = big
				}
			}
			if destination == testOpsRunReportCompletenessDestination {
				pad = shape.verdict
			}
			list = append(list, json.RawMessage(`{"org_id":"org-acme","d":"`+destination+`","i":"`+fmt.Sprintf("%08d", index)+`","pad":"`+strings.Repeat("x", pad)+`"}`))
		}
		effect, err := BuildEffectBatch(destination, EffectReadbackRequired, list)
		if err != nil {
			t.Fatal(err)
		}
		rows[destination] = effect.Rows
		effects = append(effects, effect)
	}
	return CompleteRouteBatch{Effects: effects, Result: map[string]any{"complete": true, "detail": strings.Repeat("r", 60<<10)}}, rows
}

// gateStamped makes the chunks look as the executor stamps them under the worst case: two 4 KiB cursors of characters that JSON escapes to six
// bytes, a high ordinal, no total, the final result on the last chunk.
func gateStamped(chunks []PreparedProviderChunk, batch CompleteRouteBatch) []PreparedProviderChunk {
	stamped := append([]PreparedProviderChunk(nil), chunks...)
	worst := strings.Repeat("&", maxChunkCursorBytes)
	for index := range stamped {
		stamped[index].TotalChunks, stamped[index].CursorBefore, stamped[index].CursorAfter = 0, worst, worst
		stamped[index].Ordinal = 999_000 + index
		stamped[index].InventoryComplete = index == len(stamped)-1
		stamped[index].Result = nil
		if index == len(stamped)-1 {
			stamped[index].Result = cloneChunkResult(batch.Result)
		}
		stamped[index].PayloadBytes = 0
	}
	return stamped
}

func gateStoreErr(claim Claim, chunks []PreparedProviderChunk, batch CompleteRouteBatch) (int, error) {
	stamped := gateStamped(chunks, batch)
	for index := range stamped {
		stamped[index].ManifestDigest = ""
		if _, err := buildPreparedChunkMaterial(claim, stamped[index], time.Date(2026, 12, 31, 23, 59, 59, 999_999_999, time.UTC)); err != nil {
			return index, err
		}
	}
	return -1, nil
}

// gateRealStamped stamps the chunks as the streaming executor does: the real ordinal, the cursor before on the first chunk and the cursor
// after on the last one, no total.
func gateRealStamped(chunks []PreparedProviderChunk, batch CompleteRouteBatch, stamps *splitStamps) []PreparedProviderChunk {
	stamped := append([]PreparedProviderChunk(nil), chunks...)
	for index := range stamped {
		stamped[index].TotalChunks, stamped[index].CursorBefore, stamped[index].CursorAfter = 0, "", ""
		stamped[index].Ordinal = stamps.ordinalBase + index
		stamped[index].InventoryComplete = index == len(stamped)-1
		stamped[index].Result = nil
		if index == 0 {
			stamped[index].CursorBefore = stamps.cursorBefore
		}
		if index == len(stamped)-1 {
			stamped[index].CursorAfter = stamps.cursorAfter
			stamped[index].Result = cloneChunkResult(batch.Result)
		}
		stamped[index].PayloadBytes = 0
	}
	return stamped
}

func gateStoreErrWith(claim Claim, chunks []PreparedProviderChunk, batch CompleteRouteBatch, stamps *splitStamps) (int, error) {
	if stamps == nil {
		return gateStoreErr(claim, chunks, batch)
	}
	stamped := gateRealStamped(chunks, batch, stamps)
	for index := range stamped {
		stamped[index].ManifestDigest = ""
		if _, err := buildPreparedChunkMaterial(claim, stamped[index], gitHubFilesWidestInstant); err != nil {
			return index, err
		}
	}
	return -1, nil
}

func gateLargest(chunk PreparedProviderChunk) int {
	largest := 0
	for _, effect := range chunk.Effects {
		largest = max(largest, len(effect.Rows))
	}
	return largest
}

// gateTunedPad returns the pad that puts the LAST case row's chunk (the last row of every destination, the verdict and the final result)
// `below` bytes under the 2 MiB policy limit.
func gateTunedPad(t *testing.T, shape gateShape, below int) int {
	t.Helper()
	measure := func(pad int) int {
		probe := shape
		probe.pads = map[int]int{shape.cases - 1: pad}
		batch, _ := probe.batch(t)
		effects := make([]EffectBatch, 0, 7)
		for _, effect := range batch.Effects {
			rows := effect.Rows
			if effect.Destination != testOpsRunReportCompletenessDestination && len(rows) > 0 {
				rows = rows[len(rows)-1:]
			}
			part, err := BuildEffectBatch(effect.Destination, effect.Recovery, rows)
			if err != nil {
				t.Fatal(err)
			}
			effects = append(effects, part)
		}
		encoded, err := encodedPreparedChunkPayload(PreparedProviderChunk{SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion, Effects: effects, InventoryComplete: true, Result: batch.Result})
		if err != nil {
			t.Fatal(err)
		}
		return len(encoded)
	}
	target := DefaultChunkPolicy().MaxPreparedBytes - below
	pad := target - 130_000
	for step := 0; step < 40 && abs(measure(pad)-target) > 6; step++ {
		pad += target - measure(pad)
	}
	return pad
}

// The worst-case arm is the product input of a caller that has no real stamps (the non-streaming executor, the coalescing fit check).
func TestSplitGateWorstCaseStamps(t *testing.T) {
	t.Parallel()
	runSplitGate(t, nil)
}

// The real-stamp arm is the streaming executor: the oracle is main's split built with the stamps the executor puts on it.
func TestSplitGate(t *testing.T) {
	t.Parallel()
	runSplitGate(t, &splitStamps{ordinalBase: 0, cursorBefore: `{"page":1}`, cursorAfter: `{"page":2}`})
}

func runSplitGate(t *testing.T, stamps *splitStamps) {
	started := time.Now()
	testops, byDefault := TestOpsRouteChunkPolicy(), DefaultChunkPolicy()
	var shapes []gateShape
	for _, cases := range []int{1, 2, 99, 100, 101, 499, 500, 501, 5000, 13875} {
		for _, filled := range []bool{true, false} {
			shapes = append(shapes, gateShape{name: fmt.Sprintf("n=%d filled=%t plain", cases, filled), cases: cases, filled: filled, policy: testops})
		}
	}
	shapes = append(shapes, gateShape{name: "n=2000 x ~5 KB failing rows", cases: 2000, basePad: 5000, policy: testops})
	for _, cases := range []int{250, 500} {
		for _, basePad := range []int{15_000, 18_000, 19_500} {
			shapes = append(shapes, gateShape{name: fmt.Sprintf("n=%d x ~%d B rows (100 fit, 125 do not)", cases, basePad), cases: cases, basePad: basePad, policy: testops})
		}
	}
	for _, below := range []int{52_000, 56_000, 60_000, 64_000, 70_000} {
		probe := gateShape{cases: 1, filled: true, policy: testops}
		shapes = append(shapes, gateShape{name: fmt.Sprintf("n=1 one row %d under 2 MiB (store accepts, worst stamps do not)", below), cases: 1, filled: true, policy: testops, pads: map[int]int{0: gateTunedPad(t, probe, below)}})
	}
	for _, cases := range []int{1, 2, 101, 500, 501, 5000, 13875} {
		for _, where := range []string{"head", "middle", "tail"} {
			for _, pad := range []int{2_000_000, 2_020_000, 2_070_000, 2_300_000} {
				index := map[string]int{"head": 0, "middle": cases / 2, "tail": cases - 1}[where]
				shapes = append(shapes, gateShape{name: fmt.Sprintf("n=%d big=%d@%s", cases, pad, where), cases: cases, filled: cases%2 == 1, pads: map[int]int{index: pad}, policy: testops})
			}
		}
	}
	for _, cases := range []int{500, 5000} {
		shapes = append(shapes,
			gateShape{name: fmt.Sprintf("n=%d two big adjacent", cases), cases: cases, filled: true, pads: map[int]int{cases / 2: 2_020_000, cases/2 + 1: 2_020_000}, policy: testops},
			gateShape{name: fmt.Sprintf("n=%d two big apart", cases), cases: cases, filled: true, pads: map[int]int{3: 2_050_000, cases - 3: 2_050_000}, policy: testops})
	}
	for _, cases := range []int{5000, 13875} {
		shapes = append(shapes, gateShape{name: fmt.Sprintf("n=%d mixed sizes plus 1 MB rows", cases), cases: cases, filled: true, randomUp: 3000,
			pads: map[int]int{cases / 3: 1_000_000, cases / 2: 1_000_000, cases - 2: 1_000_000}, policy: testops})
	}
	for pad := 2_012_300; pad <= 2_013_500; pad += 11 {
		shapes = append(shapes, gateShape{name: fmt.Sprintf("n=2 boundary pad=%d", pad), cases: 2, pads: map[int]int{0: pad}, policy: testops})
	}
	// The last body row within 4 KiB of 2 MiB with the verdict and the 60 KiB final result, 1 and 501 cases, the four routes.
	for _, route := range [][2]string{{"github", "cicd"}, {"github", "tests"}, {"gitlab", "cicd"}, {"gitlab", "tests"}} {
		for _, cases := range []int{1, 501} {
			for _, below := range []int{64, 1_000, 4_000} {
				probe := gateShape{provider: route[0], dataset: route[1], cases: cases, filled: true, policy: testops}
				shapes = append(shapes, gateShape{name: fmt.Sprintf("%s/%s n=%d last row %d under 2 MiB with verdict and result", route[0], route[1], cases, below),
					provider: route[0], dataset: route[1], cases: cases, filled: true, policy: testops,
					pads: map[int]int{cases - 1: gateTunedPad(t, probe, below)}})
			}
		}
	}
	checked := 0
	for _, shape := range shapes {
		claim := shape.claim()
		batch, rows := shape.batch(t)
		capRows := min(shape.policy.MaxEffectRows, shape.policy.MaxSourceItems)
		mainChunks, mainErr := mainSplitCompleteRouteBatch(batch, ShadowComparison{}, byDefault)
		mainOK := mainErr == nil
		if mainOK {
			if _, err := gateStoreErrWith(claim, mainChunks, batch, stamps); err != nil {
				mainOK = false
			}
		}
		chunks, err := splitUnderTestStamped(claim, stamps, batch, shape.policy)
		if err != nil {
			if mainOK {
				t.Errorf("%s: refused (%v) where the main arm succeeds with %d chunks", shape.name, err, len(mainChunks))
			}
			continue
		}
		checked++
		if mainOK && len(chunks) > len(mainChunks) {
			t.Errorf("%s: %d chunks, the main arm needs only %d", shape.name, len(chunks), len(mainChunks))
		}
		for _, destination := range gateDestinations {
			var got []json.RawMessage
			for _, chunk := range chunks {
				for _, effect := range chunk.Effects {
					if effect.Destination == destination {
						if len(effect.Rows) > capRows {
							t.Errorf("%s: a chunk holds %d %s rows, over the cap %d", shape.name, len(effect.Rows), destination, capRows)
						}
						got = append(got, effect.Rows...)
					}
				}
			}
			want := rows[destination]
			if len(got) != len(want) {
				t.Errorf("%s: %s has %d rows in the chunks, want %d", shape.name, destination, len(got), len(want))
				continue
			}
			for index := range want {
				if !bytes.Equal(got[index], want[index]) {
					t.Errorf("%s: %s row %d is missing, duplicated or out of order", shape.name, destination, index)
					break
				}
			}
		}
		stamped := gateStamped(chunks, batch)
		instant := time.Date(2026, 12, 31, 23, 59, 59, 999_999_999, time.UTC)
		if stamps != nil {
			stamped, instant = gateRealStamped(chunks, batch, stamps), gitHubFilesWidestInstant
		}
		for index := range stamped {
			stamped[index].ManifestDigest = ""
			if gateLargest(chunks[index]) < 2 {
				continue
			}
			if _, err := buildPreparedChunkMaterial(claim, stamped[index], instant); err != nil {
				t.Errorf("%s: the store builder refuses chunk %d of %d (%d rows) under the stamps: %v", shape.name, index, len(chunks), gateLargest(chunks[index]), err)
				break
			}
		}
	}
	t.Logf("split gate: %d shapes, %d split, %s", len(shapes), checked, time.Since(started).Round(time.Millisecond))
	_ = errors.Is
}

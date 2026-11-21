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

// THE GATE of the split rule: one property test over splitCompleteRouteBatch itself. The store builder (buildPreparedChunkMaterial) is the
// oracle for acceptance, never the split's own arithmetic. Seeded, deterministic, no containers.

var splitPropertyDestinations = []string{"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks", "test_suite_results",
	"test_case_results", "coverage_snapshots", testOpsRunReportCompletenessDestination}

type splitShape struct {
	name     string
	cases    int
	filled   bool // all 7 destinations filled (the 6 others with realistic small counts) vs test_case_results only
	pads     map[int]int
	randomUp int // a seeded mix of row sizes up to this many pad bytes (0 = none)
	basePad  int // pad bytes of an ordinary case row (0 = 60)
	policy   ChunkPolicy
}

func (shape splitShape) rowCounts() map[string]int {
	counts := map[string]int{"test_case_results": shape.cases}
	if shape.filled {
		counts["ci_pipeline_runs"], counts["ci_job_runs"] = 1, min(shape.cases, 50)
		counts["ci_acceptance_checks"], counts["test_suite_results"] = min(shape.cases, 10), min(shape.cases, 20)
		counts["coverage_snapshots"], counts[testOpsRunReportCompletenessDestination] = min(shape.cases, 5), 1
	}
	return counts
}

func (shape splitShape) batch(t *testing.T) (CompleteRouteBatch, map[string][]json.RawMessage) {
	t.Helper()
	generator := rand.New(rand.NewSource(14411))
	counts := shape.rowCounts()
	rows := map[string][]json.RawMessage{}
	effects := make([]EffectBatch, 0, 7)
	for _, destination := range splitPropertyDestinations {
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
			list = append(list, json.RawMessage(`{"org_id":"org-acme","d":"`+destination+`","i":"`+fmt.Sprintf("%08d", index)+`","pad":"`+strings.Repeat("x", pad)+`"}`))
		}
		effect, err := BuildEffectBatch(destination, EffectReadbackRequired, list)
		if err != nil {
			t.Fatal(err)
		}
		// BuildEffectBatch keeps rows in canonical (byte-sorted) order, and the zero-padded index makes that the generation order.
		rows[destination] = effect.Rows
		effects = append(effects, effect)
	}
	return CompleteRouteBatch{Effects: effects, Result: map[string]any{"complete": true, "detail": strings.Repeat("r", 60<<10)}}, rows
}

// sliceEncodedBytes is the exact encoded size of the chunk made of rows [from, to) of every destination except the verdict one.
func sliceEncodedBytes(t *testing.T, rows map[string][]json.RawMessage, from, to int) int {
	t.Helper()
	effects := make([]EffectBatch, 0, 7)
	for _, destination := range splitPropertyDestinations {
		var part []json.RawMessage
		if destination != testOpsRunReportCompletenessDestination {
			list := rows[destination]
			lo, hi := min(from, len(list)), min(to, len(list))
			part = list[lo:hi]
		}
		effect, err := BuildEffectBatch(destination, EffectReadbackRequired, part)
		if err != nil {
			t.Fatal(err)
		}
		effects = append(effects, effect)
	}
	encoded, err := encodedPreparedChunkPayload(PreparedProviderChunk{SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion, Effects: effects})
	if err != nil {
		t.Fatal(err)
	}
	return len(encoded)
}

func TestSplitPropertyGate(t *testing.T) {
	t.Parallel()
	started := time.Now()
	claim := nativeTestClaim("github", "cicd")
	stamp := time.Date(2026, 12, 31, 23, 59, 59, 999_999_999, time.UTC)
	const slack = 128
	var shapes []splitShape
	testops, byDefault := TestOpsRouteChunkPolicy(), DefaultChunkPolicy()
	counts := []int{1, 2, 99, 100, 101, 499, 500, 501, 5000, 13875}
	for _, cases := range counts {
		for _, filled := range []bool{true, false} {
			shapes = append(shapes, splitShape{name: fmt.Sprintf("n=%d filled=%t plain", cases, filled), cases: cases, filled: filled, policy: testops})
		}
	}
	for _, cases := range []int{1, 2, 101, 500, 501, 5000, 13875} {
		for _, where := range []string{"head", "middle", "tail"} {
			for _, pad := range []int{2_000_000, 2_020_000, 2_070_000, 2_300_000} {
				index := map[string]int{"head": 0, "middle": cases / 2, "tail": cases - 1}[where]
				shapes = append(shapes, splitShape{name: fmt.Sprintf("n=%d big=%d@%s", cases, pad, where), cases: cases, filled: cases%2 == 1, pads: map[int]int{index: pad}, policy: testops})
			}
		}
	}
	for _, cases := range []int{500, 5000} {
		shapes = append(shapes,
			splitShape{name: fmt.Sprintf("n=%d two big adjacent", cases), cases: cases, filled: true, pads: map[int]int{cases / 2: 2_020_000, cases/2 + 1: 2_020_000}, policy: testops},
			splitShape{name: fmt.Sprintf("n=%d two big apart", cases), cases: cases, filled: true, pads: map[int]int{3: 2_050_000, cases - 3: 2_050_000}, policy: testops})
	}
	for _, cases := range []int{5000, 13875} {
		shapes = append(shapes, splitShape{name: fmt.Sprintf("n=%d mixed sizes plus 1 MB rows", cases), cases: cases, filled: true, randomUp: 3000,
			pads: map[int]int{cases / 3: 1_000_000, cases / 2: 1_000_000, cases - 2: 1_000_000}, policy: testops})
	}
	// Boundary sweep: two cases whose joint payload crosses the reserved limit by a few bytes either way, so an estimate that is a few
	// bytes low would put a chunk over the limit.
	for pad := 2_012_300; pad <= 2_013_500; pad += 11 {
		shapes = append(shapes, splitShape{name: fmt.Sprintf("n=2 boundary pad=%d", pad), cases: 2, pads: map[int]int{0: pad}, policy: testops})
	}
	for _, cases := range []int{101, 501} {
		shapes = append(shapes, splitShape{name: fmt.Sprintf("n=%d big middle, default policy", cases), cases: cases, filled: true, pads: map[int]int{cases / 2: 2_050_000}, policy: byDefault})
	}
	checked := 0
	for _, shape := range shapes {
		batch, rows := shape.batch(t)
		capRows := min(shape.policy.MaxEffectRows, shape.policy.MaxSourceItems)
		bigRows := 0
		for _, pad := range shape.pads {
			if pad >= 500_000 {
				bigRows++
			}
		}
		if shape.randomUp > 0 {
			bigRows = 3
		}
		// assertion 5: refused only when a row ALONE (that row of every destination) is over the policy limit.
		aloneOver := false
		for index := range rows["test_case_results"] {
			if sliceEncodedBytes(t, rows, index, index+1) > shape.policy.MaxPreparedBytes {
				aloneOver = true
				break
			}
		}
		chunks, err := SplitCompleteRouteBatch(batch, shape.policy)
		if aloneOver {
			if !errors.Is(err, ErrChunkPolicyExceeded) {
				t.Errorf("%s: a row alone is over the policy limit, want ErrChunkPolicyExceeded, got %d chunks err=%v", shape.name, len(chunks), err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: refused (%v) although no row alone is over the policy limit", shape.name, err)
			continue
		}
		checked++
		// assertion 1 and 2: every row exactly once, in order; no slice over the cap.
		for _, destination := range splitPropertyDestinations {
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
				var compact bytes.Buffer
				if err := json.Compact(&compact, want[index]); err != nil || !bytes.Equal(got[index], compact.Bytes()) {
					t.Errorf("%s: %s row %d is missing, duplicated or out of order", shape.name, destination, index)
					break
				}
			}
		}
		// assertion 7: the verdict row rides the last chunk, the last chunk carries the final result.
		last := chunks[len(chunks)-1]
		for _, chunk := range chunks[:len(chunks)-1] {
			for _, effect := range chunk.Effects {
				if effect.Destination == testOpsRunReportCompletenessDestination && len(effect.Rows) > 0 {
					t.Errorf("%s: a verdict row sits before the last chunk", shape.name)
				}
			}
		}
		if last.Result == nil || !last.InventoryComplete {
			t.Errorf("%s: the last chunk does not carry the final result", shape.name)
		}
		// assertion 6: chunk count.
		if bound := (shape.cases+capRows-1)/capRows + 2*bigRows; len(chunks) > bound {
			t.Errorf("%s: %d chunks, want at most %d (ceil(rows/cap) + 2 x big rows)", shape.name, len(chunks), bound)
		}
		// assertion 3: the store builder accepts every chunk as the executor stamps it (worst-case cursors, a real high ordinal, the final result).
		stamped := append([]PreparedProviderChunk(nil), chunks...)
		worst := strings.Repeat("&", maxChunkCursorBytes)
		resetEmissionChunks(stamped, ChunkRouteEmission{CursorBefore: worst, CursorAfter: worst})
		stamped[len(stamped)-1].InventoryComplete = true
		stamped[len(stamped)-1].Result = cloneChunkResult(batch.Result)
		for index := range stamped {
			largest := 0
			for _, effect := range stamped[index].Effects {
				largest = max(largest, len(effect.Rows))
			}
			if largest <= 1 { // a one-row chunk is judged by the store as on main: real-sized cursors
				if stamped[index].CursorBefore != "" {
					stamped[index].CursorBefore = "before"
				}
				if stamped[index].CursorAfter != "" {
					stamped[index].CursorAfter = "after"
				}
			}
		}
		if err := sizeEmissionChunks(stamped); err != nil {
			t.Fatal(err)
		}
		for index := range stamped {
			stamped[index].Ordinal = 999_000 + index
			stamped[index].ManifestDigest = preparedChunkDigest(stamped[index])
			largest := 0
			for _, effect := range stamped[index].Effects {
				largest = max(largest, len(effect.Rows))
			}
			_, err := buildPreparedChunkMaterial(claim, stamped[index], stamp)
			// two or more rows: the reserve guarantees the store accepts it. One row: the store decides, as on main, and may refuse only
			// within its own ledger/digest allowance of the 2 MiB edge.
			if err != nil && (largest >= 2 || stamped[index].PayloadBytes <= shape.policy.MaxPreparedBytes-4096) {
				t.Errorf("%s: the store builder refuses chunk %d of %d (%d rows, %d bytes): %v", shape.name, index, len(stamped), largest, stamped[index].PayloadBytes, err)
				break
			}
		}
		// assertion 8: no chunk over its byte limit (two or more rows: the reserved limit, one row: the policy limit), measured by the exact encode.
		for index, chunk := range chunks {
			largest := 0
			for _, effect := range chunk.Effects {
				largest = max(largest, len(effect.Rows))
			}
			limit := splitPayloadLimit(shape.policy)
			if largest <= 1 {
				limit = shape.policy.MaxPreparedBytes
			}
			if chunk.PayloadBytes > limit {
				t.Errorf("%s: chunk %d holds %d rows and %d bytes, over its limit %d", shape.name, index, largest, chunk.PayloadBytes, limit)
				break
			}
		}
		// assertion 4 (greedy maximality): a chunk could not take one more row. In particular a 1-row chunk exists only where that row
		// cannot share a chunk with the next one under the reserved limit.
		start := 0
		for index, chunk := range chunks[:len(chunks)-1] {
			taken := 0
			for _, effect := range chunk.Effects {
				if effect.Destination == "test_case_results" {
					taken = len(effect.Rows)
				}
			}
			if taken == 0 {
				break
			}
			if taken < capRows && start+taken < len(rows["test_case_results"]) {
				extended := sliceEncodedBytes(t, rows, start, start+taken+1)
				if limit := splitPayloadLimit(shape.policy); extended <= limit-slack {
					t.Errorf("%s: chunk %d takes %d rows although %d rows fit the reserved limit (%d <= %d)", shape.name, index, taken, taken+1, extended, limit)
					break
				}
			}
			start += taken
		}
	}
	t.Logf("split property gate: %d shapes, %d split and checked, %s", len(shapes), checked, time.Since(started).Round(time.Millisecond))
}

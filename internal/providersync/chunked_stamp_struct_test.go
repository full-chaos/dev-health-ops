package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

// stepClock advances one millisecond per read, so two reads are never the same instant.
func stepClock(start time.Time) func() time.Time {
	clock := start
	return func() time.Time {
		clock = clock.Add(time.Millisecond)
		return clock
	}
}

// stampsOf is what the executor stamps for an emission: its cursors, its final metadata, the ordinal base and the instant.
func stampsOf(t *testing.T, claim Claim, emission ChunkRouteEmission, ordinalBase int, stampAt time.Time) (CompleteRouteBatch, chunkStamps) {
	t.Helper()
	batch, comparison := oracleEmissionBatch(t, claim, emission)
	return batch, chunkStamps{
		Claim: claim, CursorBefore: emission.CursorBefore, CursorAfter: emission.CursorAfter, Final: emission.Final,
		Result: batch.Result, Watermark: batch.Watermark, Evidence: batch.Evidence, Comparison: comparison,
		WorklogObservations: batch.WorklogObservations, OrdinalBase: ordinalBase, StampAt: stampAt,
	}
}

// E-struct: the store is handed exactly the chunks the split measured (DeepEqual with the split re-run on the recorded stamps), with the
// split's instant as stampAt and a later clock read as leaseNow; every stamp is on the stored chunk; the stored ledger and the checkpoint's
// NormalizedAt carry the stamp instant.
func TestChunkStampStoreGetsTheChunksTheSplitMeasured(t *testing.T) {
	t.Parallel()
	emissions := stampScript(t, []stampShape{
		{cases: 501, filled: true, basePad: 1500},
		{cases: 3, filled: true, pads: map[int]int{1: 2_000_000}},
		{cases: 120, filled: true},
		{cases: 1200, filled: true, basePad: 1500},
	}, true, 4000, 60<<10)
	start := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	harness := newStampHarness(t, "github", "cicd", emissions, start)
	harness.executor.Now = stepClock(start)
	if err := harness.run(t); err != nil {
		t.Fatal(err)
	}
	if len(harness.store.groups) != len(emissions) {
		t.Fatalf("%d groups for %d emissions", len(harness.store.groups), len(emissions))
	}
	if last := harness.store.groups[len(emissions)-1]; len(last) < 3 || last[0].CursorBefore == "" {
		t.Fatalf("the last emission is %d chunks, want a group of three or more after a cursor (the shape that pins the stamps)", len(last))
	}
	policy := harness.descriptor.ChunkPolicy
	ordinal := 0
	for index, group := range harness.store.groups {
		stampAt, leaseNow := harness.store.stampAts[index], harness.store.leaseNows[index]
		if !stampAt.Before(leaseNow) {
			t.Fatalf("emission %d: stampAt %s is not before leaseNow %s: the lease fence must read a clock after the split", index, stampAt, leaseNow)
		}
		batch, stamps := stampsOf(t, harness.claim, emissions[index], ordinal, stampAt)
		want, err := splitStampedEmission(batch, stamps, policy)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(want, group) {
			t.Fatalf("emission %d: the store got chunks other than the ones the split measured", index)
		}
		for position, chunk := range group {
			last := position == len(group)-1
			if chunk.Ordinal != ordinal+position || chunk.TotalChunks != 0 {
				t.Fatalf("emission %d chunk %d: ordinal %d total %d", index, position, chunk.Ordinal, chunk.TotalChunks)
			}
			wantBefore, wantAfter := "", ""
			if position == 0 {
				wantBefore = emissions[index].CursorBefore
			}
			if last {
				wantAfter = emissions[index].CursorAfter
			}
			if chunk.CursorBefore != wantBefore || chunk.CursorAfter != wantAfter {
				t.Fatalf("emission %d chunk %d: cursors %q,%q want %q,%q", index, position, chunk.CursorBefore, chunk.CursorAfter, wantBefore, wantAfter)
			}
			final := last && emissions[index].Final
			if chunk.InventoryComplete != final || (chunk.Result != nil) != final {
				t.Fatalf("emission %d chunk %d: inventory_complete %t result %t, want %t", index, position, chunk.InventoryComplete, chunk.Result != nil, final)
			}
			stored, err := harness.store.LoadPreparedChunk(context.Background(), harness.claim, chunk.Ordinal, leaseNow)
			if err != nil {
				t.Fatal(err)
			}
			if !stored.Ledger.CreatedAt.Equal(stampAt) {
				t.Fatalf("emission %d chunk %d: ledger created_at %s, want the split's instant %s", index, position, stored.Ledger.CreatedAt, stampAt)
			}
			payload, ledger := oracleStoredBytes(t, harness.claim, chunk, stampAt)
			if stored.PayloadBytes != payload || ledger < 1 {
				t.Fatalf("emission %d chunk %d: stored payload_bytes %d, the split measured %d", index, position, stored.PayloadBytes, payload)
			}
		}
		ordinal += len(group)
	}
	checkpoint, err := harness.store.LoadChunkCheckpoint(context.Background(), harness.claim, time.Now())
	if err != nil || !checkpoint.NormalizedAt.Equal(harness.store.stampAts[0]) {
		t.Fatalf("checkpoint normalized_at %s err=%v, want the first emission's stamp instant %s", checkpoint.NormalizedAt, err, harness.store.stampAts[0])
	}
}

// tuneStamped is the pad of row 1 of a two-row emission whose ONE stamped chunk is exactly target stored bytes under the given stamps.
func tuneStamped(t *testing.T, claim Claim, target int, final bool, stampsFor func(ChunkRouteEmission) (CompleteRouteBatch, chunkStamps)) []ChunkRouteEmission {
	t.Helper()
	build := edgeEmissions(t, final, 4000)
	pad := target - 60_000
	for range 60 {
		emissions := build(pad)
		batch, stamps := stampsFor(emissions[0])
		ordered := append([]EffectBatch(nil), batch.Effects...)
		sortEffectBatches(ordered)
		chunk, err := stampEmissionChunk(ordered, 0, 1, stamps)
		if err != nil {
			t.Fatal(err)
		}
		payload, ledger := oracleStoredBytes(t, claim, chunk, stamps.StampAt)
		gap := target - (payload + ledger)
		if gap == 0 {
			return emissions
		}
		pad += gap
	}
	t.Fatalf("no pad gives %d stored bytes", target)
	return nil
}

// Every stamp is measured: a chunk tuned to exactly the limit under the real stamps is ONE chunk; one more digit of ordinal, a wider
// instant, a longer cursor or a longer result makes the same rows halve. (A split that ignores the stamp keeps one chunk the store refuses.)
func TestChunkStampEveryStampIsMeasured(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "cicd")
	base := time.Date(2026, 10, 1, 12, 0, 0, 120_000_000, time.UTC)
	type variant struct {
		name  string
		final bool
		edit  func(*chunkStamps)
	}
	for _, v := range []variant{
		{"ordinal one more digit", false, func(stamps *chunkStamps) { stamps.OrdinalBase = 1_000_000 }},
		{"instant one more fraction digit", false, func(stamps *chunkStamps) { stamps.StampAt = base.Add(3 * time.Millisecond) }},
		{"cursor_after longer", false, func(stamps *chunkStamps) { stamps.CursorAfter += "&" }},
		{"cursor_before longer", false, func(stamps *chunkStamps) { stamps.CursorBefore += "&" }},
		{"final result longer", true, func(stamps *chunkStamps) { stamps.Result["detail"] = stamps.Result["detail"].(string) + "r" }},
	} {
		t.Run(v.name, func(t *testing.T) {
			t.Parallel()
			stampsFor := func(emission ChunkRouteEmission) (CompleteRouteBatch, chunkStamps) {
				batch, stamps := stampsOf(t, claim, emission, 999_999, base)
				if stamps.CursorBefore == "" {
					stamps.CursorBefore = strings.Repeat("&", 3000)
				}
				if v.final {
					stamps.Result = map[string]any{"complete": true, "detail": strings.Repeat("r", 60<<10)}
				}
				return batch, stamps
			}
			emissions := tuneStamped(t, claim, maxChunkPayloadBytes, v.final, stampsFor)
			batch, stamps := stampsFor(emissions[0])
			chunks, err := splitStampedEmission(batch, stamps, DefaultChunkPolicy())
			if err != nil || len(chunks) != 1 {
				t.Fatalf("at the limit: %d chunks err=%v, want one", len(chunks), err)
			}
			if _, err := buildPreparedChunkMaterial(claim, chunks[0], stamps.StampAt); err != nil {
				t.Fatalf("at the limit the store refuses: %v", err)
			}
			v.edit(&stamps)
			chunks, err = splitStampedEmission(batch, stamps, DefaultChunkPolicy())
			if err != nil || len(chunks) != 2 {
				t.Fatalf("one byte over under the changed stamp: %d chunks err=%v, want the emission halved into two", len(chunks), err)
			}
			for _, chunk := range chunks {
				if _, err := buildPreparedChunkMaterial(claim, chunk, stamps.StampAt); err != nil {
					t.Fatalf("a halved chunk the store refuses: %v", err)
				}
			}
		})
	}
}

// F1: the store's refusal is classified by cause. Both size bands are size; a refusal that halving cannot cure is returned at once.
func TestChunkStampRefusalKind(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "cicd")
	emptyLedger := len(mustJSON(t, EffectLedgerState{}))
	for _, band := range []struct {
		over   int
		reason string
	}{{1, "payload_and_ledger_size"}, {emptyLedger, "payload_and_ledger_size"}, {emptyLedger + 1, "chunk_invalid"}} {
		build := edgeEmissions(t, false, 4000)
		pad := tuneSinglePad(t, claim, stampAt, maxChunkPayloadBytes+band.over, build)
		chunk := oracleSingleChunk(t, claim, build(pad)[0])
		kind, err := preparedChunkStoreRefusal(claim, chunk, stampAt)
		if kind != chunkRefusalSize || err == nil || !strings.Contains(err.Error(), band.reason) {
			t.Fatalf("over by %d: kind=%d err=%v, want size with %q", band.over, kind, err, band.reason)
		}
		_, storeErr := buildPreparedChunkMaterial(claim, chunk, stampAt)
		if storeErr == nil || storeErr.Error() != err.Error() {
			t.Fatalf("over by %d: the size function says %v, the store %v", band.over, err, storeErr)
		}
	}
	// Not size: a claim for another dataset fails the chunk's validation whatever its size; the split returns it, it does not halve.
	emissions := stampScript(t, []stampShape{{cases: 300, filled: true}}, false, 0, 0)
	batch, stamps := stampsOf(t, claim, emissions[0], 0, stampAt)
	stamps.Claim.Dataset = ""
	if _, err := splitStampedEmission(batch, stamps, DefaultChunkPolicy()); err == nil || errors.Is(err, ErrChunkPolicyExceeded) {
		t.Fatalf("err=%v, want the store's own non-size refusal, not a halving to the end", err)
	}
	if kind, err := preparedChunkStoreRefusal(stamps.Claim, PreparedProviderChunk{RouteVersion: chunkRouteVersion, Effects: batch.Effects}, stampAt); kind != chunkRefusalOther || err == nil {
		t.Fatalf("kind=%d err=%v, want other", kind, err)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// F2: a unit continued after the split took time resumes its route with now == the checkpoint's NormalizedAt == the first emission's
// stamp instant, and stores every row once.
func TestChunkStampResumeUsesTheStampInstant(t *testing.T) {
	t.Parallel()
	emissions := stampScript(t, []stampShape{{cases: 40, filled: true}, {cases: 40, filled: true}, {cases: 40, filled: true}}, false, 0, 0)
	start := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	harness := newStampHarness(t, "github", "cicd", emissions, start)
	harness.executor.Now = stepClock(start)
	policy := DefaultChunkPolicy()
	policy.MaxChunksPerAttempt = 1
	harness.descriptor.ChunkPolicy = policy
	if err := harness.run(t); err != nil {
		t.Fatal(err)
	}
	checkRowsOnceInOrder(t, harness.sink, emissions)
	nows := *harness.handler.nows
	if len(nows) < 2 {
		t.Fatalf("%d route calls, want a resumed one", len(nows))
	}
	first := harness.store.stampAts[0]
	for index, now := range nows[1:] {
		if !now.Equal(first) {
			t.Fatalf("resumed route call %d got now %s, want the first emission's stamp instant %s", index+1, now, first)
		}
	}
}

// The policies by route: the default and the files policy keep their caps; only TestOps changes (CHAOS-14411).
func TestChunkStampPolicyCaps(t *testing.T) {
	t.Parallel()
	if got := min(DefaultChunkPolicy().MaxEffectRows, DefaultChunkPolicy().MaxSourceItems); got != 100 {
		t.Fatalf("default cap %d, want 100", got)
	}
	if got := min(GitHubFilesChunkPolicy().MaxEffectRows, GitHubFilesChunkPolicy().MaxSourceItems); got != 500 {
		t.Fatalf("files cap %d, want 500", got)
	}
	if got := min(TestOpsRouteChunkPolicy().MaxEffectRows, TestOpsRouteChunkPolicy().MaxSourceItems); got != 500 {
		t.Fatalf("TestOps cap %d, want 500", got)
	}
	for _, policy := range []struct {
		policy ChunkPolicy
		want   string
	}{
		{DefaultChunkPolicy(), "[100 50 25 12 6 3 1]"},
		{GitHubFilesChunkPolicy(), "[500 250 125 62 31 15 7 3 1]"},
		{TestOpsRouteChunkPolicy(), "[500 250 125 100 50 25 12 6 3 1]"},
	} {
		got := []int{}
		for rows := min(policy.policy.MaxEffectRows, policy.policy.MaxSourceItems); rows >= 1; rows = nextSplitCap(rows, policy.policy) {
			got = append(got, rows)
		}
		if fmt.Sprint(got) != policy.want {
			t.Fatalf("cap sequence %v, want %s", got, policy.want)
		}
	}
	for _, route := range [][2]string{{"github", "files"}} {
		descriptor, _ := Descriptor(route[0], route[1])
		if descriptor.ChunkPolicy != GitHubFilesChunkPolicy() {
			t.Fatalf("%s/%s policy %+v", route[0], route[1], descriptor.ChunkPolicy)
		}
	}
}

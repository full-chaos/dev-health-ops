package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// The TestOps routes split an emission at 500 rows per destination, not 100: the artifacts phase of a big report fills 5x fewer sub-chunks.
// The split also keeps a sub-chunk under what the store accepts: payload + ledger column + the two cursors stamped after the split.

var testOpsRoutePairs = [][2]string{{"github", "cicd"}, {"github", "tests"}, {"gitlab", "cicd"}, {"gitlab", "tests"}}

func syntheticTestCaseRows(count int, org string) []json.RawMessage {
	rows := make([]json.RawMessage, 0, count)
	for index := 0; index < count; index++ {
		rows = append(rows, json.RawMessage(`{"org_id":"`+org+`","case":`+strconv.Itoa(index)+`}`))
	}
	return rows
}

func syntheticVerdictRow() testOpsRunReportCompletenessRow {
	return testOpsRunReportCompletenessRow{
		OrgID: "org-acme", RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", RunID: "1", RunAttempt: 1, Provider: "github_actions",
		Complete: true, Reasons: []string{}, ReportsExpected: 1, ReportsAdmitted: 1,
		LastSynced: time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC), VerdictVersion: 1,
	}
}

func TestTestOpsRouteSplitsAnEmissionAtFiveHundredRows(t *testing.T) {
	t.Parallel()
	for _, route := range testOpsRoutePairs {
		descriptor, ok := Descriptor(route[0], route[1])
		if !ok {
			t.Fatalf("%s/%s has no descriptor", route[0], route[1])
		}
		cases := make([]testCaseResultRow, 2000)
		for index := range cases {
			cases[index] = testCaseResultRow{OrgID: "org-acme", RepoID: "r", RunID: "1", SuiteID: "s", CaseID: strconv.Itoa(index), CaseName: "n", Status: "passed"}
		}
		effects, err := testOpsEffects(nil, nil, nil, nil, cases, nil, syntheticVerdictRow())
		if err != nil {
			t.Fatal(err)
		}
		chunks, err := SplitCompleteRouteBatch(CompleteRouteBatch{Effects: effects}, descriptor.ChunkPolicy)
		if err != nil {
			t.Fatal(err)
		}
		if len(chunks) != 4 {
			t.Fatalf("%s/%s: 2,000 rows split into %d sub-chunks, want 4 of 500 rows", route[0], route[1], len(chunks))
		}
		for ordinal, chunk := range chunks {
			for _, effect := range chunk.Effects {
				switch effect.Destination {
				case "test_case_results":
					if len(effect.Rows) != 500 {
						t.Fatalf("%s/%s: sub-chunk %d has %d case rows, want 500", route[0], route[1], ordinal, len(effect.Rows))
					}
				case testOpsRunReportCompletenessDestination:
					if want := ordinal == len(chunks)-1; (len(effect.Rows) == 1) != want || len(effect.Rows) > 1 {
						t.Fatalf("%s/%s: sub-chunk %d holds %d verdict rows, the verdict belongs to the LAST sub-chunk only", route[0], route[1], ordinal, len(effect.Rows))
					}
				}
			}
		}
	}
}

func TestTestOpsRoutePolicyDiffersFromDefaultOnlyInTheRowBound(t *testing.T) {
	t.Parallel()
	policy, base := TestOpsRouteChunkPolicy(), DefaultChunkPolicy()
	if policy.MaxSourceItems != 500 || policy.MaxEffectRows != 500 {
		t.Fatalf("rows source=%d effect=%d, want 500 and 500 (the split bound is the smaller of the two)", policy.MaxSourceItems, policy.MaxEffectRows)
	}
	base.MaxSourceItems = policy.MaxSourceItems
	if policy != base {
		t.Fatalf("policy=%+v differs from the default beyond MaxSourceItems", policy)
	}
	if DefaultChunkPolicy().MaxSourceItems != 100 {
		t.Fatalf("the default policy row bound moved to %d", DefaultChunkPolicy().MaxSourceItems)
	}
	if files := GitHubFilesChunkPolicy(); files.MaxSourceItems != 500 || files.MaxEffectRows != 500 || files.MaxChunksPerAttempt != 64 {
		t.Fatalf("github/files policy=%+v changed", files)
	}
	if descriptor, ok := Descriptor("github", "files"); !ok || descriptor.ChunkPolicy != GitHubFilesChunkPolicy() {
		t.Fatalf("github/files descriptor policy=%+v", descriptor.ChunkPolicy)
	}
}

type rowsEmissionHandler struct{ rows int }

func (handler *rowsEmissionHandler) Collect(
	context.Context, Claim, providerfoundation.Credential, *providerfoundation.HTTPClient, time.Time,
) (CompleteRouteBatch, error) {
	return CompleteRouteBatch{}, ErrInvalidConfiguration
}

func (handler *rowsEmissionHandler) CollectChunks(
	_ context.Context, claim Claim, _ providerfoundation.Credential, _ *providerfoundation.HTTPClient,
	_ time.Time, resumeCursor string, emit func(ChunkRouteEmission) error,
) error {
	if resumeCursor == "" {
		effects := make([]EffectBatch, 0, 7)
		for _, destination := range []string{"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks",
			"test_suite_results", "test_case_results", "coverage_snapshots", testOpsRunReportCompletenessDestination} {
			var rows []json.RawMessage
			if destination == "test_case_results" {
				rows = syntheticTestCaseRows(handler.rows, claim.OrgID)
			}
			effect, err := BuildEffectBatch(destination, EffectReplaySafe, rows)
			if err != nil {
				return err
			}
			effects = append(effects, effect)
		}
		if err := emit(ChunkRouteEmission{Batch: CompleteRouteBatch{Effects: effects}, CursorAfter: `{"page":1}`}); err != nil {
			return err
		}
	}
	empty, err := testOpsEffects(nil, nil, nil, nil, nil, nil)
	if err != nil {
		return err
	}
	return emit(ChunkRouteEmission{
		Batch:        CompleteRouteBatch{Effects: empty, Result: map[string]any{"complete": true}, Watermark: claim.BeforeAt},
		CursorBefore: `{"page":1}`, CursorAfter: `{"page":1}`, Final: true,
	})
}

// An attempt at the chunk bound of 64 commits 64 sub-chunks of 500 rows: 32,000 rows, where the 100-row policy committed 6,400.
func TestTestOpsRouteAttemptCommitsFiveHundredRowSubchunks(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSessionFor(t, now, false, "github", "cicd")
	descriptor, ok := Descriptor("github", "cicd")
	if !ok || !descriptor.Chunked {
		t.Fatalf("descriptor=%+v ok=%t", descriptor, ok)
	}
	store, sink := newChunkMemoryStore(), &recoveryRowSink{}
	executor := completeRouteExecutor(now, &rowsEmissionHandler{rows: 40_000}, store, sink)
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
	_, err := executor.Execute(context.Background(), session, descriptor)
	var stopped ChunkContinuationError
	if !errors.As(err, &stopped) || stopped.Reason != ChunkStopChunkBound || stopped.Chunks != 64 {
		t.Fatalf("err=%v stopped=%+v, want the chunk bound after 64 sub-chunks", err, stopped)
	}
	sink.mu.Lock()
	written := len(sink.rows)
	sink.mu.Unlock()
	if written != 64*500 || sink.duplicates() != 0 {
		t.Fatalf("rows written=%d duplicated=%d, want %d (64 sub-chunks of 500) and 0", written, sink.duplicates(), 64*500)
	}
	if checkpoint, loadErr := store.LoadChunkCheckpoint(context.Background(), claim, now); loadErr != nil || checkpoint.PreparedChunks != 80 {
		t.Fatalf("prepared=%d err=%v, want the 40,000 rows prepared as 80 sub-chunks of 500", checkpoint.PreparedChunks, loadErr)
	}
}

func TestSplitPayloadLimitLeavesRoomForTheLedgerAndTheCursors(t *testing.T) {
	t.Parallel()
	want := maxChunkPayloadBytes - maxEffectLedgerStateBytes - 12*maxChunkCursorBytes - 1<<10
	if got := splitPayloadLimit(DefaultChunkPolicy()); got != want {
		t.Fatalf("limit for a 2 MiB policy = %d, want %d", got, want)
	}
	if got := splitPayloadLimit(ChunkPolicy{MaxPreparedBytes: 4096}); got != 4096 {
		t.Fatalf("limit for a 4 KiB policy = %d, want it unchanged", got)
	}
}

// The ledger column of one sub-chunk is bounded AS A WHOLE at write: encodeEffectLedgerState refuses a state over
// maxEffectLedgerStateBytes (effect_ledger.go), whatever the entry count. Seven entries at their maximum are far under it.
func TestEffectLedgerColumnIsBoundedAsAWholeAtWrite(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 12, 31, 23, 59, 59, 999_999_999, time.UTC)
	entry := func(index int, destination string) EffectLedgerEntry {
		return EffectLedgerEntry{
			Index: index, Destination: destination, ContentDigest: strings.Repeat("ab", 32), RowCount: maxEffectRows,
			Recovery: EffectReadbackRequired, Status: GenerationBlockCommitted, StartedAt: &at, CommittedAt: &at,
		}
	}
	state := EffectLedgerState{SchemaVersion: "v1", Generation: strings.Repeat("g", 64), Provider: "gitlab", Dataset: "cicd", CreatedAt: at, UpdatedAt: at}
	for index, destination := range []string{"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks", "test_suite_results",
		"test_case_results", "coverage_snapshots", testOpsRunReportCompletenessDestination} {
		state.Effects = append(state.Effects, entry(index, destination))
	}
	seven := encodeEffectLedgerState(state)
	if seven == nil || len(seven) > maxEffectLedgerStateBytes/8 {
		t.Fatalf("seven entries at their maximum encode to %d bytes, want a value far under the %d bound", len(seven), maxEffectLedgerStateBytes)
	}
	t.Logf("seven-entry ledger at maximum: %d bytes of %d", len(seven), maxEffectLedgerStateBytes)
	huge := state
	huge.Effects = nil
	for index := 0; index < maxEffectDestinations; index++ {
		huge.Effects = append(huge.Effects, entry(index, strings.Repeat("d", 900)+strconv.Itoa(index)))
	}
	if encodeEffectLedgerState(huge) != nil {
		t.Fatal("a ledger over the whole-column bound was encoded")
	}
}

// A payload within the ledger and cursor room below 2 MiB used to pass the split (it only tested the payload) and then be refused by the
// store (payload + ledger > 2 MiB): the split halves it instead, and the store's own builder accepts every sub-chunk, cursors included.
func TestSplitHalvesAPayloadThatTheStoreWouldRefuseWithItsLedger(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "cicd")
	build := func(pad int) EffectBatch {
		rows := make([]json.RawMessage, 100)
		for index := range rows {
			size := 20_000
			if index == 0 {
				size += pad
			}
			rows[index] = json.RawMessage(`{"org_id":"org-acme","pad":"` + strings.Repeat("x", size) + `"}`)
		}
		effect, err := BuildEffectBatch("test_case_results", EffectReadbackRequired, rows)
		if err != nil {
			t.Fatal(err)
		}
		return effect
	}
	measure := func(effect EffectBatch) int {
		chunk := PreparedProviderChunk{SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion, TotalChunks: 1, Effects: []EffectBatch{effect}, InventoryComplete: true}
		encoded, err := encodedPreparedChunkPayload(chunk)
		if err != nil {
			t.Fatal(err)
		}
		return len(encoded)
	}
	pad, target := 0, maxChunkPayloadBytes-200
	for step := 0; step < 40 && abs(measure(build(pad))-target) > 40; step++ {
		pad += target - measure(build(pad))
	}
	effect := build(pad)
	if size := measure(effect); size > maxChunkPayloadBytes || size < maxChunkPayloadBytes-1000 {
		t.Fatalf("the synthetic payload is %d bytes, want it just under 2 MiB (%d)", size, maxChunkPayloadBytes)
	}
	chunks, err := SplitCompleteRouteBatch(CompleteRouteBatch{Effects: []EffectBatch{effect}}, TestOpsRouteChunkPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) < 2 {
		t.Fatalf("a payload just under 2 MiB stayed one sub-chunk: the store refuses it with its ledger column")
	}
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	for index := range chunks {
		chunks[index].CursorBefore, chunks[index].CursorAfter = strings.Repeat("b", maxChunkCursorBytes), strings.Repeat("a", maxChunkCursorBytes)
		if err := sizeEmissionChunks(chunks[index : index+1]); err != nil {
			t.Fatal(err)
		}
		chunks[index].ManifestDigest = preparedChunkDigest(chunks[index])
		if _, err := buildPreparedChunkMaterial(claim, chunks[index], now); err != nil {
			t.Fatalf("sub-chunk %d of %d refused by the store builder: %v", index, len(chunks), err)
		}
	}
}

func abs(value int) int {
	if value < 0 {
		return -value
	}
	return value
}

// github/files pre-checks "one emission = one sub-chunk" with the SAME split (gitHubFilesPreparedChunk) and the store's builder, so the
// reserved limit is shared by construction: an emission in the reserved margin is not one sub-chunk any more and the route halves its rows.
func TestGitHubFilesPreCheckSharesTheReservedSplitLimit(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "files")
	build := func(contentBytes int) EffectBatch {
		rows := make([]gitFileRow, 100)
		stamp := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
		for index := range rows {
			content := strings.Repeat("x", contentBytes)
			rows[index] = gitFileRow{RepoID: "r", Path: "f" + strconv.Itoa(index), Contents: &content, LastSynced: stamp, OrgID: claim.OrgID}
		}
		effect, err := effectBatchFromValues("git_files", EffectReadbackRequired, rows)
		if err != nil {
			t.Fatal(err)
		}
		return effect
	}
	limit := splitPayloadLimit(GitHubFilesChunkPolicy())
	sawFit, sawSplit := false, false
	for size := 19_000; size <= 22_000; size += 50 {
		effect := build(size)
		chunk := PreparedProviderChunk{SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion, Effects: []EffectBatch{effect}}
		encoded, err := encodedPreparedChunkPayload(chunk)
		if err != nil {
			t.Fatal(err)
		}
		if gap := len(encoded) - limit; gap > -300 && gap < 300 {
			continue
		}
		fits := gitHubFilesChunkFits(claim, effect, "b", "a")
		if want := len(encoded) < limit; fits != want {
			t.Fatalf("emission of %d payload bytes (limit %d): fits=%t, want %t", len(encoded), limit, fits, want)
		}
		sawFit, sawSplit = sawFit || fits, sawSplit || !fits
	}
	if !sawFit || !sawSplit {
		t.Fatalf("the sweep did not cross the limit (fit=%t split=%t)", sawFit, sawSplit)
	}
}

// Whatever the split accepts, the store accepts: a payload at the split's limit, then the executor's own steps after the split (cursors at
// 4 KiB of characters that JSON escapes to 6 bytes each, a high ordinal, the manifest digest) and, for the final emission, a final result of
// 60 KiB, then the store's builder with the real ledger of the seven TestOps effects. Both for a non-final and for a final sub-chunk.
func TestSplitAcceptedSubchunkIsAcceptedByTheStoreAtWorstCaseCursorsAndFinalResult(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "cicd")
	policy := TestOpsRouteChunkPolicy()
	limit := splitPayloadLimit(policy)
	for _, final := range []bool{false, true} {
		var result map[string]any
		if final {
			result = map[string]any{"complete": true, "detail": strings.Repeat("r", 60<<10)}
		}
		build := func(pad int) CompleteRouteBatch {
			rows := make([]json.RawMessage, 100)
			for index := range rows {
				size := 19_000
				if index == 0 {
					size += pad
				}
				rows[index] = json.RawMessage(`{"org_id":"org-acme","pad":"` + strings.Repeat("x", size) + `"}`)
			}
			effects := make([]EffectBatch, 0, 7)
			for _, destination := range []string{"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks", "test_suite_results",
				"test_case_results", "coverage_snapshots", testOpsRunReportCompletenessDestination} {
				var destinationRows []json.RawMessage
				if destination == "test_case_results" {
					destinationRows = rows
				}
				effect, err := BuildEffectBatch(destination, EffectReadbackRequired, destinationRows)
				if err != nil {
					t.Fatal(err)
				}
				effects = append(effects, effect)
			}
			return CompleteRouteBatch{Effects: effects, Result: result}
		}
		measure := func(batch CompleteRouteBatch) int {
			chunk := PreparedProviderChunk{SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion, TotalChunks: 1,
				Effects: batch.Effects, InventoryComplete: true, Result: batch.Result}
			encoded, err := encodedPreparedChunkPayload(chunk)
			if err != nil {
				t.Fatal(err)
			}
			return len(encoded)
		}
		pad, target := 0, limit-200
		for step := 0; step < 40 && abs(measure(build(pad))-target) > 40; step++ {
			pad += target - measure(build(pad))
		}
		batch := build(pad)
		if size := measure(batch); size > limit || size < limit-1000 {
			t.Fatalf("final=%t: the synthetic sub-chunk is %d bytes, want it just under the split limit %d", final, size, limit)
		}
		chunks, err := splitCompleteRouteBatch(batch, ShadowComparison{}, policy)
		if err != nil || len(chunks) != 1 {
			t.Fatalf("final=%t: split = %d chunks, err=%v, want one sub-chunk under the limit", final, len(chunks), err)
		}
		worst := strings.Repeat("&", maxChunkCursorBytes)
		resetEmissionChunks(chunks, ChunkRouteEmission{CursorBefore: worst, CursorAfter: worst})
		if final {
			chunks[0].InventoryComplete, chunks[0].Result = true, cloneChunkResult(batch.Result)
		}
		if err := sizeEmissionChunks(chunks); err != nil {
			t.Fatal(err)
		}
		chunks[0].Ordinal = 999_999
		chunks[0].ManifestDigest = preparedChunkDigest(chunks[0])
		material, err := buildPreparedChunkMaterial(claim, chunks[0], time.Date(2026, 12, 31, 23, 59, 59, 999_999_999, time.UTC))
		if err != nil {
			t.Fatalf("final=%t: the store refuses what the split accepted: %v", final, err)
		}
		t.Logf("final=%t: payload %d + ledger %d = %d of %d", final, len(material.payloadRaw), len(material.ledgerRaw),
			len(material.payloadRaw)+len(material.ledgerRaw), maxChunkPayloadBytes)
	}
}

// A chunk that cannot shrink any further (one row per destination) is judged on the plain policy limit, so a single row in the reserve
// margin is accepted by the split and by the store, as before the reserve existed. The reserve is the HALVING threshold only.
func marginRow(t *testing.T, pad int) json.RawMessage {
	t.Helper()
	return json.RawMessage(`{"org_id":"org-acme","pad":"` + strings.Repeat("x", pad) + `"}`)
}

func marginBatch(t *testing.T, caseRows []json.RawMessage) CompleteRouteBatch {
	t.Helper()
	effects := make([]EffectBatch, 0, 7)
	for _, destination := range []string{"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks", "test_suite_results",
		"test_case_results", "coverage_snapshots", testOpsRunReportCompletenessDestination} {
		rows := []json.RawMessage{json.RawMessage(`{"org_id":"org-acme","d":"` + destination + `"}`)}
		if destination == "test_case_results" {
			rows = caseRows
		}
		effect, err := BuildEffectBatch(destination, EffectReplaySafe, rows)
		if err != nil {
			t.Fatal(err)
		}
		effects = append(effects, effect)
	}
	return CompleteRouteBatch{Effects: effects}
}

// tunedMarginPad returns the row pad that makes ONE case row's chunk payload land `above` bytes over the reserved limit.
func tunedMarginPad(t *testing.T, above int) int {
	t.Helper()
	measure := func(pad int) int {
		chunk := PreparedProviderChunk{SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion, TotalChunks: 1,
			Effects: marginBatch(t, []json.RawMessage{marginRow(t, pad)}).Effects, InventoryComplete: true}
		encoded, err := encodedPreparedChunkPayload(chunk)
		if err != nil {
			t.Fatal(err)
		}
		return len(encoded)
	}
	target := splitPayloadLimit(TestOpsRouteChunkPolicy()) + above
	pad := target - 2000
	for step := 0; step < 40 && abs(measure(pad)-target) > 8; step++ {
		pad += target - measure(pad)
	}
	if got := measure(pad); abs(got-target) > 64 {
		t.Fatalf("pad %d gives %d bytes, want about %d", pad, got, target)
	}
	return pad
}

func storeAcceptsChunks(t *testing.T, chunks []PreparedProviderChunk) error {
	t.Helper()
	claim := nativeTestClaim("github", "cicd")
	resetEmissionChunks(chunks, ChunkRouteEmission{CursorBefore: "before", CursorAfter: "after"})
	for index := range chunks {
		if err := sizeEmissionChunks(chunks[index : index+1]); err != nil {
			t.Fatal(err)
		}
		chunks[index].Ordinal = 40 + index
		chunks[index].ManifestDigest = preparedChunkDigest(chunks[index])
		if _, err := buildPreparedChunkMaterial(claim, chunks[index], time.Date(2026, 12, 31, 23, 59, 59, 999_999_999, time.UTC)); err != nil {
			return err
		}
	}
	return nil
}

func TestSingleRowInTheReserveMarginIsAcceptedOnEveryTestOpsRoute(t *testing.T) {
	t.Parallel()
	pad := tunedMarginPad(t, 3000)
	for _, route := range testOpsRoutePairs {
		descriptor, ok := Descriptor(route[0], route[1])
		if !ok {
			t.Fatalf("%s/%s has no descriptor", route[0], route[1])
		}
		chunks, err := SplitCompleteRouteBatch(marginBatch(t, []json.RawMessage{marginRow(t, pad)}), descriptor.ChunkPolicy)
		if err != nil || len(chunks) != 1 {
			t.Fatalf("%s/%s: a single row in the margin split into %d chunks, err=%v, want 1 and nil", route[0], route[1], len(chunks), err)
		}
		written := 0
		for _, effect := range chunks[0].Effects {
			written += len(effect.Rows)
		}
		if written != 7 {
			t.Fatalf("%s/%s: the chunk holds %d rows, want the 7 rows of the 7 destinations", route[0], route[1], written)
		}
		if err := storeAcceptsChunks(t, chunks); err != nil {
			t.Fatalf("%s/%s: the store refuses a single-row chunk in the margin: %v", route[0], route[1], err)
		}
	}
}

// Above what the store takes the refusal stays: the split refuses a one-row payload over 2 MiB, and the store refuses payload + ledger
// over 2 MiB. Both are the behaviour on main before the reserve existed, not a change of this PR.
func TestSingleRowAboveWhatTheStoreAcceptsIsRefusedAsOnMain(t *testing.T) {
	t.Parallel()
	over := tunedMarginPad(t, maxChunkPayloadBytes-splitPayloadLimit(TestOpsRouteChunkPolicy())+4000)
	if _, err := SplitCompleteRouteBatch(marginBatch(t, []json.RawMessage{marginRow(t, over)}), TestOpsRouteChunkPolicy()); !errors.Is(err, ErrChunkPolicyExceeded) {
		t.Fatalf("a one-row payload over 2 MiB: err=%v, want the policy refusal", err)
	}
	between := tunedMarginPad(t, maxChunkPayloadBytes-splitPayloadLimit(TestOpsRouteChunkPolicy())-300)
	chunks, err := SplitCompleteRouteBatch(marginBatch(t, []json.RawMessage{marginRow(t, between)}), TestOpsRouteChunkPolicy())
	if err != nil || len(chunks) != 1 {
		t.Fatalf("a one-row payload just under 2 MiB: %d chunks, err=%v, want the split to accept it", len(chunks), err)
	}
	if err := storeAcceptsChunks(t, chunks); err == nil {
		t.Fatal("the store accepted payload + ledger over 2 MiB")
	}
}

// Rows that are each in the margin halve down to one row per chunk and every row is written.
func TestRowsEachInTheReserveMarginHalveToSingleRowsAndAreAllKept(t *testing.T) {
	t.Parallel()
	pad := tunedMarginPad(t, 3000)
	rows := []json.RawMessage{marginRow(t, pad), marginRow(t, pad+1), marginRow(t, pad+2)}
	chunks, err := SplitCompleteRouteBatch(marginBatch(t, rows), TestOpsRouteChunkPolicy())
	if err != nil || len(chunks) != 3 {
		t.Fatalf("3 rows each in the margin split into %d chunks, err=%v, want 3 single-row chunks", len(chunks), err)
	}
	kept := 0
	for _, chunk := range chunks {
		for _, effect := range chunk.Effects {
			if effect.Destination == "test_case_results" {
				kept += len(effect.Rows)
			}
		}
	}
	if kept != 3 {
		t.Fatalf("the chunks keep %d of 3 rows", kept)
	}
	if err := storeAcceptsChunks(t, chunks); err != nil {
		t.Fatalf("the store refuses a chunk of the halved emission: %v", err)
	}
}

// github/files: one row in the margin is one sub-chunk the store accepts, so the route keeps its contents (the pre-check shares the split).
func TestGitHubFilesSingleRowInTheReserveMarginKeepsItsContents(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "files")
	build := func(length int) EffectBatch {
		text := strings.Repeat("x", length)
		row := gitFileRow{RepoID: "r", Path: "src/a.go", Contents: &text, LastSynced: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC), OrgID: claim.OrgID}
		effect, err := effectBatchFromValues("git_files", EffectReadbackRequired, []gitFileRow{row})
		if err != nil {
			t.Fatal(err)
		}
		return effect
	}
	limit := splitPayloadLimit(GitHubFilesChunkPolicy())
	length := limit + 3000
	for step := 0; step < 40; step++ {
		chunk := PreparedProviderChunk{SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion, TotalChunks: 1,
			Effects: []EffectBatch{build(length)}, InventoryComplete: true}
		encoded, err := encodedPreparedChunkPayload(chunk)
		if err != nil {
			t.Fatal(err)
		}
		if abs(len(encoded)-(limit+3000)) <= 8 {
			break
		}
		length += limit + 3000 - len(encoded)
	}
	if !gitHubFilesChunkFits(claim, build(length), "before", "after") {
		t.Fatalf("a single file row %d bytes over the reserved limit is refused by the pre-check although the store takes it", 3000)
	}
}

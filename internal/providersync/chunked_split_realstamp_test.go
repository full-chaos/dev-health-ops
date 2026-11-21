package providersync

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const realStampCursor = `{"phase":"artifacts","offset":1}`
const realStampRepoID = "c7198fbc-1945-3717-05d8-eb78866b4e79"

type realStampHandler struct {
	effects, final []EffectBatch
}

func (h realStampHandler) Collect(context.Context, Claim, providerfoundation.Credential, *providerfoundation.HTTPClient, time.Time) (CompleteRouteBatch, error) {
	return CompleteRouteBatch{}, ErrInvalidConfiguration
}

func (h realStampHandler) CollectChunks(_ context.Context, _ Claim, _ providerfoundation.Credential, _ *providerfoundation.HTTPClient,
	_ time.Time, cursor string, emit func(ChunkRouteEmission) error) error {
	if cursor == "" {
		if err := emit(ChunkRouteEmission{Batch: CompleteRouteBatch{Effects: h.effects}, CursorAfter: realStampCursor}); err != nil {
			return err
		}
	}
	return emit(ChunkRouteEmission{Batch: CompleteRouteBatch{Effects: h.final, Result: map[string]any{"complete": true}},
		CursorBefore: realStampCursor, CursorAfter: realStampCursor, Final: true})
}

func realStampEffects(t *testing.T, claim Claim, now time.Time, count, largeRow, pad int) []EffectBatch {
	t.Helper()
	rows := make([]testCaseResultRow, count)
	for i := range rows {
		rows[i] = testCaseResultRow{RepoID: realStampRepoID, RunID: "1", SuiteID: "s", CaseID: fmt.Sprintf("%04d", i),
			CaseName: fmt.Sprintf("test case %04d", i), Status: "passed", OrgID: claim.OrgID, LastSynced: now}
		if i == largeRow || largeRow == -2 {
			detail := strings.Repeat("x", pad)
			rows[i].Status = "failed"
			if claim.Provider == "github" {
				rows[i].FailureMessage = &detail
			} else {
				rows[i].ClassName = &detail
			}
		}
	}
	var verdict []testOpsRunReportCompletenessRow
	if count != 0 {
		provider := "github_actions"
		if claim.Provider == "gitlab" {
			provider = "gitlab_ci"
		}
		verdict = append(verdict, testOpsRunReportCompletenessRow{OrgID: claim.OrgID, RepoID: realStampRepoID, RunID: "1",
			RunAttempt: 1, Provider: provider, Complete: true, Reasons: []string{}, ReportsExpected: 1,
			ReportsAdmitted: 1, LastSynced: now, VerdictVersion: testOpsRunReportVerdictVersion(true, now)})
	}
	effects, err := testOpsEffects(nil, nil, nil, nil, rows, nil, verdict...)
	if err != nil {
		t.Fatal(err)
	}
	return effects
}

func realStampMaterialAt(t *testing.T, claim Claim, now time.Time, ordinal, pad int) (int, error) {
	t.Helper()
	chunk := PreparedProviderChunk{SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion,
		Ordinal: ordinal, CursorAfter: realStampCursor, Effects: realStampEffects(t, claim, now, 1, 0, pad)}
	material, err := buildPreparedChunkMaterial(claim, chunk, now)
	return len(material.payloadRaw) + len(material.ledgerRaw), err
}

type realStampRun struct {
	prepared, rows, duplicates int
	err                        error
}

func realStampExecute(t *testing.T, route [2]string, now time.Time, cases, largeRow, pad int) realStampRun {
	t.Helper()
	descriptor, ok := Descriptor(route[0], route[1])
	if !ok || !descriptor.Chunked {
		t.Fatal("missing chunked descriptor")
	}
	if !descriptor.Plannable {
		descriptor, ok = Descriptor(route[0], descriptor.CanonicalDataset)
		if !ok || !descriptor.Plannable {
			t.Fatal("missing canonical descriptor")
		}
	}
	claim, session := completeRouteSessionFor(t, now, false, route[0], descriptor.RouteDataset)
	handler := realStampHandler{effects: realStampEffects(t, claim, now, cases, largeRow, pad), final: realStampEffects(t, claim, now, 0, -1, 0)}
	sink := &recoveryRowSink{}
	store := NewInProcessChunkLedger(claim)
	executor := completeRouteExecutor(now, handler, store, sink)
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: route[0]}
	executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
	executor.BudgetLimits = map[CostClass]int{claim.CostClass: 1}
	var err error
	for attempts := 0; attempts < 5; attempts++ {
		_, err = executor.Execute(context.Background(), session, descriptor)
		if !errors.Is(err, ErrChunkContinuation) {
			break
		}
	}
	checkpoint, _ := store.LoadChunkCheckpoint(context.Background(), claim, now)
	return realStampRun{prepared: checkpoint.PreparedChunks, rows: len(sink.rows), duplicates: sink.duplicates(), err: err}
}

var realStampRoutes = [][2]string{{"github", "cicd"}, {"github", "tests"}, {"gitlab", "cicd"}, {"gitlab", "tests"}}

// A 101-case emission with one near-limit row needs at most main's three prepared chunks: the split must not halve it down to single rows
// because a worst-case ordinal or cursor would push a chunk over 2 MiB when the real stamps do not.
func TestRealStampsKeepANearLimitRowInMainsChunkCount(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 123456789, time.UTC)
	for _, route := range realStampRoutes {
		for _, position := range []int{0, 50, 100} {
			run := realStampExecute(t, route, now, 101, position, 2_055_000)
			if run.err != nil || run.prepared > 3 || run.prepared < 2 || run.rows != 102 || run.duplicates != 0 {
				t.Errorf("%s/%s row %d: prepared=%d rows=%d duplicates=%d err=%v, want at most main's 3 chunks and 102 rows", route[0], route[1], position, run.prepared, run.rows, run.duplicates, run.err)
			}
		}
	}
}

// The last body row fits the store at ordinal 1 to the byte; at the real ordinal the emission is accepted, as on main.
func TestRealStampsAcceptAChunkThatFitsAtItsRealOrdinalToTheByte(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 123456789, time.UTC)
	for _, route := range realStampRoutes {
		descriptor, _ := Descriptor(route[0], route[1])
		if !descriptor.Plannable {
			descriptor, _ = Descriptor(route[0], descriptor.CanonicalDataset)
		}
		claim, _ := completeRouteSessionFor(t, now, false, route[0], descriptor.RouteDataset)
		pad := 2_055_000
		size, err := realStampMaterialAt(t, claim, now, 1, pad)
		if err != nil {
			t.Fatal(err)
		}
		pad += maxChunkPayloadBytes - size - 1
		if size, err = realStampMaterialAt(t, claim, now, 1, pad); err != nil || size != maxChunkPayloadBytes-1 {
			t.Fatalf("ordinal 1 material: bytes=%d err=%v", size, err)
		}
		run := realStampExecute(t, route, now, 101, 100, pad)
		if run.err != nil || run.prepared > 3 || run.prepared < 2 || run.rows != 102 || run.duplicates != 0 {
			t.Errorf("%s/%s byte edge: prepared=%d rows=%d err=%v, want accepted with at most main's 3 chunks", route[0], route[1], run.prepared, run.rows, run.err)
		}
	}
}

// One row 10 to 40 KB under 2 MiB with the real cursor of a TestOps emission: the store accepts it with the real stamps, so the split
// keeps it as one chunk (the worst-case cursors, 49 KB of escaped text, would refuse it).
func TestRealStampsKeepASingleRowInTheCursorMarginAsOneChunk(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 123456789, time.UTC)
	for _, margin := range []int{10_000, 20_000, 40_000} {
		descriptor, _ := Descriptor("github", "cicd")
		claim, _ := completeRouteSessionFor(t, now, false, "github", descriptor.RouteDataset)
		pad := 2_000_000
		size, err := realStampMaterialAt(t, claim, now, 0, pad)
		if err != nil {
			t.Fatal(err)
		}
		pad += maxChunkPayloadBytes - margin - size
		if size, err = realStampMaterialAt(t, claim, now, 0, pad); err != nil || size > maxChunkPayloadBytes-margin+8 || size < maxChunkPayloadBytes-margin-8 {
			t.Fatalf("margin %d: material bytes=%d err=%v", margin, size, err)
		}
		run := realStampExecute(t, [2]string{"github", "cicd"}, now, 1, 0, pad)
		if run.err != nil || run.prepared != 2 || run.rows != 2 || run.duplicates != 0 {
			t.Errorf("margin %d: prepared=%d rows=%d err=%v, want the row as one data chunk plus the final chunk", margin, run.prepared, run.rows, run.err)
		}
	}
}

func TestGitHubFilesHundredFilesStayInOneDataChunkAndTheInventoryCountsIt(t *testing.T) {
	fake := newFilesChunkFake(t, 0, 0, 0)
	text := "export const view = (label: string) => <>\n" + strings.Repeat("  <p>{label}</p>\n", 543) + "</>\n"
	for index := range 100 {
		path := fmt.Sprintf("src/part-%03d.tsx", index)
		fake.trees[fake.head] = append(fake.trees[fake.head], path)
		fake.sizes[path] = len(text)
		fake.texts[path] = text
	}
	run, err := runFilesChunks(t, fake, "", 0)
	if err != nil || len(run.emissions) != 2 {
		t.Fatalf("provider emissions=%d err=%v", len(run.emissions), err)
	}
	emission := run.emissions[0]
	if _, ok := gitHubFilesPreparedChunk(emission.Batch.Effects[0], emission.CursorBefore, emission.CursorAfter); !ok || len(emission.Batch.Effects[0].Rows) != 100 {
		t.Fatal("files precheck did not accept the emission")
	}
	sink := &filesReadbackSink{}
	executor, session, descriptor, store := filesChunkExecutor(t, fake, sink)
	result, err := executor.Execute(context.Background(), session, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := store.LoadChunkCheckpoint(context.Background(), session.Claim, executor.now())
	if err != nil || result.CommittedRows != 100 || len(sink.rows) != 100 {
		t.Fatalf("files rows=%d distinct=%d err=%v", result.CommittedRows, len(sink.rows), err)
	}
	if checkpoint.PreparedChunks != 2 || fmt.Sprint(result.Result["content_chunks"]) != "1" {
		t.Fatalf("prepared=%d content_chunks=%v, want 2 prepared (one data chunk and the final one) and content_chunks 1 as on main", checkpoint.PreparedChunks, result.Result["content_chunks"])
	}
}

// Two rows, each 10 to 40 KB under 2 MiB with the real cursor: no two fit one chunk, so the split halves down to single rows, and the store
// accepts each one with its real stamps. A refusal at one row with the worst-case cursors would end the emission without a write.
func TestRealStampsKeepTwoSingleRowChunksInTheCursorMargin(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 123456789, time.UTC)
	for _, margin := range []int{20_000, 40_000} {
		descriptor, _ := Descriptor("github", "cicd")
		claim, _ := completeRouteSessionFor(t, now, false, "github", descriptor.RouteDataset)
		pad := 2_000_000
		size, err := realStampMaterialAt(t, claim, now, 0, pad)
		if err != nil {
			t.Fatal(err)
		}
		pad += maxChunkPayloadBytes - margin - size
		run := realStampExecute(t, [2]string{"github", "cicd"}, now, 2, -2, pad)
		if run.err != nil || run.prepared != 3 || run.rows != 3 || run.duplicates != 0 {
			t.Errorf("margin %d: prepared=%d rows=%d err=%v, want two single-row data chunks, the final one and 3 rows written", margin, run.prepared, run.rows, run.err)
		}
	}
}

func realStampFilesEffect(t *testing.T, claim Claim, now time.Time, contents int) EffectBatch {
	t.Helper()
	rows := make([]gitFileRow, 2)
	for index := range rows {
		text := strings.Repeat("x", contents)
		rows[index] = gitFileRow{RepoID: realStampRepoID, Path: fmt.Sprintf("src/part-%d.txt", index), Contents: &text, LastSynced: now, OrgID: claim.OrgID}
	}
	effect, err := effectBatchFromValues("git_files", EffectReadbackRequired, rows)
	if err != nil {
		t.Fatal(err)
	}
	return effect
}

// A files emission that the files pre-check accepts by raw size (gitHubFilesPreparedChunk: nil claim, no ledger) but whose payload and
// ledger together pass 2 MiB. The files route owns that decision (it halves its own rows); the shared split must not halve it again, so
// the emission stays one chunk and the store refuses it at prepare, as on main. The files route passes a nil claim to the split.
func TestGitHubFilesEmissionAcceptedByRawSizeStaysOneChunkAndIsRefusedByTheStore(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 123456789, time.UTC)
	descriptor, ok := Descriptor("github", "files")
	if !ok || !descriptor.Chunked {
		t.Fatal("missing chunked files descriptor")
	}
	claim, session := completeRouteSessionFor(t, now, false, "github", descriptor.RouteDataset)
	contents := 1_040_000
	for ; contents < 1_060_000; contents += 25 {
		effect := realStampFilesEffect(t, claim, now, contents)
		chunk, fits := gitHubFilesPreparedChunk(effect, "", realStampCursor)
		if !fits {
			t.Fatalf("contents %d: the raw pre-check refuses before the store's ledger does", contents)
		}
		chunk.Ordinal = 0
		if _, err := buildPreparedChunkMaterial(claim, chunk, gitHubFilesWidestInstant); err != nil {
			break
		}
	}
	effect := realStampFilesEffect(t, claim, now, contents)
	if _, fits := gitHubFilesPreparedChunk(effect, "", realStampCursor); !fits {
		t.Fatalf("contents %d: the raw pre-check refuses", contents)
	}
	final, err := effectBatchFromValues("git_files", EffectReadbackRequired, []gitFileRow(nil))
	if err != nil {
		t.Fatal(err)
	}
	handler := realStampHandler{effects: []EffectBatch{effect}, final: []EffectBatch{final}}
	store := NewInProcessChunkLedger(claim)
	executor := completeRouteExecutor(now, handler, store, &recoveryRowSink{})
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
	executor.BudgetLimits = map[CostClass]int{claim.CostClass: 1}
	_, err = executor.Execute(context.Background(), session, descriptor)
	checkpoint, _ := store.LoadChunkCheckpoint(context.Background(), claim, now)
	if !errors.Is(err, ErrChunkCheckpointConflict) || checkpoint.PreparedChunks != 0 {
		t.Fatalf("contents %d: err=%v prepared=%d, want the store to refuse the one chunk at prepare (prepared 0), as on main", contents, err, checkpoint.PreparedChunks)
	}
}

// Every chunked descriptor of the registry is served by a handler that streams (ChunkedCompleteRouteHandler), so executeChunked, whose
// split has no real stamps, is not reached by a production route.
func TestEveryChunkedDescriptorHasAStreamingHandler(t *testing.T) {
	seen := 0
	for key := range providerExecutorRegistry {
		provider, dataset, _ := strings.Cut(key, "/")
		descriptor, ok := Descriptor(provider, dataset)
		if !ok || !descriptor.Chunked {
			continue
		}
		seen++
		route, selected := SelectGitFamilyRoute(provider, dataset, GitFamilyDeps{})
		if !selected {
			t.Errorf("%s: a chunked descriptor with no known route handler", key)
			continue
		}
		if _, streams := route.Handler.(ChunkedCompleteRouteHandler); !streams {
			t.Errorf("%s: a chunked descriptor whose handler %T does not stream", key, route.Handler)
		}
	}
	if seen < 5 {
		t.Fatalf("saw %d chunked descriptors, want at least the github and gitlab cicd and tests routes and github files", seen)
	}
}

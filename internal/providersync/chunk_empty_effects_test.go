package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

type recordingEffectSink struct{ writes []string }

func (sink *recordingEffectSink) WriteEffect(_ context.Context, _ Claim, batch EffectBatch) error {
	sink.writes = append(sink.writes, batch.Destination)
	return nil
}

// noOpRecordingEffectSink declares its empty writes no-ops, as the TestOps and GitHub files sinks do.
type noOpRecordingEffectSink struct{ recordingEffectSink }

func (*noOpRecordingEffectSink) emptyEffectIsNoOp(string) bool { return true }

// emptyEffectsChunk is one TestOps sub-chunk: the seven destinations, only ci_job_runs with rows.
func emptyEffectsChunk(t *testing.T, ordinal int, rows ...string) PreparedProviderChunk {
	t.Helper()
	destinations := []string{
		"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks", "test_suite_results", "test_case_results",
		"coverage_snapshots", testOpsRunReportCompletenessDestination,
	}
	effects := make([]EffectBatch, 0, len(destinations))
	for _, destination := range destinations {
		if destination == "ci_job_runs" {
			effects = append(effects, effectBatchFixture(t, destination, EffectReadbackRequired, rows...))
			continue
		}
		effects = append(effects, effectBatchFixture(t, destination, EffectReadbackRequired))
	}
	return PreparedProviderChunk{
		SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion,
		Ordinal: ordinal, CursorAfter: "c1", Effects: effects,
	}
}

type inProcessChunkSnapshot struct {
	ledger, status string
	checkpoint     ChunkCheckpoint
}

func snapshotInProcessChunk(t *testing.T, store *InProcessChunkLedger, ordinal int) inProcessChunkSnapshot {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	row := store.chunks[ordinal]
	return inProcessChunkSnapshot{ledger: string(row.ledgerRaw), status: row.status, checkpoint: *store.checkpoint}
}

func TestChunkEmptyEffectsAreCommittedByTheMarkInProcess(t *testing.T) {
	ctx := context.Background()
	claim := nativeTestClaim("github", "tests")
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	store := NewInProcessChunkLedger(claim)
	prepared, err := store.PrepareChunk(ctx, claim, emptyEffectsChunk(t, 0, `{"org_id":"org-acme","run_id":"1","job_id":"j"}`), now)
	if err != nil {
		t.Fatal(err)
	}
	sink := &noOpRecordingEffectSink{}
	result, err := commitPreparedChunk(ctx, claim, prepared, sink, nil, clock, store)
	if err != nil || result.Written != 7 || result.Skipped != 0 || len(sink.writes) != 1 || sink.writes[0] != "ci_job_runs" {
		t.Fatalf("result=%+v writes=%v err=%v: only the effect with rows is written, every effect counts as written", result, sink.writes, err)
	}
	loaded, err := store.LoadPreparedChunk(ctx, claim, 0, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range loaded.Ledger.Effects {
		want := GenerationBlockPending
		if entry.Destination == "ci_job_runs" {
			want = GenerationBlockCommitted
		}
		if entry.Status != want {
			t.Fatalf("%s=%s before the mark, want %s", entry.Destination, entry.Status, want)
		}
	}
	if err := store.MarkChunkCommitted(ctx, claim, 0, prepared.ManifestDigest, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	marked := snapshotInProcessChunk(t, store, 0)
	loaded, err = store.LoadPreparedChunk(ctx, claim, 0, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range loaded.Ledger.Effects {
		if entry.Status != GenerationBlockCommitted || entry.StartedAt == nil || entry.CommittedAt == nil {
			t.Fatalf("%s after the mark: %+v", entry.Destination, entry)
		}
	}
	if marked.status != "committed" || marked.checkpoint.NextOrdinal != 1 || marked.checkpoint.CommittedRows != 1 {
		t.Fatalf("mark: %+v", marked)
	}
	if err := store.MarkChunkCommitted(ctx, claim, 0, prepared.ManifestDigest, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	replayed := snapshotInProcessChunk(t, store, 0)
	if replayed.ledger != marked.ledger || replayed.checkpoint.CommittedRows != 1 {
		t.Fatalf("a replayed mark changed the chunk: ledger equal=%v rows=%d", replayed.ledger == marked.ledger, replayed.checkpoint.CommittedRows)
	}
}

func TestChunkEmptyEffectsMarkRefusesAnEntryThatIsNotProvablyEmpty(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	row := `{"org_id":"org-acme","run_id":"1","job_id":"j"}`
	for _, testCase := range []struct {
		name  string
		setup func(t *testing.T, claim Claim, store *InProcessChunkLedger, chunk PreparedProviderChunk)
		chunk func(t *testing.T) PreparedProviderChunk
	}{
		{name: "pending effect with rows", chunk: func(t *testing.T) PreparedProviderChunk { return emptyEffectsChunk(t, 0, row) }},
		{name: "writing empty effect", chunk: func(t *testing.T) PreparedProviderChunk { return emptyEffectsChunk(t, 0) },
			setup: func(t *testing.T, claim Claim, store *InProcessChunkLedger, chunk PreparedProviderChunk) {
				if err := store.BeginChunkEffect(ctx, claim, 0, 0, chunk.Ledger.Effects[0].ContentDigest, now); err != nil {
					t.Fatal(err)
				}
			}},
		{name: "empty rows with a membership rejection", chunk: func(t *testing.T) PreparedProviderChunk {
			chunk := emptyEffectsChunk(t, 0)
			chunk.Effects[0].MembershipRejections = []json.RawMessage{json.RawMessage(`{"reason":"x"}`)}
			return chunk
		}},
		{name: "entry digest is not the empty digest", chunk: func(t *testing.T) PreparedProviderChunk { return emptyEffectsChunk(t, 0) },
			setup: func(t *testing.T, _ Claim, store *InProcessChunkLedger, chunk PreparedProviderChunk) {
				ledger := chunk.Ledger
				ledger.Effects = append([]EffectLedgerEntry(nil), ledger.Effects...)
				ledger.Effects[0].ContentDigest = effectBatchFixture(t, ledger.Effects[0].Destination, EffectReadbackRequired, row).ContentDigest
				store.mu.Lock()
				store.chunks[0].ledgerRaw = encodeEffectLedgerState(ledger)
				store.mu.Unlock()
			}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			claim := nativeTestClaim("github", "tests")
			store := NewInProcessChunkLedger(claim)
			prepared, err := store.PrepareChunk(ctx, claim, testCase.chunk(t), now)
			if err != nil {
				t.Fatal(err)
			}
			if testCase.setup != nil {
				testCase.setup(t, claim, store, prepared)
			}
			before := snapshotInProcessChunk(t, store, 0)
			err = store.MarkChunkCommitted(ctx, claim, 0, prepared.ManifestDigest, now.Add(time.Second))
			if !errors.Is(err, ErrChunkCheckpointConflict) {
				t.Fatalf("mark err=%v, want ErrChunkCheckpointConflict", err)
			}
			if after := snapshotInProcessChunk(t, store, 0); !reflect.DeepEqual(after, before) {
				t.Fatalf("a refused mark changed the chunk:\nbefore %+v\nafter  %+v", before, after)
			}
		})
	}
}

func TestChunkEmptyEffectsStayOnTheOldPathWithoutBothOptIns(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	row := `{"org_id":"org-acme","run_id":"1","job_id":"j"}`
	claim := nativeTestClaim("github", "tests")

	inProcess := NewInProcessChunkLedger(claim)
	prepared, err := inProcess.PrepareChunk(ctx, claim, emptyEffectsChunk(t, 0, row), now)
	if err != nil {
		t.Fatal(err)
	}
	plain := &recordingEffectSink{}
	if result, err := commitPreparedChunk(ctx, claim, prepared, plain, nil, clock, inProcess); err != nil || result.Written != 7 || len(plain.writes) != 7 {
		t.Fatalf("sink without the opt-in: result=%+v writes=%d err=%v", result, len(plain.writes), err)
	}

	memory := newChunkMemoryStore()
	prepared, err = memory.PrepareChunk(ctx, claim, emptyEffectsChunk(t, 0, row), now)
	if err != nil {
		t.Fatal(err)
	}
	opted := &noOpRecordingEffectSink{}
	if result, err := commitPreparedChunk(ctx, claim, prepared, opted, nil, clock, memory); err != nil || result.Written != 7 || len(opted.writes) != 7 {
		t.Fatalf("store without the opt-in: result=%+v writes=%d err=%v", result, len(opted.writes), err)
	}

	journal := &noOpRecordingEffectSink{}
	committer := EffectCommitter{Ledger: &memoryEffectLedger{}, Sink: journal, Now: clock}
	effects := emptyEffectsChunk(t, 0, row).Effects
	if result, err := committer.Commit(ctx, claim, effects, now); err != nil || result.Written != 7 || len(journal.writes) != 7 {
		t.Fatalf("unit-journal ledger: result=%+v writes=%d err=%v", result, len(journal.writes), err)
	}
	if result, err := committer.Commit(ctx, claim, effects, now); err != nil || result.Written != 0 || result.Skipped != 7 {
		t.Fatalf("unit-journal retry: result=%+v err=%v", result, err)
	}
}

func TestChunkEmptyEffectsMarkIsFencedInProcess(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("github", "tests")
	store := NewInProcessChunkLedger(claim)
	prepared, err := store.PrepareChunk(ctx, claim, emptyEffectsChunk(t, 0), now)
	if err != nil {
		t.Fatal(err)
	}
	sink := &noOpRecordingEffectSink{}
	if result, err := commitPreparedChunk(ctx, claim, prepared, sink, nil, func() time.Time { return now }, store); err != nil ||
		result.Written != 7 || len(sink.writes) != 0 {
		t.Fatalf("all-empty chunk: result=%+v writes=%v err=%v", result, sink.writes, err)
	}
	before := snapshotInProcessChunk(t, store, 0)
	stranger := claim
	stranger.Owner = "77777777-7777-4777-8777-777777777777"
	if err := store.MarkChunkCommitted(ctx, stranger, 0, prepared.ManifestDigest, now); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("mark under a lost lease: %v", err)
	}
	after := snapshotInProcessChunk(t, store, 0)
	if !reflect.DeepEqual(after, before) || after.status != "pending" || after.checkpoint.NextOrdinal != 0 || !bytes.Contains([]byte(after.ledger), []byte(`"status":"pending"`)) {
		t.Fatalf("a fenced mark changed the chunk: %+v", after)
	}
}

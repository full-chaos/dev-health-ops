package providersync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// The executor's coalescing is opt-in per emission. A route that marks nothing keeps today's group and ledger sequence
// byte for byte: the digests below were taken on the base binary (ops 081bd929e5d7, before coalescing existed) and must
// not move. Covered: the GitHub files route end to end, and a synthetic route whose emissions split into several
// sub-chunks. The digest reads every stored chunk (ordinal, totals, cursors, manifest, payload size, effects, ledger
// statuses) and the final checkpoint; timestamps are left out.
const (
	noLeakFilesDigest        = "7c0114565ce2b851797b678d0661bca87efc6be9b299974aff947fcb8f0147a3"
	noLeakSyntheticDigest    = "f08668fd256956f220e0b590ba7beb6490d8d0128eeb72c40a9b71daba113e86"
	noLeakSyntheticDigest500 = "ce42e7bc67716b56ff95fd4ed13e4af5b1340b636a5394f1ad8d5039f4a5bd2c"
)

func noLeakChunkDigest(t *testing.T, chunks []PreparedProviderChunk, checkpoint ChunkCheckpoint) string {
	t.Helper()
	hash := sha256.New()
	for _, chunk := range chunks {
		fmt.Fprintf(hash, "chunk %d/%d %q>%q final=%t manifest=%s bytes=%d\n", chunk.Ordinal, chunk.TotalChunks,
			chunk.CursorBefore, chunk.CursorAfter, chunk.InventoryComplete, chunk.ManifestDigest, chunk.PayloadBytes)
		for _, effect := range chunk.Effects {
			fmt.Fprintf(hash, " effect %s %s %s rows=%d bytes=%d\n", effect.Destination, effect.ContentDigest,
				effect.Recovery, len(effect.Rows), effect.PayloadBytes)
		}
		for _, entry := range chunk.Ledger.Effects {
			fmt.Fprintf(hash, " ledger %d %s %s\n", entry.Index, entry.ContentDigest, entry.Status)
		}
	}
	fmt.Fprintf(hash, "checkpoint next=%d prepared=%d total=%d rows=%d complete=%t cursor=%q\n", checkpoint.NextOrdinal,
		checkpoint.PreparedChunks, checkpoint.TotalChunks, checkpoint.CommittedRows, checkpoint.InventoryComplete,
		checkpoint.NextCursor)
	return hex.EncodeToString(hash.Sum(nil))
}

func TestChunkedRouteThatMarksNothingKeepsTheBaseGroupSequence(t *testing.T) {
	t.Run("github files route", func(t *testing.T) {
		fake := newFilesChunkFake(t, 1_200, 9_000, 150)
		sink := &filesReadbackSink{}
		executor, session, descriptor, store := filesChunkExecutor(t, fake, sink)
		for attempt := 1; ; attempt++ {
			if attempt > 50 {
				t.Fatal("no completion in 50 attempts")
			}
			_, err := executor.Execute(context.Background(), session, descriptor)
			if err == nil {
				break
			}
			if _, ok := ChunkContinuationDelay(err); !ok {
				t.Fatalf("attempt %d: %v", attempt, err)
			}
		}
		checkpoint, err := store.LoadChunkCheckpoint(context.Background(), session.Claim, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		chunks := make([]PreparedProviderChunk, 0, checkpoint.TotalChunks)
		for ordinal := 0; ordinal < checkpoint.TotalChunks; ordinal++ {
			chunk, err := store.LoadPreparedChunk(context.Background(), session.Claim, ordinal, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			chunks = append(chunks, chunk)
		}
		got := noLeakChunkDigest(t, chunks, checkpoint)
		t.Logf("NOLEAK files: %d chunks digest %s", len(chunks), got)
		if got != noLeakFilesDigest {
			t.Fatalf("files route group sequence digest %s, want the base %s", got, noLeakFilesDigest)
		}
	})
	t.Run("synthetic route with split emissions", func(t *testing.T) {
		now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
		claim, session := completeRouteSessionFor(t, now, false, "github", "cicd")
		descriptor, ok := Descriptor("github", "cicd")
		if !ok {
			t.Fatal("descriptor")
		}
		// The digest pins the sequence at the 100-row bound: the large emissions below must still split.
		descriptor.ChunkPolicy.MaxSourceItems = 100
		store := newChunkMemoryStore()
		executor := completeRouteExecutor(now, &noLeakHandler{t: t, claim: claim}, store, &recoveryRowSink{})
		executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
		executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
		for attempt := 1; ; attempt++ {
			if attempt > 50 {
				t.Fatal("no completion in 50 attempts")
			}
			_, err := executor.Execute(context.Background(), session, descriptor)
			if err == nil {
				break
			}
			if _, ok := ChunkContinuationDelay(err); !ok {
				t.Fatalf("attempt %d: %v", attempt, err)
			}
		}
		store.mu.Lock()
		chunks := make([]PreparedProviderChunk, 0, len(store.chunks))
		for ordinal := 0; ordinal < len(store.chunks); ordinal++ {
			chunks = append(chunks, store.chunks[ordinal])
		}
		checkpoint := store.checkpoint
		store.mu.Unlock()
		got := noLeakChunkDigest(t, chunks, checkpoint)
		t.Logf("NOLEAK synthetic: %d chunks digest %s", len(chunks), got)
		if got != noLeakSyntheticDigest {
			t.Fatalf("synthetic route group sequence digest %s, want the base %s", got, noLeakSyntheticDigest)
		}
	})
	t.Run("synthetic route at the TestOps row bound (500)", func(t *testing.T) {
		now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
		claim, session := completeRouteSessionFor(t, now, false, "github", "cicd")
		descriptor, ok := Descriptor("github", "cicd")
		if !ok {
			t.Fatal("descriptor")
		}
		// The descriptor policy as shipped (500 rows): the emissions below no longer split; the digest is this policy's own.
		store := newChunkMemoryStore()
		executor := completeRouteExecutor(now, &noLeakHandler{t: t, claim: claim}, store, &recoveryRowSink{})
		executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
		executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
		for attempt := 1; ; attempt++ {
			if attempt > 50 {
				t.Fatal("no completion in 50 attempts")
			}
			_, err := executor.Execute(context.Background(), session, descriptor)
			if err == nil {
				break
			}
			if _, ok := ChunkContinuationDelay(err); !ok {
				t.Fatalf("attempt %d: %v", attempt, err)
			}
		}
		store.mu.Lock()
		chunks := make([]PreparedProviderChunk, 0, len(store.chunks))
		for ordinal := 0; ordinal < len(store.chunks); ordinal++ {
			chunks = append(chunks, store.chunks[ordinal])
		}
		checkpoint := store.checkpoint
		store.mu.Unlock()
		got := noLeakChunkDigest(t, chunks, checkpoint)
		t.Logf("NOLEAK synthetic500: %d chunks digest %s", len(chunks), got)
		if got != noLeakSyntheticDigest500 {
			t.Fatalf("synthetic route group sequence digest %s at the TestOps bound, want %s", got, noLeakSyntheticDigest500)
		}
	})
}

// noLeakHandler emits six contiguous small and large emissions (1..250 pipeline rows; the large ones split into
// several sub-chunks), marks none of them, then the final metadata emission. It resumes past what the cursor holds.
type noLeakHandler struct {
	t     *testing.T
	claim Claim
}

func (handler *noLeakHandler) Collect(
	context.Context, Claim, providerfoundation.Credential, *providerfoundation.HTTPClient, time.Time,
) (CompleteRouteBatch, error) {
	return CompleteRouteBatch{}, ErrInvalidConfiguration
}

func (handler *noLeakHandler) CollectChunks(
	_ context.Context, claim Claim, _ providerfoundation.Credential,
	_ *providerfoundation.HTTPClient, _ time.Time, resumeCursor string,
	emit func(ChunkRouteEmission) error,
) error {
	sizes := []int{1, 3, 250, 2, 120, 1}
	start := 0
	if resumeCursor != "" {
		parsed, err := strconv.Atoi(resumeCursor)
		if err != nil {
			return err
		}
		start = parsed
	}
	for index := start; index < len(sizes); index++ {
		effects := make([]EffectBatch, 0, 7)
		for _, destination := range []string{"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks",
			"test_suite_results", "test_case_results", "coverage_snapshots", testOpsRunReportCompletenessDestination} {
			var rows []json.RawMessage
			if destination == "ci_pipeline_runs" || destination == "ci_job_runs" {
				for row := 0; row < sizes[index]; row++ {
					rows = append(rows, json.RawMessage(fmt.Sprintf(`{"org_id":"org-acme","run_id":"%d-%d"}`, index, row)))
				}
			}
			effect, err := BuildEffectBatch(destination, EffectReadbackRequired, rows)
			if err != nil {
				return err
			}
			effects = append(effects, effect)
		}
		if err := emit(ChunkRouteEmission{
			Batch: CompleteRouteBatch{Effects: effects}, CursorBefore: strconv.Itoa(index), CursorAfter: strconv.Itoa(index + 1),
		}); err != nil {
			return err
		}
	}
	empty, err := testOpsEffects(nil, nil, nil, nil, nil, nil)
	if err != nil {
		return err
	}
	last := strconv.Itoa(len(sizes))
	return emit(ChunkRouteEmission{
		Batch:        CompleteRouteBatch{Effects: empty, Result: map[string]any{"complete": true}, Watermark: claim.BeforeAt},
		CursorBefore: last, CursorAfter: last, Final: true,
	})
}

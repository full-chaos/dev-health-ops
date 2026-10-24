package providersync

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func conflictFixtureStore(t *testing.T) (context.Context, Claim, *InProcessChunkLedger, time.Time) {
	t.Helper()
	claim := nativeTestClaim("github", "tests")
	return context.Background(), claim, NewInProcessChunkLedger(claim), time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
}

func conflictChunk(t *testing.T, ordinal, total int, complete bool, cursor string, runID ...string) PreparedProviderChunk {
	t.Helper()
	run := "1"
	if len(runID) > 0 {
		run = runID[0]
	}
	effect := effectBatchFixture(t, "ci_pipeline_runs", EffectReadbackRequired, `{"org_id":"org-acme","run_id":"`+run+`"}`)
	return PreparedProviderChunk{
		SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion,
		Ordinal: ordinal, TotalChunks: total, InventoryComplete: complete, CursorAfter: cursor,
		Effects: []EffectBatch{effect},
	}
}

func requireConflict(t *testing.T, err error, site string, detail ...string) {
	t.Helper()
	if !errors.Is(err, ErrChunkCheckpointConflict) {
		t.Fatalf("err = %v, want a checkpoint conflict", err)
	}
	var store *ChunkStoreError
	if errors.As(err, &store) {
		t.Fatalf("err = %v: a state mismatch must not look like a store error", err)
	}
	if got := ChunkConflictSite(err); got != site {
		t.Fatalf("conflict site = %q, want %q (err %v)", got, site, err)
	}
	for _, want := range detail {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want detail %q", err, want)
		}
	}
}

func TestChunkConflictNamesTheStaleTotal(t *testing.T) {
	ctx, claim, store, now := conflictFixtureStore(t)
	if _, err := store.PrepareChunk(ctx, claim, conflictChunk(t, 0, 1, true, "end"), now); err != nil {
		t.Fatal(err)
	}
	_, err := store.PrepareChunk(ctx, claim, conflictChunk(t, 1, 2, true, "end2"), now)
	requireConflict(t, err, "prepareChunk.1", "checkpoint_total=1", "chunk_total=2", "ordinal=1", "prepared=1")
}

func TestChunkConflictNamesTheOrdinalGap(t *testing.T) {
	ctx, claim, store, now := conflictFixtureStore(t)
	if _, err := store.PrepareChunk(ctx, claim, conflictChunk(t, 0, 0, false, "c1"), now); err != nil {
		t.Fatal(err)
	}
	_, err := store.PrepareChunk(ctx, claim, conflictChunk(t, 2, 0, false, "c3"), now)
	requireConflict(t, err, "prepareChunk.1", "ordinal=2", "prepared=1", "route_match=true")
}

func TestChunkConflictNamesTheDigestMismatch(t *testing.T) {
	ctx, claim, store, now := conflictFixtureStore(t)
	if _, err := store.PrepareChunk(ctx, claim, conflictChunk(t, 0, 0, false, "c1"), now); err != nil {
		t.Fatal(err)
	}
	_, err := store.PrepareChunk(ctx, claim, conflictChunk(t, 0, 0, false, "c1", "2"), now)
	requireConflict(t, err, "prepareChunk.3", "ordinal=0", "prepared=1", "digest_match=false")
}

func TestChunkConflictNamesTheReplayRevalidation(t *testing.T) {
	ctx, claim, store, now := conflictFixtureStore(t)
	prepared, err := store.PrepareChunk(ctx, claim, conflictChunk(t, 0, 0, false, "c1"), now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadPreparedChunk(ctx, claim, 0, now); err != nil {
		t.Fatalf("an intact row must load: %v", err)
	}
	store.chunks[0].manifestDigest = prepared.ManifestDigest + "-changed"
	_, err = store.LoadPreparedChunk(ctx, claim, 0, now)
	requireConflict(t, err, "loadRow.1", "digest_match=false", "ordinal=0", "stored_ordinal=0", "route_match=true")
}

func TestChunkConflictErrorCarriesNoPayloadAndKeepsTheSentinel(t *testing.T) {
	err := chunkConflict("Site.1", "total=2")
	if !errors.Is(err, ErrChunkCheckpointConflict) || ChunkConflictSite(err) != "Site.1" {
		t.Fatalf("err = %v", err)
	}
	if ChunkConflictSite(ErrChunkCheckpointConflict) != "" || ChunkConflictSite(NewChunkStoreError(errors.New("x"))) != "" {
		t.Fatal("the bare sentinel and a store error carry no conflict site")
	}
	if want := ErrChunkCheckpointConflict.Error() + ": Site.1 (total=2)"; err.Error() != want {
		t.Fatalf("Error() = %q, want %q", err.Error(), want)
	}
}

func TestProviderCursorConflictsCarrySites(t *testing.T) {
	huge := strings.Repeat("a", maxChunkCursorBytes+1)
	_, githubDecode := decodeGitHubTestsChunkCursor("{not json")
	requireConflict(t, githubDecode, "decodeGitHubTestsChunkCursor.1", "raw_bytes=9", "phase_known=false")
	_, gitlabDecode := decodeGitLabTestsChunkCursor("{not json")
	requireConflict(t, gitlabDecode, "decodeGitLabTestsChunkCursor.1", "raw_bytes=9", "phase_known=false")
	_, gitlabDuplicate := decodeGitLabTestsChunkCursor(`{"phase":"pipelines","truncated":["pipelines","pipelines"]}`)
	if ChunkConflictSite(gitlabDuplicate) == "" || !errors.Is(gitlabDuplicate, ErrChunkCheckpointConflict) {
		t.Fatalf("a duplicate truncated component must carry a site: %v", gitlabDuplicate)
	}
	_, githubEncode := encodeGitHubTestsChunkCursor(githubTestsChunkCursor{Phase: "runs", NextURL: huge})
	requireConflict(t, githubEncode, "encodeGitHubTestsChunkCursor.1", "marshal_err=false", "max=4096")
	_, gitlabEncode := encodeGitLabTestsChunkCursor(gitLabTestsChunkCursor{Phase: huge})
	requireConflict(t, gitlabEncode, "encodeGitLabTestsChunkCursor.1", "marshal_err=false", "max=4096")
}

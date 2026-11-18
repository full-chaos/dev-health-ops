package providersync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"testing"
	"time"
)

// traceChunkStore records the streaming executor's checkpoint store calls. The first commit mark of ordinal 0 lands
// and then fails (a lost acknowledgement).
type traceChunkStore struct {
	*chunkMemoryStore
	trace   []string
	faulted bool
}

func (s *traceChunkStore) LoadChunkCheckpoint(ctx context.Context, claim Claim, now time.Time) (ChunkCheckpoint, error) {
	s.trace = append(s.trace, "load")
	return s.chunkMemoryStore.LoadChunkCheckpoint(ctx, claim, now)
}

func (s *traceChunkStore) LoadPreparedChunk(ctx context.Context, claim Claim, ordinal int, now time.Time) (PreparedProviderChunk, error) {
	s.trace = append(s.trace, fmt.Sprintf("load-prepared %d", ordinal))
	return s.chunkMemoryStore.LoadPreparedChunk(ctx, claim, ordinal, now)
}

func (s *traceChunkStore) PrepareChunkGroup(ctx context.Context, claim Claim, chunks []PreparedProviderChunk, now time.Time) ([]PreparedProviderChunk, error) {
	s.trace = append(s.trace, fmt.Sprintf("prepare %d+%d", chunks[0].Ordinal, len(chunks)))
	return s.chunkMemoryStore.PrepareChunkGroup(ctx, claim, chunks, now)
}

func (s *traceChunkStore) MarkChunkCommitted(ctx context.Context, claim Claim, ordinal int, digest string, now time.Time) error {
	err := s.chunkMemoryStore.MarkChunkCommitted(ctx, claim, ordinal, digest, now)
	if err == nil && !s.faulted && ordinal == 0 {
		s.faulted = true
		s.trace = append(s.trace, "mark 0 lost-ack")
		return io.ErrUnexpectedEOF
	}
	s.trace = append(s.trace, fmt.Sprintf("mark %d err=%v", ordinal, err))
	return err
}

// A route without a commit observer runs the streaming executor exactly as before the observer existed: a failed commit
// mark returns at once, with no checkpoint read back, and the retry resumes past the landed chunk. The expected trace,
// result and stream calls are the ones origin/main 92b57ce3 produces for this cell.
func TestStreamingExecutorWithoutCommitObserverKeepsItsStoreCalls(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSessionFor(t, now, false, "github", "cicd")
	descriptor, ok := Descriptor("github", "cicd")
	if !ok {
		t.Fatal("descriptor missing")
	}
	descriptor.ChunkPolicy = ChunkPolicy{
		MaxSourceItems: 2, MaxEffectRows: 2, MaxPreparedBytes: 64 << 10,
		MaxChunksPerAttempt: 8, MaxWallTime: time.Minute,
	}
	handler := &streamingChunkRouteHandler{batch: chunkedTestBatch(t, claim)}
	store := &traceChunkStore{chunkMemoryStore: newChunkMemoryStore()}
	sink := &memoryEffectSink{}
	executor := completeRouteExecutor(now, handler, store, sink)
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
	if _, err := executor.Execute(context.Background(), session, descriptor); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("first attempt err=%v, want the lost acknowledgement", err)
	}
	firstAttempt := len(store.trace)
	result, err := executor.Execute(context.Background(), session, descriptor)
	if err != nil {
		t.Fatalf("retry err=%v", err)
	}
	t.Logf("trace=%q first_attempt=%d calls=%d cursor=%q complete=%v rows=%d", store.trace, firstAttempt, handler.calls, handler.lastCursor, result.Result["complete"], result.CommittedRows)
	want := []string{
		"load", "load", "prepare 0+1", "mark 0 lost-ack",
		"load", "load", "load-prepared 0", "prepare 1+1", "mark 1 err=<nil>", "prepare 2+1", "mark 2 err=<nil>", "load", "load", "load-prepared 2",
	}
	if !slices.Equal(store.trace, want) || handler.calls != 2 || result.Result["complete"] != true {
		t.Fatalf("trace %q calls=%d result=%v, want trace %q, 2 stream calls and a complete result", store.trace, handler.calls, result.Result, want)
	}
}

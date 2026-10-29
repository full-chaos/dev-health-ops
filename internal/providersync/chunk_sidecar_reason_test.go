package providersync

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// CHAOS-11772: a refused stored sidecar says WHICH check refused it (a fixed vocabulary, no row data); the
// payload_bytes mismatch carries both numbers. Every refusal stays the bare, deterministic checkpoint conflict.
func TestStoredSidecarRefusalsNameTheirReason(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		reason string
		break_ func(row *inProcessChunkRow)
	}{
		{"status", func(row *inProcessChunkRow) { row.status = "bogus" }},
		{"schema_version", func(row *inProcessChunkRow) { row.schemaVersion = "v0" }},
		{"route_version", func(row *inProcessChunkRow) { row.routeVersion = "other" }},
		{"ordinal", func(row *inProcessChunkRow) { row.ordinal = 7 }},
		{"cursor_before", func(row *inProcessChunkRow) { row.cursorBefore = "x" }},
		{"cursor_after", func(row *inProcessChunkRow) { row.cursorAfter = "x" }},
		{"inventory_complete", func(row *inProcessChunkRow) { row.inventoryComplete = !row.inventoryComplete }},
		{"manifest_digest", func(row *inProcessChunkRow) { row.manifestDigest = strings.Repeat("0", 64) }},
		{"payload_decode", func(row *inProcessChunkRow) { row.payloadRaw = []byte("{") }},
		{"ledger_decode", func(row *inProcessChunkRow) { row.ledgerRaw = []byte("{") }},
		{"total_chunks", func(row *inProcessChunkRow) { row.totalChunks = -1 }},
		{"ledger_invalid", func(row *inProcessChunkRow) { row.ledgerRaw = []byte("{}") }},
	}
	for _, c := range cases {
		t.Run(c.reason, func(t *testing.T) {
			claim := nativeTestClaim("github", "cicd")
			store := NewInProcessChunkLedger(claim)
			if _, err := store.PrepareChunk(ctx, claim, paddedChunk(t, 10), now); err != nil {
				t.Fatal(err)
			}
			c.break_(store.chunks[0])
			_, err := store.LoadPreparedChunk(ctx, claim, 0, now)
			var storeErr *ChunkStoreError
			if !errors.Is(err, ErrChunkCheckpointConflict) || errors.As(err, &storeErr) {
				t.Fatalf("err = %v: want the bare checkpoint conflict", err)
			}
			if !strings.Contains(err.Error(), "("+c.reason+")") {
				t.Fatalf("err = %q: want the reason %q", err, c.reason)
			}
		})
	}
}

func TestPayloadBytesMismatchNamesBothNumbersAndNothingElse(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("github", "cicd")
	store := NewInProcessChunkLedger(claim)
	if _, err := store.PrepareChunk(ctx, claim, paddedChunk(t, 10), now); err != nil {
		t.Fatal(err)
	}
	store.chunks[0].payloadBytes += 1
	stored := store.chunks[0].payloadBytes
	_, err := store.LoadPreparedChunk(ctx, claim, 0, now)
	var mismatch *ChunkSidecarMismatchError
	if !errors.As(err, &mismatch) || mismatch.Field != "payload_bytes" || mismatch.Stored != stored || mismatch.Payload != stored-1 {
		t.Fatalf("err = %v (%+v)", err, mismatch)
	}
	want := fmt.Sprintf("payload_bytes mismatch (column=%d payload=%d)", stored, stored-1)
	if !strings.Contains(err.Error(), want) || strings.Contains(err.Error(), "org-acme") {
		t.Fatalf("err = %q: want %q and no row data", err, want)
	}
	var storeErr *ChunkStoreError
	if !errors.Is(err, ErrChunkCheckpointConflict) || errors.As(err, &storeErr) {
		t.Fatalf("err = %v: want the bare checkpoint conflict", err)
	}
}

func TestPreparedChunkRefusalsNameTheirReason(t *testing.T) {
	claim := nativeTestClaim("github", "cicd")
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	chunk := paddedChunk(t, 10)
	chunk.SchemaVersion = "v0"
	_, err := buildPreparedChunkMaterial(claim, chunk, now)
	if !errors.Is(err, ErrChunkCheckpointConflict) || !strings.Contains(err.Error(), "(chunk_invalid)") {
		t.Fatalf("err = %v: want a chunk_invalid conflict", err)
	}
}

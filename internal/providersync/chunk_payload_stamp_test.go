package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CHAOS-11772: payload_bytes is a number inside the sidecar JSON, so the stamp has to be a fixed point:
// embedded == len(encoded). The legacy loop stopped after three passes and, for lengths a few bytes above a power
// of ten, returned bytes embedding len-1 while the column held len; loadPreparedChunkRow then refused the chunk.

// legacyEncodedPayload is the loop as it was before the fix, kept as the reference for byte identity.
func legacyEncodedPayload(chunk PreparedProviderChunk) []byte {
	chunk.Ledger = EffectLedgerState{}
	chunk.PayloadBytes = 0
	var encoded []byte
	for attempt := 0; attempt < 3; attempt++ {
		encoded, _ = json.Marshal(chunk)
		if chunk.PayloadBytes == len(encoded) {
			return encoded
		}
		chunk.PayloadBytes = len(encoded)
	}
	return encoded
}

func embeddedPayloadBytes(t *testing.T, encoded []byte) int {
	t.Helper()
	var probe struct {
		PayloadBytes int `json:"payload_bytes"`
	}
	if err := json.Unmarshal(encoded, &probe); err != nil {
		t.Fatal(err)
	}
	return probe.PayloadBytes
}

// paddedChunk is a prepared chunk whose one row carries pad bytes of filler, so its encoded length moves one byte
// per pad byte.
func paddedChunk(t *testing.T, pad int) PreparedProviderChunk {
	t.Helper()
	row := `{"org_id":"org-acme","run_id":"1","pad":"` + strings.Repeat("x", pad) + `"}`
	chunk := PreparedProviderChunk{
		SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion,
		Ordinal: 0, CursorAfter: "c1",
		Effects: []EffectBatch{effectBatchFixture(t, "ci_pipeline_runs", EffectReadbackRequired, row)},
	}
	chunk.ManifestDigest = preparedChunkDigest(chunk)
	return chunk
}

// chunkEndingAt returns a chunk whose legacy loop ends with exactly target bytes (the stored payload_bytes column of
// the old binary), or fails the test when no filler length reaches it.
func chunkEndingAt(t *testing.T, target int) PreparedProviderChunk {
	t.Helper()
	base := len(legacyEncodedPayload(paddedChunk(t, 0)))
	for pad := max(0, target-base-12); pad <= target-base+3; pad++ {
		chunk := paddedChunk(t, pad)
		if len(legacyEncodedPayload(chunk)) == target {
			return chunk
		}
	}
	t.Fatalf("no filler length ends the legacy loop at %d bytes", target)
	return PreparedProviderChunk{}
}

// the lengths at which the legacy loop does not converge (the arithmetic is in TestStampPayloadBytesConverges...).
var badStampLengths = []int{1001, 1002, 10001, 10002, 10003, 100001, 100002, 100003, 100004,
	1000001, 1000002, 1000003, 1000004, 1000005}

func TestEncodedPreparedChunkPayloadEmbedsItsOwnLength(t *testing.T) {
	for _, target := range append([]int{999, 1003, 9999, 10004, 99999, 100005}, badStampLengths...) {
		chunk := chunkEndingAt(t, target)
		encoded, err := encodedPreparedChunkPayload(chunk)
		if err != nil {
			t.Fatalf("length %d: %v", target, err)
		}
		if got := embeddedPayloadBytes(t, encoded); got != len(encoded) {
			t.Errorf("length %d: the bytes embed payload_bytes=%d but are %d bytes long", target, got, len(encoded))
		}
	}
}

func digitsOf(value int) int { return len(strconv.Itoa(value)) }

func TestStampPayloadBytesConvergesForEveryLength(t *testing.T) {
	// A sidecar of B0 bytes with payload_bytes=0 has B0-1 bytes plus the digits of the stamp.
	for withZero := 2; withZero <= 2_200_000; withZero++ {
		rest := withZero - 1
		measure := func(stamp int) int { return rest + digitsOf(stamp) }
		stamp, converged := stampPayloadBytes(measure)
		if !converged || measure(stamp) != stamp {
			t.Fatalf("a sidecar of %d bytes with a zero stamp: stamp %d converged=%v measures %d", withZero, stamp, converged, measure(stamp))
		}
		if digitsOf(stamp) == 0 {
			t.Fatal("unreachable")
		}
	}
}

func TestStampPayloadBytesKeepsTheLegacyResultWhereTheLegacyLoopConverged(t *testing.T) {
	legacy := func(measure func(int) int) (int, bool) {
		stamp := 0
		for pass := 0; pass < 3; pass++ {
			length := measure(stamp)
			if length == stamp {
				return stamp, true
			}
			stamp = length
		}
		return stamp, false
	}
	different := 0
	for withZero := 2; withZero <= 2_200_000; withZero++ {
		rest := withZero - 1
		measure := func(stamp int) int { return rest + digitsOf(stamp) }
		want, legacyConverged := legacy(measure)
		got, _ := stampPayloadBytes(measure)
		if legacyConverged && got != want {
			t.Fatalf("a sidecar of %d bytes: stamp %d, the legacy loop converged to %d", withZero, got, want)
		}
		if !legacyConverged {
			different++
		}
	}
	if different != 15 {
		t.Fatalf("the legacy loop failed for %d lengths up to 2.2e6, the arithmetic says 15", different)
	}
}

func TestStampPayloadBytesRefusesAStampThatNeverSettles(t *testing.T) {
	if _, converged := stampPayloadBytes(func(stamp int) int { return stamp + 1 }); converged {
		t.Fatal("a measure that never settles reported convergence")
	}
}

func TestEncodedPreparedChunkPayloadIsByteIdenticalWhereTheLegacyLoopConverged(t *testing.T) {
	bad := map[int]bool{}
	for _, length := range badStampLengths {
		bad[length] = true
	}
	for pad := 0; pad < 3000; pad += 7 {
		chunk := paddedChunk(t, pad)
		old := legacyEncodedPayload(chunk)
		if bad[len(old)] {
			continue
		}
		got, err := encodedPreparedChunkPayload(chunk)
		if err != nil || !bytes.Equal(got, old) {
			t.Fatalf("pad %d (%d bytes): the encoding changed or failed: %v", pad, len(old), err)
		}
	}
}

func TestPreparedChunkSurvivesTheStoreRoundTripAtTheCrossings(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	for _, target := range []int{1001, 1002, 10001, 10002, 10003, 100001, 100004} {
		claim := nativeTestClaim("github", "cicd")
		store := NewInProcessChunkLedger(claim)
		chunk := chunkEndingAt(t, target)
		prepared, err := store.PrepareChunk(ctx, claim, chunk, now)
		if err != nil {
			t.Fatalf("length %d: prepare: %v", target, err)
		}
		if err := store.BeginChunkEffect(ctx, claim, 0, 0, prepared.Effects[0].ContentDigest, now); err != nil {
			t.Errorf("length %d: the first begin of the chunk failed: %v", target, err)
		}
		if _, err := store.LoadPreparedChunk(ctx, claim, 0, now); err != nil {
			t.Errorf("length %d: loading the prepared chunk failed: %v", target, err)
		}
	}
}

func TestNonConvergenceIsAPolicyRefusalNeverACheckpointConflict(t *testing.T) {
	if errors.Is(errChunkPayloadStamp, ErrChunkCheckpointConflict) || !errors.Is(errChunkPayloadStamp, ErrChunkPolicyExceeded) {
		t.Fatalf("errChunkPayloadStamp = %v: want ErrChunkPolicyExceeded and not ErrChunkCheckpointConflict", errChunkPayloadStamp)
	}
	var storeErr *ChunkStoreError
	if errors.As(errChunkPayloadStamp, &storeErr) {
		t.Fatal("the stamp refusal is a ChunkStoreError")
	}
}

// paddedEmissionHandler emits one non-final emission whose first row carries pad bytes, then the final metadata.
type paddedEmissionHandler struct{ pad int }

func (paddedEmissionHandler) Collect(
	context.Context, Claim, providerfoundation.Credential, *providerfoundation.HTTPClient, time.Time,
) (CompleteRouteBatch, error) {
	return CompleteRouteBatch{}, ErrInvalidConfiguration
}

func (handler paddedEmissionHandler) CollectChunks(
	_ context.Context, claim Claim, _ providerfoundation.Credential, _ *providerfoundation.HTTPClient,
	_ time.Time, resumeCursor string, emit func(ChunkRouteEmission) error,
) error {
	destinations := []string{
		"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks",
		"test_suite_results", "test_case_results", "coverage_snapshots",
	}
	effects := make([]EffectBatch, 0, len(destinations))
	for index, destination := range destinations {
		pad := ""
		if index == 0 {
			pad = strings.Repeat("x", handler.pad)
		}
		effect, err := BuildEffectBatch(destination, EffectReadbackRequired, []json.RawMessage{
			json.RawMessage(`{"org_id":"` + claim.OrgID + `","id":1,"pad":"` + pad + `"}`),
		})
		if err != nil {
			return err
		}
		effects = append(effects, effect)
	}
	if resumeCursor == "" {
		if err := emit(ChunkRouteEmission{
			Batch: CompleteRouteBatch{Effects: effects}, CursorBefore: `{"page":0}`, CursorAfter: `{"page":1}`,
		}); err != nil {
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

func runPaddedEmission(t *testing.T, pad int) (storedPayloadBytes int, err error) {
	t.Helper()
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSessionFor(t, now, false, "github", "cicd")
	descriptor, ok := Descriptor("github", "cicd")
	if !ok || !descriptor.Chunked {
		t.Fatalf("descriptor=%+v ok=%t", descriptor, ok)
	}
	descriptor.ChunkPolicy = DefaultChunkPolicy()
	store := NewInProcessChunkLedger(claim)
	executor := completeRouteExecutor(now, paddedEmissionHandler{pad: pad}, store, &memoryEffectSink{})
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
	_, err = executor.Execute(context.Background(), session, descriptor)
	if chunk, loadErr := store.LoadPreparedChunk(context.Background(), claim, 0, now); loadErr == nil {
		storedPayloadBytes = chunk.PayloadBytes
	}
	return storedPayloadBytes, err
}

// The streaming executor prepares and commits an emission whose sidecar length lands on each crossing; before the
// fix the first BeginChunkEffect refused it with the bare checkpoint conflict.
func TestStreamingExecutorFinishesAnEmissionWhoseSidecarLengthCrossesAPowerOfTen(t *testing.T) {
	zeroPad, err := runPaddedEmission(t, 0)
	if err != nil || zeroPad < 1 {
		t.Fatalf("the unpadded emission: payload_bytes=%d err=%v", zeroPad, err)
	}
	for _, target := range []int{1001, 10001, 100001} {
		for pad := target - zeroPad - 8; pad <= target-zeroPad+8; pad++ {
			if pad < 0 {
				continue
			}
			if _, err := runPaddedEmission(t, pad); err != nil {
				t.Errorf("pad %d (sidecar about %d bytes): %v", pad, zeroPad+pad, err)
			}
		}
	}
}

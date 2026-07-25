package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestEffectCommitterRecoversCrashWindowsByDestinationPolicy(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("launchdarkly", "feature-flags")
	batches := []EffectBatch{
		effectBatchFixture(t, "feature_flag", EffectReplaySafe, `{"flag_key":"checkout"}`),
		effectBatchFixture(t, "feature_flag_event", EffectReadbackRequired, `{"dedupe_key":"event-1"}`),
		effectBatchFixture(t, "work_graph_edges", EffectReplaySafe, `{"edge_id":"edge-1"}`),
	}
	ledger := &memoryEffectLedger{}
	crash := errors.New("simulated process exit after durable write")
	firstSink := &memoryEffectSink{failAfterWrite: "feature_flag_event", failure: crash}
	_, err := (EffectCommitter{
		Ledger: ledger, Sink: firstSink, Now: func() time.Time { return now },
	}).Commit(context.Background(), claim, batches)
	if !errors.Is(err, crash) {
		t.Fatalf("first commit error=%v", err)
	}
	if ledger.state.Effects[0].Status != GenerationBlockCommitted ||
		ledger.state.Effects[1].Status != GenerationBlockWriting ||
		ledger.state.Effects[2].Status != GenerationBlockPending {
		t.Fatalf("crash ledger=%+v", ledger.state.Effects)
	}

	freshSink := &memoryEffectSink{}
	result, err := (EffectCommitter{
		Ledger: ledger,
		Sink:   freshSink,
		Readback: staticEffectReadback{
			inspections: map[string]EffectInspection{"feature_flag_event": EffectExact},
		},
		Now: func() time.Time { return now.Add(time.Minute) },
	}).Commit(context.Background(), claim, batches)
	if err != nil {
		t.Fatal(err)
	}
	if result != (EffectCommitResult{
		Written: 1, Skipped: 1, MarkedCommitted: 1,
	}) {
		t.Fatalf("recovery result=%+v", result)
	}
	if len(freshSink.destinations) != 1 ||
		freshSink.destinations[0] != "work_graph_edges" {
		t.Fatalf("fresh writes=%v", freshSink.destinations)
	}
	for _, effect := range ledger.state.Effects {
		if effect.Status != GenerationBlockCommitted {
			t.Fatalf("recovered effect=%+v", effect)
		}
	}
}

func TestEffectCommitterResetsAbsentAndReplaysProvenSafeEffects(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("linear", "work-items")
	for _, test := range []struct {
		name       string
		recovery   EffectRecoveryPolicy
		readback   EffectReadback
		wantResult EffectCommitResult
	}{
		{
			name:     "exact readback reports absent",
			recovery: EffectReadbackRequired,
			readback: staticEffectReadback{
				inspections: map[string]EffectInspection{"work_items": EffectAbsent},
			},
			wantResult: EffectCommitResult{Written: 1, ResetForReplay: 1},
		},
		{
			name:       "proven safe effect replays without readback",
			recovery:   EffectReplaySafe,
			wantResult: EffectCommitResult{Written: 1, IdempotentReplay: 1},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			batch := effectBatchFixture(
				t, "work_items", test.recovery, `{"work_item_id":"linear:ENG-1"}`,
			)
			state, err := NewEffectLedgerState(claim, []EffectBatch{batch}, now)
			if err != nil {
				t.Fatal(err)
			}
			state.Effects[0].Status = GenerationBlockWriting
			state.Effects[0].StartedAt = &now
			ledger := &memoryEffectLedger{state: state}
			sink := &memoryEffectSink{}
			result, err := (EffectCommitter{
				Ledger: ledger, Sink: sink, Readback: test.readback,
				Now: func() time.Time { return now.Add(time.Minute) },
			}).Commit(context.Background(), claim, []EffectBatch{batch})
			if err != nil || result != test.wantResult ||
				len(sink.destinations) != 1 ||
				ledger.state.Effects[0].Status != GenerationBlockCommitted {
				t.Fatalf(
					"result=%+v error=%v writes=%v state=%+v",
					result, err, sink.destinations, ledger.state,
				)
			}
		})
	}
}

func TestEffectCommitterFailsClosedForAmbiguousOrDriftedRecovery(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("launchdarkly", "feature-flags")
	for _, test := range []struct {
		name     string
		recovery EffectRecoveryPolicy
		readback EffectReadback
		want     error
	}{
		{
			name:     "readback conflict",
			recovery: EffectReadbackRequired,
			readback: staticEffectReadback{
				inspections: map[string]EffectInspection{"feature_flag_event": EffectConflict},
			},
			want: ErrEffectRecoveryAmbiguous,
		},
		{
			name:     "missing required readback",
			recovery: EffectReadbackRequired,
			want:     ErrEffectRecoveryAmbiguous,
		},
		{
			name:     "blocked recovery",
			recovery: EffectRecoveryBlocked,
			want:     ErrEffectRecoveryAmbiguous,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			batch := effectBatchFixture(
				t, "feature_flag_event", test.recovery, `{"dedupe_key":"event-1"}`,
			)
			state, err := NewEffectLedgerState(claim, []EffectBatch{batch}, now)
			if err != nil {
				t.Fatal(err)
			}
			state.Effects[0].Status = GenerationBlockWriting
			state.Effects[0].StartedAt = &now
			ledger := &memoryEffectLedger{state: state}
			sink := &memoryEffectSink{}
			_, err = (EffectCommitter{
				Ledger: ledger, Sink: sink, Readback: test.readback,
				Now: func() time.Time { return now.Add(time.Minute) },
			}).Commit(context.Background(), claim, []EffectBatch{batch})
			if !errors.Is(err, test.want) || len(sink.destinations) != 0 ||
				ledger.state.Effects[0].Status != GenerationBlockWriting {
				t.Fatalf("error=%v writes=%v state=%+v", err, sink.destinations, ledger.state)
			}
		})
	}

	oldBatch := effectBatchFixture(
		t, "feature_flag_event", EffectReadbackRequired, `{"dedupe_key":"old"}`,
	)
	oldState, err := NewEffectLedgerState(claim, []EffectBatch{oldBatch}, now)
	if err != nil {
		t.Fatal(err)
	}
	ledger := &memoryEffectLedger{state: oldState}
	newBatch := effectBatchFixture(
		t, "feature_flag_event", EffectReadbackRequired, `{"dedupe_key":"new"}`,
	)
	_, err = (EffectCommitter{
		Ledger: ledger, Sink: &memoryEffectSink{},
		Now: func() time.Time { return now.Add(time.Minute) },
	}).Commit(context.Background(), claim, []EffectBatch{newBatch})
	if !errors.Is(err, ErrEffectLedgerConflict) {
		t.Fatalf("drifted manifest error=%v", err)
	}
}

func TestEffectLedgerPersistsOnlyBoundedManifestMetadata(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("linear", "work-items")
	batch := effectBatchFixture(
		t,
		"work_items",
		EffectReplaySafe,
		`{"work_item_id":"linear:ENG-1","title":"payload-must-not-be-persisted"}`,
	)
	state, err := NewEffectLedgerState(claim, []EffectBatch{batch}, now)
	if err != nil {
		t.Fatal(err)
	}
	encoded := encodeEffectLedgerState(state)
	if len(encoded) == 0 || len(encoded) > maxEffectLedgerStateBytes {
		t.Fatalf("encoded manifest bytes=%d", len(encoded))
	}
	if json.Valid(encoded) == false ||
		containsBytes(encoded, []byte("payload-must-not-be-persisted")) {
		t.Fatalf("manifest leaked payload: %s", encoded)
	}
}

func effectBatchFixture(
	t *testing.T,
	destination string,
	recovery EffectRecoveryPolicy,
	rows ...string,
) EffectBatch {
	t.Helper()
	raw := make([]json.RawMessage, len(rows))
	for index, row := range rows {
		raw[index] = json.RawMessage(row)
	}
	batch, err := BuildEffectBatch(destination, recovery, raw)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func containsBytes(value, part []byte) bool {
	for index := 0; index+len(part) <= len(value); index++ {
		if string(value[index:index+len(part)]) == string(part) {
			return true
		}
	}
	return false
}

type memoryEffectLedger struct {
	mu    sync.Mutex
	state EffectLedgerState
}

func (ledger *memoryEffectLedger) LoadEffects(
	context.Context,
	Claim,
	time.Time,
) (EffectLedgerState, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.state.SchemaVersion == "" {
		return EffectLedgerState{}, ErrEffectLedgerConflict
	}
	return ledger.state, nil
}

func (ledger *memoryEffectLedger) PrepareEffects(
	_ context.Context,
	_ Claim,
	desired EffectLedgerState,
	_ time.Time,
) (EffectLedgerState, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.state.SchemaVersion == "" {
		ledger.state = desired
		return ledger.state, nil
	}
	if !sameEffectManifest(ledger.state, desired) {
		return EffectLedgerState{}, ErrEffectLedgerConflict
	}
	return ledger.state, nil
}

func (ledger *memoryEffectLedger) BeginEffect(
	_ context.Context,
	_ Claim,
	index int,
	digest string,
	now time.Time,
) error {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	effect := &ledger.state.Effects[index]
	if effect.ContentDigest != digest || effect.Status != GenerationBlockPending {
		return ErrEffectLedgerConflict
	}
	effect.Status = GenerationBlockWriting
	now = now.UTC()
	effect.StartedAt = &now
	return nil
}

func (ledger *memoryEffectLedger) CommitEffect(
	_ context.Context,
	_ Claim,
	index int,
	digest string,
	now time.Time,
) error {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	effect := &ledger.state.Effects[index]
	if effect.ContentDigest != digest || effect.Status != GenerationBlockWriting {
		return ErrEffectLedgerConflict
	}
	effect.Status = GenerationBlockCommitted
	now = now.UTC()
	effect.CommittedAt = &now
	return nil
}

func (ledger *memoryEffectLedger) ResolveEffect(
	_ context.Context,
	_ Claim,
	index int,
	digest string,
	resolution GenerationBlockResolution,
	now time.Time,
) error {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	effect := &ledger.state.Effects[index]
	if effect.ContentDigest != digest || effect.Status != GenerationBlockWriting {
		return ErrEffectLedgerConflict
	}
	now = now.UTC()
	switch resolution {
	case GenerationBlockMarkCommitted:
		effect.Status = GenerationBlockCommitted
		effect.CommittedAt = &now
	case GenerationBlockRetryPending:
		effect.Status = GenerationBlockPending
		effect.StartedAt = nil
	default:
		return ErrInvalidConfiguration
	}
	return nil
}

type memoryEffectSink struct {
	mu             sync.Mutex
	destinations   []string
	failAfterWrite string
	failure        error
}

func (sink *memoryEffectSink) WriteEffect(
	_ context.Context,
	_ Claim,
	batch EffectBatch,
) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.destinations = append(sink.destinations, batch.Destination)
	if batch.Destination == sink.failAfterWrite {
		return sink.failure
	}
	return nil
}

type staticEffectReadback struct {
	inspections map[string]EffectInspection
	err         error
}

func (readback staticEffectReadback) InspectEffect(
	_ context.Context,
	_ Claim,
	batch EffectBatch,
) (EffectInspection, error) {
	return readback.inspections[batch.Destination], readback.err
}

var _ EffectLedger = (*memoryEffectLedger)(nil)
var _ EffectSink = (*memoryEffectSink)(nil)
var _ EffectReadback = staticEffectReadback{}

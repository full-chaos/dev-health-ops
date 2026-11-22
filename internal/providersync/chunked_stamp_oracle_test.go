package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// The stored chunk of an emission is measured by the split with the stamps the executor writes and the store's own refusal (CHAOS-14411).
// MAIN is the oracle: a frozen copy of main's split (100-row cap) and main's executor stamping, measured by the store's material builder at
// the same instant. The product executor runs the same emissions against the in-process chunk store (the store's material builder). Every
// shape: refused only where MAIN refuses; every row once, in canonical order, verdict rows after the other rows; data chunks per emission
// never above MAIN's.

// oracleMainSplit is main's splitCompleteRouteBatch at d88f200db793, copied verbatim (renamed).
func oracleMainSplit(
	batch CompleteRouteBatch,
	comparison ShadowComparison,
	policy ChunkPolicy,
) ([]PreparedProviderChunk, error) {
	if policy.Validate() != nil || len(batch.Effects) == 0 {
		return nil, ErrInvalidConfiguration
	}
	ordered := append([]EffectBatch(nil), batch.Effects...)
	sortEffectBatches(ordered)
	maxRows := min(policy.MaxEffectRows, policy.MaxSourceItems)
	for maxRows >= 1 {
		count := 1
		for _, effect := range ordered {
			if rows := (len(effect.Rows) + maxRows - 1) / maxRows; rows > count {
				count = rows
			}
		}
		chunks := make([]PreparedProviderChunk, 0, count)
		valid := true
		for ordinal := 0; ordinal < count; ordinal++ {
			effects := make([]EffectBatch, 0, len(ordered))
			for _, effect := range ordered {
				shift := 0
				if effectSplitsLast(effect.Destination) {
					shift = count - (len(effect.Rows)+maxRows-1)/maxRows
				}
				start := (ordinal - shift) * maxRows
				if ordinal < shift || start > len(effect.Rows) {
					start = len(effect.Rows)
				}
				end := start + maxRows
				if end > len(effect.Rows) {
					end = len(effect.Rows)
				}
				part, err := BuildEffectBatch(effect.Destination, effect.Recovery, effect.Rows[start:end])
				if err != nil {
					return nil, err
				}
				effects = append(effects, part)
			}
			payloadBytes := 0
			for _, effect := range effects {
				payloadBytes += effect.PayloadBytes
			}
			if payloadBytes > policy.MaxPreparedBytes {
				valid = false
				break
			}
			chunk := PreparedProviderChunk{
				SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion,
				Ordinal: ordinal, TotalChunks: count, Effects: effects,
				InventoryComplete: ordinal == count-1, PayloadBytes: payloadBytes,
			}
			if chunk.InventoryComplete {
				chunk.Result = cloneChunkResult(batch.Result)
				chunk.Watermark = batch.Watermark
				chunk.Evidence = batch.Evidence
				chunk.Comparison = comparison
				chunk.WorklogObservations = append([]JiraWorklogFetchObservation(nil), batch.WorklogObservations...)
			}
			encoded, encodeErr := encodedPreparedChunkPayload(chunk)
			if encodeErr != nil {
				return nil, encodeErr
			}
			chunk.PayloadBytes = len(encoded)
			if chunk.PayloadBytes > policy.MaxPreparedBytes {
				valid = false
				break
			}
			chunk.ManifestDigest = preparedChunkDigest(chunk)
			chunks = append(chunks, chunk)
		}
		if valid {
			return chunks, nil
		}
		if maxRows == 1 {
			break
		}
		maxRows /= 2
	}
	return nil, ErrChunkPolicyExceeded
}

// oracleMainStamp is main's prepareEmission stamping after the split (chunked_stream_executor.go:195-221 at d88f200db793).
func oracleMainStamp(chunks []PreparedProviderChunk, emission ChunkRouteEmission, batch CompleteRouteBatch,
	comparison ShadowComparison, nextOrdinal int) ([]PreparedProviderChunk, error) {
	for index := range chunks {
		chunks[index].TotalChunks, chunks[index].CursorBefore, chunks[index].CursorAfter = 0, "", ""
		chunks[index].InventoryComplete, chunks[index].Result, chunks[index].Watermark = false, nil, nil
		chunks[index].Evidence, chunks[index].Comparison, chunks[index].WorklogObservations = FetchEvidence{}, ShadowComparison{}, nil
	}
	last := len(chunks) - 1
	chunks[0].CursorBefore, chunks[last].CursorAfter = emission.CursorBefore, emission.CursorAfter
	if emission.Final {
		chunks[last].InventoryComplete = true
		chunks[last].Result = cloneChunkResult(batch.Result)
		chunks[last].Watermark = batch.Watermark
		chunks[last].Evidence = batch.Evidence
		chunks[last].Comparison = comparison
		chunks[last].WorklogObservations = append([]JiraWorklogFetchObservation(nil), batch.WorklogObservations...)
	}
	for index := range chunks {
		encoded, err := encodedPreparedChunkPayload(chunks[index])
		if err != nil {
			return nil, err
		}
		chunks[index].PayloadBytes = len(encoded)
		chunks[index].Ordinal = nextOrdinal + index
		chunks[index].ManifestDigest = preparedChunkDigest(chunks[index])
	}
	return chunks, nil
}

type oracleEmission struct {
	chunks []PreparedProviderChunk
	err    error
}

// oracleMain runs the emissions through MAIN at one instant: the chunks main's executor hands its store, and the store's verdict.
func oracleMain(t *testing.T, claim Claim, emissions []ChunkRouteEmission, at time.Time) []oracleEmission {
	t.Helper()
	outcomes := make([]oracleEmission, 0, len(emissions))
	next := 0
	for _, emission := range emissions {
		batch, comparison := oracleEmissionBatch(t, claim, emission)
		chunks, err := oracleMainSplit(batch, comparison, DefaultChunkPolicy())
		if err == nil {
			chunks, err = oracleMainStamp(chunks, emission, batch, comparison, next)
		}
		for index := 0; err == nil && index < len(chunks); index++ {
			_, err = buildPreparedChunkMaterial(claim, chunks[index], at)
		}
		outcomes = append(outcomes, oracleEmission{chunks: chunks, err: err})
		if err != nil {
			return outcomes
		}
		next += len(chunks)
	}
	return outcomes
}

// oracleStoredBytes is what the store keeps of a chunk: the sidecar without the ledger, and the ledger column, measured apart from the
// store's own builder (so it also measures a chunk the builder refuses).
func oracleStoredBytes(t *testing.T, claim Claim, chunk PreparedProviderChunk, at time.Time) (payload, ledger int) {
	t.Helper()
	chunk.Ledger = EffectLedgerState{}
	if chunk.ManifestDigest == "" {
		chunk.ManifestDigest = preparedChunkDigest(chunk)
	}
	encoded, err := encodedPreparedChunkPayload(chunk)
	if err != nil {
		t.Fatal(err)
	}
	state, err := NewEffectLedgerState(claim, chunk.Effects, at)
	if err != nil {
		t.Fatal(err)
	}
	return len(encoded), len(encodeEffectLedgerState(state))
}

var stampDestinations = []string{"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks", "test_suite_results",
	"test_case_results", "coverage_snapshots", testOpsRunReportCompletenessDestination}

// stampShape is one emission body of the seven TestOps destinations.
type stampShape struct {
	cases      int
	filled     bool
	basePad    int
	pads       map[int]int
	verdicts   int
	verdictPad int
}

func stampRow(destination string, index, pad int) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"org_id":"org-acme","d":%q,"i":"%08d","pad":"%s"}`, destination, index, strings.Repeat("x", pad)))
}

func (shape stampShape) effects(t *testing.T) []EffectBatch {
	t.Helper()
	counts := map[string]int{"test_case_results": shape.cases, testOpsRunReportCompletenessDestination: shape.verdicts}
	if shape.filled {
		counts["ci_pipeline_runs"], counts["ci_job_runs"] = 1, min(shape.cases, 50)
		counts["ci_acceptance_checks"], counts["test_suite_results"] = min(shape.cases, 10), min(shape.cases, 20)
		counts["coverage_snapshots"] = min(shape.cases, 5)
		if shape.verdicts == 0 {
			counts[testOpsRunReportCompletenessDestination] = 1
		}
	}
	effects := make([]EffectBatch, 0, len(stampDestinations))
	for _, destination := range stampDestinations {
		rows := make([]json.RawMessage, 0, counts[destination])
		for index := 0; index < counts[destination]; index++ {
			pad := 40
			switch destination {
			case "test_case_results":
				if shape.basePad > 0 {
					pad = shape.basePad
				}
				if big, ok := shape.pads[index]; ok {
					pad = big
				}
			case testOpsRunReportCompletenessDestination:
				pad = 40 + shape.verdictPad
			}
			rows = append(rows, stampRow(destination, index, pad))
		}
		effect, err := BuildEffectBatch(destination, EffectReadbackRequired, rows)
		if err != nil {
			t.Fatal(err)
		}
		effects = append(effects, effect)
	}
	return effects
}

func stampCursor(index, pad int) string {
	return fmt.Sprintf(`{"e":%d,"p":"%s"}`, index, strings.Repeat("&", pad))
}

// stampScript is the emissions of one unit: each body a non-final emission (or, finalRows, the rows ride on the final emission), then
// the final metadata emission with a result of resultPad bytes.
func stampScript(t *testing.T, bodies []stampShape, finalRows bool, cursorPad, resultPad int) []ChunkRouteEmission {
	t.Helper()
	emissions := make([]ChunkRouteEmission, 0, len(bodies)+1)
	cursor := ""
	for index, body := range bodies {
		after := stampCursor(index+1, cursorPad)
		emissions = append(emissions, ChunkRouteEmission{
			Batch: CompleteRouteBatch{Effects: body.effects(t)}, CursorBefore: cursor, CursorAfter: after,
		})
		cursor = after
	}
	result := map[string]any{"complete": true, "detail": strings.Repeat("r", resultPad)}
	if finalRows && len(emissions) > 0 {
		last := &emissions[len(emissions)-1]
		last.Final, last.Batch.Result = true, result
		return emissions
	}
	empty := stampShape{}.effects(t)
	return append(emissions, ChunkRouteEmission{
		Batch: CompleteRouteBatch{Effects: empty, Result: result}, CursorBefore: cursor, CursorAfter: cursor, Final: true,
	})
}

type stampScriptHandler struct {
	emissions []ChunkRouteEmission
	nows      *[]time.Time
}

func (stampScriptHandler) Collect(
	context.Context, Claim, providerfoundation.Credential, *providerfoundation.HTTPClient, time.Time,
) (CompleteRouteBatch, error) {
	return CompleteRouteBatch{}, ErrInvalidConfiguration
}

func (handler stampScriptHandler) CollectChunks(
	_ context.Context, _ Claim, _ providerfoundation.Credential, _ *providerfoundation.HTTPClient,
	now time.Time, resumeCursor string, emit func(ChunkRouteEmission) error,
) error {
	if handler.nows != nil {
		*handler.nows = append(*handler.nows, now)
	}
	start := -1
	if resumeCursor == "" {
		start = 0
	}
	for index, emission := range handler.emissions {
		if start < 0 && emission.CursorBefore == resumeCursor {
			start = index
			break
		}
	}
	if start < 0 {
		return ErrChunkCheckpointConflict
	}
	for _, emission := range handler.emissions[start:] {
		if err := emit(emission); err != nil {
			return err
		}
	}
	return nil
}

// stampRecordingStore is the in-process chunk store that also keeps every group it prepared, as it received it.
type stampRecordingStore struct {
	*InProcessChunkLedger
	mu        sync.Mutex
	groups    [][]PreparedProviderChunk
	stampAts  []time.Time
	leaseNows []time.Time
}

func (store *stampRecordingStore) recordGroup(chunks []PreparedProviderChunk, stampAt, leaseNow time.Time) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.groups = append(store.groups, append([]PreparedProviderChunk(nil), chunks...))
	store.stampAts = append(store.stampAts, stampAt)
	store.leaseNows = append(store.leaseNows, leaseNow)
}

func (store *stampRecordingStore) PrepareChunkGroup(
	ctx context.Context, claim Claim, chunks []PreparedProviderChunk, stampAt, leaseNow time.Time,
) ([]PreparedProviderChunk, error) {
	prepared, err := store.InProcessChunkLedger.PrepareChunkGroup(ctx, claim, chunks, stampAt, leaseNow)
	if err == nil {
		store.recordGroup(chunks, stampAt, leaseNow)
	}
	return prepared, err
}

type stampOrderedSink struct {
	mu     sync.Mutex
	writes []string
}

func (sink *stampOrderedSink) WriteEffect(_ context.Context, _ Claim, batch EffectBatch) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	for _, row := range batch.Rows {
		sink.writes = append(sink.writes, batch.Destination+"|"+string(row))
	}
	return nil
}

type stampHarness struct {
	claim      Claim
	session    *LeaseSession
	descriptor CompleteRouteDescriptor
	executor   CompleteRouteExecutor
	store      *stampRecordingStore
	sink       *stampOrderedSink
	handler    stampScriptHandler
}

func newStampHarness(t *testing.T, provider, dataset string, emissions []ChunkRouteEmission, at time.Time) *stampHarness {
	t.Helper()
	claim, session := completeRouteSessionFor(t, at, false, provider, dataset)
	descriptor, ok := Descriptor(provider, dataset)
	if !ok || !descriptor.Chunked {
		t.Fatalf("%s/%s descriptor=%+v ok=%t", provider, dataset, descriptor, ok)
	}
	store := &stampRecordingStore{InProcessChunkLedger: NewInProcessChunkLedger(claim)}
	sink := &stampOrderedSink{}
	handler := stampScriptHandler{emissions: emissions, nows: &[]time.Time{}}
	executor := completeRouteExecutor(at, handler, store, sink)
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: provider}
	executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
	executor.Now = func() time.Time { return at }
	executor.BudgetLimits = map[CostClass]int{claim.CostClass: 1}
	return &stampHarness{claim: claim, session: session, descriptor: descriptor, executor: executor, store: store, sink: sink, handler: handler}
}

// run executes attempts until the unit completes or fails with something that is not a continuation.
func (harness *stampHarness) run(t *testing.T) error {
	t.Helper()
	for attempt := 0; attempt < 400; attempt++ {
		// The streaming path itself: the historical tests aliases are not plannable, so Execute refuses them before it.
		_, err := harness.executor.executeChunkedStreaming(context.Background(), harness.session, harness.descriptor, harness.handler)
		if err == nil {
			return nil
		}
		if !errors.As(err, new(ChunkContinuationError)) {
			return err
		}
	}
	t.Fatal("the unit does not complete in 400 attempts")
	return nil
}

func (harness *stampHarness) policyCap() int {
	policy := harness.descriptor.ChunkPolicy
	if policy == (ChunkPolicy{}) {
		policy = DefaultChunkPolicy()
	}
	return min(policy.MaxEffectRows, policy.MaxSourceItems)
}

// checkAgainstMain asserts the product result against MAIN's outcome for the same emissions and instant. It returns the product's data
// chunks per emission.
func checkAgainstMain(t *testing.T, harness *stampHarness, emissions []ChunkRouteEmission, at time.Time, runErr error) []int {
	t.Helper()
	oracle := oracleMain(t, harness.claim, emissions, at)
	mainRefused := oracle[len(oracle)-1].err != nil
	if runErr != nil {
		if !mainRefused {
			t.Fatalf("refused where MAIN stores every chunk: %v", runErr)
		}
		return nil
	}
	harness.store.mu.Lock()
	groups := append([][]PreparedProviderChunk(nil), harness.store.groups...)
	harness.store.mu.Unlock()
	if len(groups) != len(emissions) {
		t.Fatalf("%d groups prepared for %d emissions", len(groups), len(emissions))
	}
	counts := make([]int, len(groups))
	capRows := harness.policyCap()
	for index, group := range groups {
		counts[index] = len(group)
		if !mainRefused && len(group) > len(oracle[index].chunks) {
			t.Fatalf("emission %d: %d chunks, MAIN stores %d", index, len(group), len(oracle[index].chunks))
		}
		for position, chunk := range group {
			for _, effect := range chunk.Effects {
				if len(effect.Rows) > capRows {
					t.Fatalf("emission %d chunk %d: %s has %d rows over the cap %d", index, position, effect.Destination, len(effect.Rows), capRows)
				}
			}
			wantComplete := emissions[index].Final && position == len(group)-1
			if chunk.InventoryComplete != wantComplete {
				t.Fatalf("emission %d chunk %d: inventory_complete=%t want %t", index, position, chunk.InventoryComplete, wantComplete)
			}
		}
	}
	checkRowsOnceInOrder(t, harness.sink, emissions)
	checkpoint, err := harness.store.LoadChunkCheckpoint(context.Background(), harness.claim, at)
	if err != nil || !checkpoint.InventoryComplete {
		t.Fatalf("checkpoint complete=%t err=%v", checkpoint.InventoryComplete, err)
	}
	return counts
}

// checkRowsOnceInOrder: per destination the sink saw the emission's canonical rows in order, each once, and a verdict row of an emission is
// written after every other row of that emission.
func checkRowsOnceInOrder(t *testing.T, sink *stampOrderedSink, emissions []ChunkRouteEmission) {
	t.Helper()
	sink.mu.Lock()
	writes := append([]string(nil), sink.writes...)
	sink.mu.Unlock()
	want := map[string][]string{}
	total := 0
	for _, emission := range emissions {
		for _, effect := range emission.Batch.Effects {
			for _, row := range effect.Rows {
				want[effect.Destination] = append(want[effect.Destination], effect.Destination+"|"+string(row))
				total++
			}
		}
	}
	if len(writes) != total {
		t.Fatalf("sink wrote %d rows, the emissions hold %d", len(writes), total)
	}
	got := map[string][]string{}
	for _, write := range writes {
		destination, _, _ := strings.Cut(write, "|")
		got[destination] = append(got[destination], write)
	}
	for destination, rows := range want {
		if strings.Join(got[destination], "\n") != strings.Join(rows, "\n") {
			t.Fatalf("%s: rows written out of order or not once (%d written, %d emitted)", destination, len(got[destination]), len(rows))
		}
	}
	// Verdict after the other rows: per emission, the verdict rows' first write follows the last other row of that emission.
	position := map[string]int{}
	for index, write := range writes {
		position[write] = index
	}
	for _, emission := range emissions {
		lastOther, firstVerdict := -1, len(writes)
		for _, effect := range emission.Batch.Effects {
			for _, row := range effect.Rows {
				at := position[effect.Destination+"|"+string(row)]
				if effectSplitsLast(effect.Destination) {
					firstVerdict = min(firstVerdict, at)
				} else {
					lastOther = max(lastOther, at)
				}
			}
		}
		if firstVerdict < lastOther {
			t.Fatalf("a verdict row was written at %d, before a report row of its emission at %d", firstVerdict, lastOther)
		}
	}
}

var stampAt = time.Date(2026, 10, 1, 12, 0, 0, 123_456_789, time.UTC)

// oracleEmissionBatch is the batch and comparison MAIN's prepareEmission splits for an emission.
func oracleEmissionBatch(t *testing.T, claim Claim, emission ChunkRouteEmission) (CompleteRouteBatch, ShadowComparison) {
	t.Helper()
	batch := emission.Batch
	var comparison ShadowComparison
	if emission.Final {
		var err error
		batch.Result, batch.Watermark, err = applyGitHubWorkItemsIncompletePolicy(claim.Provider, claim.Dataset, batch.Result, batch.Watermark)
		if err != nil {
			t.Fatal(err)
		}
		comparison, _ = matchingCompleteRouteComparator{}.CompareCompleteRoute(context.Background(), claim, batch)
	} else {
		batch.Result, batch.Watermark, batch.Evidence, batch.WorklogObservations = nil, nil, FetchEvidence{}, nil
	}
	return batch, comparison
}

// oracleSingleChunk is the one chunk MAIN's executor hands its store for an emission that fits one chunk of rows, built without any size
// test (so it also builds a chunk over the limit), at ordinal 0.
func oracleSingleChunk(t *testing.T, claim Claim, emission ChunkRouteEmission) PreparedProviderChunk {
	t.Helper()
	batch, comparison := oracleEmissionBatch(t, claim, emission)
	ordered := append([]EffectBatch(nil), batch.Effects...)
	sortEffectBatches(ordered)
	chunk := PreparedProviderChunk{
		SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion, TotalChunks: 1, Effects: ordered,
	}
	chunks, err := oracleMainStamp([]PreparedProviderChunk{chunk}, emission, batch, comparison, 0)
	if err != nil {
		t.Fatal(err)
	}
	return chunks[0]
}

// tuneSinglePad finds the pad of one row so that MAIN's stored chunk of the first emission (payload + ledger) is exactly target bytes.
func tuneSinglePad(t *testing.T, claim Claim, at time.Time, target int, build func(pad int) []ChunkRouteEmission) int {
	t.Helper()
	pad := target - 20_000
	for range 60 {
		payload, ledger := oracleStoredBytes(t, claim, oracleSingleChunk(t, claim, build(pad)[0]), at)
		gap := target - (payload + ledger)
		if gap == 0 {
			return pad
		}
		pad += gap
	}
	t.Fatalf("no pad gives %d stored bytes", target)
	return 0
}

// ---------------------------------------------------------------------------------------------------------------- the gate

type stampGateCase struct {
	name      string
	provider  string
	dataset   string
	bodies    []stampShape
	finalRows bool
	cursorPad int
	resultPad int
}

func stampGateCases() []stampGateCase {
	cases := []stampGateCase{}
	for _, rows := range []int{1, 2, 99, 100, 101, 499, 500, 501, 5000} {
		for _, filled := range []bool{false, true} {
			cases = append(cases, stampGateCase{name: fmt.Sprintf("cases=%d filled=%t", rows, filled), bodies: []stampShape{{cases: rows, filled: filled}}})
		}
	}
	for _, rows := range []int{101, 501} {
		for _, where := range []string{"head", "middle", "tail"} {
			index := map[string]int{"head": 0, "middle": rows / 2, "tail": rows - 1}[where]
			for _, pad := range []int{1_000_000, 2_000_000, 2_060_000} {
				cases = append(cases, stampGateCase{
					name:   fmt.Sprintf("cases=%d big row %d at %s", rows, pad, where),
					bodies: []stampShape{{cases: rows, filled: true, pads: map[int]int{index: pad}}},
				})
			}
		}
	}
	cases = append(cases,
		stampGateCase{name: "two adjacent 1 MB rows", bodies: []stampShape{{cases: 300, filled: true, pads: map[int]int{150: 1_000_000, 151: 1_000_000}}}},
		stampGateCase{name: "two 1 MB rows apart", bodies: []stampShape{{cases: 300, filled: true, pads: map[int]int{10: 1_000_000, 290: 1_000_000}}}},
		stampGateCase{name: "failing rows 1 KB", bodies: []stampShape{{cases: 1200, filled: true, basePad: 1000}}},
		stampGateCase{name: "failing rows 5 KB", bodies: []stampShape{{cases: 1200, filled: true, basePad: 5000}}},
		stampGateCase{name: "4 KiB cursors, 60 KiB result, rows on the final emission", bodies: []stampShape{{cases: 501, filled: true, basePad: 3000}},
			finalRows: true, cursorPad: 4000, resultPad: 60 << 10},
		stampGateCase{name: "verdict 60 KiB with a big last report row, 1 case", bodies: []stampShape{{cases: 1, pads: map[int]int{0: 2_030_000}, verdicts: 1, verdictPad: 60 << 10}},
			finalRows: true, resultPad: 60 << 10},
		stampGateCase{name: "verdict 60 KiB with a big last report row, 501 cases", bodies: []stampShape{{cases: 501, pads: map[int]int{500: 2_030_000}, verdicts: 1, verdictPad: 60 << 10}},
			finalRows: true, resultPad: 60 << 10},
		stampGateCase{name: "three emissions", bodies: []stampShape{{cases: 700, filled: true}, {cases: 1, filled: true}, {cases: 250, filled: true, basePad: 2000}}, cursorPad: 4000},
		stampGateCase{name: "gitlab cicd", provider: "gitlab", dataset: "cicd", bodies: []stampShape{{cases: 501, filled: true, pads: map[int]int{250: 2_000_000}}}},
		stampGateCase{name: "github tests", provider: "github", dataset: "tests", bodies: []stampShape{{cases: 501, filled: true, pads: map[int]int{500: 2_000_000}}}},
		stampGateCase{name: "gitlab tests", provider: "gitlab", dataset: "tests", bodies: []stampShape{{cases: 501, filled: true}}},
	)
	return cases
}

func TestChunkStampGateAgainstMainOracle(t *testing.T) {
	t.Parallel()
	for _, gate := range stampGateCases() {
		t.Run(gate.name, func(t *testing.T) {
			t.Parallel()
			provider, dataset := gate.provider, gate.dataset
			if provider == "" {
				provider, dataset = "github", "cicd"
			}
			emissions := stampScript(t, gate.bodies, gate.finalRows, gate.cursorPad, gate.resultPad)
			harness := newStampHarness(t, provider, dataset, emissions, stampAt)
			err := harness.run(t)
			counts := checkAgainstMain(t, harness, emissions, stampAt, err)
			oracle := oracleMain(t, harness.claim, emissions, stampAt)
			mainCounts := make([]int, len(oracle))
			for index, outcome := range oracle {
				mainCounts[index] = len(outcome.chunks)
			}
			t.Logf("product chunks %v err=%v; MAIN chunks %v err=%v", counts, err, mainCounts, oracle[len(oracle)-1].err)
		})
	}
}

// ---------------------------------------------------------------------------------------------------------------- counts

// A 13,875-case report (cases of about 450 bytes, the 7 destinations filled) is ONE emission: 139 data chunks at 100 rows, 28 at 500.
func TestChunkStampCountOfA13875CaseReport(t *testing.T) {
	t.Parallel()
	emissions := stampScript(t, []stampShape{{cases: 13875, filled: true, basePad: 380}}, false, 0, 0)
	harness := newStampHarness(t, "github", "cicd", emissions, stampAt)
	err := harness.run(t)
	counts := checkAgainstMain(t, harness, emissions, stampAt, err)
	oracle := oracleMain(t, harness.claim, emissions, stampAt)
	t.Logf("13,875-case report: %d data chunks (MAIN %d)", counts[0], len(oracle[0].chunks))
	if len(oracle[0].chunks) != 139 || counts[0] != 28 {
		t.Fatalf("13,875-case report: %d data chunks, MAIN %d; want 28 under the TestOps policy (MAIN 139)", counts[0], len(oracle[0].chunks))
	}
}

// ---------------------------------------------------------------------------------------------------------------- the size edge

func edgeEmissions(t *testing.T, final bool, cursorPad int) func(pad int) []ChunkRouteEmission {
	return func(pad int) []ChunkRouteEmission {
		// Row 0 (30 KB) leaves the big row's chunk when the emission halves: more than either size band is over the limit.
		return stampScript(t, []stampShape{{cases: 2, pads: map[int]int{0: 30_000, 1: pad}}}, final, cursorPad, 0)
	}
}

// A chunk exactly at the store's limit is stored, with inventory_complete false (a non-final emission) and true (rows on the final
// emission), and one byte below; MAIN stores both. (r5 P1: the split measured "true" where the executor stamps "false".)
func TestChunkStampExactStoreLimitIsStored(t *testing.T) {
	t.Parallel()
	for _, final := range []bool{false, true} {
		for _, target := range []int{maxChunkPayloadBytes, maxChunkPayloadBytes - 1} {
			t.Run(fmt.Sprintf("final=%t bytes=%d", final, target), func(t *testing.T) {
				t.Parallel()
				claim := nativeTestClaim("github", "cicd")
				build := edgeEmissions(t, final, 4000)
				pad := tuneSinglePad(t, claim, stampAt, target, build)
				emissions := build(pad)
				harness := newStampHarness(t, "github", "cicd", emissions, stampAt)
				err := harness.run(t)
				counts := checkAgainstMain(t, harness, emissions, stampAt, err)
				if err != nil || counts[0] != 1 {
					t.Fatalf("chunks=%v err=%v, want the one chunk stored", counts, err)
				}
				stored := harness.store.groups[0][0]
				payload, ledger := oracleStoredBytes(t, harness.claim, stored, stampAt)
				if payload+ledger != target {
					t.Fatalf("stored %d bytes, want %d", payload+ledger, target)
				}
			})
		}
	}
}

// Over the store's limit, at a cap above one, the emission halves and is stored, in both size bands: payload + ledger just over 2 MiB (the
// store refuses with payload_and_ledger_size) and far enough over that the whole-chunk encoding of Validate trips first (chunk_invalid).
// MAIN refuses both emissions (its split never saw the cursors and the ledger).
func TestChunkStampOverTheStoreLimitHalvesInBothSizeBands(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "cicd")
	emptyLedger, err := json.Marshal(EffectLedgerState{})
	if err != nil {
		t.Fatal(err)
	}
	for _, band := range []struct {
		name   string
		over   int
		reason string
	}{
		{"payload_and_ledger_size", 1, "payload_and_ledger_size"},
		{"validate whole-chunk size", len(emptyLedger) + 1, "chunk_invalid"},
	} {
		t.Run(band.name, func(t *testing.T) {
			t.Parallel()
			build := edgeEmissions(t, false, 4000)
			pad := tuneSinglePad(t, claim, stampAt, maxChunkPayloadBytes+band.over, build)
			emissions := build(pad)
			oracle := oracleMain(t, claim, emissions, stampAt)
			mainErr := oracle[len(oracle)-1].err
			if mainErr == nil || !strings.Contains(mainErr.Error(), band.reason) {
				t.Fatalf("MAIN err=%v, want a store refusal %q (the band is wrong)", mainErr, band.reason)
			}
			harness := newStampHarness(t, "github", "cicd", emissions, stampAt)
			runErr := harness.run(t)
			if runErr != nil {
				t.Fatalf("refused (%v): a cap above one halves a size refusal of the store", runErr)
			}
			checkRowsOnceInOrder(t, harness.sink, emissions)
			if got := len(harness.store.groups[0]); got != 2 {
				t.Fatalf("%d chunks, want the two rows in two chunks", got)
			}
		})
	}
}

// The ledger's two instants are RFC3339Nano: 0..9 fraction digits. At every width a chunk exactly at the limit is stored in one chunk.
// (r5 P2: the split measured the widest clock where the store used a shorter one.)
func TestChunkStampEveryInstantWidthAtTheExactLimit(t *testing.T) {
	t.Parallel()
	fractions := []int{0, 100_000_000, 120_000_000, 123_000_000, 123_400_000, 123_450_000, 123_456_000, 123_456_700, 123_456_780, 123_456_789}
	for digits, fraction := range fractions {
		at := time.Date(2026, 10, 1, 12, 0, 0, fraction, time.UTC)
		t.Run(fmt.Sprintf("digits=%d", digits), func(t *testing.T) {
			t.Parallel()
			claim := nativeTestClaim("github", "cicd")
			build := edgeEmissions(t, false, 100)
			pad := tuneSinglePad(t, claim, at, maxChunkPayloadBytes, build)
			emissions := build(pad)
			harness := newStampHarness(t, "github", "cicd", emissions, at)
			err := harness.run(t)
			counts := checkAgainstMain(t, harness, emissions, at, err)
			if err != nil || counts[0] != 1 {
				t.Fatalf("chunks=%v err=%v at %s, want one chunk stored", counts, err, at.Format(time.RFC3339Nano))
			}
		})
	}
}

// One row alone: under the store's limit it is one stored chunk; over it, the unit is refused with MAIN's own error.
func TestChunkStampOneBigRowAlone(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "cicd")
	build := func(pad int) []ChunkRouteEmission {
		return stampScript(t, []stampShape{{cases: 1, pads: map[int]int{0: pad}}}, false, 100, 0)
	}
	for _, target := range []int{maxChunkPayloadBytes, maxChunkPayloadBytes + 1, maxChunkPayloadBytes + 4096} {
		t.Run(fmt.Sprintf("bytes=%d", target), func(t *testing.T) {
			t.Parallel()
			pad := tuneSinglePad(t, claim, stampAt, target, build)
			emissions := build(pad)
			harness := newStampHarness(t, "github", "cicd", emissions, stampAt)
			err := harness.run(t)
			oracle := oracleMain(t, claim, emissions, stampAt)
			mainErr := oracle[len(oracle)-1].err
			if (err == nil) != (mainErr == nil) {
				t.Fatalf("product err=%v, MAIN err=%v: one row alone must be judged as MAIN judges it", err, mainErr)
			}
			if err != nil && err.Error() != mainErr.Error() {
				t.Fatalf("product err=%q, MAIN err=%q: the same refusal", err, mainErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------------------------------------------- guards

// Every chunked descriptor is served by a handler that streams (ChunkedCompleteRouteHandler): the non-streaming executeChunked, whose split
// has no real stamps (MAIN's behaviour), is not reached by a production route. (Carried over from the closed CHAOS-14411 branch.)
func TestEveryChunkedDescriptorHasAStreamingHandler(t *testing.T) {
	t.Parallel()
	seen := 0
	keys := make([]string, 0, len(providerExecutorRegistry))
	for key := range providerExecutorRegistry {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
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

// No chunked descriptor uses the prepared-manifest recovery: the NormalizedAt equality checks of that path (complete_route.go
// manifest.NormalizedAt, prepared_route_snapshot.go snapshot.NormalizedAt) never read a chunk checkpoint's NormalizedAt.
func TestNoChunkedDescriptorUsesPreparedManifestRecovery(t *testing.T) {
	t.Parallel()
	for key := range providerExecutorRegistry {
		provider, dataset, _ := strings.Cut(key, "/")
		descriptor, ok := Descriptor(provider, dataset)
		if ok && descriptor.Chunked && descriptor.PreparedManifestRecovery {
			t.Errorf("%s: chunked and prepared-manifest recovery", key)
		}
	}
}

// ---------------------------------------------------------------------------------------------------------------- github/files

// The files pre-check judges a chunk with widest stamps; the executor stores it at its real ordinal. Every chunk the pre-check accepts
// is stored by the store at any ordinal below 1,000,000 with the widest instant (99999 was not a bound: one extra digit at 100,000).
func TestGitHubFilesPreCheckDominatesEveryOrdinalBelowAMillion(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "files")
	edge := tuneFilesEdge(t, claim)
	edgeBytes := len(edge.Rows[0])
	// The largest chunk the pre-check accepts, and two below it.
	for _, delta := range []int{0, -1, -4096} {
		effect := edge
		if delta != 0 {
			effect = fitsTestEffectSized(t, edgeBytes+delta)
		}
		if !gitHubFilesChunkFits(claim, effect, "before", "after") {
			t.Fatalf("delta %d: the pre-check refuses a chunk at or under its own edge", delta)
		}
		chunk, ok := gitHubFilesPreparedChunk(effect, "before", "after")
		if !ok {
			t.Fatal("the pre-check chunk is not one chunk")
		}
		for _, ordinal := range []int{0, 99_999, 100_000, 999_999} {
			real := chunk
			real.Ordinal = ordinal
			real.ManifestDigest = preparedChunkDigest(real)
			if _, err := buildPreparedChunkMaterial(claim, real, gitHubFilesWidestInstant); err != nil {
				t.Fatalf("delta %d: the pre-check accepted a chunk the store refuses at ordinal %d: %v", delta, ordinal, err)
			}
		}
	}
}

var filesEdgeOnce struct {
	sync.Once
	length int
}

// tuneFilesEdge is the files effect whose pre-check chunk is the largest the pre-check accepts (searched once per test binary).
func tuneFilesEdge(t *testing.T, claim Claim) EffectBatch {
	t.Helper()
	filesEdgeOnce.Do(func() { filesEdgeOnce.length = searchFilesEdge(t, claim) })
	if filesEdgeOnce.length == 0 {
		t.Fatal("the files pre-check edge was not found")
	}
	return fitsTestEffect(t, filesEdgeOnce.length)
}

func searchFilesEdge(t *testing.T, claim Claim) int {
	t.Helper()
	length := 2_080_000
	for range 20 {
		chunk, ok := gitHubFilesPreparedChunk(fitsTestEffect(t, length), "before", "after")
		if !ok {
			t.Errorf("no pre-check chunk at %d", length)
			return 0
		}
		payload, ledger := oracleStoredBytes(t, claim, chunk, gitHubFilesWidestInstant)
		gap := maxChunkPayloadBytes - (payload + ledger)
		if gap == 0 {
			if !gitHubFilesChunkFits(claim, fitsTestEffect(t, length), "before", "after") ||
				gitHubFilesChunkFits(claim, fitsTestEffect(t, length+1), "before", "after") {
				t.Errorf("the pre-check edge is not at %d", length)
				return 0
			}
			return length
		}
		length += gap
	}
	t.Errorf("no files pre-check edge found")
	return 0
}

// Every files emission the pre-check accepts is ONE stored chunk in the executor (count equality), around the edge.
func TestGitHubFilesPreCheckedEmissionIsOneStoredChunk(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "files")
	edge := tuneFilesEdge(t, claim)
	edgeBytes := 0
	for _, row := range edge.Rows {
		edgeBytes += len(row)
	}
	for _, delta := range []int{-3000, -1, 0} {
		effect := edge
		if delta != 0 {
			effect = fitsTestEffectSized(t, edgeBytes+delta)
		}
		if !gitHubFilesChunkFits(claim, effect, "before", "after") {
			t.Fatalf("delta %d: the pre-check refuses a chunk at or under its own edge", delta)
		}
		final, err := BuildEffectBatch("git_files", EffectReadbackRequired, nil)
		if err != nil {
			t.Fatal(err)
		}
		emissions := []ChunkRouteEmission{
			{Batch: CompleteRouteBatch{Effects: []EffectBatch{effect}}, CursorBefore: "before", CursorAfter: "after"},
			{Batch: CompleteRouteBatch{Effects: []EffectBatch{final}, Result: map[string]any{"complete": true}}, CursorBefore: "after", CursorAfter: "after", Final: true},
		}
		harness := newStampHarness(t, "github", "files", emissions, gitHubFilesWidestInstant)
		if err := harness.run(t); err != nil {
			t.Fatalf("delta %d: %v", delta, err)
		}
		if got := len(harness.store.groups[0]); got != 1 {
			t.Fatalf("delta %d: the pre-checked emission is %d stored chunks, want 1", delta, got)
		}
	}
}

// fitsTestEffectSized is a one-row git_files effect whose row is n bytes long.
func fitsTestEffectSized(t *testing.T, n int) EffectBatch {
	t.Helper()
	base := fitsTestEffect(t, 0)
	over := n - len(base.Rows[0])
	if over < 0 {
		t.Fatalf("row of %d bytes is under the empty row", n)
	}
	return fitsTestEffect(t, over)
}

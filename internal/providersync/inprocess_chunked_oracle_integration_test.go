//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestInProcessChunkLedgerMatchesPostgres is the differential oracle of the
// in-process chunk store (CHAOS-6711): the same generated sequences of store calls
// run against PostgresRepository (real PostgreSQL, the real migration chain: the
// reference) and InProcessChunkLedger, and after EVERY call the two must agree on
// the outcome (the value returned, or the error's class) and on the whole visible
// state (the checkpoint and every prepared chunk, read back through each store's
// own loaders). The sequences walk the object model of the store: ordinals in
// order, ahead of the checkpoint and replayed; chunk contents that match and that
// conflict with what is stored; totals and inventory completion; cursors that are
// empty and that are not; groups of sub-chunks (atomic); every effect transition,
// with right and wrong digests; commit marks; inventory completion; and a claim of
// another owner.
//
// Named divergence, not generated: PostgresRepository fences on the unit's lease
// expiry (a `now` past lease_expires_at is ErrLeaseLost); an in-process run has no
// lease row and its heartbeat has nothing to renew, so the store does not.
func TestInProcessChunkLedgerMatchesPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if closeErr := instance.Close(closeContext); closeErr != nil {
			t.Errorf("terminate PostgreSQL: %v", closeErr)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	if _, err := pool.Exec(ctx, `UPDATE public.sync_run_units SET dataset_key='cicd', processor_flags='{"sync_git":true}'::jsonb WHERE id=$1`, firstUnitID); err != nil {
		t.Fatal(err)
	}
	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	claim, err := repository.Claim(ctx, ClaimRequest{UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now, LeaseDuration: time.Minute, AllowExpiredRecovery: true})
	if err != nil {
		t.Fatal(err)
	}
	stranger := claim
	stranger.Owner = uuid.NewString()

	sequences, steps := 250, 28
	if value := os.Getenv("DHO_CHUNK_ORACLE_SEQUENCES"); value != "" {
		sequences, _ = strconv.Atoi(value)
	}
	compared := 0
	outcomes := map[string]int{} // "<call kind> -> <error class>" seen against the reference
	completed := 0
	for seed := 0; seed < sequences; seed++ {
		if _, err := pool.Exec(ctx, `DELETE FROM public.sync_run_unit_effect_chunks WHERE sync_run_unit_id=$1::uuid`, firstUnitID); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `DELETE FROM public.sync_run_unit_chunk_checkpoints WHERE sync_run_unit_id=$1::uuid`, firstUnitID); err != nil {
			t.Fatal(err)
		}
		memory := NewInProcessChunkLedger(claim)
		generator := &chunkOpGenerator{t: t, rng: rand.New(rand.NewSource(int64(seed) + 1))}
		clock := now.Add(time.Second)
		for step := 0; step < steps; step++ {
			clock = clock.Add(time.Second)
			operation := generator.next()
			useClaim := claim
			if generator.rng.Intn(20) == 0 {
				useClaim = stranger
			}
			// A call with a context that is already cancelled: PostgreSQL fails it (a
			// transaction cannot begin, a query fails), and so must the in-process store,
			// without touching its state.
			callContext, cancelled := ctx, false
			if generator.rng.Intn(12) == 0 {
				var stop context.CancelFunc
				callContext, stop = context.WithCancel(ctx)
				stop()
				cancelled = true
			}
			label := fmt.Sprintf("seed %d step %d %s (claim=%v cancelled=%v)", seed, step, operation.name, useClaim.Owner == claim.Owner, cancelled)
			wantValue, wantErr := operation.run(callContext, repository, useClaim, clock)
			gotValue, gotErr := operation.run(callContext, memory, useClaim, clock)
			if wantClass, gotClass := errorClass(wantErr), errorClass(gotErr); wantClass != gotClass {
				t.Fatalf("%s: postgres error %q (%v), in-process error %q (%v)", label, wantClass, wantErr, gotClass, gotErr)
			}
			kind := strings.Fields(operation.name)[0]
			if strings.HasPrefix(operation.name, "commit chunk") {
				kind = "commit-chunk"
			}
			if cancelled {
				kind = "cancelled " + kind
			}
			outcomes[kind+" -> "+errorClass(wantErr)]++
			if want, got := canonicalJSON(t, wantValue), canonicalJSON(t, gotValue); want != got {
				t.Fatalf("%s: value differs\npostgres:   %s\nin-process: %s", label, want, got)
			}
			if want, got := visibleChunkState(ctx, t, repository, claim, clock), visibleChunkState(ctx, t, memory, claim, clock); want != got {
				t.Fatalf("%s: state differs\npostgres:   %s\nin-process: %s", label, want, got)
			}
			compared++
			if wantErr == nil && operation.advanceTo > generator.sent {
				generator.sent = operation.advanceTo
			}
		}
		if checkpoint, err := repository.LoadChunkCheckpoint(ctx, claim, clock); err == nil && checkpoint.InventoryComplete {
			completed++
		}
	}
	t.Logf("%d store calls compared over %d sequences, every one with its state readback; %d sequences ended with the inventory complete", compared, sequences, completed)
	keys := make([]string, 0, len(outcomes))
	for key := range outcomes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		t.Logf("  %5d  %s", outcomes[key], key)
	}
	// The sequences must reach what the store exists for, or a pass says nothing:
	// committed chunks, a completed inventory, and the refusals (conflict, lost
	// lease, invalid input) on the same calls.
	if sequences >= 100 {
		for _, required := range []string{"commit-chunk -> ok", "mark -> ok", "prepare -> ok", "prepare -> " + ErrChunkCheckpointConflict.Error(),
			"begin -> ok", "commit -> ok", "resolve -> ok", "prepare -> " + ErrLeaseLost.Error(),
			"cancelled prepare -> " + ErrInvalidConfiguration.Error(), "cancelled load -> " + ErrChunkCheckpointConflict.Error(),
			"cancelled begin -> " + ErrInvalidConfiguration.Error(), "cancelled mark -> " + ErrInvalidConfiguration.Error()} {
			if outcomes[required] == 0 {
				t.Errorf("no sequence reached %q: the generator does not cover it", required)
			}
		}
		if completed == 0 {
			t.Error("no sequence completed its inventory")
		}
	}
}

var chunkErrorSentinels = []error{
	ErrInvalidConfiguration, ErrChunkCheckpointNotFound, ErrPreparedChunkNotFound, ErrChunkCheckpointConflict,
	ErrChunkPolicyExceeded, ErrLeaseLost, ErrEffectRecoveryAmbiguous, ErrEffectRecoveryUnsafe,
}

func errorClass(err error) string {
	if err == nil {
		return "ok"
	}
	for _, sentinel := range chunkErrorSentinels {
		if errors.Is(err, sentinel) {
			return sentinel.Error()
		}
	}
	return "other: " + err.Error()
}

func canonicalJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(normalizeTimes(value))
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

// normalizeTimes puts the checkpoint's timestamps in UTC (PostgreSQL returns them in
// the session's zone).
func normalizeTimes(value any) any {
	switch typed := value.(type) {
	case ChunkCheckpoint:
		typed.NormalizedAt, typed.LeaseExpiresAt = typed.NormalizedAt.UTC(), typed.LeaseExpiresAt.UTC()
		typed.CreatedAt, typed.UpdatedAt = typed.CreatedAt.UTC(), typed.UpdatedAt.UTC()
		return typed
	}
	return value
}

func visibleChunkState(ctx context.Context, t *testing.T, store ChunkedEffectStore, claim Claim, now time.Time) string {
	t.Helper()
	var parts []string
	checkpoint, err := store.LoadChunkCheckpoint(ctx, claim, now)
	parts = append(parts, "checkpoint="+canonicalJSON(t, checkpoint)+" err="+errorClass(err))
	for ordinal := 0; ordinal < 6; ordinal++ {
		chunk, err := store.LoadPreparedChunk(ctx, claim, ordinal, now)
		parts = append(parts, fmt.Sprintf("chunk%d=%s err=%s", ordinal, canonicalJSON(t, chunk), errorClass(err)))
	}
	return strings.Join(parts, "\n")
}

// chunkOperation is one call of the ChunkedEffectStore interface.
type chunkOperation struct {
	name string
	// advanceTo is the number of ordinals the sequence has prepared once the call
	// succeeds (0: the call prepares none).
	advanceTo int
	run       func(ctx context.Context, store ChunkedEffectStore, claim Claim, now time.Time) (any, error)
}

// chunkOpGenerator draws the next call from the sequence's own view of what it has
// sent (never from either store's state), so both stores see the same call.
type chunkOpGenerator struct {
	t       *testing.T
	rng     *rand.Rand
	sent    int // ordinals sent in order so far
	digests map[int][]string
}

func (g *chunkOpGenerator) chunk(ordinal, total int, complete bool, cursor string, variant int, withResult bool) PreparedProviderChunk {
	g.t.Helper()
	effects := make([]EffectBatch, 1+g.rng.Intn(2))
	digests := make([]string, len(effects))
	for i := range effects {
		rows := make([]string, 1+g.rng.Intn(3))
		for r := range rows {
			rows[r] = fmt.Sprintf(`{"org_id":"org-acme","run_id":"%d-%d-%d-%d"}`, ordinal, variant, i, r)
		}
		policy := EffectReadbackRequired
		effects[i] = effectBatchFixture(g.t, []string{"ci_pipeline_runs", "ci_job_runs"}[i], policy, rows...)
		digests[i] = effects[i].ContentDigest
	}
	if g.digests == nil {
		g.digests = map[int][]string{}
	}
	g.digests[ordinal] = digests
	chunk := PreparedProviderChunk{
		SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion,
		Ordinal: ordinal, TotalChunks: total, CursorAfter: cursor, InventoryComplete: complete, Effects: effects,
	}
	if withResult {
		chunk.Result = map[string]any{"records": float64(ordinal), "note": "final"}
	}
	return chunk
}

func (g *chunkOpGenerator) cursor() string {
	return []string{"", "", "c1", "c2", "cursor-with-more-text"}[g.rng.Intn(5)]
}

func (g *chunkOpGenerator) digestFor(ordinal, index int) string {
	if g.rng.Intn(6) == 0 {
		return "not-a-digest"
	}
	if list := g.digests[ordinal]; index < len(list) {
		return list[index]
	}
	return "sha256:absent"
}

func (g *chunkOpGenerator) next() chunkOperation {
	roll := g.rng.Intn(100)
	ordinal := g.rng.Intn(4)
	index := g.rng.Intn(2)
	switch {
	case roll < 26: // the next ordinal in order
		o := g.sent
		complete := g.rng.Intn(4) == 0
		total := 0
		if complete {
			total = o + 1
		}
		chunk := g.chunk(o, total, complete, g.cursor(), 0, complete || g.rng.Intn(8) == 0)
		return chunkOperation{fmt.Sprintf("prepare in order %d complete=%v", o, complete), o + 1, func(ctx context.Context, s ChunkedEffectStore, c Claim, now time.Time) (any, error) {
			return s.PrepareChunk(ctx, c, chunk, now)
		}}
	case roll < 30: // ahead of the checkpoint, or a replay of an earlier ordinal (same or other content)
		o := g.rng.Intn(g.sent + 3)
		variant := g.rng.Intn(2)
		chunk := g.chunk(o, 0, false, g.cursor(), variant, false)
		return chunkOperation{fmt.Sprintf("prepare ordinal %d variant %d", o, variant), o + 1, func(ctx context.Context, s ChunkedEffectStore, c Claim, now time.Time) (any, error) {
			return s.PrepareChunk(ctx, c, chunk, now)
		}}
	case roll < 38: // a group of sub-chunks (atomic)
		start := g.sent
		n := 2 + g.rng.Intn(2)
		chunks := make([]PreparedProviderChunk, n)
		for i := range chunks {
			cursor := ""
			if i == n-1 {
				cursor = g.cursor()
			}
			chunks[i] = g.chunk(start+i, 0, false, cursor, 0, false)
		}
		if g.rng.Intn(4) == 0 { // a conflicting sub-chunk in the middle
			chunks[n-1].RouteVersion = "other-route"
		}
		return chunkOperation{fmt.Sprintf("prepare group %d..%d", start, start+n-1), start + n, func(ctx context.Context, s ChunkedEffectStore, c Claim, now time.Time) (any, error) {
			return s.PrepareChunkGroup(ctx, c, chunks, now)
		}}
	case roll < 42: // a bad chunk
		chunk := g.chunk(g.sent, 0, false, "", 0, false)
		switch g.rng.Intn(4) {
		case 0:
			chunk.RouteVersion = ""
		case 1:
			chunk.Effects = nil
		case 2:
			chunk.Ordinal = -1
		case 3:
			chunk.SchemaVersion = "v0"
		}
		return chunkOperation{"prepare invalid chunk", 0, func(ctx context.Context, s ChunkedEffectStore, c Claim, now time.Time) (any, error) {
			return s.PrepareChunk(ctx, c, chunk, now)
		}}
	case roll < 50:
		return chunkOperation{"load checkpoint", 0, func(ctx context.Context, s ChunkedEffectStore, c Claim, now time.Time) (any, error) {
			return s.LoadChunkCheckpoint(ctx, c, now)
		}}
	case roll < 56:
		return chunkOperation{fmt.Sprintf("load chunk %d", ordinal), 0, func(ctx context.Context, s ChunkedEffectStore, c Claim, now time.Time) (any, error) {
			return s.LoadPreparedChunk(ctx, c, ordinal, now)
		}}
	case roll < 66:
		digest := g.digestFor(ordinal, index)
		return chunkOperation{fmt.Sprintf("begin effect %d/%d", ordinal, index), 0, func(ctx context.Context, s ChunkedEffectStore, c Claim, now time.Time) (any, error) {
			return nil, s.BeginChunkEffect(ctx, c, ordinal, index, digest, now)
		}}
	case roll < 76:
		digest := g.digestFor(ordinal, index)
		return chunkOperation{fmt.Sprintf("commit effect %d/%d", ordinal, index), 0, func(ctx context.Context, s ChunkedEffectStore, c Claim, now time.Time) (any, error) {
			return nil, s.CommitChunkEffect(ctx, c, ordinal, index, digest, now)
		}}
	case roll < 82:
		digest := g.digestFor(ordinal, index)
		resolution := []GenerationBlockResolution{GenerationBlockMarkCommitted, GenerationBlockRetryPending, GenerationBlockResolution("bogus")}[g.rng.Intn(3)]
		return chunkOperation{fmt.Sprintf("resolve effect %d/%d %v", ordinal, index, resolution), 0, func(ctx context.Context, s ChunkedEffectStore, c Claim, now time.Time) (any, error) {
			return nil, s.ResolveChunkEffect(ctx, c, ordinal, index, digest, resolution, now)
		}}
	case roll < 86:
		wrong := g.rng.Intn(6) == 0
		return chunkOperation{fmt.Sprintf("mark chunk %d committed wrong=%v", ordinal, wrong), 0, func(ctx context.Context, s ChunkedEffectStore, c Claim, now time.Time) (any, error) {
			digest := "not-a-digest"
			if !wrong {
				loaded, err := s.LoadPreparedChunk(ctx, c, ordinal, now)
				if err == nil {
					digest = loaded.ManifestDigest
				}
			}
			return nil, s.MarkChunkCommitted(ctx, c, ordinal, digest, now)
		}}
	case roll < 94: // the whole commit cycle of one chunk, as the executor does it
		return chunkOperation{fmt.Sprintf("commit chunk %d completely", ordinal), 0, func(ctx context.Context, s ChunkedEffectStore, c Claim, now time.Time) (any, error) {
			loaded, err := s.LoadPreparedChunk(ctx, c, ordinal, now)
			if err != nil {
				return nil, err
			}
			for i, effect := range loaded.Ledger.Effects {
				if err := s.BeginChunkEffect(ctx, c, ordinal, i, effect.ContentDigest, now); err != nil {
					return nil, err
				}
				if err := s.CommitChunkEffect(ctx, c, ordinal, i, effect.ContentDigest, now); err != nil {
					return nil, err
				}
			}
			return nil, s.MarkChunkCommitted(ctx, c, ordinal, loaded.ManifestDigest, now)
		}}
	default:
		return chunkOperation{"mark inventory complete", 0, func(ctx context.Context, s ChunkedEffectStore, c Claim, now time.Time) (any, error) {
			return nil, s.MarkInventoryComplete(ctx, c, now)
		}}
	}
}

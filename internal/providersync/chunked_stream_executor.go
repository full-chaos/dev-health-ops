package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// executeChunkedStreaming is the page-at-a-time path. The handler owns
// provider pagination; this executor owns durable preparation, effect-ledger
// readback, and the lease fence. At most one provider page and one prepared
// sidecar are live in this function at a time.
func (executor CompleteRouteExecutor) executeChunkedStreaming(
	ctx context.Context,
	session *LeaseSession,
	descriptor CompleteRouteDescriptor,
	handler ChunkedCompleteRouteHandler,
) (CompleteRouteExecutionResult, error) {
	store, ok := executor.Committer.Ledger.(ChunkedEffectStore)
	if !ok || store == nil || (executor.Committer.Sink == nil && executor.EffectsFactory == nil) || handler == nil {
		return CompleteRouteExecutionResult{}, ErrInvalidConfiguration
	}
	policy := descriptor.ChunkPolicy
	if policy == (ChunkPolicy{}) {
		policy = DefaultChunkPolicy()
	}
	if policy.Validate() != nil {
		return CompleteRouteExecutionResult{}, ErrInvalidConfiguration
	}
	observer, _ := handler.(ChunkCommitObserver)
	var result CompleteRouteExecutionResult
	err := session.Run(ctx, executor.HeartbeatInterval, func(
		workContext context.Context,
		guard providerfoundation.LeaseGuard,
	) error {
		now := executor.now()
		attemptElapsed := executor.attemptTimer()
		checkpoint, checkpointErr := store.LoadChunkCheckpoint(workContext, session.Claim, now)
		if checkpointErr != nil && !errors.Is(checkpointErr, ErrChunkCheckpointNotFound) {
			return checkpointErr
		}
		if errors.Is(checkpointErr, ErrChunkCheckpointNotFound) {
			checkpoint = ChunkCheckpoint{}
		}
		result.CommittedRows = checkpoint.CommittedRows
		nextOrdinal := checkpoint.PreparedChunks

		// Recovery always drains already-prepared sidecars first. A restart
		// never recollects a page whose normalized payload is durable.
		committedThisAttempt := 0
		for ordinal := checkpoint.NextOrdinal; ordinal < checkpoint.PreparedChunks; ordinal++ {
			chunk, loadErr := store.LoadPreparedChunk(workContext, session.Claim, ordinal, executor.now())
			if loadErr != nil {
				return loadErr
			}
			commitResult, commitErr := commitPreparedChunk(
				workContext, session.Claim, chunk, executor.Committer.Sink,
				executor.Committer.Readback, executor.Committer.Now, store,
			)
			result.Effects.Written += commitResult.Written
			result.Effects.Skipped += commitResult.Skipped
			result.Effects.MarkedCommitted += commitResult.MarkedCommitted
			result.Effects.ResetForReplay += commitResult.ResetForReplay
			result.Effects.IdempotentReplay += commitResult.IdempotentReplay
			if commitErr != nil {
				return commitErr
			}
			if err := executor.markChunkCommitted(workContext, session.Claim, store, observer, ordinal, chunk); err != nil {
				return err
			}
			committedThisAttempt++
			if (committedThisAttempt >= policy.MaxChunksPerAttempt || attemptElapsed() >= policy.MaxWallTime) && ordinal+1 < checkpoint.PreparedChunks {
				return newChunkContinuation(policy, executor.now().Add(time.Second), committedThisAttempt, attemptElapsed())
			}
		}
		checkpoint, checkpointErr = store.LoadChunkCheckpoint(workContext, session.Claim, executor.now())
		if checkpointErr != nil && !errors.Is(checkpointErr, ErrChunkCheckpointNotFound) {
			return checkpointErr
		}
		if errors.Is(checkpointErr, ErrChunkCheckpointNotFound) {
			checkpoint = ChunkCheckpoint{}
		}
		result.CommittedRows = checkpoint.CommittedRows
		if checkpoint.InventoryComplete {
			if checkpoint.TotalChunks < 1 || checkpoint.NextOrdinal != checkpoint.TotalChunks || checkpoint.PreparedChunks != checkpoint.TotalChunks {
				return ErrChunkCheckpointConflict
			}
			return loadChunkedFinalResult(workContext, session.Claim, store, checkpoint.TotalChunks, &result, executor.now())
		}
		// A committed final sidecar means the inventory scan already finished
		// on an earlier attempt and only MarkInventoryComplete is outstanding.
		// Finalize from durable state instead of calling the provider: the
		// route's terminal cursor would otherwise re-enter pagination and
		// refetch the whole final phase (CHAOS-3820).
		if checkpoint.PreparedChunks > 0 && checkpoint.NextOrdinal == checkpoint.PreparedChunks {
			final, finalErr := store.LoadPreparedChunk(
				workContext, session.Claim, checkpoint.PreparedChunks-1, executor.now())
			if finalErr != nil && !errors.Is(finalErr, ErrPreparedChunkNotFound) {
				return finalErr
			}
			if finalErr == nil && final.InventoryComplete {
				if err := store.MarkInventoryComplete(workContext, session.Claim, executor.now()); err != nil {
					return err
				}
				checkpoint, checkpointErr = store.LoadChunkCheckpoint(workContext, session.Claim, executor.now())
				if checkpointErr != nil || !checkpoint.InventoryComplete || checkpoint.TotalChunks < 1 {
					if checkpointErr != nil {
						return checkpointErr
					}
					return ErrChunkCheckpointConflict
				}
				return loadChunkedFinalResult(
					workContext, session.Claim, store, checkpoint.TotalChunks, &result, executor.now())
			}
		}

		// An attempt that drained prepared chunks up to a bound hands over BEFORE it asks the provider for anything: the drain
		// loop above does not hand over at its last prepared chunk (nothing was left to drain), and without this check the
		// attempt would fetch, prepare and commit one more sub-chunk past the bound (CHAOS-8328). The attempt must have committed
		// at least one chunk, so a bound that is already reached at the start of an attempt still lets every attempt progress.
		if committedThisAttempt > 0 && (committedThisAttempt >= policy.MaxChunksPerAttempt || attemptElapsed() >= policy.MaxWallTime) {
			return newChunkContinuation(policy, executor.now().Add(time.Second), committedThisAttempt, attemptElapsed())
		}

		credential, resolveErr := executor.Credentials.Resolve(
			workContext, guard, session.Claim.TenantScope(),
		)
		if resolveErr != nil {
			return resolveErr
		}
		if executor.EffectsFactory != nil {
			executor.Committer.Sink, executor.Committer.Readback, resolveErr = executor.EffectsFactory(credential)
			if resolveErr != nil || executor.Committer.Sink == nil {
				if resolveErr == nil {
					resolveErr = ErrInvalidConfiguration
				}
				return resolveErr
			}
		}
		client, clientErr := (Executor{Doer: executor.Doer, Retry: executor.Retry}).newClient(credential, guard)
		if clientErr != nil {
			return clientErr
		}
		client.Budget = executor.Budget
		client.BudgetKey = providerfoundation.BudgetKey{
			Provider: session.Claim.Provider, OrgID: session.Claim.OrgID,
			Host: client.BaseURL.Hostname(), CostClass: string(session.Claim.CostClass),
			Limit: executor.BudgetLimits[session.Claim.CostClass], TTL: executor.BudgetTTL,
		}
		client.Gate = executor.Gate(session.Claim, client)
		if client.Gate == nil {
			return ErrInvalidConfiguration
		}
		client.Metrics = executor.Metrics

		finalSeen := false
		var finalComparison ShadowComparison
		var callbackErr error
		prepareAndCommit := func(emission ChunkRouteEmission) error {
			batch := emission.Batch
			if emission.Final {
				batch.Result, batch.Watermark, callbackErr = applyGitHubWorkItemsIncompletePolicy(
					session.Claim.Provider, session.Claim.Dataset, batch.Result, batch.Watermark,
				)
				if callbackErr != nil {
					return callbackErr
				}
				finalComparison, callbackErr = executor.Comparator.CompareCompleteRoute(workContext, session.Claim, batch)
				if callbackErr != nil {
					return callbackErr
				}
				if !finalComparison.Match {
					return ErrShadowMismatch
				}
				finalSeen = true
			} else {
				// Non-final pages cannot publish unit metadata. Keep the
				// evidence in the final emission only.
				batch.Result, batch.Watermark = nil, nil
				batch.Evidence = FetchEvidence{}
				batch.WorklogObservations = nil
			}
			chunks, splitErr := splitCompleteRouteBatch(batch, finalComparison, policy)
			if splitErr != nil {
				return splitErr
			}
			last := len(chunks) - 1
			if last < 0 {
				return ErrChunkPolicyExceeded
			}
			resetEmissionChunks(chunks, emission)
			if emission.Final {
				chunks[last].InventoryComplete = true
				chunks[last].Result = cloneChunkResult(batch.Result)
				chunks[last].Watermark = batch.Watermark
				chunks[last].Evidence = batch.Evidence
				chunks[last].Comparison = finalComparison
				chunks[last].WorklogObservations = append([]JiraWorklogFetchObservation(nil), batch.WorklogObservations...)
			}
			if err := sizeEmissionChunks(chunks); err != nil {
				return err
			}
			// The whole emission is made durable BEFORE any of it reaches
			// the sink. Preparing and committing one sub-chunk at a time
			// left a window where committed rows existed for an emission
			// the cursor had not advanced past, so recovery refetched the
			// item and wrote those rows again (CHAOS-3821).
			group := make([]PreparedProviderChunk, 0, len(chunks))
			for _, candidate := range chunks {
				candidate.Ordinal = nextOrdinal + len(group)
				candidate.ManifestDigest = preparedChunkDigest(candidate)
				group = append(group, candidate)
			}
			preparedGroup, prepareErr := store.PrepareChunkGroup(
				workContext, session.Claim, group, executor.now())
			if prepareErr != nil {
				return prepareErr
			}
			for position, prepared := range preparedGroup {
				commitResult, commitErr := commitPreparedChunk(
					workContext, session.Claim, prepared, executor.Committer.Sink,
					executor.Committer.Readback, executor.Committer.Now, store,
				)
				result.Effects.Written += commitResult.Written
				result.Effects.Skipped += commitResult.Skipped
				result.Effects.MarkedCommitted += commitResult.MarkedCommitted
				result.Effects.ResetForReplay += commitResult.ResetForReplay
				result.Effects.IdempotentReplay += commitResult.IdempotentReplay
				if commitErr != nil {
					return commitErr
				}
				if err := executor.markChunkCommitted(workContext, session.Claim, store, observer, prepared.Ordinal, prepared); err != nil {
					return err
				}
				nextOrdinal++
				committedThisAttempt++
				// The bounds are checked after EVERY sub-chunk, also inside this emission (CHAOS-8328): one emission can hold
				// hundreds of sub-chunks. The rest of the group is already durable (prepared as one group above), so the
				// continuation costs nothing: the next attempt drains the prepared chunks before it asks the provider for anything.
				// After the LAST sub-chunk of the FINAL emission there is nothing left to hand over.
				more := position+1 < len(preparedGroup) || !emission.Final
				if more && (committedThisAttempt >= policy.MaxChunksPerAttempt || attemptElapsed() >= policy.MaxWallTime) {
					return newChunkContinuation(policy, executor.now().Add(time.Second), committedThisAttempt, attemptElapsed())
				}
			}
			return nil
		}
		// pending is a coalesced group of contiguous coalescable emissions that is
		// not durable yet. It is committed (flushed) before any emission that
		// cannot join it, at the wall bound, and before the final emission. A
		// pending group that never flushes leaves the durable cursor at its first
		// CursorBefore, so a restart re-reads it: nothing is skipped or written twice.
		var pending *ChunkRouteEmission
		flush := func() error {
			if pending == nil {
				return nil
			}
			group := *pending
			pending = nil
			return prepareAndCommit(group)
		}
		hold := func(group ChunkRouteEmission) error {
			pending = &group
			if attemptElapsed() >= policy.MaxWallTime {
				return flush()
			}
			return nil
		}
		streamErr := handler.CollectChunks(
			workContext, session.Claim, credential, client, checkpointTime(checkpoint, now), checkpoint.NextCursor,
			func(emission ChunkRouteEmission) error {
				if err := emission.Batch.validate(descriptor); err != nil {
					return err
				}
				if pending != nil {
					if merged, ok := coalesceChunkEmissions(session.Claim, policy, *pending, emission, executor.now()); ok {
						return hold(merged)
					}
					if err := flush(); err != nil {
						return err
					}
				}
				if emission.Coalescable && !emission.Final && coalescedEmissionFits(session.Claim, policy, emission, executor.now()) {
					return hold(emission)
				}
				return prepareAndCommit(emission)
			},
		)
		if streamErr == nil && pending != nil {
			streamErr = ErrChunkCheckpointConflict
		}
		if streamErr != nil {
			return streamErr
		}
		if !finalSeen {
			return ErrChunkCheckpointConflict
		}
		if err := store.MarkInventoryComplete(workContext, session.Claim, executor.now()); err != nil {
			return err
		}
		checkpoint, checkpointErr = store.LoadChunkCheckpoint(workContext, session.Claim, executor.now())
		if checkpointErr != nil || !checkpoint.InventoryComplete || checkpoint.TotalChunks < 1 {
			if checkpointErr != nil {
				return checkpointErr
			}
			return ErrChunkCheckpointConflict
		}
		result.Comparison = finalComparison
		return loadChunkedFinalResult(workContext, session.Claim, store, checkpoint.TotalChunks, &result, executor.now())
	})
	return result, err
}

// markChunkCommitted marks one prepared chunk committed and tells the route's
// ChunkCommitObserver (nil: none) of a commit it confirmed. A failed mark may
// still be durable (a lost acknowledgement), and no later recovery drains a
// chunk the checkpoint already moved past, so the checkpoint read back decides;
// a read back that fails too leaves the commit unknown.
func (executor CompleteRouteExecutor) markChunkCommitted(
	ctx context.Context, claim Claim, store ChunkedEffectStore, observer ChunkCommitObserver,
	ordinal int, chunk PreparedProviderChunk,
) error {
	err := store.MarkChunkCommitted(ctx, claim, ordinal, chunk.ManifestDigest, executor.now())
	if observer == nil {
		return err
	}
	if err != nil {
		checkpoint, loadErr := store.LoadChunkCheckpoint(ctx, claim, executor.now())
		if loadErr != nil {
			observer.ObserveUnconfirmedChunk(claim, chunk)
			return err
		}
		if checkpoint.NextOrdinal <= ordinal {
			return err
		}
	}
	observer.ObserveCommittedChunk(claim, chunk, executor.Metrics)
	return err
}

func checkpointTime(checkpoint ChunkCheckpoint, fallback time.Time) time.Time {
	if !checkpoint.NormalizedAt.IsZero() {
		return checkpoint.NormalizedAt
	}
	return fallback
}

// loadChunkedFinalResult publishes completion state from durable chunks. The
// comparison's record counts come from the checkpoint's cumulative committed
// row count, NOT from the final chunk: that chunk carries only metadata and no
// rows, so comparing against it reported zero records for a sync of any size
// and could not detect an omitted or duplicated chunk (CHAOS-3823).
func loadChunkedFinalResult(
	ctx context.Context,
	claim Claim,
	store ChunkedEffectStore,
	total int,
	result *CompleteRouteExecutionResult,
	now time.Time,
) error {
	if total < 1 || result == nil {
		return ErrChunkCheckpointConflict
	}
	checkpoint, checkpointErr := store.LoadChunkCheckpoint(ctx, claim, now)
	if checkpointErr != nil {
		return checkpointErr
	}
	final, err := store.LoadPreparedChunk(ctx, claim, total-1, now)
	if err != nil || final.Ordinal != total-1 || final.TotalChunks != total || !final.InventoryComplete {
		if err != nil {
			return err
		}
		return ErrChunkCheckpointConflict
	}
	result.Fetch, result.Result, result.Watermark = final.Evidence, final.Result, final.Watermark
	result.Comparison = final.Comparison
	result.Comparison.NativeRecords = int(checkpoint.CommittedRows)
	result.Comparison.PythonRecords = int(checkpoint.CommittedRows)
	result.CommittedRows = checkpoint.CommittedRows
	result.WorklogObservations = final.WorklogObservations
	return nil
}

// resetEmissionChunks clears the per-emission metadata a split inherits and
// stamps the emission's cursors on its first and last sub-chunk. The streaming
// executor and the routes that pre-check a chunk's persisted size share it.
func resetEmissionChunks(chunks []PreparedProviderChunk, emission ChunkRouteEmission) {
	for index := range chunks {
		chunks[index].TotalChunks = 0
		chunks[index].CursorBefore = ""
		chunks[index].CursorAfter = ""
		chunks[index].InventoryComplete = false
		chunks[index].Result = nil
		chunks[index].Watermark = nil
		chunks[index].Evidence = FetchEvidence{}
		chunks[index].Comparison = ShadowComparison{}
		chunks[index].WorklogObservations = nil
	}
	chunks[0].CursorBefore = emission.CursorBefore
	chunks[len(chunks)-1].CursorAfter = emission.CursorAfter
}

// sizeEmissionChunks stamps each sub-chunk with its encoded payload size.
func sizeEmissionChunks(chunks []PreparedProviderChunk) error {
	for index := range chunks {
		encoded, err := encodedPreparedChunkPayload(chunks[index])
		if err != nil {
			return err
		}
		chunks[index].PayloadBytes = len(encoded)
	}
	return nil
}

// coalesceChunkEmissions merges next into the pending coalesced group when both
// are coalescable and non-final, next starts at the cursor the group ends at,
// every shared destination has one recovery policy, and the merged rows still
// fit ONE sub-chunk of the policy. The merged emission keeps the group's
// CursorBefore and takes next's CursorAfter.
func coalesceChunkEmissions(
	claim Claim, policy ChunkPolicy, pending, next ChunkRouteEmission, now time.Time,
) (ChunkRouteEmission, bool) {
	if !pending.Coalescable || !next.Coalescable || pending.Final || next.Final ||
		next.CursorBefore == "" || next.CursorBefore != pending.CursorAfter {
		return ChunkRouteEmission{}, false
	}
	rows := map[string][]json.RawMessage{}
	recovery := map[string]EffectRecoveryPolicy{}
	order := make([]string, 0, len(pending.Batch.Effects)+len(next.Batch.Effects))
	for _, effect := range append(append([]EffectBatch(nil), pending.Batch.Effects...), next.Batch.Effects...) {
		if len(effect.MembershipRejections) > 0 {
			return ChunkRouteEmission{}, false
		}
		known, seen := recovery[effect.Destination]
		if !seen {
			recovery[effect.Destination] = effect.Recovery
			order = append(order, effect.Destination)
		} else if known != effect.Recovery {
			return ChunkRouteEmission{}, false
		}
		rows[effect.Destination] = append(rows[effect.Destination], effect.Rows...)
	}
	effects := make([]EffectBatch, 0, len(order))
	for _, destination := range order {
		effect, err := BuildEffectBatch(destination, recovery[destination], rows[destination])
		if err != nil {
			return ChunkRouteEmission{}, false
		}
		effects = append(effects, effect)
	}
	merged := ChunkRouteEmission{
		Batch: CompleteRouteBatch{Effects: effects}, CursorBefore: pending.CursorBefore,
		CursorAfter: next.CursorAfter, Coalescable: true,
	}
	if !coalescedEmissionFits(claim, policy, merged, now) {
		return ChunkRouteEmission{}, false
	}
	return merged, true
}

// coalescedEmissionFits reports whether a non-final emission is ONE sub-chunk
// of the policy that the chunk store accepts: the split, the size stamp and the
// store's own material builder run on it, so no estimate decides.
func coalescedEmissionFits(claim Claim, policy ChunkPolicy, emission ChunkRouteEmission, now time.Time) bool {
	if len(emission.Batch.Effects) == 0 {
		return false
	}
	batch := emission.Batch
	batch.Result, batch.Watermark = nil, nil
	batch.Evidence = FetchEvidence{}
	batch.WorklogObservations = nil
	chunks, err := splitCompleteRouteBatch(batch, ShadowComparison{}, policy)
	if err != nil || len(chunks) != 1 {
		return false
	}
	resetEmissionChunks(chunks, emission)
	if sizeEmissionChunks(chunks) != nil || chunks[0].PayloadBytes > policy.MaxPreparedBytes {
		return false
	}
	_, err = buildPreparedChunkMaterial(claim, chunks[0], now)
	return err == nil
}

package providersync

import (
	"context"
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
	var result CompleteRouteExecutionResult
	err := session.Run(ctx, executor.HeartbeatInterval, func(
		workContext context.Context,
		guard providerfoundation.LeaseGuard,
	) error {
		now := executor.now()
		attemptStarted := time.Now()
		checkpoint, checkpointErr := store.LoadChunkCheckpoint(workContext, session.Claim, now)
		if checkpointErr != nil && !errors.Is(checkpointErr, ErrChunkCheckpointNotFound) {
			return checkpointErr
		}
		if errors.Is(checkpointErr, ErrChunkCheckpointNotFound) {
			checkpoint = ChunkCheckpoint{}
		}
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
			if err := store.MarkChunkCommitted(workContext, session.Claim, ordinal, chunk.ManifestDigest, executor.now()); err != nil {
				return err
			}
			committedThisAttempt++
			if committedThisAttempt >= policy.MaxChunksPerAttempt && ordinal+1 < checkpoint.PreparedChunks {
				return ChunkContinuationError{Next: executor.now().Add(time.Second)}
			}
		}
		checkpoint, checkpointErr = store.LoadChunkCheckpoint(workContext, session.Claim, executor.now())
		if checkpointErr != nil && !errors.Is(checkpointErr, ErrChunkCheckpointNotFound) {
			return checkpointErr
		}
		if errors.Is(checkpointErr, ErrChunkCheckpointNotFound) {
			checkpoint = ChunkCheckpoint{}
		}
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
		streamErr := handler.CollectChunks(
			workContext, session.Claim, credential, client, checkpointTime(checkpoint, now), checkpoint.NextCursor,
			func(emission ChunkRouteEmission) error {
				if err := emission.Batch.validate(descriptor); err != nil {
					return err
				}
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
				last := len(chunks) - 1
				if last < 0 {
					return ErrChunkPolicyExceeded
				}
				chunks[0].CursorBefore = emission.CursorBefore
				chunks[last].CursorAfter = emission.CursorAfter
				if emission.Final {
					chunks[last].InventoryComplete = true
					chunks[last].Result = cloneChunkResult(batch.Result)
					chunks[last].Watermark = batch.Watermark
					chunks[last].Evidence = batch.Evidence
					chunks[last].Comparison = finalComparison
					chunks[last].WorklogObservations = append([]JiraWorklogFetchObservation(nil), batch.WorklogObservations...)
				}
				for index := range chunks {
					encoded, encodeErr := encodedPreparedChunkPayload(chunks[index])
					if encodeErr != nil {
						return encodeErr
					}
					chunks[index].PayloadBytes = len(encoded)
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
				for _, prepared := range preparedGroup {
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
					if err := store.MarkChunkCommitted(workContext, session.Claim, prepared.Ordinal, prepared.ManifestDigest, executor.now()); err != nil {
						return err
					}
					nextOrdinal++
					committedThisAttempt++
				}
				if !emission.Final && (committedThisAttempt >= policy.MaxChunksPerAttempt || time.Since(attemptStarted) >= policy.MaxWallTime) {
					return ChunkContinuationError{Next: executor.now().Add(time.Second)}
				}
				return nil
			},
		)
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
	result.WorklogObservations = final.WorklogObservations
	return nil
}

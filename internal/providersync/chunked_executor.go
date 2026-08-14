package providersync

import (
	"context"
	"errors"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func (executor CompleteRouteExecutor) executeChunked(
	ctx context.Context,
	session *LeaseSession,
	descriptor CompleteRouteDescriptor,
) (CompleteRouteExecutionResult, error) {
	if streamer, ok := executor.Handler.(ChunkedCompleteRouteHandler); ok {
		return executor.executeChunkedStreaming(ctx, session, descriptor, streamer)
	}
	store, ok := executor.Committer.Ledger.(ChunkedEffectStore)
	if !ok || store == nil || executor.Committer.Sink == nil {
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
		checkpoint, checkpointErr := store.LoadChunkCheckpoint(
			workContext, session.Claim, now,
		)
		var totalChunks int
		if checkpointErr == nil {
			if checkpoint.TotalChunks < 1 || checkpoint.PreparedChunks != checkpoint.TotalChunks {
				return ErrChunkCheckpointConflict
			}
			totalChunks = checkpoint.TotalChunks
		} else if !errors.Is(checkpointErr, ErrChunkCheckpointNotFound) {
			return checkpointErr
		} else {
			credential, resolveErr := executor.Credentials.Resolve(
				workContext, guard, session.Claim.TenantScope(),
			)
			if resolveErr != nil {
				return resolveErr
			}
			if executor.EffectsFactory != nil {
				executor.Committer.Sink, executor.Committer.Readback, resolveErr =
					executor.EffectsFactory(credential)
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
			batch, collectErr := executor.Handler.Collect(
				workContext, session.Claim, credential, client, now,
			)
			if collectErr != nil {
				return collectErr
			}
			batch.Result, batch.Watermark, collectErr = applyGitHubWorkItemsIncompletePolicy(
				session.Claim.Provider, session.Claim.Dataset, batch.Result, batch.Watermark,
			)
			if collectErr != nil {
				return collectErr
			}
			if err := batch.validate(descriptor); err != nil {
				return err
			}
			comparison, compareErr := executor.Comparator.CompareCompleteRoute(
				workContext, session.Claim, batch,
			)
			if compareErr != nil {
				return compareErr
			}
			if !comparison.Match {
				return ErrShadowMismatch
			}
			chunks, collectErr := SplitCompleteRouteBatchWithComparison(
				batch, comparison, policy,
			)
			if collectErr != nil {
				return collectErr
			}
			totalChunks = len(chunks)
			for index := range chunks {
				chunks[index].RouteVersion = chunkRouteVersion
				prepared, prepareErr := store.PrepareChunk(
					workContext, session.Claim, chunks[index], now,
				)
				if prepareErr != nil {
					return prepareErr
				}
				chunks[index] = prepared
			}
			checkpoint, checkpointErr = store.LoadChunkCheckpoint(
				workContext, session.Claim, executor.now(),
			)
			if checkpointErr != nil {
				return checkpointErr
			}
		}

		if totalChunks == 0 {
			return ErrChunkCheckpointConflict
		}
		startOrdinal := checkpoint.NextOrdinal
		if startOrdinal < 0 || startOrdinal > totalChunks {
			return ErrChunkCheckpointConflict
		}
		committedThisAttempt := 0
		for ordinal := startOrdinal; ordinal < totalChunks; ordinal++ {
			chunk, loadErr := store.LoadPreparedChunk(
				workContext, session.Claim, ordinal, executor.now(),
			)
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
			if err := store.MarkChunkCommitted(
				workContext, session.Claim, ordinal, chunk.ManifestDigest, executor.now(),
			); err != nil {
				return err
			}
			committedThisAttempt++
			if (committedThisAttempt >= policy.MaxChunksPerAttempt || time.Since(attemptStarted) >= policy.MaxWallTime) && ordinal+1 < totalChunks {
				return ChunkContinuationError{Next: executor.now().Add(time.Second)}
			}
		}
		if err := store.MarkInventoryComplete(workContext, session.Claim, executor.now()); err != nil {
			return err
		}
		final, loadErr := store.LoadPreparedChunk(
			workContext, session.Claim, totalChunks-1, executor.now(),
		)
		if loadErr != nil || final.Ordinal != totalChunks-1 || final.TotalChunks != totalChunks {
			if loadErr != nil {
				return loadErr
			}
			return ErrChunkCheckpointConflict
		}
		result.Fetch, result.Result, result.Watermark = final.Evidence, final.Result, final.Watermark
		result.Comparison = final.Comparison
		result.WorklogObservations = final.WorklogObservations
		return nil
	})
	return result, err
}

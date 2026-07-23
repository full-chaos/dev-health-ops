package providersync

import (
	"context"
	"sort"
	"time"
)

type EffectSink interface {
	WriteEffect(context.Context, Claim, EffectBatch) error
}

type EffectReadback interface {
	InspectEffect(context.Context, Claim, EffectBatch) (EffectInspection, error)
}

type EffectCommitResult struct {
	Written          int
	Skipped          int
	MarkedCommitted  int
	ResetForReplay   int
	IdempotentReplay int
}

type EffectCommitter struct {
	Ledger   EffectLedger
	Sink     EffectSink
	Readback EffectReadback
	Now      func() time.Time
}

func (committer EffectCommitter) now() time.Time {
	if committer.Now != nil {
		return committer.Now().UTC()
	}
	return time.Now().UTC()
}

// Commit writes a complete multi-destination route batch under a Postgres
// effect manifest. The manifest never persists provider payloads. A fresh
// process refetches the frozen unit window, rebuilds the same content digest,
// and may then reconcile a writing effect from its in-memory rows.
func (committer EffectCommitter) Commit(
	ctx context.Context,
	claim Claim,
	batches []EffectBatch,
) (EffectCommitResult, error) {
	if ctx == nil || claim.Validate() != nil || committer.Ledger == nil ||
		committer.Sink == nil {
		return EffectCommitResult{}, ErrInvalidConfiguration
	}
	ordered := append([]EffectBatch(nil), batches...)
	sort.Slice(ordered, func(left, right int) bool {
		return ordered[left].Destination < ordered[right].Destination
	})
	desired, err := NewEffectLedgerState(claim, ordered, committer.now())
	if err != nil {
		return EffectCommitResult{}, err
	}
	persisted, err := committer.Ledger.PrepareEffects(
		ctx, claim, desired, committer.now(),
	)
	if err != nil {
		return EffectCommitResult{}, err
	}
	var result EffectCommitResult
	for index, batch := range ordered {
		effect := &persisted.Effects[index]
		switch effect.Status {
		case GenerationBlockCommitted:
			result.Skipped++
			continue
		case GenerationBlockWriting:
			switch effect.Recovery {
			case EffectReplaySafe:
				if err := committer.Ledger.ResolveEffect(
					ctx, claim, effect.Index, effect.ContentDigest,
					GenerationBlockRetryPending, committer.now(),
				); err != nil {
					return result, err
				}
				effect.Status = GenerationBlockPending
				effect.StartedAt = nil
				result.IdempotentReplay++
			case EffectReadbackRequired:
				if committer.Readback == nil {
					return result, ErrEffectRecoveryAmbiguous
				}
				inspection, inspectErr := committer.Readback.InspectEffect(
					ctx, claim, batch,
				)
				if inspectErr != nil {
					return result, inspectErr
				}
				switch inspection {
				case EffectExact:
					if err := committer.Ledger.ResolveEffect(
						ctx, claim, effect.Index, effect.ContentDigest,
						GenerationBlockMarkCommitted, committer.now(),
					); err != nil {
						return result, err
					}
					effect.Status = GenerationBlockCommitted
					result.MarkedCommitted++
					continue
				case EffectAbsent:
					if err := committer.Ledger.ResolveEffect(
						ctx, claim, effect.Index, effect.ContentDigest,
						GenerationBlockRetryPending, committer.now(),
					); err != nil {
						return result, err
					}
					effect.Status = GenerationBlockPending
					effect.StartedAt = nil
					result.ResetForReplay++
				case EffectConflict:
					return result, ErrEffectRecoveryAmbiguous
				default:
					return result, ErrInvalidConfiguration
				}
			case EffectRecoveryBlocked:
				return result, ErrEffectRecoveryAmbiguous
			default:
				return result, ErrEffectLedgerConflict
			}
		case GenerationBlockPending:
		default:
			return result, ErrEffectLedgerConflict
		}
		if err := committer.Ledger.BeginEffect(
			ctx, claim, effect.Index, effect.ContentDigest, committer.now(),
		); err != nil {
			return result, err
		}
		if err := committer.Sink.WriteEffect(ctx, claim, batch); err != nil {
			return result, err
		}
		if err := committer.Ledger.CommitEffect(
			ctx, claim, effect.Index, effect.ContentDigest, committer.now(),
		); err != nil {
			return result, err
		}
		result.Written++
	}
	return result, nil
}

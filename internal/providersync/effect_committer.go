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
//
// normalizedAt is the single instant the caller stamped into these rows. It
// becomes the persisted ledger CreatedAt, so a later attempt that reloads the
// ledger rebuilds byte-identical rows and reproduces the same digest. Reading
// a fresh clock here instead would persist a *different* instant than the one
// the rows were built with, and every retry would regenerate the digest and be
// rejected by PrepareEffects — the wedge this parameter exists to prevent.
// The committer's own clock still stamps the begin/commit audit transitions,
// which are not part of any digest.
func (committer EffectCommitter) Commit(
	ctx context.Context,
	claim Claim,
	batches []EffectBatch,
	normalizedAt time.Time,
) (EffectCommitResult, error) {
	if ctx == nil || claim.Validate() != nil || committer.Ledger == nil ||
		committer.Sink == nil || normalizedAt.IsZero() {
		return EffectCommitResult{}, ErrInvalidConfiguration
	}
	normalizedAt = normalizedAt.UTC()
	ordered := append([]EffectBatch(nil), batches...)
	sort.Slice(ordered, func(left, right int) bool {
		return effectBatchLess(ordered[left], ordered[right])
	})
	desired, err := NewEffectLedgerState(claim, ordered, normalizedAt)
	if err != nil {
		return EffectCommitResult{}, err
	}
	persisted, err := committer.Ledger.PrepareEffects(
		ctx, claim, desired, normalizedAt,
	)
	if err != nil {
		return EffectCommitResult{}, err
	}
	return committer.commitPrepared(ctx, claim, ordered, persisted)
}

// CommitPrepared continues an effect commit after the ledger and an optional
// exact recovery snapshot were durably prepared together. It deliberately
// does not call PrepareEffects again: doing so would split the atomic prepare
// boundary that the snapshot sidecar exists to provide.
func (committer EffectCommitter) CommitPrepared(
	ctx context.Context,
	claim Claim,
	batches []EffectBatch,
	persisted EffectLedgerState,
) (EffectCommitResult, error) {
	if ctx == nil || claim.Validate() != nil || committer.Ledger == nil ||
		committer.Sink == nil || persisted.validate() != nil {
		return EffectCommitResult{}, ErrInvalidConfiguration
	}
	ordered := append([]EffectBatch(nil), batches...)
	sort.Slice(ordered, func(left, right int) bool {
		return ordered[left].Destination < ordered[right].Destination
	})
	desired, err := NewEffectLedgerState(claim, ordered, persisted.CreatedAt)
	if err != nil || !sameEffectEntries(persisted, desired) {
		return EffectCommitResult{}, ErrEffectLedgerConflict
	}
	return committer.commitPrepared(ctx, claim, ordered, persisted)
}

func (committer EffectCommitter) commitPrepared(
	ctx context.Context,
	claim Claim,
	ordered []EffectBatch,
	persisted EffectLedgerState,
) (EffectCommitResult, error) {
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

func effectBatchLess(left, right EffectBatch) bool {
	leftPriority := effectCommitPriority(left.Destination)
	rightPriority := effectCommitPriority(right.Destination)
	if leftPriority != rightPriority {
		return leftPriority < rightPriority
	}
	return left.Destination < right.Destination
}

func effectCommitPriority(destination string) int {
	// GitHub blame path identity must be durable before any blame row can be
	// accepted. A later retry can then reconstruct the exact in-flight batch
	// before coverage excludes those accepted rows.
	if destination == "github_blame_path_progress" {
		return -1
	}
	return 0
}

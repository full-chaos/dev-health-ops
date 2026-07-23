package providersync

import (
	"context"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type ReconciliationResult struct {
	MarkedCommitted int
	ResetPending    int
	AlreadySafe     int
}

// OperatorReconciler is an explicit recovery tool for journal blocks left in
// writing after a process exit. It never guesses: exact destination content is
// committed, wholly absent content is reset for retry, and mixed/conflicting
// content remains blocked for human investigation.
type OperatorReconciler struct {
	Journal  GenerationJournal
	Readback providerfoundation.GenerationBlockReadback
	Now      func() time.Time
}

func (reconciler OperatorReconciler) now() time.Time {
	if reconciler.Now != nil {
		return reconciler.Now().UTC()
	}
	return time.Now().UTC()
}

func (reconciler OperatorReconciler) Reconcile(
	ctx context.Context,
	claim Claim,
) (ReconciliationResult, error) {
	if ctx == nil || claim.Validate() != nil || reconciler.Journal == nil ||
		reconciler.Readback == nil {
		return ReconciliationResult{}, ErrInvalidConfiguration
	}
	persisted, err := reconciler.Journal.Load(ctx, claim, reconciler.now())
	if err != nil {
		return ReconciliationResult{}, err
	}
	blocks, err := persisted.recoveryBlocks()
	if err != nil {
		return ReconciliationResult{}, err
	}
	var result ReconciliationResult
	for index, block := range blocks {
		switch persisted.Blocks[index].Status {
		case GenerationBlockPending, GenerationBlockCommitted:
			result.AlreadySafe++
			continue
		case GenerationBlockWriting:
		default:
			return result, ErrGenerationJournalConflict
		}
		inspection, err := reconciler.Readback.InspectGenerationBlock(ctx, block)
		if err != nil {
			return result, err
		}
		switch inspection {
		case providerfoundation.GenerationBlockExact:
			err = reconciler.Journal.ResolveBlock(
				ctx, claim, block.Index(), block.ContentDigest(),
				GenerationBlockMarkCommitted, reconciler.now(),
			)
			if err == nil {
				result.MarkedCommitted++
			}
		case providerfoundation.GenerationBlockAbsent:
			err = reconciler.Journal.ResolveBlock(
				ctx, claim, block.Index(), block.ContentDigest(),
				GenerationBlockRetryPending, reconciler.now(),
			)
			if err == nil {
				result.ResetPending++
			}
		case providerfoundation.GenerationBlockConflict:
			return result, ErrGenerationBlockAmbiguous
		default:
			return result, ErrInvalidConfiguration
		}
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type staticGenerationReadback struct {
	inspection providerfoundation.GenerationBlockInspection
	err        error
}

func (readback staticGenerationReadback) InspectGenerationBlock(
	context.Context,
	providerfoundation.GenerationBlock,
) (providerfoundation.GenerationBlockInspection, error) {
	return readback.inspection, readback.err
}

func TestOperatorReconcilerResolvesOnlyExactOrWhollyAbsentWritingBlock(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	blocks, err := providerfoundation.BuildGenerationBlocks(
		nativeTestClaim("github", "repo-metadata").GenerationKey(),
		"provider_records",
		[]providerfoundation.NormalizedEnvelope{{
			SchemaVersion: "v1", Provider: "github", OrgID: "org-acme",
			IntegrationID: firstIntegrationID, EntityType: "repository",
			SourceID:   "github:repo:acme/api",
			DedupeKey:  "github:repository:github:repo:acme/api",
			ObservedAt: now,
			Provenance: providerfoundation.Provenance{
				Source: "github_rest", Confidence: "1.0",
			},
			Attributes: map[string]string{"name": "acme/api"},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name       string
		inspection providerfoundation.GenerationBlockInspection
		wantStatus GenerationBlockStatus
		wantResult ReconciliationResult
		wantErr    error
	}{
		{
			name:       "exact destination block commits journal",
			inspection: providerfoundation.GenerationBlockExact,
			wantStatus: GenerationBlockCommitted,
			wantResult: ReconciliationResult{MarkedCommitted: 1},
		},
		{
			name:       "wholly absent destination block resets retry",
			inspection: providerfoundation.GenerationBlockAbsent,
			wantStatus: GenerationBlockPending,
			wantResult: ReconciliationResult{ResetPending: 1},
		},
		{
			name:       "mixed destination block remains blocked",
			inspection: providerfoundation.GenerationBlockConflict,
			wantStatus: GenerationBlockWriting,
			wantErr:    ErrGenerationBlockAmbiguous,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			state, err := NewGenerationJournalState(blocks, now)
			if err != nil {
				t.Fatal(err)
			}
			state.Blocks[0].Status = GenerationBlockWriting
			state.Blocks[0].StartedAt = &now
			journal := &memoryGenerationJournal{state: state}
			reconciler := OperatorReconciler{
				Journal:  journal,
				Readback: staticGenerationReadback{inspection: test.inspection},
				Now:      func() time.Time { return now.Add(time.Minute) },
			}
			result, err := reconciler.Reconcile(
				context.Background(),
				nativeTestClaim("github", "repo-metadata"),
				blocks,
			)
			if !errors.Is(err, test.wantErr) || result != test.wantResult ||
				journal.state.Blocks[0].Status != test.wantStatus {
				t.Fatalf(
					"result=%+v error=%v status=%s",
					result, err, journal.state.Blocks[0].Status,
				)
			}
			if test.wantStatus == GenerationBlockPending &&
				journal.state.Blocks[0].StartedAt != nil {
				t.Fatal("safe retry retained stale writing timestamp")
			}
		})
	}
}

var _ providerfoundation.GenerationBlockReadback = staticGenerationReadback{}

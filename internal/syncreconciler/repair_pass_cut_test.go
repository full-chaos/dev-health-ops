package syncreconciler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"

	"github.com/jackc/pgx/v5"
)

func orphanedPassCandidates(n int) []orphanedCandidate {
	candidates := make([]orphanedCandidate, 0, n)
	for index := 0; index < n; index++ {
		id := "e784daf9-592c-5fca-b6f7-81d2c9de940" + string(rune('1'+index))
		candidates = append(candidates, orphanedCandidate{
			outboxID: id, dedupeKey: jobcontract.KindSyncProviderUnit + ":" + id, unitID: id,
			runID: "1410329c-51aa-5895-89c7-b36442da361e", riverJobID: int64(101 + index),
			disposition: dispositionOrphanMissing,
		})
	}
	return candidates
}

func orphanedPassLine(t *testing.T, repair *OrphanedUnitRepair) map[string]any {
	t.Helper()
	captured, restore := captureSlogRecords(t)
	defer restore()
	_, _ = repair.Step(context.Background(), time.Now(), 50)
	line, found := findSlogRecord(*captured, "syncreconciler.orphaned_unit_pass")
	if !found {
		t.Fatal("no orphaned_unit_pass line")
	}
	return line
}

type failingLockTx struct{ pgx.Tx }

type failingRow struct{}

func (failingRow) Scan(...any) error { return errors.New("lock wait cut by the stage budget") }

func (failingLockTx) QueryRow(context.Context, string, ...any) pgx.Row { return failingRow{} }

func (failingLockTx) Rollback(context.Context) error { return nil }

// CHAOS-14238 acp cell 3: the pass line of a failed orphaned-unit pass must
// differ from an idle pass, and say where it failed and how much was left.
// Before it, a survey cut by the budget logged orphaned_units_found=0 with
// every counter zero: the idle line, byte for byte (RECORD-291-s291a-4).
func TestOrphanedUnitPassLineNamesAFailedPass(t *testing.T) {
	idle := orphanedPassLine(t, &OrphanedUnitRepair{
		queryQueue: func(context.Context, string, ...any) (pgx.Rows, error) {
			return &partialSurveyRows{}, nil
		},
		beginDomain: func(context.Context) (pgx.Tx, error) { return nil, errors.New("unused") },
		surveyQuery: "select 1", staleAge: time.Minute,
	})
	if idle["ran"] != true || idle["failed_step"] != "" || idle["left"] != int64(0) {
		t.Fatalf("idle pass line = %v, want ran=true failed_step=\"\" left=0", idle)
	}

	surveyCut := orphanedPassLine(t, &OrphanedUnitRepair{
		queryQueue: func(context.Context, string, ...any) (pgx.Rows, error) {
			return &partialSurveyRows{remaining: orphanedPassCandidates(2), failWith: context.DeadlineExceeded}, nil
		},
		beginDomain: func(context.Context) (pgx.Tx, error) { return nil, errors.New("unused") },
		surveyQuery: "select 1", staleAge: time.Minute,
	})
	if surveyCut["ran"] != false || surveyCut["failed_step"] != "survey_rows" {
		t.Fatalf("survey-cut pass line = %v, want ran=false failed_step=survey_rows", surveyCut)
	}

	rearmCut := orphanedPassLine(t, &OrphanedUnitRepair{
		queryQueue: func(context.Context, string, ...any) (pgx.Rows, error) {
			return &partialSurveyRows{remaining: orphanedPassCandidates(2)}, nil
		},
		beginDomain: func(context.Context) (pgx.Tx, error) { return failingLockTx{}, nil },
		surveyQuery: "select 1", staleAge: time.Minute,
	})
	if rearmCut["ran"] != false || rearmCut["failed_step"] != "unit_lock" || rearmCut["left"] != int64(2) {
		t.Fatalf("re-arm-cut pass line = %v, want ran=false failed_step=unit_lock left=2 (both rolled back)", rearmCut)
	}
}

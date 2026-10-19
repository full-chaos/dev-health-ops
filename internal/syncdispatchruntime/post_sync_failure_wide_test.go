package syncdispatchruntime

import (
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// A wide error whose first 70 branches are one repeated leaf must not hide the stage and the SQLSTATE that come after them.
func TestStageAndSQLStateSurviveAWideRepeatedBranchTree(t *testing.T) {
	leaf := errors.New("repeated")
	branches := make([]error, 0, 72)
	for range 70 {
		branches = append(branches, leaf)
	}
	branches = append(branches, unavailable(stageDaily, nil), &pgconn.PgError{Code: "23505", Message: "dup"})
	wide := errors.Join(branches...)

	if got := postSyncStageOf(wide); got != stageDaily {
		t.Fatalf("stage = %q, want %q", got, stageDaily)
	}
	if detail := failureDetail(wide); !strings.Contains(detail, " stage=daily") || !strings.Contains(detail, " code=23505") {
		t.Fatalf("failureDetail = %q, want stage=daily and code=23505", detail)
	}
}

// A self-unwrapping error beside the staged one must not use up the walk.
func TestStageSurvivesACyclicSiblingTree(t *testing.T) {
	cycle := &cyclicStageError{}
	branches := make([]error, 0, 66)
	for range 65 {
		branches = append(branches, cycle)
	}
	branches = append(branches, unavailable(stageDora, nil))
	if got := postSyncStageOf(errors.Join(branches...)); got != stageDora {
		t.Fatalf("stage = %q, want %q", got, stageDora)
	}
}

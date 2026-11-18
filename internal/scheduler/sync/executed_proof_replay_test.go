package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

type committedUnitRows struct {
	pgx.Rows
	rows [][2]string
	next int
}

func (rows *committedUnitRows) Next() bool { rows.next++; return rows.next <= len(rows.rows) }
func (rows *committedUnitRows) Scan(dest ...any) error {
	row := rows.rows[rows.next-1]
	if len(dest) == 2 {
		*dest[0].(*string) = row[0]
		*dest[1].(*string) = row[1]
	}
	return nil
}
func (rows *committedUnitRows) Err() error { return nil }
func (rows *committedUnitRows) Close()     {}

type committedUnitReader struct {
	runExists bool
	units     [][2]string
	errOn     string
	queries   []string
}

func (reader *committedUnitReader) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	reader.queries = append(reader.queries, sql)
	if sql == reader.errOn {
		return nil, errors.New("boom")
	}
	if sql == committedUnitsRunSQL {
		if reader.runExists {
			return &committedUnitRows{rows: [][2]string{{}}}, nil
		}
		return &committedUnitRows{}, nil
	}
	return &committedUnitRows{rows: reader.units}, nil
}

func TestLoadCommittedUnitsReadsTheUnitsOfTheRun(t *testing.T) {
	got, err := loadCommittedUnits(context.Background(), &committedUnitReader{
		runExists: true, units: [][2]string{{"a", "cicd"}, {"b", "security"}},
	}, "run")
	if err != nil || got == nil || !got.holds("a", "cicd") || !got.holds("b", "security") || got.holds("a", "security") {
		t.Fatalf("committed = %+v err %v, want a/cicd and b/security only", got, err)
	}

	got, err = loadCommittedUnits(context.Background(), &committedUnitReader{runExists: true}, "run")
	if err != nil || got == nil || got.holds("a", "cicd") {
		t.Fatalf("committed = %+v err %v, want a replay that holds no unit", got, err)
	}

	first := &committedUnitReader{units: [][2]string{{"a", "cicd"}}}
	if got, err = loadCommittedUnits(context.Background(), first, "run"); err != nil || got != nil || len(first.queries) != 1 {
		t.Fatalf("first attempt = %+v err %v queries %d, want nil after one query", got, err, len(first.queries))
	}
}

// A failed read fails the attempt with a plain error, never ErrInvalidPlan: the occurrence is retried, not quarantined.
func TestLoadCommittedUnitsReadFailureIsRetryable(t *testing.T) {
	for _, failing := range []string{committedUnitsRunSQL, committedUnitsSQL} {
		reader := &committedUnitReader{runExists: true, errOn: failing}
		got, err := loadCommittedUnits(context.Background(), reader, "run")
		if err == nil || got != nil || errors.Is(err, ErrInvalidPlan) {
			t.Fatalf("read of %q: committed %+v err %v, want a plain error", failing, got, err)
		}
	}
}

func replayCommitted(pairs ...[2]string) *CommittedUnits {
	committed := &CommittedUnits{}
	for _, pair := range pairs {
		committed.add(pair[0], pair[1])
	}
	return committed
}

// On a replay the executed-proof gate reads the committed graph, at the plain, fold and work-items sites of both
// planners, waiver or not; a first attempt still reads the waiver and the snapshot.
func TestPlannerExecutedProofReplayKeepsTheCommittedUnitSet(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	since, before := now.Add(-48*time.Hour), now
	unproven := func(dataset string) *providersync.ExecutedProofEvidence {
		return &providersync.ExecutedProofEvidence{Proven: map[string]bool{}, Attempted: map[string]bool{"github/" + dataset: true}}
	}
	proven := func(dataset string) *providersync.ExecutedProofEvidence {
		return &providersync.ExecutedProofEvidence{Proven: map[string]bool{"github/" + dataset: true}, Attempted: map[string]bool{"github/" + dataset: true}}
	}
	degraded := &providersync.ExecutedProofEvidence{Degraded: true}
	for _, backfill := range []bool{false, true} {
		for _, test := range []struct {
			name, dataset, unitDataset string
			evidence                   *providersync.ExecutedProofEvidence
			committed                  *CommittedUnits
			want                       bool
		}{
			{"first attempt, attempted and unproven", "security", "security", unproven("security"), nil, false},
			{"replay holds the unit, attempted and unproven", "security", "security", unproven("security"), replayCommitted([2]string{"source", "security"}), true},
			{"replay holds the unit, degraded snapshot", "security", "security", degraded, replayCommitted([2]string{"source", "security"}), true},
			{"replay holds no unit, proven", "security", "security", proven("security"), replayCommitted(), false},
			{"replay holds the unit of another source", "security", "security", proven("security"), replayCommitted([2]string{"other", "security"}), false},
			{"fold: replay holds the unit, attempted and unproven", "pr-reviews", "prs", unproven("prs"), replayCommitted([2]string{"source", "prs"}), true},
			{"fold: replay holds no unit, proven", "pr-reviews", "prs", proven("prs"), replayCommitted(), false},
			{"waiver: first attempt", "repo-metadata", "repo-metadata", unproven("repo-metadata"), nil, true},
			{"waiver: replay holds the unit", "repo-metadata", "repo-metadata", unproven("repo-metadata"), replayCommitted([2]string{"source", "repo-metadata"}), true},
			{"waiver: replay holds no unit", "repo-metadata", "repo-metadata", unproven("repo-metadata"), replayCommitted(), false},
			{"work-items waiver: replay holds the unit", "work-items", "work-items", unproven("work-items"), replayCommitted([2]string{"source", "work-items"}), true},
			{"work-items waiver: replay holds no unit", "work-items", "work-items", unproven("work-items"), replayCommitted(), false},
		} {
			name := test.name
			if backfill {
				name = "backfill: " + name
			}
			t.Run(name, func(t *testing.T) {
				input := PlannerInput{
					OrgID: "org", IntegrationID: "integration", Mode: SyncModeIncremental, Now: now,
					Sources:        []PlanSource{{ID: "source", ExternalID: "owner/repo", Provider: "github", FullName: "owner/repo"}},
					Datasets:       []PlanDataset{{Key: test.dataset}},
					ExecutedProof:  test.evidence,
					CommittedUnits: test.committed,
				}
				var units []PlannedUnit
				var err error
				if backfill {
					input.Mode, input.Since, input.Before = SyncModeBackfill, &since, &before
					units, err = BuildBackfillPlan(input)
				} else {
					units, err = BuildScheduledPlan(input)
				}
				if err != nil {
					t.Fatal(err)
				}
				planned := false
				for _, unit := range units {
					if unit.Dataset == test.unitDataset {
						planned = true
					}
				}
				if planned != test.want {
					t.Fatalf("units %+v: %s planned %v, want %v", units, test.unitDataset, planned, test.want)
				}
			})
		}
	}
}

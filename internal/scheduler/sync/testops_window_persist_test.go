package sync

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// wireUnitTx is fakeUnitTx with every stored window end passed through pgx's binary timestamptz codec, which keeps
// microseconds as PostgreSQL does. The verifier then compares the stored values with the plan exactly.
type wireUnitTx struct {
	*fakeUnitTx
	t *testing.T
}

func (tx *wireUnitTx) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	results := tx.fakeUnitTx.SendBatch(ctx, batch)
	codec := pgtype.NewMap()
	for _, row := range tx.table {
		for _, target := range []**time.Time{&row.since, &row.before} {
			if *target == nil {
				continue
			}
			wire, err := codec.Encode(pgtype.TimestamptzOID, pgtype.BinaryFormatCode, **target, nil)
			if err != nil {
				tx.t.Fatal(err)
			}
			var decoded time.Time
			if err := codec.Scan(pgtype.TimestamptzOID, pgtype.BinaryFormatCode, wire, &decoded); err != nil {
				tx.t.Fatal(err)
			}
			*target = &decoded
		}
	}
	return results
}

const persistSource = "00000000-0000-4000-8000-000000000002"

var persistSelections = []struct {
	name string
	keys []string
}{
	{"tests", []string{"tests"}},
	{"cicd", []string{"cicd"}},
	{"both", []string{"tests", "cicd"}},
}

func persistPlan(t *testing.T, mode string, now, watermark time.Time, blocked map[string]int, replay map[string]time.Time, keys []string) PlannedUnit {
	t.Helper()
	input := PlannerInput{
		OrgID: "org", IntegrationID: "00000000-0000-4000-8000-000000000001", Mode: mode, Now: now,
		Sources:              []PlanSource{{ID: persistSource, ExternalID: "owner/repo", Provider: "github", FullName: "owner/repo"}},
		Watermarks:           map[WatermarkKey]time.Time{},
		TestOpsBudgetBlocked: blocked,
		TestOpsReplayEnds:    replay,
	}
	for _, key := range keys {
		input.Datasets = append(input.Datasets, PlanDataset{Key: key})
		input.Watermarks[WatermarkKey{SourceID: "owner/repo", Dataset: key}] = watermark
	}
	units, err := BuildScheduledPlan(input)
	if err != nil || len(units) != 1 {
		t.Fatalf("units=%+v err=%v", units, err)
	}
	return units[0]
}

// CHAOS-13827 (Codex r1 P1-1): a halved end must survive PostgreSQL's microsecond timestamptz, or the verifier refuses
// the persisted unit and the occurrence is quarantined. Odd-microsecond starts and ends, a cap, many halvings and 16
// successive hourly ticks all persist and verify.
func TestTestOpsNarrowedWindowPersistsAndVerifies(t *testing.T) {
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	whole := now.Add(-72 * time.Hour)
	for _, selection := range persistSelections {
		for _, cell := range []struct {
			name           string
			now, watermark time.Time
			blocked, ticks int
		}{
			{"odd microsecond watermark", now, whole.Add(time.Microsecond), 1, 1},
			{"odd microsecond end", now.Add(time.Microsecond), whole, 1, 1},
			{"both odd", now.Add(time.Microsecond), whole.Add(time.Microsecond), 3, 1},
			{"the floor", now, whole.Add(time.Microsecond), 12, 1},
			{"over the cap", now, now.Add(-240*time.Hour + time.Microsecond), 1, 1},
			{"ten blocks from whole minutes", now, now.Add(-65*24*time.Hour - time.Minute), 10, 1},
			{"16 successive hourly ticks", now, whole, 1, 16},
		} {
			t.Run(selection.name+"/"+cell.name, func(t *testing.T) {
				_ = captureLogs(t)
				clock, watermark := cell.now, cell.watermark
				for tick := 1; tick <= cell.ticks; tick++ {
					unit := persistPlan(t, SyncModeIncremental, clock, watermark, map[string]int{persistSource: cell.blocked}, nil, selection.keys)
					tx := &wireUnitTx{fakeUnitTx: newFakeUnitTx(), t: t}
					if err := persistPlannedUnits(context.Background(), tx, chunkTestIDs(t), []PlannedUnit{unit}, clock, unitChunkSize); err != nil {
						t.Fatalf("tick %d: planned end %s: %v", tick, unit.WindowEnd.Format(time.RFC3339Nano), err)
					}
					watermark = *unit.WindowEnd
					clock = clock.Add(time.Hour)
				}
			})
		}
	}
}

// CHAOS-13827 (Codex r1 P1-2): a replay of an occurrence after its domain graph committed keeps the committed window
// end, whatever the budget history or its read gives this time (a nil count is what a failed or skipped read gives).
// The replay plan then verifies against the committed rows. A committed end that no narrowing of the same window can
// produce is not taken: the replay plans as usual and the verifier refuses it.
func TestTestOpsReplayKeepsTheCommittedWindow(t *testing.T) {
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	count := func(n int) map[string]int {
		if n < 0 {
			return nil
		}
		return map[string]int{persistSource: n}
	}
	for _, selection := range persistSelections {
		for _, cell := range []struct {
			name          string
			first, second int
			span          time.Duration
		}{
			{"same count", 1, 1, 72 * time.Hour},
			{"a block appears", 0, 1, 72 * time.Hour},
			{"another block", 1, 2, 72 * time.Hour},
			{"fewer blocks", 2, 1, 72 * time.Hour},
			{"the last block ages out", 1, 0, 72 * time.Hour},
			{"counts past the floor", 20, 21, 72 * time.Hour},
			{"the read fails on the replay", 1, -1, 72 * time.Hour},
			{"the read recovers on the replay", -1, 1, 72 * time.Hour},
			{"natural span under the floor", 1, 0, 30 * time.Minute},
		} {
			t.Run(selection.name+"/"+cell.name, func(t *testing.T) {
				_ = captureLogs(t)
				watermark := now.Add(-cell.span)
				first := persistPlan(t, SyncModeIncremental, now, watermark, count(cell.first), nil, selection.keys)
				tx := &wireUnitTx{fakeUnitTx: newFakeUnitTx(), t: t}
				ids := chunkTestIDs(t)
				if err := persistPlannedUnits(context.Background(), tx, ids, []PlannedUnit{first}, now, unitChunkSize); err != nil {
					t.Fatal(err)
				}
				replay := persistPlan(t, SyncModeIncremental, now, watermark, count(cell.second), committedEnds(tx), selection.keys)
				if err := persistPlannedUnits(context.Background(), tx, ids, []PlannedUnit{replay}, now, unitChunkSize); err != nil {
					t.Fatalf("replay end %s, committed %s: %v", replay.WindowEnd, first.WindowEnd, err)
				}
				if !replay.WindowEnd.Equal(*first.WindowEnd) {
					t.Fatalf("replay end %s, want the committed %s", replay.WindowEnd, first.WindowEnd)
				}
			})
		}
		t.Run(selection.name+"/a committed end no narrowing produces", func(t *testing.T) {
			_ = captureLogs(t)
			watermark := now.Add(-72 * time.Hour)
			first := persistPlan(t, SyncModeIncremental, now, watermark, count(1), nil, selection.keys)
			tx := &wireUnitTx{fakeUnitTx: newFakeUnitTx(), t: t}
			ids := chunkTestIDs(t)
			if err := persistPlannedUnits(context.Background(), tx, ids, []PlannedUnit{first}, now, unitChunkSize); err != nil {
				t.Fatal(err)
			}
			foreign := first.WindowEnd.Add(-7 * time.Minute)
			for _, row := range tx.table {
				row.before = &foreign
			}
			replay := persistPlan(t, SyncModeIncremental, now, watermark, count(1), committedEnds(tx), selection.keys)
			if !replay.WindowEnd.Equal(*first.WindowEnd) {
				t.Fatalf("replay end %s, want the planned %s (the foreign end is not taken)", replay.WindowEnd, first.WindowEnd)
			}
			err := persistPlannedUnits(context.Background(), tx, ids, []PlannedUnit{replay}, now, unitChunkSize)
			if !errors.Is(err, ErrInvalidPlan) {
				t.Fatalf("replay over a foreign committed end: err=%v, want ErrInvalidPlan", err)
			}
		})
	}
}

// committedEnds is what loadTestOpsReplayEnds reads from the committed rows: the cicd unit's window end per source.
func committedEnds(tx *wireUnitTx) map[string]time.Time {
	ends := map[string]time.Time{}
	for _, row := range tx.table {
		if row.dataset == canonicalTestOpsDataset && row.before != nil {
			ends[row.sourceID] = *row.before
		}
	}
	if len(ends) == 0 {
		panic(fmt.Sprintf("no committed cicd row in %d rows", len(tx.table)))
	}
	return ends
}

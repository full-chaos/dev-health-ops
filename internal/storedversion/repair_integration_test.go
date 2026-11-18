//go:build integration

package storedversion

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const keptOrg = "org-kept"

// keptRepo is the repo_id of every cell key.
const keptRepo = "c7198fbc-1945-3717-05d8-eb78866b4e79"

// keptAt is the cell time 2026-07-23 12:00 UTC plus offset.
func keptAt(offset time.Duration) time.Time {
	return time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC).Add(offset)
}

// newKeptConn migrates a ClickHouse container through the whole chain (migration 160 included) and stops merges of
// git_pull_requests, so every inserted version stays a physical row.
func newKeptConn(t *testing.T) (context.Context, driver.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, `SYSTEM STOP MERGES git_pull_requests`); err != nil {
		t.Fatal(err)
	}
	return ctx, conn
}

// writeReview inserts one git_pull_requests version of key number with the review group (first_review_at, count).
func writeReview(ctx context.Context, t *testing.T, conn driver.Conn, number uint32, version time.Time, firstReview *time.Time, reviews uint32) {
	t.Helper()
	var reviewed any
	if firstReview != nil {
		reviewed = *firstReview
	}
	if err := conn.Exec(ctx, `INSERT INTO git_pull_requests (org_id, repo_id, number, title, state, created_at, first_review_at, reviews_count, last_synced)
VALUES (?, ?, ?, 'cell', 'open', ?, ?, ?, ?)`, keptOrg, keptRepo, number, keptAt(-time.Hour), reviewed, reviews, version); err != nil {
		t.Fatal(err)
	}
}

type reviewState struct {
	firstReview string
	reviews     uint32
	version     string
	rows        uint64
}

func readReview(ctx context.Context, t *testing.T, conn driver.Conn, number uint32) (final, kept reviewState) {
	t.Helper()
	if err := conn.QueryRow(ctx, `SELECT ifNull(toString(first_review_at), 'NULL'), reviews_count, toString(last_synced)
FROM git_pull_requests FINAL WHERE org_id = ? AND repo_id = ? AND number = ?`, keptOrg, keptRepo, number).Scan(
		&final.firstReview, &final.reviews, &final.version); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT count() FROM git_pull_requests WHERE org_id = ? AND repo_id = ? AND number = ?`,
		keptOrg, keptRepo, number).Scan(&final.rows); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT ifNull(toString(g.1), 'NULL'), g.3 FROM (
SELECT argMaxIfMerge(review) AS g FROM git_pull_request_kept WHERE org_id = ? AND repo_id = ? AND number = ?)`,
		keptOrg, keptRepo, number).Scan(&kept.firstReview, &kept.reviews); err != nil {
		t.Fatal(err)
	}
	return final, kept
}

func at(offset time.Duration) *time.Time {
	value := keptAt(offset)
	return &value
}

// hookConn runs before(n) ahead of the n-th repair insert (n from 1) and after(n) once it is done.
type hookConn struct {
	driver.Conn
	before, after func(n int)
	inserts       int
}

func (c *hookConn) Exec(ctx context.Context, query string, args ...any) error {
	if !strings.HasPrefix(query, "INSERT INTO git_pull_requests SELECT") {
		return c.Conn.Exec(ctx, query, args...)
	}
	c.inserts++
	if c.before != nil {
		c.before(c.inserts)
	}
	err := c.Conn.Exec(ctx, query, args...)
	if err == nil && c.after != nil {
		c.after(c.inserts)
	}
	return err
}

func repairKey(ctx context.Context, t *testing.T, conn Repairer, number uint32) {
	t.Helper()
	if err := PullRequestKept.Repair(ctx, conn, "cell", keptOrg, [][]any{{keptRepo, number}}, nil); err != nil {
		t.Fatalf("Repair: %v", err)
	}
}

// CHAOS-6014 cells of the repair itself, on the migrated schema (migration 160).
func TestKeptGroupRepairAgainstRealClickHouse(t *testing.T) {
	ctx, conn := newKeptConn(t)

	// The ticket's loss: held NULL v1; b states (11:30, 2) at v2 after a's read; a inserts its carry (NULL, 0) at v3.
	t.Run("a null lead over a stated group is filled at the version plus 1 ms", func(t *testing.T) {
		writeReview(ctx, t, conn, 1, keptAt(0), nil, 0)
		writeReview(ctx, t, conn, 1, keptAt(time.Hour), at(-30*time.Minute), 2)
		writeReview(ctx, t, conn, 1, keptAt(2*time.Hour), nil, 0)
		repairKey(ctx, t, conn, 1)
		final, kept := readReview(ctx, t, conn, 1)
		if final.firstReview != "2026-07-23 11:30:00.000" || final.reviews != 2 || final.version != "2026-07-23 14:00:00.001" || final.rows != 4 {
			t.Fatalf("FINAL = %+v; want (11:30, 2) at 14:00:00.001 in 4 rows", final)
		}
		if kept.firstReview != final.firstReview || kept.reviews != final.reviews {
			t.Fatalf("kept %+v != FINAL %+v", kept, final)
		}
		repairKey(ctx, t, conn, 1)
		if again, _ := readReview(ctx, t, conn, 1); again.rows != 4 {
			t.Fatalf("a second repair wrote %d rows; want none", again.rows-4)
		}
	})

	// C2: fill-null-only. A non-null FINAL lead is never replaced, even by a newer stated group; a side miss repairs nothing.
	t.Run("a non-null lead is never replaced and a kept miss repairs nothing", func(t *testing.T) {
		writeReview(ctx, t, conn, 2, keptAt(0), at(-40*time.Minute), 1)
		writeReview(ctx, t, conn, 2, keptAt(time.Hour), at(-30*time.Minute), 2)
		writeReview(ctx, t, conn, 2, keptAt(2*time.Hour), at(-40*time.Minute), 1)
		repairKey(ctx, t, conn, 2)
		final, kept := readReview(ctx, t, conn, 2)
		if final.rows != 3 || final.reviews != 1 {
			t.Fatalf("FINAL = %+v; want the carried (11:20, 1), no repair row (the named residual 1)", final)
		}
		t.Logf("CELL6014 RESIDUAL case=carry_over_non_null final=(%s,%d) kept=(%s,%d)", final.firstReview, final.reviews, kept.firstReview, kept.reviews)

		writeReview(ctx, t, conn, 3, keptAt(0), at(-30*time.Minute), 2)
		writeReview(ctx, t, conn, 3, keptAt(time.Hour), nil, 0)
		if err := conn.Exec(clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"mutations_sync": 2})),
			`ALTER TABLE git_pull_request_kept DELETE WHERE org_id = ? AND number = 3`, keptOrg); err != nil {
			t.Fatal(err)
		}
		repairKey(ctx, t, conn, 3)
		if final, _ := readReview(ctx, t, conn, 3); final.rows != 2 || final.firstReview != "NULL" {
			t.Fatalf("FINAL = %+v after a kept miss; want today's result (NULL), no repair row", final)
		}
	})

	// C12: an equal-version tie. b states and a inserts its NULL carry at the same last_synced, a last: FINAL takes a.
	t.Run("an equal version tie is repaired", func(t *testing.T) {
		writeReview(ctx, t, conn, 4, keptAt(0), nil, 0)
		writeReview(ctx, t, conn, 4, keptAt(time.Hour), at(-30*time.Minute), 2)
		writeReview(ctx, t, conn, 4, keptAt(time.Hour), nil, 0)
		repairKey(ctx, t, conn, 4)
		if final, _ := readReview(ctx, t, conn, 4); final.firstReview != "2026-07-23 11:30:00.000" || final.reviews != 2 {
			t.Fatalf("FINAL = %+v; want the tied statement (11:30, 2)", final)
		}
	})

	// C1b: repair against repair. Repair A counts the key; before A inserts, repair B runs whole (kept = S1), then b2's
	// newer statement S2 lands at a version below the key's. A's insert then finds FINAL's lead non-null and writes nothing.
	t.Run("repair against repair", func(t *testing.T) {
		writeReview(ctx, t, conn, 5, keptAt(0), at(-50*time.Minute), 1)
		writeReview(ctx, t, conn, 5, keptAt(2*time.Hour), nil, 0)
		hooked := &hookConn{Conn: conn, before: func(n int) {
			if n != 1 {
				return
			}
			repairKey(ctx, t, conn, 5)
			writeReview(ctx, t, conn, 5, keptAt(time.Hour), at(-30*time.Minute), 2)
		}}
		repairKey(ctx, t, hooked, 5)
		final, kept := readReview(ctx, t, conn, 5)
		if final.firstReview == "NULL" || final.firstReview != kept.firstReview || final.reviews != kept.reviews {
			t.Fatalf("FINAL %+v, kept %+v; want them equal and non-null", final, kept)
		}
		won := "S2"
		if final.reviews == 1 {
			won = "S1 (stale snapshot: the named residual 2)"
		}
		t.Logf("CELL6014 repair-vs-repair rounds=%d won=%s final=(%s,%d) rows=%d", hooked.inserts, won, final.firstReview, final.reviews, final.rows)
	})

	// C1c + C1d: a stale-null writer at a newer version lands after every repair. Exactly RepairRounds inserts, one WARN,
	// the counter +1, nil; then the migration's own repair of every key heals the key.
	t.Run("the bound warns, counts and returns nil, and the migration repair heals the key", func(t *testing.T) {
		reader := sdkmetric.NewManualReader()
		counter, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("cell").Int64Counter(RepairUnconvergedMetric)
		if err != nil {
			t.Fatal(err)
		}
		previous := repairUnconverged
		repairUnconverged = counter
		t.Cleanup(func() { repairUnconverged = previous })
		var logs bytes.Buffer
		previousLogger := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
		t.Cleanup(func() { slog.SetDefault(previousLogger) })

		writeReview(ctx, t, conn, 6, keptAt(0), at(-30*time.Minute), 2)
		writeReview(ctx, t, conn, 6, keptAt(time.Hour), nil, 0)
		hooked := &hookConn{Conn: conn, after: func(n int) {
			writeReview(ctx, t, conn, 6, keptAt(time.Hour+time.Duration(n)*time.Second), nil, 0)
		}}
		repairKey(ctx, t, hooked, 6)
		if hooked.inserts != RepairRounds {
			t.Fatalf("repair inserts = %d, want %d", hooked.inserts, RepairRounds)
		}
		if got := strings.Count(logs.String(), RepairUnconvergedEvent); got != 1 || !strings.Contains(logs.String(), "keys=1") {
			t.Fatalf("%d %s lines, want 1 naming keys=1:\n%s", got, RepairUnconvergedEvent, logs.String())
		}
		var data metricdata.ResourceMetrics
		if err := reader.Collect(ctx, &data); err != nil {
			t.Fatal(err)
		}
		if sum := data.ScopeMetrics[0].Metrics[0].Data.(metricdata.Sum[int64]); len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 1 {
			t.Fatalf("counter = %+v, want 1", sum.DataPoints)
		}
		chain, err := chmigrate.LoadChain()
		if err != nil {
			t.Fatal(err)
		}
		swept := false
		for _, file := range chain {
			if file.Version != "160_stored_version_kept_groups.sql" {
				continue
			}
			for _, statement := range chmigrate.SplitStatements(file.SQL) {
				if strings.HasPrefix(statement, "INSERT INTO git_pull_requests") {
					if err := conn.Exec(ctx, statement); err != nil {
						t.Fatal(err)
					}
					swept = true
				}
			}
		}
		if !swept {
			t.Fatal("migration 160 has no git_pull_requests repair statement")
		}
		if final, _ := readReview(ctx, t, conn, 6); final.firstReview != "2026-07-23 11:30:00.000" || final.reviews != 2 {
			t.Fatalf("FINAL = %+v after the migration repair; want (11:30, 2)", final)
		}
	})

	// C4b: SyncContext drops a dedup token, so a repair insert is never dropped as a duplicate of the write before it.
	t.Run("a repair insert carries no dedup token", func(t *testing.T) {
		if err := conn.Exec(ctx, `CREATE TABLE kept_token_cell (k UInt8) ENGINE = MergeTree ORDER BY k SETTINGS non_replicated_deduplication_window = 100`); err != nil {
			t.Fatal(err)
		}
		tokened := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"insert_deduplication_token": "cell-token", "insert_deduplicate": 1}))
		for _, insert := range []struct {
			ctx   context.Context
			value uint8
		}{{tokened, 1}, {tokened, 2}, {SyncContext(tokened), 3}} {
			if err := conn.Exec(insert.ctx, `INSERT INTO kept_token_cell VALUES (?)`, insert.value); err != nil {
				t.Fatal(err)
			}
		}
		var rows string
		if err := conn.QueryRow(ctx, `SELECT toString(groupArray(k)) FROM (SELECT k FROM kept_token_cell ORDER BY k)`).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != "[1,3]" {
			t.Fatalf("rows = %s; want [1,3]: the token drops 2, and the SyncContext insert lands", rows)
		}
	})
}

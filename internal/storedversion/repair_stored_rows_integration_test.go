//go:build integration

package storedversion

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Throwaway leg (do not merge): does Kept.Repair store its repair rows on both kept tables?

const (
	storedRowsOrg  = "f0f0f0f0-0000-4000-8000-000000014603"
	storedRowsRepo = "f0f0f0f0-0000-4000-8000-0000000000a1"
)

type storedRowsCase struct {
	kept   Kept
	key    []any
	where  string
	insert string
	stated []any
	null   []any
}

func storedRowsCases(variant int) []storedRowsCase {
	number := uint32(100 + variant)
	deployment := fmt.Sprintf("d-stored-%d", variant)
	return []storedRowsCase{
		{
			kept: PullRequestKept, key: []any{storedRowsRepo, number}, where: fmt.Sprintf("number = %d", number),
			insert: `INSERT INTO git_pull_requests (org_id, repo_id, number, title, state, created_at, merged_at, first_review_at, changes_requested_count, reviews_count, last_synced)
VALUES (?, ?, ?, 'cell', 'open', ?, ?, ?, ?, ?, ?)`,
			stated: []any{number, keptAt(-time.Hour), keptAt(-10 * time.Minute), keptAt(-30 * time.Minute), uint32(1), uint32(2), keptAt(0)},
			null:   []any{number, keptAt(-time.Hour), nil, nil, uint32(0), uint32(0), keptAt(time.Hour)},
		},
		{
			kept: DeploymentKept, key: []any{storedRowsRepo, deployment}, where: fmt.Sprintf("deployment_id = '%s'", deployment),
			insert: `INSERT INTO deployments (org_id, repo_id, deployment_id, status, merged_at, pull_request_number, pull_request_tier, last_synced)
VALUES (?, ?, ?, 'success', ?, ?, ?, ?)`,
			stated: []any{deployment, keptAt(-10 * time.Minute), uint32(41), "direct", keptAt(0)},
			null:   []any{deployment, nil, nil, "", keptAt(time.Hour)},
		},
	}
}

func openStoredRowsConns(t *testing.T) (context.Context, driver.Conn, driver.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	native, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = native.Close() })
	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := parsed.User.Password()
	httpConn, err := clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.HTTP,
		Addr:     []string{parsed.Host},
		Auth: clickhouse.Auth{
			Database: strings.TrimPrefix(parsed.Path, "/"),
			Username: parsed.User.Username(),
			Password: password,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = httpConn.Close() })
	for _, statement := range []string{"SYSTEM STOP MERGES git_pull_requests", "SYSTEM STOP MERGES deployments"} {
		if err := native.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	return ctx, native, httpConn
}

func storedRowsPending(t *testing.T, ctx context.Context, conn driver.Conn, c storedRowsCase) uint64 {
	t.Helper()
	filter, args := c.kept.filter(storedRowsOrg, [][]any{c.key})
	var pending uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM ("+c.kept.selectToRepair(filter)+")", append(args, args...)...).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	return pending
}

func seedStoredRows(t *testing.T, ctx context.Context, seed driver.Conn, c storedRowsCase) {
	t.Helper()
	for _, row := range [][]any{c.stated, c.null} {
		if err := seed.Exec(ctx, c.insert, append([]any{storedRowsOrg, storedRowsRepo}, row...)...); err != nil {
			t.Fatal(err)
		}
	}
	if before := storedRowsPending(t, ctx, seed, c); before == 0 {
		t.Fatalf("%s: control: pending = 0 before Repair; the cell would pass without a repair", c.kept.Base)
	}
}

// checkRepairStored seeds a stated version and a newer NULL-lead version of one key, runs repair, and asserts
// the repair row is stored: the lead from the stated version at the NULL version plus 1 ms, 3 physical rows, recount 0.
func checkRepairStored(t *testing.T, ctx context.Context, seed driver.Conn, conn Repairer, c storedRowsCase, repair func() error) {
	t.Helper()
	seedStoredRows(t, ctx, seed, c)
	if err := repair(); err != nil {
		t.Fatalf("%s: Repair returned %v", c.kept.Base, err)
	}
	where := fmt.Sprintf("org_id = '%s' AND repo_id = '%s' AND %s", storedRowsOrg, storedRowsRepo, c.where)
	var lead, version string
	var rows uint64
	if err := seed.QueryRow(ctx, `SELECT ifNull(toString(merged_at), 'NULL'), toString(last_synced) FROM `+c.kept.Base+` FINAL WHERE `+where).Scan(&lead, &version); err != nil {
		t.Fatal(err)
	}
	if err := seed.QueryRow(ctx, `SELECT count() FROM `+c.kept.Base+` WHERE `+where).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	pending := storedRowsPending(t, ctx, seed, c)
	t.Logf("STORED-ROWS %s (FINAL read): lead=%s version=%s physical_rows=%d pending=%d", c.kept.Base, lead, version, rows, pending)
	if lead != "2026-07-23 11:50:00.000" || version != "2026-07-23 13:00:00.001" || rows != 3 || pending != 0 {
		t.Errorf("%s: FINAL lead %s at %s in %d rows, pending %d; want 11:50:00.000 at 13:00:00.001 in 3 rows, pending 0",
			c.kept.Base, lead, version, rows, pending)
	}
}

func TestKeptRepairStoresItsRowsOnBothKeptTables(t *testing.T) {
	ctx, native, httpConn := openStoredRowsConns(t)
	repairWith := func(ctx context.Context, conn Repairer, c storedRowsCase) func() error {
		return func() error { return c.kept.Repair(ctx, conn, "cell", storedRowsOrg, [][]any{c.key}, nil) }
	}

	t.Run("native connection", func(t *testing.T) {
		for _, c := range storedRowsCases(1) {
			checkRepairStored(t, ctx, native, native, c, repairWith(ctx, native, c))
		}
	})

	t.Run("async_insert=1 and wait_for_async_insert=0 on the caller context", func(t *testing.T) {
		async := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"async_insert": 1, "wait_for_async_insert": 0}))
		readBack := func(ctx context.Context) string {
			var value string
			if err := native.QueryRow(ctx, `SELECT concat(toString(getSetting('async_insert')), ',', toString(getSetting('wait_for_async_insert')))`).Scan(&value); err != nil {
				t.Fatal(err)
			}
			return value
		}
		caller, repair := readBack(async), readBack(SyncContext(async))
		t.Logf("SYNCCONTEXT-OVERRIDE proven=%v caller_session=%s repair_session=%s", caller == "1,0" && repair == "0,1", caller, repair)
		for _, c := range storedRowsCases(2) {
			checkRepairStored(t, ctx, native, native, c, repairWith(async, native, c))
		}
	})

	t.Run("HTTP protocol connection with no settings", func(t *testing.T) {
		for _, c := range storedRowsCases(3) {
			checkRepairStored(t, ctx, native, httpConn, c, repairWith(ctx, httpConn, c))
		}
	})

	t.Run("no MATERIALIZED, ALIAS or EPHEMERAL column on either table", func(t *testing.T) {
		for _, table := range []string{"git_pull_requests", "deployments"} {
			var hidden uint64
			if err := native.QueryRow(ctx, `SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = ?
				AND default_kind IN ('MATERIALIZED', 'ALIAS', 'EPHEMERAL')`, table).Scan(&hidden); err != nil {
				t.Fatal(err)
			}
			t.Logf("HIDDEN-COLUMNS %s: %d", table, hidden)
			if hidden != 0 {
				t.Errorf("%s has %d MATERIALIZED/ALIAS/EPHEMERAL columns: SELECT * of the repair leaves them out", table, hidden)
			}
		}
	})

	t.Run("planted 0-row repair insert warns and counts unconverged", func(t *testing.T) {
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

		for _, c := range storedRowsCases(5) {
			seedStoredRows(t, ctx, native, c)
			planted := &plantedZeroRowConn{Conn: native, base: c.kept.Base}
			if err := c.kept.Repair(ctx, planted, "cell-planted", storedRowsOrg, [][]any{c.key}, nil); err != nil {
				t.Fatalf("%s: Repair returned %v", c.kept.Base, err)
			}
			after := storedRowsPending(t, ctx, native, c)
			t.Logf("PLANTED %s: dropped_inserts=%d pending_after=%d", c.kept.Base, planted.dropped, after)
			if planted.dropped != RepairRounds || after == 0 {
				t.Errorf("%s: dropped %d inserts, pending after %d; want %d dropped and pending > 0", c.kept.Base, planted.dropped, after, RepairRounds)
			}
			want := fmt.Sprintf("msg=%s writer=cell-planted table=%s keys=1", RepairUnconvergedEvent, c.kept.Base)
			if !strings.Contains(logs.String(), want) {
				t.Errorf("%s: no WARN line %q in:\n%s", c.kept.Base, want, logs.String())
			}
		}
		var data metricdata.ResourceMetrics
		if err := reader.Collect(ctx, &data); err != nil {
			t.Fatal(err)
		}
		seen := map[string]int64{}
		for _, scope := range data.ScopeMetrics {
			for _, m := range scope.Metrics {
				if m.Name != RepairUnconvergedMetric {
					continue
				}
				for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
					writer, _ := point.Attributes.Value(attribute.Key("writer"))
					table, _ := point.Attributes.Value(attribute.Key("table"))
					seen[writer.AsString()+"/"+table.AsString()] += point.Value
				}
			}
		}
		t.Logf("PLANTED-COUNTER %v", seen)
		for _, table := range []string{"git_pull_requests", "deployments"} {
			if seen["cell-planted/"+table] != 1 {
				t.Errorf("counter %s for cell-planted/%s = %d, want 1", RepairUnconvergedMetric, table, seen["cell-planted/"+table])
			}
		}
	})

	t.Run("twice in a row with non_replicated_deduplication_window 100 on the table", func(t *testing.T) {
		for _, table := range []string{"git_pull_requests", "deployments"} {
			if err := native.Exec(ctx, "ALTER TABLE "+table+" MODIFY SETTING non_replicated_deduplication_window = 100"); err != nil {
				t.Fatal(err)
			}
		}
		for _, c := range storedRowsCases(4) {
			checkRepairStored(t, ctx, native, native, c, func() error {
				if err := repairWith(ctx, native, c)(); err != nil {
					return err
				}
				return repairWith(ctx, native, c)()
			})
		}
	})
}

// plantedZeroRowConn returns nil from the repair INSERT and stores nothing; every other call passes through.
type plantedZeroRowConn struct {
	driver.Conn
	base    string
	dropped int
}

func (c *plantedZeroRowConn) Exec(ctx context.Context, query string, args ...any) error {
	if strings.HasPrefix(query, "INSERT INTO "+c.base+" SELECT") {
		c.dropped++
		return nil
	}
	return c.Conn.Exec(ctx, query, args...)
}

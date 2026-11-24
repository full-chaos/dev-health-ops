//go:build integration

package chmigrate_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Throwaway exec proof of the prod read form PRE-TTL-14542-2-r2 (CHAOS-14542): not for merge. preTTL14542s2Form is the
// form file byte for byte; its sha256 is pinned.
const preTTL14542s2FormSHA256 = "7ca49ba13641e5268a99414d934420795523663bb93b05d5bb222ba49484a3e9"

const preTTL14542s2Form = `-- PRE-TTL-14542-2-r2 (fcdev lane l14542, 2026-11-24): NO-THROW read of the active parts of the local SigNoz trace table, after
-- PRE-TTL-14542-1 (fe48b20a1f06) threw STOP-MIN-TIME-UNFILLED on prod. Reads system.parts only (part metadata, no data).
-- r2 = PRE-TTL-14542-2 (5442558a55c5) + 2 columns at the end: unfilled_empty_parts, unfilled_parts_rows (rows per unfilled part).
-- Output: ONE row of counts, days, and the partition ids, levels and row counts of the parts with min_time < 2020. No row data.
-- WANT active_parts > 0 (control; 0 = case (a), no active part = STOP). Other columns: read per PRE-TTL-14542-2-r2.md.
-- No SET, no SETTINGS, no throwIf, no write.
SELECT
    count() AS active_parts,
    countIf(min_time < toDateTime('2020-01-01')) AS parts_min_time_lt_2020,
    sumIf(rows, min_time < toDateTime('2020-01-01')) AS rows_in_those_parts,
    if(countIf(min_time < toDateTime('2020-01-01')) = 0, 'none', toString(toDate(maxIf(max_time, min_time < toDateTime('2020-01-01'))))) AS unfilled_max_day,
    if(countIf(min_time >= toDateTime('2020-01-01')) = 0, 'none', toString(toDate(minIf(min_time, min_time >= toDateTime('2020-01-01'))))) AS filled_min_day,
    if(countIf(min_time >= toDateTime('2020-01-01')) = 0, -1, dateDiff('day', minIf(min_time, min_time >= toDateTime('2020-01-01')), now())) AS oldest_days,
    arrayStringConcat(arraySort(groupUniqArrayIf(50)(partition_id, min_time < toDateTime('2020-01-01'))), ',') AS unfilled_partition_ids,
    arrayStringConcat(arrayMap(x -> toString(x), arraySort(groupUniqArrayIf(50)(level, min_time < toDateTime('2020-01-01')))), ',') AS unfilled_levels,
    countIf(min_time < toDateTime('2020-01-01') AND rows = 0) AS unfilled_empty_parts,
    arrayStringConcat(arraySort(groupArrayIf(50)(concat(partition_id, ':', toString(level), ':', toString(rows)), min_time < toDateTime('2020-01-01'))), ',') AS unfilled_parts_rows
FROM system.parts
WHERE database = 'signoz_traces' AND table = 'signoz_index_v3' AND active
`

// The SigNoz v3 local trace table cut to the columns of its partition key and sort key (signoz-otel-collector
// traces_migrations.go; the full DDL ran in PR #6919 ci/form14542-sqlcheck/ddl_local.sql). Same engine, PARTITION BY,
// ORDER BY, timestamp type and SETTINGS. The TTL clause is left out: TTL is not an input of system.parts min_time, and
// a TTL merge would drop the 1970 fixture parts.
const preTTL14542s2DDL = `CREATE TABLE signoz_traces.signoz_index_v3
(
    ts_bucket_start UInt64 CODEC(DoubleDelta, LZ4),
    resource_fingerprint String CODEC(ZSTD(1)),
    timestamp DateTime64(9) CODEC(DoubleDelta, LZ4),
    trace_id FixedString(32) CODEC(ZSTD(1)),
    span_id String CODEC(ZSTD(1)),
    name LowCardinality(String) CODEC(ZSTD(1)),
    has_error Bool CODEC(T64, ZSTD(1)),
    INDEX idx_timestamp timestamp TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (ts_bucket_start, resource_fingerprint, has_error, name, timestamp)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1`

func TestPreTTL14542s2ReadFormNoThrowEveryColumnAndMutants(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	sum := sha256.Sum256([]byte(preTTL14542s2Form))
	if got := hex.EncodeToString(sum[:]); got != preTTL14542s2FormSHA256 {
		t.Fatalf("PRE-TTL-14542-2-r2 sha256 = %s, want %s", got, preTTL14542s2FormSHA256)
	}
	t.Logf("PRE-TTL-14542-2-r2 sha256 %s (sha12 %s)", preTTL14542s2FormSHA256, preTTL14542s2FormSHA256[:12])
	var code []string
	for _, line := range strings.Split(preTTL14542s2Form, "\n") {
		if !strings.HasPrefix(line, "--") {
			code = append(code, line)
		}
	}
	upper := strings.ToUpper(strings.Join(code, "\n"))
	for _, banned := range []string{"SET ", "SETTINGS", "THROWIF", "EXPLAIN", "INSERT", "ALTER", "CREATE", "DROP", "SYSTEM "} {
		if strings.Contains(upper, banned) {
			t.Fatalf("form code holds %q", banned)
		}
	}

	instance, err := containers.StartClickHouseIsolated(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	options, err := clickhouse.ParseDSN(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := clickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	var version, tz string
	if err := conn.QueryRow(ctx, "SELECT version(), timezone()").Scan(&version, &tz); err != nil {
		t.Fatal(err)
	}
	t.Logf("clickhouse version %s, server timezone %s", version, tz)

	exec := func(statement string) {
		t.Helper()
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%v\n%s", err, statement)
		}
	}
	exec("CREATE DATABASE signoz_traces")
	exec(preTTL14542s2DDL)
	// Merges stay off so each insert stays one part; only M2 merges, on purpose.
	exec("SYSTEM STOP MERGES signoz_traces.signoz_index_v3")
	insert := func(label, timeExpr string, n int) {
		t.Helper()
		exec(fmt.Sprintf(`INSERT INTO signoz_traces.signoz_index_v3 (ts_bucket_start, timestamp, trace_id, span_id, name)
			SELECT intDiv(toUInt64(toUnixTimestamp(%[1]s)), 1800) * 1800, toDateTime64(%[1]s, 9, 'UTC'),
			       leftPad(toString(number + 1), 32, '0'), toString(number + 1), '%[2]s'
			FROM numbers(%[3]d)`, timeExpr, label, n))
	}
	scalar := func(query string) string {
		t.Helper()
		var v string
		if err := conn.QueryRow(ctx, query).Scan(&v); err != nil {
			t.Fatalf("%v\n%s", err, query)
		}
		return v
	}
	logParts := func(phase string) {
		t.Helper()
		rows, err := conn.Query(ctx, `SELECT partition_id, name, level, rows, toString(min_time), toString(max_time)
			FROM system.parts WHERE database = 'signoz_traces' AND table = 'signoz_index_v3' AND active ORDER BY name`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var partition, name, minTime, maxTime string
			var level uint32
			var n uint64
			if err := rows.Scan(&partition, &name, &level, &n, &minTime, &maxTime); err != nil {
				t.Fatal(err)
			}
			t.Logf("%s system.parts: partition %s part %s level %d rows %d min_time %s max_time %s", phase, partition, name, level, n, minTime, maxTime)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}
	columns := []string{"active_parts", "parts_min_time_lt_2020", "rows_in_those_parts", "unfilled_max_day",
		"filled_min_day", "oldest_days", "unfilled_partition_ids", "unfilled_levels", "unfilled_empty_parts", "unfilled_parts_rows"}
	// run executes the form text unchanged, as pm does: one statement, one row. A throw fails the cell (NO-THROW form).
	run := func(phase string) map[string]string {
		t.Helper()
		comment := fmt.Sprintf("preTTL14542s2-%s-%d", phase, time.Now().UnixNano())
		rows, err := conn.Query(clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment})), preTTL14542s2Form)
		if err != nil {
			t.Fatalf("%s: the form threw: %v", phase, err)
		}
		defer rows.Close()
		types := rows.ColumnTypes()
		if len(types) != len(columns) {
			t.Fatalf("%s: %d columns, want %d", phase, len(types), len(columns))
		}
		var typeNames []string
		for i, ct := range types {
			if ct.Name() != columns[i] {
				t.Fatalf("%s: column %d = %s, want %s", phase, i, ct.Name(), columns[i])
			}
			if strings.Contains(ct.DatabaseTypeName(), "Nullable") || strings.Contains(ct.DatabaseTypeName(), "Array") {
				t.Fatalf("%s: column %s type %s (want a plain number or String)", phase, ct.Name(), ct.DatabaseTypeName())
			}
			typeNames = append(typeNames, ct.Name()+" "+ct.DatabaseTypeName())
		}
		got := map[string]string{}
		n := 0
		for rows.Next() {
			n++
			dest := make([]any, len(types))
			for i, ct := range types {
				dest[i] = reflect.New(ct.ScanType()).Interface()
			}
			if err := rows.Scan(dest...); err != nil {
				t.Fatal(err)
			}
			for i, d := range dest {
				got[columns[i]] = fmt.Sprint(reflect.ValueOf(d).Elem().Interface())
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("%s: %d rows, want 1", phase, n)
		}
		exec("SYSTEM FLUSH LOGS")
		readRows := scalar(fmt.Sprintf(`SELECT toString(read_rows) FROM system.query_log
			WHERE log_comment = '%s' AND type = 'QueryFinish' AND is_initial_query`, comment))
		t.Logf("%s types: %s", phase, strings.Join(typeNames, ", "))
		var line []string
		for _, c := range columns {
			line = append(line, c+"="+got[c])
		}
		t.Logf("%s form row: %s; query_log read_rows %s", phase, strings.Join(line, " "), readRows)
		return got
	}
	check := func(phase string, got, want map[string]string) {
		t.Helper()
		for _, c := range columns {
			if got[c] != want[c] {
				t.Errorf("%s: %s = %q, want %q", phase, c, got[c], want[c])
			}
		}
		if t.Failed() {
			t.FailNow()
		}
	}
	clone := func(m map[string]string, kv ...string) map[string]string {
		out := map[string]string{}
		for k, v := range m {
			out[k] = v
		}
		for i := 0; i < len(kv); i += 2 {
			out[kv[i]] = kv[i+1]
		}
		return out
	}
	delta := func(name string, before, after map[string]string, moved ...string) {
		t.Helper()
		want := map[string]bool{}
		for _, c := range moved {
			want[c] = true
		}
		for _, c := range columns {
			changed := before[c] != after[c]
			if changed != want[c] {
				t.Errorf("%s: %s changed = %v (%q -> %q), want changed = %v", name, c, changed, before[c], after[c], want[c])
			} else if changed {
				t.Logf("%s delta: %s %s -> %s", name, c, before[c], after[c])
			}
		}
		if t.Failed() {
			t.FailNow()
		}
	}

	// Case (a): no active part. The form returns its row (no throw): control 0.
	empty := run("empty")
	emptyWant := map[string]string{"active_parts": "0", "parts_min_time_lt_2020": "0", "rows_in_those_parts": "0",
		"unfilled_max_day": "none", "filled_min_day": "none", "oldest_days": "-1", "unfilled_partition_ids": "",
		"unfilled_levels": "", "unfilled_empty_parts": "0", "unfilled_parts_rows": ""}
	check("empty", empty, emptyWant)

	// Base: 1 normal part (1 row at today - 3 d, 12:00) + 1 unfilled part (1 row at 1970-01-02 12:00:00 UTC).
	insert("normal", "toDateTime(today() - 3) + toIntervalHour(12)", 1)
	insert("unfilled", "toDateTime('1970-01-02 12:00:00', 'UTC')", 1)
	logParts("base")
	day3 := scalar("SELECT toString(today() - 3)")
	base := run("base")
	baseWant := map[string]string{"active_parts": "2", "parts_min_time_lt_2020": "1", "rows_in_those_parts": "1",
		"unfilled_max_day": "1970-01-02", "filled_min_day": day3, "oldest_days": "3", "unfilled_partition_ids": "19700102",
		"unfilled_levels": "0", "unfilled_empty_parts": "0", "unfilled_parts_rows": "19700102:0:1"}
	check("base", base, baseWant)

	// M1 (unfilled input): a second unfilled part, 2 rows, 1970-01-05 12:00 UTC.
	insert("unfilled-2", "toDateTime('1970-01-05 12:00:00', 'UTC')", 2)
	logParts("M1")
	m1 := run("M1")
	check("M1", m1, clone(baseWant, "active_parts", "3", "parts_min_time_lt_2020", "2", "rows_in_those_parts", "3",
		"unfilled_max_day", "1970-01-05", "unfilled_partition_ids", "19700102,19700105",
		"unfilled_parts_rows", "19700102:0:1,19700105:0:2"))
	delta("M1", base, m1, "active_parts", "parts_min_time_lt_2020", "rows_in_those_parts", "unfilled_max_day",
		"unfilled_partition_ids", "unfilled_parts_rows")

	// M2 (level input): merge the 1970-01-05 part into a level 1 part. Part and row counts stay.
	exec("SYSTEM START MERGES signoz_traces.signoz_index_v3")
	exec("OPTIMIZE TABLE signoz_traces.signoz_index_v3 PARTITION ID '19700105' FINAL")
	exec("SYSTEM STOP MERGES signoz_traces.signoz_index_v3")
	logParts("M2")
	m2 := run("M2")
	check("M2", m2, clone(m1, "unfilled_levels", "0,1", "unfilled_parts_rows", "19700102:0:1,19700105:1:2"))
	delta("M2", m1, m2, "unfilled_levels", "unfilled_parts_rows")

	// M3 (filled input): an older normal part, today - 5 d.
	insert("normal-old", "toDateTime(today() - 5) + toIntervalHour(12)", 1)
	logParts("M3")
	day5 := scalar("SELECT toString(today() - 5)")
	m3 := run("M3")
	check("M3", m3, clone(m2, "active_parts", "4", "filled_min_day", day5, "oldest_days", "5"))
	delta("M3", m2, m3, "active_parts", "filled_min_day", "oldest_days")

	// M4 (filled parts only): drop the unfilled partitions. The unfilled columns go to their empty values.
	exec("ALTER TABLE signoz_traces.signoz_index_v3 DROP PARTITION ID '19700102'")
	exec("ALTER TABLE signoz_traces.signoz_index_v3 DROP PARTITION ID '19700105'")
	logParts("M4")
	m4 := run("M4")
	check("M4", m4, clone(m3, "active_parts", "2", "parts_min_time_lt_2020", "0", "rows_in_those_parts", "0",
		"unfilled_max_day", "none", "unfilled_partition_ids", "", "unfilled_levels", "", "unfilled_parts_rows", ""))
	delta("M4", m3, m4, "active_parts", "parts_min_time_lt_2020", "rows_in_those_parts", "unfilled_max_day",
		"unfilled_partition_ids", "unfilled_levels", "unfilled_parts_rows")

	// M5 (empty part): an ALTER DELETE removes every row of the today - 5 d part. remove_empty_parts = 0 keeps the empty
	// result part ACTIVE (prod default 1 drops it within about 1 s), so the cell sees it every run. Its min_time is 1970.
	exec("ALTER TABLE signoz_traces.signoz_index_v3 MODIFY SETTING remove_empty_parts = 0")
	exec("SYSTEM START MERGES signoz_traces.signoz_index_v3")
	if err := conn.Exec(clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"mutations_sync": 2})),
		"ALTER TABLE signoz_traces.signoz_index_v3 DELETE WHERE name = 'normal-old'"); err != nil {
		t.Fatal(err)
	}
	exec("SYSTEM STOP MERGES signoz_traces.signoz_index_v3")
	logParts("M5")
	day5Partition := scalar("SELECT toString(toYYYYMMDD(today() - 5))")
	m5 := run("M5")
	check("M5", m5, clone(m4, "parts_min_time_lt_2020", "1", "unfilled_max_day", "1970-01-01", "filled_min_day", day3,
		"oldest_days", "3", "unfilled_partition_ids", day5Partition, "unfilled_levels", "0", "unfilled_empty_parts", "1",
		"unfilled_parts_rows", day5Partition+":0:0"))
	delta("M5", m4, m5, "parts_min_time_lt_2020", "unfilled_max_day", "filled_min_day", "oldest_days",
		"unfilled_partition_ids", "unfilled_levels", "unfilled_empty_parts", "unfilled_parts_rows")

	// M6 (mixed): + a part of real rows with an old timestamp (1970-01-03 12:00 UTC). Rows per part tell the two apart.
	insert("unfilled-3", "toDateTime('1970-01-03 12:00:00', 'UTC')", 1)
	logParts("M6")
	m6 := run("M6")
	check("M6", m6, clone(m5, "active_parts", "3", "parts_min_time_lt_2020", "2", "rows_in_those_parts", "1",
		"unfilled_max_day", "1970-01-03", "unfilled_partition_ids", "19700103,"+day5Partition,
		"unfilled_parts_rows", "19700103:0:1,"+day5Partition+":0:0"))
	delta("M6", m5, m6, "active_parts", "parts_min_time_lt_2020", "rows_in_those_parts", "unfilled_max_day",
		"unfilled_partition_ids", "unfilled_parts_rows")
}

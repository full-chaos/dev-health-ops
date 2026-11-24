//go:build integration

package chmigrate_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Throwaway exec proof of the prod gate PRE-TTL-14542-3 (CHAOS-14542): not for merge. preTTL14542s3Gate is the form file
// byte for byte; preTTL14542s1Gate is the frozen PRE-TTL-14542-1 gate, the red cell's old text. Both sha256 are pinned.
const preTTL14542s3GateSHA256 = "c8abea06474d0c7119ec9af0e1d3dcf420506b5d0ff489b92e190051e6097a8a"

const preTTL14542s3Gate = `-- PRE-TTL-14542-3 (fcdev lane l14542, 2026-11-24): PRE-TTL-14542-1 (fe48b20a1f06) with an empty-part exclusion INSIDE the gate.
-- Metadata-only retention check of the local SigNoz trace table. Reads system.parts only.
-- Why: PRE-TTL-14542-2 on prod (pm out 8ad9cc23072a): 2 active parts with min_time 1970, both rows 0 (empty parts left by a TTL drop).
-- Kept part = NOT (min_time < 2020 AND rows = 0). An empty unfilled part is skipped by the gate and by oldest_days.
-- Output: active_parts (all active parts), oldest_days (days from the oldest kept part min_time to now()), empty_unfilled_parts (INFO). Counts and days only.
-- WANT active_parts > 0 AND oldest_days >= 2. Gates (a throw = STOP, no further read): no kept part throws STOP-MIN-TIME-UNFILLED
-- (as -1 on no active part); an unfilled part with rows > 0 throws STOP-MIN-TIME-UNFILLED-ROWS.
-- No SET, no SETTINGS.
SELECT count() AS active_parts,
    dateDiff('day', minIf(min_time, NOT (min_time < toDateTime('2020-01-01') AND rows = 0)), now()) AS oldest_days,
    countIf(min_time < toDateTime('2020-01-01') AND rows = 0) AS empty_unfilled_parts
FROM system.parts
WHERE database = 'signoz_traces' AND table = 'signoz_index_v3' AND active
HAVING throwIf(countIf(NOT (min_time < toDateTime('2020-01-01') AND rows = 0)) = 0, 'STOP-MIN-TIME-UNFILLED') = 0
    AND throwIf(countIf(min_time < toDateTime('2020-01-01') AND rows > 0) > 0, 'STOP-MIN-TIME-UNFILLED-ROWS') = 0
`

const preTTL14542s1GateSHA256 = "fe48b20a1f06414682b1fbc9585928ac48247d8fb0985af1fa228e98e162232e"

const preTTL14542s1Gate = `-- PRE-TTL-14542-1 (fcsup lane l14542, 2026-11-23): metadata-only retention check of the local SigNoz trace table. Reads system.parts only.
-- Output: active_parts, oldest_days (days from the oldest active part min_time to now()). Counts and days only.
-- WANT active_parts > 0 AND oldest_days >= 2. Gate: an unfilled min_time (1970) throws STOP-MIN-TIME-UNFILLED = STOP, no further read.
-- No SET, no SETTINGS.
SELECT count() AS active_parts, dateDiff('day', min(min_time), now()) AS oldest_days
FROM system.parts
WHERE database = 'signoz_traces' AND table = 'signoz_index_v3' AND active
HAVING throwIf(min(min_time) < toDateTime('2020-01-01'), 'STOP-MIN-TIME-UNFILLED') = 0
`

// The SigNoz v3 local trace table cut to the columns of its partition key and sort key (signoz-otel-collector
// 0476e453f957 traces_migrations.go:14-134). Same engine, PARTITION BY, ORDER BY, timestamp type and SETTINGS. The TTL
// clause is left out: TTL is not an input of system.parts min_time or rows.
const preTTL14542s3DDL = `CREATE TABLE signoz_traces.signoz_index_v3
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

func TestPreTTL14542s3GateSkipsEmptyUnfilledPartsAndThrowsOnUnfilledRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	for _, pin := range []struct{ name, text, want string }{
		{"PRE-TTL-14542-3", preTTL14542s3Gate, preTTL14542s3GateSHA256},
		{"PRE-TTL-14542-1", preTTL14542s1Gate, preTTL14542s1GateSHA256},
	} {
		sum := sha256.Sum256([]byte(pin.text))
		if got := hex.EncodeToString(sum[:]); got != pin.want {
			t.Fatalf("%s sha256 = %s, want %s", pin.name, got, pin.want)
		}
		t.Logf("%s sha256 %s (sha12 %s)", pin.name, pin.want, pin.want[:12])
	}
	var code []string
	for _, line := range strings.Split(preTTL14542s3Gate, "\n") {
		if !strings.HasPrefix(line, "--") {
			code = append(code, line)
		}
	}
	upper := strings.ToUpper(strings.Join(code, "\n"))
	for _, banned := range []string{"SET ", "SETTINGS", "EXPLAIN", "INSERT", "ALTER", "CREATE", "DROP", "SYSTEM "} {
		if strings.Contains(upper, banned) {
			t.Fatalf("PRE-TTL-14542-3 code holds %q", banned)
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
	scalar := func(query string) string {
		t.Helper()
		var v string
		if err := conn.QueryRow(ctx, query).Scan(&v); err != nil {
			t.Fatalf("%v\n%s", err, query)
		}
		return v
	}
	exec("CREATE DATABASE signoz_traces")
	exec(preTTL14542s3DDL)
	exec("SYSTEM STOP MERGES signoz_traces.signoz_index_v3")
	insert := func(label, timeExpr string) {
		t.Helper()
		exec(fmt.Sprintf(`INSERT INTO signoz_traces.signoz_index_v3 (ts_bucket_start, timestamp, trace_id, span_id, name)
			SELECT intDiv(toUInt64(toUnixTimestamp(%[1]s)), 1800) * 1800, toDateTime64(%[1]s, 9, 'UTC'),
			       '00000000000000000000000000000001', '1', '%[2]s'
			FROM numbers(1)`, timeExpr, label))
	}
	// deleteRows empties the parts that hold label. remove_empty_parts = 0 (set below) keeps the empty result part
	// ACTIVE every run; on prod the TTL drop leaves such a part (PRE-TTL-14542-2 out 8ad9cc23072a).
	deleteRows := func(where string) {
		t.Helper()
		exec("SYSTEM START MERGES signoz_traces.signoz_index_v3")
		if err := conn.Exec(clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"mutations_sync": 2})),
			"ALTER TABLE signoz_traces.signoz_index_v3 DELETE WHERE "+where); err != nil {
			t.Fatal(err)
		}
		exec("SYSTEM STOP MERGES signoz_traces.signoz_index_v3")
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
	// run executes a gate text unchanged, as pm does: one statement. It returns the row as "col=value" pairs, or the error.
	run := func(text string) (string, error) {
		rows, err := conn.Query(ctx, text)
		if err != nil {
			return "", err
		}
		defer rows.Close()
		types := rows.ColumnTypes()
		var out []string
		n := 0
		for rows.Next() {
			n++
			dest := make([]any, len(types))
			for i, ct := range types {
				dest[i] = reflect.New(ct.ScanType()).Interface()
			}
			if err := rows.Scan(dest...); err != nil {
				return "", err
			}
			for i, ct := range types {
				out = append(out, fmt.Sprintf("%s=%v", ct.Name(), reflect.ValueOf(dest[i]).Elem().Interface()))
			}
		}
		if err := rows.Err(); err != nil {
			return "", err
		}
		if n != 1 {
			return "", fmt.Errorf("%d rows, want 1", n)
		}
		return strings.Join(out, " "), nil
	}
	pass := func(phase, name, text, want string) {
		t.Helper()
		got, err := run(text)
		if err != nil {
			t.Fatalf("%s: %s threw, want PASS: %v", phase, name, err)
		}
		if got != want {
			t.Fatalf("%s: %s row = %q, want %q", phase, name, got, want)
		}
		t.Logf("%s: %s PASS row %s", phase, name, got)
	}
	throws := func(phase, name, text, stop string) {
		t.Helper()
		got, err := run(text)
		if err == nil {
			t.Fatalf("%s: %s returned %q, want a throw %s", phase, name, got, stop)
		}
		var exception *clickhouse.Exception
		if !errors.As(err, &exception) || exception.Code != 395 {
			t.Fatalf("%s: %s error is not code 395: %v", phase, name, err)
		}
		// The message starts with the throwIf text. STOP-MIN-TIME-UNFILLED is a prefix of STOP-MIN-TIME-UNFILLED-ROWS,
		// so the match includes the ":" after the text.
		if !strings.HasPrefix(exception.Message, stop+":") {
			t.Fatalf("%s: %s threw %q, want a message that starts with %s:", phase, name, exception.Message, stop)
		}
		first := exception.Message
		if i := strings.Index(first, "\n"); i >= 0 {
			first = first[:i]
		}
		if len(first) > 200 {
			first = first[:200]
		}
		t.Logf("%s: %s THROWS code %d %s (%s)", phase, name, exception.Code, stop, first)
	}

	// G0: no active part. -3 throws STOP-MIN-TIME-UNFILLED (no kept part), as -1 does.
	throws("G0 no part", "-3", preTTL14542s3Gate, "STOP-MIN-TIME-UNFILLED")
	throws("G0 no part", "-1", preTTL14542s1Gate, "STOP-MIN-TIME-UNFILLED")

	// G1: normal parts only (today - 3 d and today - 5 d, 12:00). -3 = -1 on the shared columns.
	insert("normal", "toDateTime(today() - 3) + toIntervalHour(12)")
	insert("normal-old", "toDateTime(today() - 5) + toIntervalHour(12)")
	logParts("G1")
	pass("G1 normal only", "-3", preTTL14542s3Gate, "active_parts=2 oldest_days=5 empty_unfilled_parts=0")
	pass("G1 normal only", "-1", preTTL14542s1Gate, "active_parts=2 oldest_days=5")

	// Leg 1: empty the today - 5 d part (the oldest day, as prod partition 20261024). 1 empty unfilled part + 1 normal
	// part = PASS; oldest_days comes from the kept part only (3, not 5 and not the 1970 epoch).
	exec("ALTER TABLE signoz_traces.signoz_index_v3 MODIFY SETTING remove_empty_parts = 0")
	deleteRows("name = 'normal-old'")
	logParts("LEG1")
	day5Partition := scalar("SELECT toString(toYYYYMMDD(today() - 5))")
	var emptyParts uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM system.parts WHERE database = 'signoz_traces' AND table = 'signoz_index_v3'
		AND active AND rows = 0 AND min_time < toDateTime('2020-01-01') AND partition_id = ?`, day5Partition).Scan(&emptyParts); err != nil {
		t.Fatal(err)
	}
	if emptyParts != 1 {
		t.Fatalf("leg 1 fixture: %d empty unfilled parts in partition %s, want 1", emptyParts, day5Partition)
	}
	pass("LEG1 empty unfilled + normal", "-3", preTTL14542s3Gate, "active_parts=2 oldest_days=3 empty_unfilled_parts=1")

	// Leg 3 (red cell): the frozen -1 gate on the leg 1 table throws: the empty part alone fails the old gate.
	throws("LEG3 empty unfilled + normal", "-1", preTTL14542s1Gate, "STOP-MIN-TIME-UNFILLED")

	// Leg 2: + 1 unfilled part with rows > 0 (1 row at 1970-01-03 12:00 UTC). -3 throws STOP-MIN-TIME-UNFILLED-ROWS.
	insert("unfilled-rows", "toDateTime('1970-01-03 12:00:00', 'UTC')")
	logParts("LEG2")
	throws("LEG2 unfilled rows + empty + normal", "-3", preTTL14542s3Gate, "STOP-MIN-TIME-UNFILLED-ROWS")
	throws("LEG2 unfilled rows + empty + normal", "-1", preTTL14542s1Gate, "STOP-MIN-TIME-UNFILLED")

	// G4: only empty parts left (the unfilled-row part and the normal part emptied too): no kept part, so -3 throws
	// STOP-MIN-TIME-UNFILLED (not -ROWS).
	deleteRows("name IN ('normal', 'unfilled-rows')")
	logParts("G4")
	if got := scalar(`SELECT toString(countIf(rows = 0)) || '/' || toString(count()) FROM system.parts
		WHERE database = 'signoz_traces' AND table = 'signoz_index_v3' AND active`); got != "3/3" {
		t.Fatalf("G4 fixture: empty/active parts = %s, want 3/3", got)
	}
	throws("G4 only empty parts", "-3", preTTL14542s3Gate, "STOP-MIN-TIME-UNFILLED")
}

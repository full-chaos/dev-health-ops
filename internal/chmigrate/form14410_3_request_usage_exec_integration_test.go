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

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// Throwaway exec proof of the prod read form FORM-14410-3 (CHAOS-14410): not for merge. form14410s1Form is the form file
// with its org prefix (8 hex) replaced by the synthetic "f0f0f0f0" in all 5 places; its sha256 is pinned. The form file
// itself (real prefix) has sha256 d900d16e19f46c952246594d54ce2a389ea9a45bb2beb7c2d163798a6d703ef1.
const form14410s1FormSHA256 = "fcc209f0452895d602c8dc6b4db75e5db444e8dbb2af5a6617944992f11386a7"

//ttlclock:allow fixed FORM-14410-3 window data (a throwaway exec cell), the row lives until 2027-12-17
const form14410s1Form = `-- FORM-14410-3 (fcdev lane l14542, 2026-11-24; = FORM-14410-2 76d7a168e881 + acp READ-FORM-14410-2-1 fixes: stmt1o, stmt2 INFO cols, stmt3 INFO): the GitHub installation's real primary rate limit and its peak use, from the
-- STORED response headers in provider_request_usage (app ClickHouse). CHAOS-14410; merge gate 2 of PLAN-14542-3.
-- Six statements, ONE call each, in order: a block runs from its "-- stmtN" line to its ";". Read-only.
-- No SET, no SETTINGS, no FINAL, no EXPLAIN. A throw or a WANT miss = STOP, no further read.
-- Decision window (24 h): recorded_at in [2026-11-23 06:00:00, 2026-11-24 06:00:00) UTC. INFO window (7 d): from 2026-11-17 06:00:00.
-- stmt0: table + columns at the LIVE prod schema. WANT cols = 11 (0 = no table: STOP, fall back to the log line).
SELECT count() AS cols
FROM system.columns
WHERE database = currentDatabase() AND table = 'provider_request_usage'
  AND name IN ('org_id', 'provider', 'integration_id', 'rate_limit_remaining', 'rate_limit_limit', 'rate_limit_used',
               'rate_limit_resource', 'rate_limit_reset', 'requests', 'window_started_at', 'recorded_at');
-- stmt1o: ORG-1 and the control. WANT orgs = 1 AND rows_7d > 0.
WITH (SELECT sum(rows) + 2 * count() + 1 FROM system.parts WHERE active AND database = currentDatabase()
      AND table = 'provider_request_usage' AND partition_id >= '202611') AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT uniqExact(org_id) AS orgs,
       count() AS rows_7d,
       read_rows_cap
FROM provider_request_usage
WHERE startsWith(org_id, 'f0f0f0f0')
  AND window_started_at >= toDateTime64('2026-11-17 05:00:00', 3, 'UTC')
  AND cost_gate = 0;
-- stmt1: the writer is live. One row per integration id 8-char cut. WANT rows_24h > 0 AND last_flush_age_h < 24.
WITH (SELECT sum(rows) + 2 * count() + 1 FROM system.parts WHERE active AND database = currentDatabase()
      AND table = 'provider_request_usage' AND partition_id >= '202611') AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT substring(integration_id, 1, 8) AS integration8,
       count() AS rows_from_7d_start,
       countIf(recorded_at >= toDateTime64('2026-11-23 06:00:00', 3, 'UTC') AND recorded_at < toDateTime64('2026-11-24 06:00:00', 3, 'UTC')) AS rows_24h,
       dateDiff('hour', max(recorded_at), now()) AS last_flush_age_h,
       read_rows_cap
FROM provider_request_usage
WHERE startsWith(org_id, 'f0f0f0f0') AND provider = 'github'
  AND window_started_at >= toDateTime64('2026-11-17 05:00:00', 3, 'UTC')
  AND cost_gate = 0
GROUP BY integration8
ORDER BY integration8
LIMIT 100;
-- stmt2: DECISION (the gate), 24 h. One row per (integration id 8-char cut, rate-limit resource).
-- Gate 2 (resource core, every integration row): min_remaining_share >= 0.45 (= max_used_share <= 0.55) and rows_with_limit > 0.
WITH (SELECT sum(rows) + 2 * count() + 1 FROM system.parts WHERE active AND database = currentDatabase()
      AND table = 'provider_request_usage' AND partition_id >= '202611') AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT substring(integration_id, 1, 8) AS integration8,
       rate_limit_resource AS resource,
       count() AS rows_n,
       countIf(rate_limit_limit > 0) AS rows_with_limit,
       min(rate_limit_limit) AS min_limit,
       max(rate_limit_limit) AS max_limit,
       min(rate_limit_remaining) AS min_remaining,
       max(rate_limit_used) AS max_used,
       round(minIf(rate_limit_remaining / rate_limit_limit, rate_limit_limit > 0), 4) AS min_remaining_share,
       round(maxIf(rate_limit_used / rate_limit_limit, rate_limit_limit > 0), 4) AS max_used_share,
       countIf(rate_limit_remaining < 0.45 * rate_limit_limit) AS rows_remaining_under_045,
       uniqExactIf(toStartOfHour(recorded_at), rate_limit_remaining < 0.45 * rate_limit_limit) AS hours_remaining_under_045,
       uniqExact(toStartOfHour(recorded_at)) AS hours,
       max(requests) AS max_requests_per_row,
       countIf(toInt64OrZero(rate_limit_reset) > 0 AND toInt64OrZero(rate_limit_reset) <= toUnixTimestamp(recorded_at)) AS rows_reset_passed,
       read_rows_cap
FROM provider_request_usage
WHERE startsWith(org_id, 'f0f0f0f0') AND provider = 'github'
  AND window_started_at >= toDateTime64('2026-11-23 05:00:00', 3, 'UTC') AND window_started_at < toDateTime64('2026-11-24 06:00:00', 3, 'UTC')
  AND recorded_at >= toDateTime64('2026-11-23 06:00:00', 3, 'UTC') AND recorded_at < toDateTime64('2026-11-24 06:00:00', 3, 'UTC')
  AND cost_gate = 0
GROUP BY integration8, resource
ORDER BY integration8, resource
LIMIT 100;
-- stmt3: INFO, hour coverage of the 24 h window (not a gate). One row per integration id 8-char cut: the core rows with a
-- limit per clock hour (24 values, hour 0 = 06:00 UTC) and the count of hours with 0 such rows (an idle hour, or rows the
-- writer dropped: see the md writer-drop check).
WITH (SELECT sum(rows) + 2 * count() + 1 FROM system.parts WHERE active AND database = currentDatabase()
      AND table = 'provider_request_usage' AND partition_id >= '202611') AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT integration8,
       arrayStringConcat(arrayMap(h -> toString(countEqual(hour_index, h)), range(24)), ',') AS core_rows_per_hour,
       arrayCount(h -> countEqual(hour_index, h) = 0, range(24)) AS zero_row_hours,
       read_rows_cap
FROM (
    SELECT substring(integration_id, 1, 8) AS integration8,
           groupArray(toUInt8(dateDiff('hour', toDateTime64('2026-11-23 06:00:00', 3, 'UTC'), recorded_at))) AS hour_index
    FROM provider_request_usage
    WHERE startsWith(org_id, 'f0f0f0f0') AND provider = 'github' AND rate_limit_resource = 'core' AND rate_limit_limit > 0
      AND window_started_at >= toDateTime64('2026-11-23 05:00:00', 3, 'UTC') AND window_started_at < toDateTime64('2026-11-24 06:00:00', 3, 'UTC')
      AND recorded_at >= toDateTime64('2026-11-23 06:00:00', 3, 'UTC') AND recorded_at < toDateTime64('2026-11-24 06:00:00', 3, 'UTC')
      AND cost_gate = 0
    GROUP BY integration8
)
ORDER BY integration8
LIMIT 100;
-- stmt4: INFO, 7 d. The same text as stmt2 with the 7 d start.
WITH (SELECT sum(rows) + 2 * count() + 1 FROM system.parts WHERE active AND database = currentDatabase()
      AND table = 'provider_request_usage' AND partition_id >= '202611') AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT substring(integration_id, 1, 8) AS integration8,
       rate_limit_resource AS resource,
       count() AS rows_n,
       countIf(rate_limit_limit > 0) AS rows_with_limit,
       min(rate_limit_limit) AS min_limit,
       max(rate_limit_limit) AS max_limit,
       min(rate_limit_remaining) AS min_remaining,
       max(rate_limit_used) AS max_used,
       round(minIf(rate_limit_remaining / rate_limit_limit, rate_limit_limit > 0), 4) AS min_remaining_share,
       round(maxIf(rate_limit_used / rate_limit_limit, rate_limit_limit > 0), 4) AS max_used_share,
       countIf(rate_limit_remaining < 0.45 * rate_limit_limit) AS rows_remaining_under_045,
       uniqExactIf(toStartOfHour(recorded_at), rate_limit_remaining < 0.45 * rate_limit_limit) AS hours_remaining_under_045,
       uniqExact(toStartOfHour(recorded_at)) AS hours,
       max(requests) AS max_requests_per_row,
       countIf(toInt64OrZero(rate_limit_reset) > 0 AND toInt64OrZero(rate_limit_reset) <= toUnixTimestamp(recorded_at)) AS rows_reset_passed,
       read_rows_cap
FROM provider_request_usage
WHERE startsWith(org_id, 'f0f0f0f0') AND provider = 'github'
  AND window_started_at >= toDateTime64('2026-11-17 05:00:00', 3, 'UTC') AND window_started_at < toDateTime64('2026-11-24 06:00:00', 3, 'UTC')
  AND recorded_at >= toDateTime64('2026-11-17 06:00:00', 3, 'UTC') AND recorded_at < toDateTime64('2026-11-24 06:00:00', 3, 'UTC')
  AND cost_gate = 0
GROUP BY integration8, resource
ORDER BY integration8, resource
LIMIT 100;
`

const (
	form14410Org      = "f0f0f0f0-0000-4000-8000-000000000001"
	form14410OtherOrg = "ffffffff-0000-4000-8000-000000000002"
	form14410IntegA   = "aaaaaaaa-0000-4000-8000-00000000000a"
	form14410IntegB   = "bbbbbbbb-0000-4000-8000-00000000000b"
)

// form14410s1Blocks cuts the form into its statements: a block runs from its "-- stmtN" line to the first line that ends
// in ";", both included, exactly as pm runs it.
func form14410s1Blocks(t *testing.T) ([]string, string) {
	t.Helper()
	labels := []string{"stmt0", "stmt1o", "stmt1", "stmt2", "stmt3", "stmt4"}
	var blocks []string
	var current []string
	in := false
	for _, line := range strings.Split(form14410s1Form, "\n") {
		if len(blocks) < len(labels) && strings.HasPrefix(line, "-- "+labels[len(blocks)]+":") {
			in = true
		}
		if !in {
			continue
		}
		current = append(current, line)
		if strings.HasSuffix(line, ";") {
			blocks = append(blocks, strings.Join(current, "\n"))
			current, in = nil, false
		}
	}
	if len(blocks) != 6 || in {
		t.Fatalf("%d statement blocks, want 6", len(blocks))
	}
	// blocks[0..4] = stmt0, stmt1-4 (the indices the cell uses); stmt1o apart.
	return append([]string{blocks[0]}, blocks[2:]...), blocks[1]
}

func form14410s1Cell(v any) string {
	value := reflect.ValueOf(v)
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return "NULL"
		}
		value = value.Elem()
	}
	return fmt.Sprint(value.Interface())
}

func TestForm14410s3RequestUsageReadEveryColumnOrgCoverageMutantsAndGate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	sum := sha256.Sum256([]byte(form14410s1Form))
	if got := hex.EncodeToString(sum[:]); got != form14410s1FormSHA256 {
		t.Fatalf("FORM-14410-3 (prefix swapped) sha256 = %s, want %s", got, form14410s1FormSHA256)
	}
	t.Logf("FORM-14410-3 prefix-swapped sha256 %s (sha12 %s)", form14410s1FormSHA256, form14410s1FormSHA256[:12])
	if n := strings.Count(form14410s1Form, "startsWith(org_id, 'f0f0f0f0')"); n != 5 {
		t.Fatalf("%d org filters, want 5 (stmt1o, stmt1-4)", n)
	}
	// pm runs a block from its "-- stmtN:" line to the first line that ends in ";": no comment line may end in ";".
	for _, line := range strings.Split(form14410s1Form, "\n") {
		if strings.HasPrefix(line, "--") && strings.HasSuffix(strings.TrimSpace(line), ";") {
			t.Fatalf("a comment line ends in \";\" and would cut a block: %q", line)
		}
	}
	blocks, block1o := form14410s1Blocks(t)
	for i, block := range append(append([]string(nil), blocks...), block1o) {
		var code []string
		for _, line := range strings.Split(block, "\n") {
			if !strings.HasPrefix(line, "--") {
				code = append(code, line)
			}
		}
		upper := strings.ToUpper(strings.Join(code, "\n"))
		for _, banned := range []string{"SET ", "SETTINGS", "FINAL", "EXPLAIN", "INSERT", "ALTER", "CREATE", "DROP", "SYSTEM "} {
			if strings.Contains(upper, banned) {
				t.Fatalf("stmt%d code holds %q", i, banned)
			}
		}
	}

	scratch := scratchDatabase(t)
	conn := openDatabase(t, scratch.URI, "")
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	result, err := chmigrate.Upgrade(ctx, db, baseline, chain)
	if err != nil {
		t.Fatalf("real chain: %v", err)
	}
	var version string
	if err := conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("schema head %s, clickhouse version %s", result.Head, version)
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
	exec("SYSTEM STOP MERGES provider_request_usage")

	// One row per fixture case; execution_id keeps every row distinct on the sorting key.
	at := func(ts string) string { return fmt.Sprintf("toDateTime64('%s', 3, 'UTC')", ts) }
	// times = "<window_started_at>|<recorded_at>": one literal per fixture row (ttlclock: a marker covers one literal).
	// Row 2 carries 7 requests and a reset epoch (1795438800 = 13:00 UTC) before its 13:30 flush: a known-stale reading.
	// Row 1's reset is after its 12:00 flush. The other rows carry 1 request and no reset header.
	requestsOf := map[int]string{2: "7"}
	resetOf := map[int]string{1: "1795438800", 2: "1795438800"}
	row := func(id int, org, provider, integration, resource, limits, times string) {
		t.Helper()
		windowStarted, recorded, ok := strings.Cut(times, "|")
		if !ok {
			t.Fatalf("row %d: times %q", id, times)
		}
		if !strings.HasPrefix(windowStarted, "toStartOfHour") {
			windowStarted, recorded = at(windowStarted), at(recorded)
		}
		requests := "1"
		if n, ok := requestsOf[id]; ok {
			requests = n
		}
		exec(fmt.Sprintf(`INSERT INTO provider_request_usage (org_id, provider, dataset, integration_id, unit_id, execution_id,
			transport, flush_seq, requests, responses, rate_limit_limit, rate_limit_remaining, rate_limit_used, rate_limit_resource,
			rate_limit_reset, window_started_at, recorded_at)
			SELECT '%s', '%s', 'cicd', '%s', 'unit', 'exec-%d', 'rest', 0, %s, 1, %s, '%s', '%s', %s, %s`,
			org, provider, integration, id, requests, limits, resource, resetOf[id], windowStarted, recorded))
	}
	//ttlclock:allow fixed FORM-14410-3 window data (a throwaway exec cell), the row lives until 2027-12-23
	row(1, form14410Org, "github", form14410IntegA, "core", "5000, 3000, 2000", "2026-11-23 11:59:00|2026-11-23 12:00:00")
	//ttlclock:allow fixed FORM-14410-3 window data (a throwaway exec cell), the row lives until 2027-12-23
	row(2, form14410Org, "github", form14410IntegA, "core", "5000, 2500, 2500", "2026-11-23 13:29:00|2026-11-23 13:30:00")
	//ttlclock:allow fixed FORM-14410-3 window data (a throwaway exec cell), the row lives until 2027-12-23
	row(3, form14410Org, "github", form14410IntegA, "graphql", "5000, 4900, 100", "2026-11-23 13:59:00|2026-11-23 14:00:00")
	//ttlclock:allow fixed FORM-14410-3 window data (a throwaway exec cell), the row lives until 2027-12-23
	row(4, form14410Org, "github", form14410IntegA, "", "NULL, NULL, NULL", "2026-11-23 14:29:00|2026-11-23 14:30:00")
	//ttlclock:allow fixed FORM-14410-3 window data (a throwaway exec cell), the row lives until 2027-12-20
	row(5, form14410Org, "github", form14410IntegA, "core", "5000, 1000, 4000", "2026-11-20 09:59:00|2026-11-20 10:00:00")
	row(6, form14410Org, "github", form14410IntegA, "core", "5000, 100, 4900", "toStartOfHour(now64(3, 'UTC')) - toIntervalMinute(1)|toStartOfHour(now64(3, 'UTC'))")
	//ttlclock:allow fixed FORM-14410-3 window data (a throwaway exec cell), the row lives until 2027-12-23
	row(7, form14410OtherOrg, "github", form14410IntegA, "core", "5000, 0, 5000", "2026-11-23 15:59:00|2026-11-23 16:00:00")
	//ttlclock:allow fixed FORM-14410-3 window data (a throwaway exec cell), the row lives until 2027-12-23
	row(8, form14410Org, "gitlab", form14410IntegA, "core", "5000, 0, 5000", "2026-11-23 16:29:00|2026-11-23 16:30:00")
	//ttlclock:allow fixed FORM-14410-3 window data (a throwaway exec cell), the row lives until 2027-12-10
	row(9, form14410Org, "github", form14410IntegA, "core", "5000, 0, 5000", "2026-11-10 09:59:00|2026-11-10 10:00:00")
	//ttlclock:allow fixed FORM-14410-3 window data (a throwaway exec cell), the row lives until 2027-12-23
	row(10, form14410Org, "github", form14410IntegB, "core", "15000, 9000, 6000", "2026-11-23 14:59:00|2026-11-23 15:00:00")
	// Integration B covers every clock hour of the 24 h window with one core row (hour 9 is row 10 above): the full
	// coverage PASS case. 15000 / 10000 / 5000 keeps B's min remaining share at row 10's 0.6.
	exec(fmt.Sprintf(`INSERT INTO provider_request_usage (org_id, provider, dataset, integration_id, unit_id, execution_id,
		transport, flush_seq, requests, responses, rate_limit_limit, rate_limit_remaining, rate_limit_used, rate_limit_resource,
		window_started_at, recorded_at)
		SELECT '%s', 'github', 'cicd', '%s', 'unit', concat('exec-b-', toString(number)), 'rest', 0, 1, 1, 15000, 10000, 5000, 'core',
		       start + toIntervalHour(number) - toIntervalMinute(1), start + toIntervalHour(number)
		FROM numbers(24) AS n
		CROSS JOIN (SELECT %s AS start) AS s
		WHERE number != 9`, form14410Org, form14410IntegB,
		//ttlclock:allow fixed FORM-14410-3 window data (a throwaway exec cell), the row lives until 2027-12-23
		"toDateTime64('2026-11-23 06:30:00', 3, 'UTC')"))
	if got := scalar("SELECT toString(count()) FROM provider_request_usage"); got != "33" {
		t.Fatalf("fixture rows %s, want 33", got)
	}
	capWant := scalar(`SELECT toString(sum(rows) + 2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()
		AND table = 'provider_request_usage' AND partition_id >= '202611'`)
	t.Logf("read_rows_cap from system.parts: %s", capWant)

	// run executes one block as pm does; it returns the rows as "col=value" lines, or the error.
	run := func(label, text string) ([]string, error) {
		comment := fmt.Sprintf("form14410s1-%s-%d", label, time.Now().UnixNano())
		rows, err := conn.Query(clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment})), text)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		types := rows.ColumnTypes()
		var out []string
		for rows.Next() {
			dest := make([]any, len(types))
			for i, ct := range types {
				dest[i] = reflect.New(ct.ScanType()).Interface()
			}
			if err := rows.Scan(dest...); err != nil {
				return nil, err
			}
			var cells []string
			for i, ct := range types {
				cells = append(cells, ct.Name()+"="+form14410s1Cell(dest[i]))
			}
			out = append(out, strings.Join(cells, " "))
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		exec("SYSTEM FLUSH LOGS")
		readRows := scalar(fmt.Sprintf(`SELECT toString(read_rows) FROM system.query_log
			WHERE log_comment = '%s' AND type = 'QueryFinish' AND is_initial_query`, comment))
		t.Logf("%s: read_rows %s", label, readRows)
		if label == "stmt1o" || label == "stmt1" || label == "stmt2" || label == "stmt3" || label == "stmt4" {
			var rr, capRows uint64
			if _, err := fmt.Sscan(readRows, &rr); err != nil {
				t.Fatal(err)
			}
			if _, err := fmt.Sscan(capWant, &capRows); err != nil {
				t.Fatal(err)
			}
			if rr > capRows {
				t.Fatalf("%s: read_rows %d > read_rows_cap %d", label, rr, capRows)
			}
		}
		return out, nil
	}
	must := func(label, text string) []string {
		t.Helper()
		out, err := run(label, text)
		if err != nil {
			t.Fatalf("%s threw: %v", label, err)
		}
		for _, line := range out {
			t.Logf("%s row: %s", label, line)
		}
		return out
	}
	equal := func(label string, got, want []string) {
		t.Helper()
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("%s:\ngot\n%s\nwant\n%s", label, strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
	}
	// last_flush_age_h counts hour boundaries from the newest flush to now(): recomputed before and after the call, so an
	// hour that turns during the call cannot fail the cell.
	ageOf := func(integration string) string {
		return scalar(fmt.Sprintf(`SELECT toString(dateDiff('hour', max(recorded_at), now())) FROM provider_request_usage
			WHERE org_id = '%s' AND provider = 'github' AND integration_id = '%s'`, form14410Org, integration))
	}

	// Base values, every column.
	base := make([][]string, 5)
	base[0] = must("stmt0", blocks[0])
	equal("stmt0", base[0], []string{"cols=11"})
	// stmt1o: ORG-1 and the control. Org F, any provider, from the 7 d start: A rows 1-6, the gitlab row 8, B's 24 rows.
	base1o := must("stmt1o", block1o)
	equal("stmt1o", base1o, []string{"orgs=1 rows_7d=31 read_rows_cap=" + capWant})
	equal("stmt1o-org (the other org's row)", must("stmt1o-org", strings.ReplaceAll(block1o, "startsWith(org_id, 'f0f0f0f0')", "startsWith(org_id, '')")),
		[]string{"orgs=2 rows_7d=32 read_rows_cap=" + capWant})

	beforeA, beforeB := ageOf(form14410IntegA), ageOf(form14410IntegB)
	base[1] = must("stmt1", blocks[1])
	afterA, afterB := ageOf(form14410IntegA), ageOf(form14410IntegB)
	stmt1Want := func(ageA, ageB string) []string {
		return []string{
			"integration8=aaaaaaaa rows_from_7d_start=6 rows_24h=4 last_flush_age_h=" + ageA + " read_rows_cap=" + capWant,
			"integration8=bbbbbbbb rows_from_7d_start=24 rows_24h=24 last_flush_age_h=" + ageB + " read_rows_cap=" + capWant,
		}
	}
	if strings.Join(base[1], "\n") != strings.Join(stmt1Want(beforeA, beforeB), "\n") {
		equal("stmt1", base[1], stmt1Want(afterA, afterB))
	}
	if afterA != "0" && afterA != "1" {
		t.Fatalf("stmt1: the fresh flush is %s hour boundaries old, want 0 or 1", afterA)
	}
	if beforeB == "0" || beforeB == "1" {
		t.Fatalf("stmt1: the integration B flush reads %s hours old: the CI clock is wrong", beforeB)
	}

	cols := func(rows_n, withLimit, minLimit, maxLimit, minRemaining, maxUsed, remShare, usedShare, under, hoursUnder, hours, maxRequests, resetPassed string) string {
		return "rows_n=" + rows_n + " rows_with_limit=" + withLimit + " min_limit=" + minLimit + " max_limit=" + maxLimit +
			" min_remaining=" + minRemaining + " max_used=" + maxUsed + " min_remaining_share=" + remShare +
			" max_used_share=" + usedShare + " rows_remaining_under_045=" + under + " hours_remaining_under_045=" + hoursUnder +
			" hours=" + hours + " max_requests_per_row=" + maxRequests + " rows_reset_passed=" + resetPassed + " read_rows_cap=" + capWant
	}
	nullCols := cols("1", "0", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "0", "0", "1", "1", "0")
	graphql := "integration8=aaaaaaaa resource=graphql " + cols("1", "1", "5000", "5000", "4900", "100", "0.98", "0.02", "0", "0", "1", "1", "0")
	coreB := "integration8=bbbbbbbb resource=core " + cols("24", "24", "15000", "15000", "9000", "6000", "0.6", "0.4", "0", "0", "24", "1", "0")
	base[2] = must("stmt2", blocks[2])
	equal("stmt2", base[2], []string{
		"integration8=aaaaaaaa resource= " + nullCols,
		"integration8=aaaaaaaa resource=core " + cols("2", "2", "5000", "5000", "2500", "2500", "0.5", "0.5", "0", "0", "2", "7", "1"),
		graphql, coreB,
	})
	hours := func(set map[int]string) string {
		cells := make([]string, 24)
		for h := range cells {
			cells[h] = "0"
			if v, ok := set[h]; ok {
				cells[h] = v
			}
		}
		return strings.Join(cells, ",")
	}
	allOnes := map[int]string{}
	for h := 0; h < 24; h++ {
		allOnes[h] = "1"
	}
	coverageA := "integration8=aaaaaaaa core_rows_per_hour=" + hours(map[int]string{6: "1", 7: "1"}) + " zero_row_hours=22 read_rows_cap=" + capWant
	coverageB := "integration8=bbbbbbbb core_rows_per_hour=" + hours(allOnes) + " zero_row_hours=0 read_rows_cap=" + capWant
	base[3] = must("stmt3", blocks[3])
	equal("stmt3 (INFO: B all 24 hours, A 22 zero-row hours)", base[3], []string{coverageA, coverageB})
	base[4] = must("stmt4", blocks[4])
	equal("stmt4", base[4], []string{
		"integration8=aaaaaaaa resource= " + nullCols,
		"integration8=aaaaaaaa resource=core " + cols("3", "3", "5000", "5000", "1000", "4000", "0.2", "0.8", "1", "1", "3", "7", "1"),
		graphql, coreB,
	})

	// Org and rule mutants: each text change must move the named statement's result.
	mutate := func(i int, name, old, replacement string) []string {
		t.Helper()
		if !strings.Contains(blocks[i], old) {
			t.Fatalf("stmt%d mutant %s: %q not in the text", i, name, old)
		}
		got := must(fmt.Sprintf("stmt%d-%s", i, name), strings.ReplaceAll(blocks[i], old, replacement))
		if strings.Join(got, "\n") == strings.Join(base[i], "\n") {
			t.Fatalf("stmt%d mutant %s: the result did not change", i, name)
		}
		return got
	}
	mutate(0, "rule-column", "'recorded_at');", "'nope');")
	for i := 1; i <= 4; i++ {
		mutate(i, "org", "startsWith(org_id, 'f0f0f0f0')", "startsWith(org_id, '')")
		mutate(i, "rule-provider", "provider = 'github'", "provider != ''")
	}
	mutate(1, "rule-24h-start",
		"recorded_at >= toDateTime64('2026-11-23 06:00:00', 3, 'UTC')", //ttlclock:allow fixed FORM-14410-3 window data (a throwaway exec cell), the row lives until 2027-12-23
		"recorded_at >= toDateTime64('2026-11-17 06:00:00', 3, 'UTC')") //ttlclock:allow fixed FORM-14410-3 window data (a throwaway exec cell), the row lives until 2027-12-17
	mutate(3, "rule-resource", "rate_limit_resource = 'core'", "rate_limit_resource != ''")
	mutate(3, "rule-limit-filter", "rate_limit_limit > 0", "rate_limit_limit > 9999")
	for _, i := range []int{2, 4} {
		mutate(i, "rule-threshold", "0.45 * rate_limit_limit", "0.55 * rate_limit_limit")
		mutate(i, "rule-limit-filter", "rate_limit_limit > 0)", "rate_limit_limit > 9999)")
		mutate(i, "rule-max-requests", "max(requests) AS", "min(requests) AS")
		mutate(i, "rule-reset-passed", "<= toUnixTimestamp(recorded_at)", "> toUnixTimestamp(recorded_at)")
	}
	mutate(3, "rule-window-start",
		"recorded_at >= toDateTime64('2026-11-23 06:00:00', 3, 'UTC')", //ttlclock:allow fixed FORM-14410-3 window data (a throwaway exec cell), the row lives until 2027-12-23
		"recorded_at >= toDateTime64('2026-11-23 07:00:00', 3, 'UTC')") //ttlclock:allow fixed FORM-14410-3 window data (a throwaway exec cell), the row lives until 2027-12-23
	window := strings.ReplaceAll(strings.ReplaceAll(blocks[2],
		"toDateTime64('2026-11-23 05:00:00', 3, 'UTC')",  //ttlclock:allow fixed FORM-14410-3 window data (a throwaway exec cell), the row lives until 2027-12-23
		"toDateTime64('2026-11-17 05:00:00', 3, 'UTC')"), //ttlclock:allow fixed FORM-14410-3 window data (a throwaway exec cell), the row lives until 2027-12-17
		"recorded_at >= toDateTime64('2026-11-23 06:00:00', 3, 'UTC')", //ttlclock:allow fixed FORM-14410-3 window data (a throwaway exec cell), the row lives until 2027-12-23
		"recorded_at >= toDateTime64('2026-11-17 06:00:00', 3, 'UTC')") //ttlclock:allow fixed FORM-14410-3 window data (a throwaway exec cell), the row lives until 2027-12-17
	equal("stmt2-rule-window (= stmt4)", must("stmt2-rule-window", window), base[4])

	// Cost gate: a cap of 1 row fires the scalar throwIf inside each statement.
	gated := map[string]string{"stmt1o": block1o, "stmt1": blocks[1], "stmt2": blocks[2], "stmt3": blocks[3], "stmt4": blocks[4]}
	for _, i := range []string{"stmt1o", "stmt1", "stmt2", "stmt3", "stmt4"} {
		_, err := run(i+"-gate", strings.Replace(gated[i], "read_rows_cap >= 20000000", "read_rows_cap >= 1", 1))
		var exception *clickhouse.Exception
		if !errors.As(err, &exception) || exception.Code != 395 || !strings.HasPrefix(exception.Message, "STOP-COST-GATE") {
			t.Fatalf("%s gate: %v, want code 395 STOP-COST-GATE", i, err)
		}
		t.Logf("%s gate: THROWS code %d STOP-COST-GATE", i, exception.Code)
	}

	// Coverage data mutant (stmt3 is INFO): B loses its hour-5 row: one 0-row hour shows.
	// A mutation needs the merge pool: start merges for it, stop them again after.
	exec("SYSTEM START MERGES provider_request_usage")
	if err := conn.Exec(clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"mutations_sync": 2})),
		fmt.Sprintf("ALTER TABLE provider_request_usage DELETE WHERE integration_id = '%s' AND execution_id = 'exec-b-5'", form14410IntegB)); err != nil {
		t.Fatal(err)
	}
	exec("SYSTEM STOP MERGES provider_request_usage")
	oneGap := map[int]string{}
	for h := 0; h < 24; h++ {
		oneGap[h] = "1"
	}
	oneGap[5] = "0"
	capGap := scalar(`SELECT toString(sum(rows) + 2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()
		AND table = 'provider_request_usage' AND partition_id >= '202611'`)
	gap := must("stmt3-hour-gap", blocks[3])
	equal("stmt3-hour-gap (INFO: B one 0-row hour)", gap, []string{
		"integration8=aaaaaaaa core_rows_per_hour=" + hours(map[int]string{6: "1", 7: "1"}) + " zero_row_hours=22 read_rows_cap=" + capGap,
		"integration8=bbbbbbbb core_rows_per_hour=" + hours(oneGap) + " zero_row_hours=1 read_rows_cap=" + capGap,
	})

	exec("TRUNCATE TABLE provider_request_usage")
	// The empty table: stmt0 still sees the columns; stmt1o answers its one aggregate row with 0 (control = 0 = STOP);
	// stmt1-4 return no group.
	equal("stmt0-empty", must("stmt0-empty", blocks[0]), []string{"cols=11"})
	equal("stmt1o-empty", must("stmt1o-empty", block1o), []string{"orgs=0 rows_7d=0 read_rows_cap=1"})
	for i := 1; i <= 4; i++ {
		if got := must(fmt.Sprintf("stmt%d-empty", i), blocks[i]); len(got) != 0 {
			t.Fatalf("stmt%d on an empty table: %v, want no row", i, got)
		}
	}
}

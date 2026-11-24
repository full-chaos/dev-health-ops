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

// Throwaway exec proof of the prod read form FORM-14410-1 (CHAOS-14410): not for merge. form14410s1Form is the form file
// with its org prefix (8 hex) replaced by the synthetic "f0f0f0f0" in all 3 places; its sha256 is pinned. The form file
// itself (real prefix) has sha256 3a0e0b16699257f65ec34f80b201f16b1ccbd92eb5401eecf3cc3cad1704eeb6.
const form14410s1FormSHA256 = "563309e7d0e727b079e7b88a22ac78cfcb5d5f505cc57cd5b7e79a629babe9e9"

const form14410s1Form = `-- FORM-14410-1 (fcdev lane l14542, 2026-11-24): the GitHub installation's real primary rate limit and its peak use, from the
-- STORED response headers in provider_request_usage (app ClickHouse). CHAOS-14410; merge gate 2 of PLAN-14542-3.
-- Four statements, ONE call each, in order: a block runs from its "-- stmtN" line to its ";". Read-only.
-- No SET, no SETTINGS, no FINAL, no EXPLAIN. A throw or a WANT miss = STOP, no further read.
-- Decision window (24 h): recorded_at in [2026-11-23 06:00:00, 2026-11-24 06:00:00) UTC. INFO window (7 d): from 2026-11-17 06:00:00.
-- stmt0: table + columns at the LIVE prod schema. WANT cols = 9 (0 = no table: STOP, fall back to the log line).
SELECT count() AS cols
FROM system.columns
WHERE database = currentDatabase() AND table = 'provider_request_usage'
  AND name IN ('org_id', 'provider', 'integration_id', 'rate_limit_remaining', 'rate_limit_limit', 'rate_limit_used',
               'rate_limit_resource', 'window_started_at', 'recorded_at');
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
-- stmt2: DECISION, 24 h. One row per (integration id 8-char cut, rate-limit resource).
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
       read_rows_cap
FROM provider_request_usage
WHERE startsWith(org_id, 'f0f0f0f0') AND provider = 'github'
  AND window_started_at >= toDateTime64('2026-11-23 05:00:00', 3, 'UTC') AND window_started_at < toDateTime64('2026-11-24 06:00:00', 3, 'UTC')
  AND recorded_at >= toDateTime64('2026-11-23 06:00:00', 3, 'UTC') AND recorded_at < toDateTime64('2026-11-24 06:00:00', 3, 'UTC')
  AND cost_gate = 0
GROUP BY integration8, resource
ORDER BY integration8, resource
LIMIT 100;
-- stmt3: INFO, 7 d. The same text as stmt2 with the 7 d start.
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
func form14410s1Blocks(t *testing.T) []string {
	t.Helper()
	var blocks []string
	var current []string
	in := false
	for _, line := range strings.Split(form14410s1Form, "\n") {
		if strings.HasPrefix(line, fmt.Sprintf("-- stmt%d:", len(blocks))) {
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
	if len(blocks) != 4 || in {
		t.Fatalf("%d statement blocks, want 4", len(blocks))
	}
	return blocks
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

func TestForm14410s1RequestUsageReadEveryColumnMutantsAndGate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	sum := sha256.Sum256([]byte(form14410s1Form))
	if got := hex.EncodeToString(sum[:]); got != form14410s1FormSHA256 {
		t.Fatalf("FORM-14410-1 (prefix swapped) sha256 = %s, want %s", got, form14410s1FormSHA256)
	}
	t.Logf("FORM-14410-1 prefix-swapped sha256 %s (sha12 %s)", form14410s1FormSHA256, form14410s1FormSHA256[:12])
	if n := strings.Count(form14410s1Form, "startsWith(org_id, 'f0f0f0f0')"); n != 3 {
		t.Fatalf("%d org filters, want 3 (stmt1-3)", n)
	}
	blocks := form14410s1Blocks(t)
	for i, block := range blocks {
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
	row := func(id int, org, provider, integration, resource, limits, windowStarted, recorded string) {
		t.Helper()
		exec(fmt.Sprintf(`INSERT INTO provider_request_usage (org_id, provider, dataset, integration_id, unit_id, execution_id,
			transport, flush_seq, requests, responses, rate_limit_limit, rate_limit_remaining, rate_limit_used, rate_limit_resource,
			window_started_at, recorded_at)
			SELECT '%s', '%s', 'cicd', '%s', 'unit', 'exec-%d', 'rest', 0, 1, 1, %s, '%s', %s, %s`,
			org, provider, integration, id, limits, resource, windowStarted, recorded))
	}
	at := func(ts string) string { return fmt.Sprintf("toDateTime64('%s', 3, 'UTC')", ts) }
	row(1, form14410Org, "github", form14410IntegA, "core", "5000, 3000, 2000", at("2026-11-23 11:59:00"), at("2026-11-23 12:00:00"))
	row(2, form14410Org, "github", form14410IntegA, "core", "5000, 2500, 2500", at("2026-11-23 13:29:00"), at("2026-11-23 13:30:00"))
	row(3, form14410Org, "github", form14410IntegA, "graphql", "5000, 4900, 100", at("2026-11-23 13:59:00"), at("2026-11-23 14:00:00"))
	row(4, form14410Org, "github", form14410IntegA, "", "NULL, NULL, NULL", at("2026-11-23 14:29:00"), at("2026-11-23 14:30:00"))
	row(5, form14410Org, "github", form14410IntegA, "core", "5000, 1000, 4000", at("2026-11-20 09:59:00"), at("2026-11-20 10:00:00"))
	row(6, form14410Org, "github", form14410IntegA, "core", "5000, 100, 4900", "toStartOfHour(now64(3, 'UTC')) - toIntervalMinute(1)", "toStartOfHour(now64(3, 'UTC'))")
	row(7, form14410OtherOrg, "github", form14410IntegA, "core", "5000, 0, 5000", at("2026-11-23 15:59:00"), at("2026-11-23 16:00:00"))
	row(8, form14410Org, "gitlab", form14410IntegA, "core", "5000, 0, 5000", at("2026-11-23 16:29:00"), at("2026-11-23 16:30:00"))
	row(9, form14410Org, "github", form14410IntegA, "core", "5000, 0, 5000", at("2026-11-10 09:59:00"), at("2026-11-10 10:00:00"))
	row(10, form14410Org, "github", form14410IntegB, "core", "15000, 9000, 6000", at("2026-11-23 14:59:00"), at("2026-11-23 15:00:00"))
	if got := scalar("SELECT toString(count()) FROM provider_request_usage"); got != "10" {
		t.Fatalf("fixture rows %s, want 10", got)
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
		if label == "stmt1" || label == "stmt2" || label == "stmt3" {
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
	base := make([][]string, 4)
	base[0] = must("stmt0", blocks[0])
	equal("stmt0", base[0], []string{"cols=9"})

	beforeA, beforeB := ageOf(form14410IntegA), ageOf(form14410IntegB)
	base[1] = must("stmt1", blocks[1])
	afterA, afterB := ageOf(form14410IntegA), ageOf(form14410IntegB)
	stmt1Want := func(ageA, ageB string) []string {
		return []string{
			"integration8=aaaaaaaa rows_from_7d_start=6 rows_24h=4 last_flush_age_h=" + ageA + " read_rows_cap=" + capWant,
			"integration8=bbbbbbbb rows_from_7d_start=1 rows_24h=1 last_flush_age_h=" + ageB + " read_rows_cap=" + capWant,
		}
	}
	if strings.Join(base[1], "\n") != strings.Join(stmt1Want(beforeA, beforeB), "\n") {
		equal("stmt1", base[1], stmt1Want(afterA, afterB))
	}
	if afterA != "0" && afterA != "1" {
		t.Fatalf("stmt1: the fresh flush is %s hour boundaries old, want 0 or 1", afterA)
	}
	if beforeB == "0" || beforeB == "1" {
		t.Fatalf("stmt1: the 2026-11-23 15:00 flush reads %s hours old: the CI clock is wrong", beforeB)
	}

	cols := func(rows_n, withLimit, minLimit, maxLimit, minRemaining, maxUsed, remShare, usedShare, under, hoursUnder, hours string) string {
		return "rows_n=" + rows_n + " rows_with_limit=" + withLimit + " min_limit=" + minLimit + " max_limit=" + maxLimit +
			" min_remaining=" + minRemaining + " max_used=" + maxUsed + " min_remaining_share=" + remShare +
			" max_used_share=" + usedShare + " rows_remaining_under_045=" + under + " hours_remaining_under_045=" + hoursUnder +
			" hours=" + hours + " read_rows_cap=" + capWant
	}
	nullCols := cols("1", "0", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "0", "0", "1")
	graphql := "integration8=aaaaaaaa resource=graphql " + cols("1", "1", "5000", "5000", "4900", "100", "0.98", "0.02", "0", "0", "1")
	coreB := "integration8=bbbbbbbb resource=core " + cols("1", "1", "15000", "15000", "9000", "6000", "0.6", "0.4", "0", "0", "1")
	base[2] = must("stmt2", blocks[2])
	equal("stmt2", base[2], []string{
		"integration8=aaaaaaaa resource= " + nullCols,
		"integration8=aaaaaaaa resource=core " + cols("2", "2", "5000", "5000", "2500", "2500", "0.5", "0.5", "0", "0", "2"),
		graphql, coreB,
	})
	base[3] = must("stmt3", blocks[3])
	equal("stmt3", base[3], []string{
		"integration8=aaaaaaaa resource= " + nullCols,
		"integration8=aaaaaaaa resource=core " + cols("3", "3", "5000", "5000", "1000", "4000", "0.2", "0.8", "1", "1", "3"),
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
	for i := 1; i <= 3; i++ {
		mutate(i, "org", "startsWith(org_id, 'f0f0f0f0')", "startsWith(org_id, '')")
		mutate(i, "rule-provider", "provider = 'github'", "provider != ''")
	}
	mutate(1, "rule-24h-start", "recorded_at >= toDateTime64('2026-11-23 06:00:00', 3, 'UTC')", "recorded_at >= toDateTime64('2026-11-17 06:00:00', 3, 'UTC')")
	for i := 2; i <= 3; i++ {
		mutate(i, "rule-threshold", "0.45 * rate_limit_limit", "0.55 * rate_limit_limit")
		mutate(i, "rule-limit-filter", "rate_limit_limit > 0)", "rate_limit_limit > 9999)")
	}
	window := strings.ReplaceAll(strings.ReplaceAll(blocks[2],
		"toDateTime64('2026-11-23 05:00:00', 3, 'UTC')", "toDateTime64('2026-11-17 05:00:00', 3, 'UTC')"),
		"recorded_at >= toDateTime64('2026-11-23 06:00:00', 3, 'UTC')", "recorded_at >= toDateTime64('2026-11-17 06:00:00', 3, 'UTC')")
	equal("stmt2-rule-window (= stmt3)", must("stmt2-rule-window", window), base[3])

	// Cost gate: a cap of 1 row fires the scalar throwIf inside each statement.
	for i := 1; i <= 3; i++ {
		_, err := run(fmt.Sprintf("stmt%d-gate", i), strings.Replace(blocks[i], "read_rows_cap >= 20000000", "read_rows_cap >= 1", 1))
		var exception *clickhouse.Exception
		if !errors.As(err, &exception) || exception.Code != 395 || !strings.HasPrefix(exception.Message, "STOP-COST-GATE") {
			t.Fatalf("stmt%d gate: %v, want code 395 STOP-COST-GATE", i, err)
		}
		t.Logf("stmt%d gate: THROWS code %d STOP-COST-GATE", i, exception.Code)
	}

	// Rows with no rate headers stay out of the shares; the empty table returns no group (stmt1-3) and cols 9 (stmt0).
	exec("TRUNCATE TABLE provider_request_usage")
	equal("stmt0-empty", must("stmt0-empty", blocks[0]), []string{"cols=9"})
	for i := 1; i <= 3; i++ {
		if got := must(fmt.Sprintf("stmt%d-empty", i), blocks[i]); len(got) != 0 {
			t.Fatalf("stmt%d on an empty table: %v, want no row", i, got)
		}
	}
}

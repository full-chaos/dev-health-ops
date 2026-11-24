//go:build integration

package providersync

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Exec cell of a read FORM for rate-limit observations. The text below is the
// FORM file byte for byte, with the production org prefix swapped for a
// synthetic one.
const rateLimitObservationFormSQL = `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
-- G0 catalog gate: reads no data row. Sizes are the file sizes (pg_relation_size, metadata), so a never-analyzed table
-- (reltuples -1, relpages 0) is still bounded. WANT cost_gate_ok = 0, orgs = 1. Prints the real byte size of each table.
WITH orgs AS (SELECT count(*) AS n FROM public.organizations o WHERE starts_with(o.id::text, 'f0f0f0f0')),
rel AS (SELECT c.relname, c.reltuples::bigint AS reltuples, pg_catalog.pg_relation_size(c.oid) AS file_bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname IN ('provider_rate_limit_observations', 'sync_run_units'))
SELECT CASE WHEN (SELECT n FROM orgs) = 1
             AND (SELECT count(*) FROM rel) = 2
             AND (SELECT file_bytes FROM rel WHERE relname = 'provider_rate_limit_observations') <= 2147483648
             AND (SELECT file_bytes FROM rel WHERE relname = 'sync_run_units') <= 4294967296
             AND (SELECT reltuples FROM rel WHERE relname = 'provider_rate_limit_observations') <= 5000000
             AND (SELECT reltuples FROM rel WHERE relname = 'sync_run_units') <= 5000000
            THEN 0
            ELSE ('STOP-G0 orgs=' || (SELECT n FROM orgs)
                  || ' tables=' || (SELECT count(*) FROM rel)
                  || ' reltuples=' || COALESCE((SELECT string_agg(relname || ':' || reltuples, ',' ORDER BY relname) FROM rel), 'none')
                  || ' file_bytes=' || COALESCE((SELECT string_agg(relname || ':' || file_bytes, ',' ORDER BY relname) FROM rel), 'none'))::int
       END AS cost_gate_ok,
       (SELECT n FROM orgs) AS orgs,
       (SELECT file_bytes FROM rel WHERE relname = 'provider_rate_limit_observations') AS observations_file_bytes,
       (SELECT file_bytes FROM rel WHERE relname = 'sync_run_units') AS units_file_bytes;
-- S0 control. WANT n_units_rate_limited_7d > 0 (the writer deferForRateLimitSQL ran in the window; a lower bound, a later retry reason
-- overwrites last_retry_reason). 0 = NOT EXERCISED. n_units_first_seen_7d is INFO. One scan of sync_run_units (<= 4 GiB by G0).
SELECT count(*) FILTER (WHERE u.last_retry_reason = 'provider_rate_limited' AND u.updated_at >= now() - interval '7 days') AS n_units_rate_limited_7d,
       count(*) FILTER (WHERE u.rate_limit_first_seen_at >= now() - interval '7 days') AS n_units_first_seen_7d
FROM public.sync_run_units u
WHERE starts_with(u.org_id, 'f0f0f0f0');
-- S1 control. All observation rows of the org in 7 days. WANT n > 0. 0 = NOT EXERCISED (the writer skips an empty dimension), not "0 secondary rows".
SELECT count(*) AS n_all_7d
FROM public.provider_rate_limit_observations x
WHERE starts_with(x.org_id, 'f0f0f0f0') AND x.observed_at >= now() - interval '7 days';
-- S2 counts by provider and dimension. INFO. One scan. Counts and seconds only.
SELECT x.provider, COALESCE(x.dimension, '(none)') AS dimension, count(*) AS n,
       count(DISTINCT x.sync_run_unit_id) AS units,
       count(DISTINCT date_trunc('day', x.observed_at)) AS days,
       round(min(x.retry_after_seconds)::numeric, 1) AS min_retry_after_s,
       round(max(x.retry_after_seconds)::numeric, 1) AS max_retry_after_s,
       count(*) FILTER (WHERE x.retry_after_seconds IS NULL) AS no_retry_after
FROM public.provider_rate_limit_observations x
WHERE starts_with(x.org_id, 'f0f0f0f0') AND x.observed_at >= now() - interval '7 days'
GROUP BY x.provider, COALESCE(x.dimension, '(none)')
ORDER BY x.provider, 2;
-- S3 four-route cut (CHAOS-14552): observation rows of deferred units of github deployments, gitlab deployments,
-- github security and launchdarkly (any dataset), by provider and dataset. INFO. Observations scan + sync_run_units
-- (<= 4 GiB by G0; joined on its primary key id).
SELECT x.provider, u.dataset_key, count(*) AS n, count(DISTINCT x.sync_run_unit_id) AS units,
       count(*) FILTER (WHERE x.dimension = 'secondary_abuse_risk') AS secondary_abuse_risk
FROM public.provider_rate_limit_observations x
JOIN public.sync_run_units u ON u.id = x.sync_run_unit_id AND starts_with(u.org_id, 'f0f0f0f0')
WHERE starts_with(x.org_id, 'f0f0f0f0') AND x.observed_at >= now() - interval '7 days'
  AND ((x.provider = 'github' AND u.dataset_key IN ('deployments', 'security'))
    OR (x.provider = 'gitlab' AND u.dataset_key = 'deployments')
    OR x.provider = 'launchdarkly')
GROUP BY x.provider, u.dataset_key
ORDER BY x.provider, u.dataset_key;
ROLLBACK;
`

const (
	formOrg        = "f0f0f0f0-0000-4000-8000-000000000001"
	formOtherOrg   = "ffffffff-0000-4000-8000-000000000002"
	formUnitDeploy = "00000000-0000-4000-8000-0000000000a1"
	formUnitSec    = "00000000-0000-4000-8000-0000000000a2"
	formUnitGLDep  = "00000000-0000-4000-8000-0000000000a3"
	formUnitLD     = "00000000-0000-4000-8000-0000000000a4"
	formUnitGit    = "00000000-0000-4000-8000-0000000000a5"
	formUnitGLSec  = "00000000-0000-4000-8000-0000000000a6"
	formUnitOther  = "00000000-0000-4000-8000-0000000000a7"
	formUnitBudget = "00000000-0000-4000-8000-0000000000a8"
)

func formFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	if _, err := pool.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, 'form-org', 'form-org')`, formOrg); err != nil {
		t.Fatal(err)
	}
	day := 24 * time.Hour
	for _, unit := range []struct {
		id, org, provider, dataset, reason string
		age                                time.Duration
	}{
		{formUnitDeploy, formOrg, "github", "deployments", "provider_rate_limited", 1 * day},
		{formUnitSec, formOrg, "github", "security", "provider_rate_limited", 2 * day},
		{formUnitGLDep, formOrg, "gitlab", "deployments", "provider_rate_limited", 1 * day},
		{formUnitLD, formOrg, "launchdarkly", "feature-flags", "provider_rate_limited", 1 * day},
		{formUnitGit, formOrg, "github", "git", "provider_rate_limited", 2 * day},
		{formUnitGLSec, formOrg, "gitlab", "security", "provider_rate_limited", 8 * day},
		{formUnitOther, formOtherOrg, "github", "deployments", "provider_rate_limited", 1 * day},
		{formUnitBudget, formOrg, "github", "git", "provider_budget_contention", 1 * day},
	} {
		firstSeen := any(nil)
		if unit.reason == "provider_rate_limited" {
			firstSeen = unit.age.Seconds()
		}
		if _, err := pool.Exec(ctx, `INSERT INTO public.sync_run_units (
				id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode,
				status, attempts, created_at, updated_at, last_retry_reason, rate_limit_first_seen_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, 'medium', 'incremental', 'dispatching', 0, NOW(),
				NOW() - make_interval(secs => $8), $9,
				CASE WHEN $10::double precision IS NULL THEN NULL ELSE NOW() - make_interval(secs => $10::double precision) END)`,
			unit.id, unit.org, firstRunID, firstIntegrationID, firstSourceID, unit.provider, unit.dataset,
			unit.age.Seconds(), unit.reason, firstSeen); err != nil {
			t.Fatal(err)
		}
	}
	rows := []struct {
		org, provider, unit, dimension string
		retryAfter                     any
		age                            time.Duration
	}{
		{formOrg, "github", formUnitDeploy, "secondary_abuse_risk", 60.0, 1 * day},
		{formOrg, "github", formUnitSec, "secondary_abuse_risk", nil, 2 * day},
		{formOrg, "github", formUnitGit, "secondary_abuse_risk", 90.0, 2 * day},
		{formOrg, "github", formUnitGit, "rest_core", 30.0, 3 * day},
		{formOrg, "github", formUnitGit, "", 5.0, 3 * day},
		{formOrg, "gitlab", formUnitGLDep, "rest_core", 10.0, 1 * day},
		{formOrg, "gitlab", formUnitGLSec, "rest_core", 10.0, 1 * day},
		{formOrg, "launchdarkly", formUnitLD, "rest_core", 20.0, 1 * day},
		{formOrg, "github", formUnitDeploy, "secondary_abuse_risk", 60.0, 8 * day},
		{formOtherOrg, "github", formUnitOther, "secondary_abuse_risk", 60.0, 1 * day},
	}
	for _, row := range rows {
		if _, err := pool.Exec(ctx, `INSERT INTO public.provider_rate_limit_observations
			(id, org_id, provider, integration_id, sync_run_id, sync_run_unit_id, dimension, retry_after_seconds, reason, observed_at)
			VALUES (gen_random_uuid(), $1, $2, gen_random_uuid(), gen_random_uuid(), $3::uuid, NULLIF($4, ''), $5, 'provider_rate_limited', now() - make_interval(secs => $6))`,
			row.org, row.provider, row.unit, row.dimension, row.retryAfter, row.age.Seconds()); err != nil {
			t.Fatal(err)
		}
	}
}

func formAnalyze(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, table := range []string{"provider_rate_limit_observations", "sync_run_units"} {
		if _, err := pool.Exec(ctx, `ANALYZE public.`+table); err != nil {
			t.Fatal(err)
		}
	}
}

func formReltuples(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string) float64 {
	t.Helper()
	var reltuples float64
	if err := pool.QueryRow(ctx, `SELECT reltuples::float8 FROM pg_class WHERE oid = ('public.' || $1)::regclass`, table).Scan(&reltuples); err != nil {
		t.Fatal(err)
	}
	return reltuples
}

func runForm(ctx context.Context, pool *pgxpool.Pool, text string) ([][][]string, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	results, err := conn.Conn().PgConn().Exec(ctx, text).ReadAll()
	if err != nil {
		return nil, err
	}
	var out [][][]string
	for _, result := range results {
		if result.Err != nil {
			return nil, result.Err
		}
		var rows [][]string
		for _, raw := range result.Rows {
			row := make([]string, len(raw))
			for i, cell := range raw {
				row[i] = string(cell)
			}
			rows = append(rows, row)
		}
		out = append(out, rows)
	}
	return out, nil
}

// wantFormPass checks every WANT of the pass path on the 8 unit / 10 observation fixture.
func wantFormPass(t *testing.T, results [][][]string) {
	t.Helper()
	// BEGIN, G0, S0, S1, S2, S3, ROLLBACK
	if len(results) != 7 {
		t.Fatalf("%d result sets, want 7", len(results))
	}
	g0 := results[1]
	if len(g0) != 1 || g0[0][0] != "0" || g0[0][1] != "1" {
		t.Fatalf("G0 = %v, want cost_gate_ok 0, orgs 1", g0)
	}
	for _, column := range []int{2, 3} {
		if size, err := strconv.ParseInt(g0[0][column], 10, 64); err != nil || size <= 0 {
			t.Fatalf("G0 file bytes column %d = %q, want a positive byte size", column, g0[0][column])
		}
	}
	if got := results[2]; len(got) != 1 || got[0][0] != "5" || got[0][1] != "5" {
		t.Fatalf("S0 = %v, want 5 rate-limited units and 5 first-seen units in 7 d", got)
	}
	if got := results[3]; len(got) != 1 || got[0][0] != "8" {
		t.Fatalf("S1 = %v, want 8 observation rows in 7 d", got)
	}
	want := map[string]string{
		"github|(none)":               "1",
		"github|rest_core":            "1",
		"github|secondary_abuse_risk": "3",
		"gitlab|rest_core":            "2",
		"launchdarkly|rest_core":      "1",
	}
	got := map[string]string{}
	for _, row := range results[4] {
		got[row[0]+"|"+row[1]] = row[2]
	}
	if len(got) != len(want) {
		t.Fatalf("S2 rows = %v, want %v", results[4], want)
	}
	for key, n := range want {
		if got[key] != n {
			t.Fatalf("S2 %s = %q, want %q (all: %v)", key, got[key], n, got)
		}
	}
	cut := map[string]string{
		"github|deployments":         "1/1",
		"github|security":            "1/1",
		"gitlab|deployments":         "1/0",
		"launchdarkly|feature-flags": "1/0",
	}
	gotCut := map[string]string{}
	for _, row := range results[5] {
		gotCut[row[0]+"|"+row[1]] = row[2] + "/" + row[4]
	}
	if len(gotCut) != len(cut) {
		t.Fatalf("S3 rows = %v, want %v", results[5], cut)
	}
	for key, n := range cut {
		if gotCut[key] != n {
			t.Fatalf("S3 %s = %q, want %q (all: %v)", key, gotCut[key], n, gotCut)
		}
	}
}

func TestRateLimitObservationForm5Executes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	formFixture(t, ctx, pool)

	t.Run("never analyzed with rows: reltuples -1, G0 passes, counts right", func(t *testing.T) {
		if got := formReltuples(t, ctx, pool, "provider_rate_limit_observations"); got != -1 {
			t.Fatalf("observations reltuples = %v before any ANALYZE, want -1", got)
		}
		results, err := runForm(ctx, pool, rateLimitObservationFormSQL)
		if err != nil {
			t.Fatal(err)
		}
		wantFormPass(t, results)
	})

	formAnalyze(t, ctx, pool)

	t.Run("pass path with ANALYZE", func(t *testing.T) {
		if got := formReltuples(t, ctx, pool, "provider_rate_limit_observations"); got < 0 {
			t.Fatalf("observations reltuples = %v after ANALYZE, want >= 0", got)
		}
		results, err := runForm(ctx, pool, rateLimitObservationFormSQL)
		if err != nil {
			t.Fatal(err)
		}
		wantFormPass(t, results)
	})

	for _, mutant := range []struct{ name, from, to string }{
		{"observations byte cap 0", "'provider_rate_limit_observations') <= 2147483648", "'provider_rate_limit_observations') <= 0"},
		{"sync_run_units byte cap 0", "'sync_run_units') <= 4294967296", "'sync_run_units') <= 0"},
	} {
		t.Run("G0 fires: "+mutant.name, func(t *testing.T) {
			text := strings.Replace(rateLimitObservationFormSQL, mutant.from, mutant.to, 1)
			if text == rateLimitObservationFormSQL {
				t.Fatal("cap literal not found")
			}
			_, err := runForm(ctx, pool, text)
			if err == nil || !strings.Contains(err.Error(), "STOP-G0") || !strings.Contains(err.Error(), "file_bytes=") {
				t.Fatalf("err = %v, want STOP-G0 with the file_bytes text", err)
			}
		})
	}

	t.Run("G0 fires: org prefix matching nothing", func(t *testing.T) {
		text := strings.Replace(rateLimitObservationFormSQL, "'f0f0f0f0')", "'00000000')", 1)
		_, err := runForm(ctx, pool, text)
		if err == nil || !strings.Contains(err.Error(), "STOP-G0 orgs=0") {
			t.Fatalf("err = %v, want STOP-G0 orgs=0", err)
		}
	})

	t.Run("read only: a write inside the transaction is refused", func(t *testing.T) {
		text := strings.Replace(rateLimitObservationFormSQL, "ROLLBACK;", "DELETE FROM public.provider_rate_limit_observations;\nROLLBACK;", 1)
		_, err := runForm(ctx, pool, text)
		if err == nil || !strings.Contains(err.Error(), "read-only") {
			t.Fatalf("err = %v, want a read-only transaction error", err)
		}
	})

	t.Run("empty observations table: G0 passes, S1 0, S2 and S3 empty, control still > 0", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `DELETE FROM public.provider_rate_limit_observations`); err != nil {
			t.Fatal(err)
		}
		formAnalyze(t, ctx, pool)
		results, err := runForm(ctx, pool, rateLimitObservationFormSQL)
		if err != nil {
			t.Fatal(err)
		}
		if got := results[1]; len(got) != 1 || got[0][0] != "0" || got[0][1] != "1" {
			t.Fatalf("G0 = %v, want cost_gate_ok 0, orgs 1 on an empty table", got)
		}
		if got := results[2]; len(got) != 1 || got[0][0] != "5" {
			t.Fatalf("S0 = %v, want 5 (the unit control stays > 0)", got)
		}
		if got := results[3]; len(got) != 1 || got[0][0] != "0" {
			t.Fatalf("S1 = %v, want 0 (NOT EXERCISED)", got)
		}
		if len(results[4]) != 0 || len(results[5]) != 0 {
			t.Fatalf("S2 = %v, S3 = %v, want no rows", results[4], results[5])
		}
	})

	t.Run("control 0: no rate-limited units reads 0 (NOT EXERCISED)", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `UPDATE public.sync_run_units SET last_retry_reason = NULL`); err != nil {
			t.Fatal(err)
		}
		results, err := runForm(ctx, pool, rateLimitObservationFormSQL)
		if err != nil {
			t.Fatal(err)
		}
		if got := results[2]; len(got) != 1 || got[0][0] != "0" {
			t.Fatalf("S0 = %v, want 0 rate-limited units", got)
		}
	})
}

//go:build integration

package providersync

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Exec cell of a read FORM for rate-limit observations. The text below is the
// FORM file byte for byte.
const rateLimitObservationFormSQL = `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
-- G0 catalog gate: reads no data table. WANT cost_gate_ok = 0, orgs = 1.
WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
orgs AS (SELECT count(*) AS n FROM public.organizations o WHERE starts_with(o.id::text, 'c6a38355')),
rel AS (SELECT c.relname, c.reltuples::bigint AS reltuples,
               c.relpages::bigint * bs.block_size AS heap_bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN bs
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname IN ('provider_rate_limit_observations', 'sync_run_units'))
SELECT CASE WHEN (SELECT n FROM orgs) = 1
             AND (SELECT count(*) FROM rel) = 2
             AND (SELECT min(reltuples) FROM rel) >= 0
             AND (SELECT reltuples FROM rel WHERE relname = 'provider_rate_limit_observations') <= 5000000
             AND (SELECT heap_bytes FROM rel WHERE relname = 'provider_rate_limit_observations') <= 2147483648
             AND (SELECT reltuples FROM rel WHERE relname = 'sync_run_units') <= 5000000
             AND (SELECT heap_bytes FROM rel WHERE relname = 'sync_run_units') <= 4294967296
            THEN 0
            ELSE ('STOP-G0 orgs=' || (SELECT n FROM orgs)
                  || ' tables=' || (SELECT count(*) FROM rel)
                  || ' reltuples=' || COALESCE((SELECT string_agg(relname || ':' || reltuples, ',' ORDER BY relname) FROM rel), 'none')
                  || ' heap_bytes=' || COALESCE((SELECT string_agg(relname || ':' || heap_bytes, ',' ORDER BY relname) FROM rel), 'none'))::int
       END AS cost_gate_ok,
       (SELECT n FROM orgs) AS orgs;
-- S1 control. WANT n > 0 (0 = NOT EXERCISED, no decision). One scan of the observations table (<= 5,000,000 rows by G0).
SELECT count(*) AS n_all_7d
FROM public.provider_rate_limit_observations x
WHERE starts_with(x.org_id, 'c6a38355') AND x.observed_at >= now() - interval '7 days';
-- S2 counts by provider and dimension. INFO. One scan. Counts and seconds only.
SELECT x.provider, COALESCE(x.dimension, '(none)') AS dimension, count(*) AS n,
       count(DISTINCT x.sync_run_unit_id) AS units,
       count(DISTINCT date_trunc('day', x.observed_at)) AS days,
       round(min(x.retry_after_seconds)::numeric, 1) AS min_retry_after_s,
       round(max(x.retry_after_seconds)::numeric, 1) AS max_retry_after_s,
       count(*) FILTER (WHERE x.retry_after_seconds IS NULL) AS no_retry_after
FROM public.provider_rate_limit_observations x
WHERE starts_with(x.org_id, 'c6a38355') AND x.observed_at >= now() - interval '7 days'
GROUP BY x.provider, COALESCE(x.dimension, '(none)')
ORDER BY x.provider, 2;
-- S3 four-route cut (CHAOS-14552): observation rows of deferred units of github deployments, gitlab deployments,
-- github security and launchdarkly (any dataset), by provider and dataset. INFO. Observations scan + sync_run_units
-- (<= 5,000,000 rows by G0; joined on its primary key id).
SELECT x.provider, u.dataset_key, count(*) AS n, count(DISTINCT x.sync_run_unit_id) AS units,
       count(*) FILTER (WHERE x.dimension = 'secondary_abuse_risk') AS secondary_abuse_risk
FROM public.provider_rate_limit_observations x
JOIN public.sync_run_units u ON u.id = x.sync_run_unit_id AND starts_with(u.org_id, 'c6a38355')
WHERE starts_with(x.org_id, 'c6a38355') AND x.observed_at >= now() - interval '7 days'
  AND ((x.provider = 'github' AND u.dataset_key IN ('deployments', 'security'))
    OR (x.provider = 'gitlab' AND u.dataset_key = 'deployments')
    OR x.provider = 'launchdarkly')
GROUP BY x.provider, u.dataset_key
ORDER BY x.provider, u.dataset_key;
ROLLBACK;
`

const (
	observationFormOrg      = "c6a38355-0000-4000-8000-000000000001"
	observationFormOtherOrg = "ffffffff-0000-4000-8000-000000000002"
	formUnitGitHubDeploy    = "00000000-0000-4000-8000-0000000000a1"
	formUnitGitHubSecurity  = "00000000-0000-4000-8000-0000000000a2"
	formUnitGitLabDeploy    = "00000000-0000-4000-8000-0000000000a3"
	formUnitLaunchDarkly    = "00000000-0000-4000-8000-0000000000a4"
	formUnitGitHubGit       = "00000000-0000-4000-8000-0000000000a5"
	formUnitGitLabSecurity  = "00000000-0000-4000-8000-0000000000a6"
	formUnitOtherOrg        = "00000000-0000-4000-8000-0000000000a7"
)

func observationFormFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE public.organizations (id uuid PRIMARY KEY)`,
		`CREATE TABLE public.sync_run_units (id uuid PRIMARY KEY, org_id text NOT NULL, provider text NOT NULL, dataset_key character varying NOT NULL)`,
		`CREATE TABLE public.provider_rate_limit_observations (
			id uuid NOT NULL, org_id text NOT NULL, provider text NOT NULL, host text,
			integration_id uuid NOT NULL, sync_run_id uuid NOT NULL, sync_run_unit_id uuid NOT NULL,
			route_family text, route_family_attribution text, dimension text,
			retry_after_seconds double precision, reset_at timestamp with time zone,
			reason text, request_id text, observed_at timestamp with time zone NOT NULL)`,
		`INSERT INTO public.organizations VALUES ('` + observationFormOrg + `')`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	for _, unit := range [][4]string{
		{formUnitGitHubDeploy, observationFormOrg, "github", "deployments"},
		{formUnitGitHubSecurity, observationFormOrg, "github", "security"},
		{formUnitGitLabDeploy, observationFormOrg, "gitlab", "deployments"},
		{formUnitLaunchDarkly, observationFormOrg, "launchdarkly", "feature-flags"},
		{formUnitGitHubGit, observationFormOrg, "github", "git"},
		{formUnitGitLabSecurity, observationFormOrg, "gitlab", "security"},
		{formUnitOtherOrg, observationFormOtherOrg, "github", "deployments"},
	} {
		if _, err := pool.Exec(ctx, `INSERT INTO public.sync_run_units (id, org_id, provider, dataset_key) VALUES ($1, $2, $3, $4)`,
			unit[0], unit[1], unit[2], unit[3]); err != nil {
			t.Fatal(err)
		}
	}
}

func observationFormRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, org, provider, unit, dimension string, retryAfter any, age time.Duration) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO public.provider_rate_limit_observations
		(id, org_id, provider, integration_id, sync_run_id, sync_run_unit_id, dimension, retry_after_seconds, reason, observed_at)
		VALUES (gen_random_uuid(), $1, $2, gen_random_uuid(), gen_random_uuid(), $3::uuid, NULLIF($4, ''), $5, 'provider_rate_limited', now() - make_interval(secs => $6))`,
		org, provider, unit, dimension, retryAfter, age.Seconds()); err != nil {
		t.Fatal(err)
	}
}

func analyzeObservationFormTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, table := range []string{"provider_rate_limit_observations", "sync_run_units"} {
		if _, err := pool.Exec(ctx, `ANALYZE public.`+table); err != nil {
			t.Fatal(err)
		}
	}
}

func runObservationForm(ctx context.Context, pool *pgxpool.Pool, text string) ([][][]any, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	results, err := conn.Conn().PgConn().Exec(ctx, text).ReadAll()
	if err != nil {
		return nil, err
	}
	var out [][][]any
	for _, result := range results {
		if result.Err != nil {
			return nil, result.Err
		}
		var rows [][]any
		for _, raw := range result.Rows {
			row := make([]any, len(raw))
			for i, cell := range raw {
				row[i] = string(cell)
			}
			rows = append(rows, row)
		}
		out = append(out, rows)
	}
	return out, nil
}

func TestRateLimitObservationFormExecutes(t *testing.T) {
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
	observationFormFixture(t, ctx, pool)
	day := 24 * time.Hour
	org := observationFormOrg
	observationFormRow(t, ctx, pool, org, "github", formUnitGitHubDeploy, "secondary_abuse_risk", 60.0, 1*day)
	observationFormRow(t, ctx, pool, org, "github", formUnitGitHubSecurity, "secondary_abuse_risk", nil, 2*day)
	observationFormRow(t, ctx, pool, org, "github", formUnitGitHubGit, "secondary_abuse_risk", 90.0, 2*day)
	observationFormRow(t, ctx, pool, org, "github", formUnitGitHubGit, "rest_core", 30.0, 3*day)
	observationFormRow(t, ctx, pool, org, "github", formUnitGitHubGit, "", 5.0, 3*day)
	observationFormRow(t, ctx, pool, org, "gitlab", formUnitGitLabDeploy, "rest_core", 10.0, 1*day)
	observationFormRow(t, ctx, pool, org, "gitlab", formUnitGitLabSecurity, "rest_core", 10.0, 1*day)
	observationFormRow(t, ctx, pool, org, "launchdarkly", formUnitLaunchDarkly, "rest_core", 20.0, 1*day)
	observationFormRow(t, ctx, pool, org, "github", formUnitGitHubDeploy, "secondary_abuse_risk", 60.0, 8*day)
	observationFormRow(t, ctx, pool, observationFormOtherOrg, "github", formUnitOtherOrg, "secondary_abuse_risk", 60.0, 1*day)
	analyzeObservationFormTables(t, ctx, pool)

	t.Run("pass path: gate 0, control 8, counts by provider and dimension, four-route cut", func(t *testing.T) {
		results, err := runObservationForm(ctx, pool, rateLimitObservationFormSQL)
		if err != nil {
			t.Fatal(err)
		}
		// BEGIN, G0, S1, S2, S3, ROLLBACK
		if len(results) != 6 {
			t.Fatalf("%d result sets, want 6", len(results))
		}
		if got := results[1]; len(got) != 1 || got[0][0] != "0" || got[0][1] != "1" {
			t.Fatalf("G0 = %v, want cost_gate_ok 0, orgs 1", got)
		}
		if got := results[2]; len(got) != 1 || got[0][0] != "8" {
			t.Fatalf("S1 control = %v, want 8 rows (10 inserted, minus the 8 day old row and the other org)", got)
		}
		want := map[string]string{
			"github|(none)":               "1",
			"github|rest_core":            "1",
			"github|secondary_abuse_risk": "3",
			"gitlab|rest_core":            "2",
			"launchdarkly|rest_core":      "1",
		}
		got := map[string]string{}
		for _, row := range results[3] {
			got[row[0].(string)+"|"+row[1].(string)] = row[2].(string)
		}
		if len(got) != len(want) {
			t.Fatalf("S2 rows = %v, want %v", results[3], want)
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
		for _, row := range results[4] {
			gotCut[row[0].(string)+"|"+row[1].(string)] = row[2].(string) + "/" + row[4].(string)
		}
		if len(gotCut) != len(cut) {
			t.Fatalf("S3 rows = %v, want %v", results[4], cut)
		}
		for key, n := range cut {
			if gotCut[key] != n {
				t.Fatalf("S3 %s = %q, want %q (all: %v)", key, gotCut[key], n, gotCut)
			}
		}
	})

	t.Run("G0 fires: cap literal 0 STOP-G0", func(t *testing.T) {
		low := strings.Replace(rateLimitObservationFormSQL, "<= 5000000", "<= 0", 1)
		if low == rateLimitObservationFormSQL {
			t.Fatal("cap literal not found")
		}
		_, err := runObservationForm(ctx, pool, low)
		if err == nil || !strings.Contains(err.Error(), "STOP-G0") {
			t.Fatalf("err = %v, want STOP-G0", err)
		}
	})

	t.Run("G0 fires: sync_run_units cap literal 0 STOP-G0", func(t *testing.T) {
		low := strings.Replace(rateLimitObservationFormSQL, "'sync_run_units') <= 5000000", "'sync_run_units') <= 0", 1)
		if low == rateLimitObservationFormSQL {
			t.Fatal("cap literal not found")
		}
		_, err := runObservationForm(ctx, pool, low)
		if err == nil || !strings.Contains(err.Error(), "STOP-G0") {
			t.Fatalf("err = %v, want STOP-G0", err)
		}
	})

	t.Run("G0 fires: org prefix matching nothing", func(t *testing.T) {
		none := strings.Replace(rateLimitObservationFormSQL, "'c6a38355')", "'00000000')", 1)
		_, err := runObservationForm(ctx, pool, none)
		if err == nil || !strings.Contains(err.Error(), "STOP-G0 orgs=0") {
			t.Fatalf("err = %v, want STOP-G0 orgs=0", err)
		}
	})

	t.Run("read only: a write inside the transaction is refused", func(t *testing.T) {
		write := strings.Replace(rateLimitObservationFormSQL, "ROLLBACK;", "DELETE FROM public.provider_rate_limit_observations;\nROLLBACK;", 1)
		_, err := runObservationForm(ctx, pool, write)
		if err == nil || !strings.Contains(err.Error(), "read-only") {
			t.Fatalf("err = %v, want a read-only transaction error", err)
		}
	})

	t.Run("control fails: an emptied table reads 0 and S2 and S3 are empty", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `DELETE FROM public.provider_rate_limit_observations`); err != nil {
			t.Fatal(err)
		}
		analyzeObservationFormTables(t, ctx, pool)
		results, err := runObservationForm(ctx, pool, rateLimitObservationFormSQL)
		if err != nil {
			t.Fatal(err)
		}
		if got := results[1]; len(got) != 1 || got[0][0] != "0" || got[0][1] != "1" {
			t.Fatalf("G0 = %v, want cost_gate_ok 0, orgs 1 on an empty table", got)
		}
		if got := results[2]; len(got) != 1 || got[0][0] != "0" {
			t.Fatalf("S1 control = %v, want 0 (NOT EXERCISED)", got)
		}
		if len(results[3]) != 0 || len(results[4]) != 0 {
			t.Fatalf("S2 = %v, S3 = %v, want no rows", results[3], results[4])
		}
	})
}

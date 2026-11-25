//go:build integration

package providersync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// DO NOT MERGE. This file executes the prod read FORM-14544-SRC-2.sql byte for byte with every prod literal swapped
// to a synthetic value (the sha256 below is the sha256 of the swapped file), on the migrated Postgres schema with a synthetic
// fixture, the way psql -f runs it (one simple-protocol call, the first error stops the rest). It checks each WANT,
// fires each G0 case, runs the rule and org mutants, and checks that the SQL repo cut is the cut the GitHub routes mint
// (repositoryIdentity).
const formExecSrc2x14544SHA256 = "19b414b5cc1ff48c8d3982821f0ed372b421622548cb3bad0ed77cfd88a4e55e"

const formExecSrc2x14544SQL = `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
-- FORM-14544-SRC-2 (lane fcs-14544sel5, 2026-11-25, ops pin 3207ac6258c0). Postgres prod, read only, one transaction.
-- acp option (P) + READ-14544-P1-STOP-1: source 0c0c0c0c = repo key c796c1a6 -> its minted repo cut. TEMPLATE-5.
-- WANT text and bound math: FORM-14544-SRC-2.md. No SET of any kind, no write. Outputs = counts, bytes, one 8-char cut.
-- G0 catalog gate: reads no integration_sources row (the orgs count reads organizations). Sizes = pg_relation_size.
-- WANT cost_gate_ok = 0, orgs = 1. Else the named cast error STOP-G0 aborts the transaction (ON_ERROR_STOP).
WITH orgs AS (SELECT count(*) AS n FROM public.organizations o WHERE starts_with(o.id::text, 'f0f0f0f0')),
rel AS (SELECT c.relname, c.reltuples::bigint AS reltuples,
               pg_catalog.pg_relation_size(c.oid) AS heap_bytes,
               CASE WHEN c.reltoastrelid = 0 THEN 0 ELSE pg_catalog.pg_relation_size(c.reltoastrelid) END AS toast_bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname IN ('integration_sources', 'organizations'))
SELECT CASE WHEN (SELECT n FROM orgs) = 1
             AND (SELECT count(*) FROM rel) = 2
             AND (SELECT heap_bytes + toast_bytes FROM rel WHERE relname = 'integration_sources') <= 268435456
             AND (SELECT heap_bytes + toast_bytes FROM rel WHERE relname = 'organizations') <= 67108864
            THEN 0
            ELSE ('STOP-G0 orgs=' || (SELECT n FROM orgs)
                  || ' tables=' || (SELECT count(*) FROM rel)
                  || ' bytes=' || COALESCE((SELECT string_agg(relname || ':' || heap_bytes || '+' || toast_bytes, ',' ORDER BY relname) FROM rel), 'none')
                  || ' reltuples=' || COALESCE((SELECT string_agg(relname || ':' || reltuples, ',' ORDER BY relname) FROM rel), 'none'))::int
       END AS cost_gate_ok,
       (SELECT n FROM orgs) AS orgs,
       (SELECT heap_bytes FROM rel WHERE relname = 'integration_sources') AS sources_heap_bytes,
       (SELECT toast_bytes FROM rel WHERE relname = 'integration_sources') AS sources_toast_bytes,
       (SELECT heap_bytes + toast_bytes FROM rel WHERE relname = 'organizations') AS organizations_bytes;
-- stmt0 ORG-1. WANT orgs = 1. One scan of organizations (bounded by G0).
SELECT count(*) AS orgs
FROM public.organizations o
WHERE starts_with(o.id::text, 'f0f0f0f0');
-- stmt1 the source (keyed on its id prefix) and its minted repo cut. WANT n_sources = 1, github_repository_sources = 1,
-- name_is_external_id = 1, repo_key_match = 1 (the repo key = left(md5(lower(full_name)), 8), the label the driver used),
-- repo_cut = 8 hex chars. repo_cut = the first 8 hex of sha256(lower(btrim(full_name))) = the first 8 chars of the
-- repos.id the GitHub routes mint from that path (github_repository_route.go:287-306). Printed only when exactly one
-- source matches (else empty). Never full_name, never an id. One scan of integration_sources (bounded by G0).
SELECT count(*) AS n_sources,
       count(*) FILTER (WHERE s.provider = 'github' AND s.source_type = 'repository') AS github_repository_sources,
       count(*) FILTER (WHERE lower(btrim(s.full_name)) = lower(btrim(s.external_id))) AS name_is_external_id,
       count(*) FILTER (WHERE left(md5(lower(s.full_name)), 8) = 'c796c1a6') AS repo_key_match,
       CASE WHEN count(*) = 1
            THEN min(substr(encode(sha256(convert_to(lower(btrim(s.full_name)), 'UTF8')), 'hex'), 1, 8))
       END AS repo_cut
FROM public.integration_sources s
WHERE starts_with(s.org_id, 'f0f0f0f0') AND s.id::text LIKE '0c0c0c0c%';
-- stmt2 control. WANT github_sources > 0 (the read sees the org's GitHub sources). github_repository_sources is INFO.
-- One scan of integration_sources (bounded by G0).
SELECT count(*) AS github_sources,
       count(*) FILTER (WHERE s.source_type = 'repository') AS github_repository_sources
FROM public.integration_sources s
WHERE starts_with(s.org_id, 'f0f0f0f0') AND s.provider = 'github';
ROLLBACK;
`

const (
	formExecSrc2x14544Org      = "f0f0f0f0-1454-4000-8000-000000000001"
	formExecSrc2x14544Org2     = "f0f0f0f0-1454-4000-8000-000000000009"
	formExecSrc2x14544Other    = "d0d0d0d0-1454-4000-8000-000000000002"
	formExecSrc2x14544Source   = "0c0c0c0c-1454-4000-8000-000000000001"
	formExecSrc2x14544Twin     = "0c0c0c0c-1454-4000-8000-000000000002"
	formExecSrc2x14544OtherSrc = "0c0c0c0c-1454-4000-8000-0000000000e0"
	formExecSrc2x14544Peer     = "11111111-1454-4000-8000-000000000003"
	formExecSrc2x14544Project  = "22222222-1454-4000-8000-000000000004"
	formExecSrc2x14544Big      = "33333333-1454-4000-8000-000000000005"
	formExecSrc2x14544Name     = "  FxOrg/Form-Repo-14544 "
)

type formExecSrc2x14544Result struct {
	columns []string
	rows    [][]string
}

func formExecSrc2x14544Run(ctx context.Context, pool *pgxpool.Pool, text string) ([]formExecSrc2x14544Result, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	results, err := conn.Conn().PgConn().Exec(ctx, text).ReadAll()
	if err != nil {
		return nil, err
	}
	var out []formExecSrc2x14544Result
	for _, result := range results {
		if result.Err != nil {
			return nil, result.Err
		}
		set := formExecSrc2x14544Result{}
		for _, field := range result.FieldDescriptions {
			set.columns = append(set.columns, field.Name)
		}
		for _, raw := range result.Rows {
			row := make([]string, len(raw))
			for i, cell := range raw {
				row[i] = string(cell)
			}
			set.rows = append(set.rows, row)
		}
		out = append(out, set)
	}
	return out, nil
}

// formExecSrc2x14544Cells is one statement's single row by column name: results are BEGIN, G0, stmt0, stmt1, stmt2, ROLLBACK.
func formExecSrc2x14544Cells(t *testing.T, results []formExecSrc2x14544Result, index int, label string) map[string]string {
	t.Helper()
	if len(results) != 6 {
		t.Fatalf("%s: %d result sets, want 6 (BEGIN, G0, stmt0, stmt1, stmt2, ROLLBACK)", label, len(results))
	}
	set := results[index]
	if len(set.rows) != 1 {
		t.Fatalf("%s: result %d has %d rows, want 1", label, index, len(set.rows))
	}
	cells := map[string]string{}
	for i, name := range set.columns {
		cells[name] = set.rows[0][i]
	}
	t.Logf("%s result %d %v", label, index, cells)
	return cells
}

func formExecSrc2x14544Want(t *testing.T, label string, got map[string]string, want map[string]string) {
	t.Helper()
	for column, value := range want {
		if have, ok := got[column]; !ok || have != value {
			t.Errorf("%s: %s = %q, want %q (row %v)", label, column, have, value, got)
		}
	}
}

func formExecSrc2x14544GoCut(t *testing.T, name string) string {
	t.Helper()
	minted, err := repositoryIdentity(name)
	if err != nil {
		t.Fatalf("repositoryIdentity(%q): %v", name, err)
	}
	return minted[:8]
}

func formExecSrc2x14544AddSource(ctx context.Context, t *testing.T, pool *pgxpool.Pool, id, org, provider, sourceType, externalID, fullName string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO public.integration_sources
		  (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
		  VALUES ($1, $2, $3, $4, $5, $6, $6, $7, '{}', TRUE, NOW(), NOW())`,
		id, org, firstIntegrationID, provider, sourceType, externalID, fullName); err != nil {
		t.Fatal(err)
	}
}

func TestFormExecFcs14544Src2(t *testing.T) {
	t.Setenv("REPO_UUID", "")
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	sum := sha256.Sum256([]byte(formExecSrc2x14544SQL))
	if got := hex.EncodeToString(sum[:]); got != formExecSrc2x14544SHA256 {
		t.Fatalf("sha256 of the embedded text = %s, want %s (FORM-14544-SRC-2 synthetic copy)", got, formExecSrc2x14544SHA256)
	}
	for index, line := range strings.Split(formExecSrc2x14544SQL, "\n") {
		if strings.HasPrefix(line, "--") && strings.Contains(line, ";") {
			t.Errorf("comment line %d holds a ';': %s", index+1, line)
		}
	}
	for _, marker := range []string{"\n-- G0 ", "\n-- stmt0 ", "\n-- stmt1 ", "\n-- stmt2 "} {
		if got := strings.Count(formExecSrc2x14544SQL, marker); got != 1 {
			t.Errorf("marker %q %d time(s), want 1", strings.TrimSpace(marker), got)
		}
	}
	if !strings.HasPrefix(formExecSrc2x14544SQL, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;\n") || !strings.HasSuffix(formExecSrc2x14544SQL, "\nROLLBACK;\n") {
		t.Fatalf("the FORM is not one READ ONLY transaction that ends in ROLLBACK")
	}
	for _, banned := range []string{"\nSET ", " SET LOCAL", "INSERT ", "UPDATE ", "DELETE ", "full_name AS", "s.id AS"} {
		if strings.Contains(strings.ToUpper(formExecSrc2x14544SQL), strings.ToUpper(banned)) {
			t.Errorf("the FORM holds %q", banned)
		}
	}

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
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	var serverVersion string
	if err := pool.QueryRow(ctx, `SHOW server_version`).Scan(&serverVersion); err != nil {
		t.Fatal(err)
	}
	t.Logf("PostgreSQL server_version %s", serverVersion)
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	exec(`INSERT INTO public.organizations (id, slug, name) VALUES ($1, 'form-14544', 'form-14544'), ($2, 'form-14544-other', 'form-14544-other')`,
		formExecSrc2x14544Org, formExecSrc2x14544Other)
	formExecSrc2x14544AddSource(ctx, t, pool, formExecSrc2x14544Source, formExecSrc2x14544Org, "github", "repository", "fxorg/form-repo-14544", formExecSrc2x14544Name)
	formExecSrc2x14544AddSource(ctx, t, pool, formExecSrc2x14544Peer, formExecSrc2x14544Org, "github", "repository", "fxorg/peer-14544", "fxorg/peer-14544")
	formExecSrc2x14544AddSource(ctx, t, pool, formExecSrc2x14544Project, formExecSrc2x14544Org, "gitlab", "project", "1454400", "fxgroup/project-14544")
	formExecSrc2x14544AddSource(ctx, t, pool, formExecSrc2x14544OtherSrc, formExecSrc2x14544Other, "github", "repository", "fxorg/other-14544", "fxorg/other-14544")

	run := func(label, text string) []formExecSrc2x14544Result {
		t.Helper()
		results, err := formExecSrc2x14544Run(ctx, pool, text)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		return results
	}
	reltuples := func() string {
		var value string
		if err := pool.QueryRow(ctx, `SELECT reltuples::bigint::text FROM pg_class WHERE oid = 'public.integration_sources'::regclass`).Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	wantPass := func(label string, results []formExecSrc2x14544Result, name string) {
		t.Helper()
		g0 := formExecSrc2x14544Cells(t, results, 1, label+" G0")
		formExecSrc2x14544Want(t, label+" G0", g0, map[string]string{"cost_gate_ok": "0", "orgs": "1"})
		for _, column := range []string{"sources_heap_bytes", "organizations_bytes"} {
			if size, err := strconv.ParseInt(g0[column], 10, 64); err != nil || size <= 0 {
				t.Errorf("%s G0 %s = %q, want a positive byte size", label, column, g0[column])
			}
		}
		if size, err := strconv.ParseInt(g0["sources_toast_bytes"], 10, 64); err != nil || size < 0 {
			t.Errorf("%s G0 sources_toast_bytes = %q, want a byte size", label, g0["sources_toast_bytes"])
		}
		formExecSrc2x14544Want(t, label+" stmt0", formExecSrc2x14544Cells(t, results, 2, label+" stmt0"), map[string]string{"orgs": "1"})
		formExecSrc2x14544Want(t, label+" stmt1", formExecSrc2x14544Cells(t, results, 3, label+" stmt1"), map[string]string{
			"n_sources": "1", "github_repository_sources": "1", "name_is_external_id": "1", "repo_key_match": "1", "repo_cut": formExecSrc2x14544GoCut(t, name)})
		formExecSrc2x14544Want(t, label+" stmt2", formExecSrc2x14544Cells(t, results, 4, label+" stmt2"), map[string]string{
			"github_sources": "2", "github_repository_sources": "2"})
	}

	t.Run("pass path before ANALYZE", func(t *testing.T) {
		t.Logf("integration_sources reltuples = %s", reltuples())
		wantPass(t.Name(), run(t.Name(), formExecSrc2x14544SQL), formExecSrc2x14544Name)
	})

	t.Run("cut equality: the SQL cut is repositoryIdentity's first 8 chars", func(t *testing.T) {
		cuts := map[string]bool{}
		for _, name := range []string{formExecSrc2x14544Name, "FXORG/FORM-REPO-14544", "fxorg/form-repo-14544"} {
			exec(`UPDATE public.integration_sources SET full_name = $2 WHERE id = $1`, formExecSrc2x14544Source, name)
			results := run(t.Name(), formExecSrc2x14544SQL)
			cell := formExecSrc2x14544Cells(t, results, 3, "stmt1 "+strconv.Quote(name))
			goCut := formExecSrc2x14544GoCut(t, name)
			if cell["repo_cut"] != goCut || len(goCut) != 8 {
				t.Errorf("name %q: SQL repo_cut %q, Go repositoryIdentity cut %q, want equal", name, cell["repo_cut"], goCut)
			}
			cuts[cell["repo_cut"]] = true
			wantKey := "0"
			if name == formExecSrc2x14544Name {
				wantKey = "1"
			}
			if cell["repo_key_match"] != wantKey {
				t.Errorf("name %q: repo_key_match %q, want %s (the repo key hashes lower(full_name) untrimmed)", name, cell["repo_key_match"], wantKey)
			}
			t.Logf("CUT-EQUAL name=%q sql=%s go=%s repo_key_match=%s", name, cell["repo_cut"], goCut, cell["repo_key_match"])
		}
		if len(cuts) != 1 {
			t.Errorf("mixed case and surrounding spaces gave %d cuts, want 1: %v", len(cuts), cuts)
		}
		exec(`UPDATE public.integration_sources SET full_name = $2 WHERE id = $1`, formExecSrc2x14544Source, "fxorg/form-repo-14545")
		cell := formExecSrc2x14544Cells(t, run(t.Name(), formExecSrc2x14544SQL), 3, "stmt1 other name")
		if cuts[cell["repo_cut"]] || cell["repo_cut"] != formExecSrc2x14544GoCut(t, "fxorg/form-repo-14545") {
			t.Errorf("another name: SQL repo_cut %q, want the Go cut of that name and not %v", cell["repo_cut"], cuts)
		}
		exec(`UPDATE public.integration_sources SET full_name = $2 WHERE id = $1`, formExecSrc2x14544Source, formExecSrc2x14544Name)
	})

	exec(`ANALYZE public.integration_sources`)
	exec(`ANALYZE public.organizations`)

	t.Run("pass path after ANALYZE", func(t *testing.T) {
		t.Logf("integration_sources reltuples = %s", reltuples())
		wantPass(t.Name(), run(t.Name(), formExecSrc2x14544SQL), formExecSrc2x14544Name)
	})

	stmt1 := func(label, text string) map[string]string {
		t.Helper()
		return formExecSrc2x14544Cells(t, run(label, text), 3, label+" stmt1")
	}
	t.Run("rule mutant: wrong source prefix reads 0 and no cut", func(t *testing.T) {
		text := strings.Replace(formExecSrc2x14544SQL, "LIKE '0c0c0c0c%'", "LIKE '0c0c0c0d%'", 1)
		if text == formExecSrc2x14544SQL {
			t.Fatal("source prefix literal not found")
		}
		formExecSrc2x14544Want(t, t.Name(), stmt1(t.Name(), text), map[string]string{"n_sources": "0", "repo_cut": ""})
	})
	t.Run("key mutant: the old form keys the source id on the repo key and reads 0 (red)", func(t *testing.T) {
		var key string
		if err := pool.QueryRow(ctx, `SELECT left(md5(lower(full_name)), 8) FROM public.integration_sources WHERE id = $1`, formExecSrc2x14544Source).Scan(&key); err != nil {
			t.Fatal(err)
		}
		if key == formExecSrc2x14544Source[:8] || !strings.Contains(formExecSrc2x14544SQL, "= '"+key+"') AS repo_key_match") {
			t.Fatalf("fixture: repo key %s must differ from the source id prefix and be the FORM's repo key literal", key)
		}
		text := strings.Replace(formExecSrc2x14544SQL, "AND s.id::text LIKE '"+formExecSrc2x14544Source[:8]+"%'", "AND s.id::text LIKE '"+key+"%'", 1)
		if text == formExecSrc2x14544SQL {
			t.Fatal("source key clause not found")
		}
		formExecSrc2x14544Want(t, t.Name(), stmt1(t.Name(), text), map[string]string{"n_sources": "0", "repo_key_match": "0", "repo_cut": ""})
	})
	t.Run("rule mutant: a source whose name gives another repo key reads repo_key_match 0", func(t *testing.T) {
		exec(`UPDATE public.integration_sources SET full_name = 'fxorg/form-repo-14544' WHERE id = $1`, formExecSrc2x14544Source)
		formExecSrc2x14544Want(t, t.Name(), stmt1(t.Name(), formExecSrc2x14544SQL), map[string]string{"n_sources": "1", "repo_key_match": "0"})
		exec(`UPDATE public.integration_sources SET full_name = $2 WHERE id = $1`, formExecSrc2x14544Source, formExecSrc2x14544Name)
	})
	t.Run("rule mutant: 2 matching sources read 2 and no cut", func(t *testing.T) {
		formExecSrc2x14544AddSource(ctx, t, pool, formExecSrc2x14544Twin, formExecSrc2x14544Org, "github", "repository", "fxorg/twin-14544", "fxorg/twin-14544")
		formExecSrc2x14544Want(t, t.Name(), stmt1(t.Name(), formExecSrc2x14544SQL), map[string]string{"n_sources": "2", "github_repository_sources": "2", "repo_cut": ""})
		exec(`DELETE FROM public.integration_sources WHERE id = $1`, formExecSrc2x14544Twin)
	})
	t.Run("rule mutant: 0 matching sources read 0 and no cut", func(t *testing.T) {
		exec(`DELETE FROM public.integration_sources WHERE id = $1`, formExecSrc2x14544Source)
		formExecSrc2x14544Want(t, t.Name(), stmt1(t.Name(), formExecSrc2x14544SQL), map[string]string{"n_sources": "0", "github_repository_sources": "0", "repo_cut": ""})
		formExecSrc2x14544AddSource(ctx, t, pool, formExecSrc2x14544Source, formExecSrc2x14544Org, "github", "repository", "fxorg/form-repo-14544", formExecSrc2x14544Name)
	})
	t.Run("rule mutant: not a github repository source, name not the external id", func(t *testing.T) {
		exec(`UPDATE public.integration_sources SET source_type = 'account', external_id = 'fxorg' WHERE id = $1`, formExecSrc2x14544Source)
		formExecSrc2x14544Want(t, t.Name(), stmt1(t.Name(), formExecSrc2x14544SQL), map[string]string{
			"n_sources": "1", "github_repository_sources": "0", "name_is_external_id": "0"})
		exec(`UPDATE public.integration_sources SET source_type = 'repository', external_id = 'fxorg/form-repo-14544' WHERE id = $1`, formExecSrc2x14544Source)
	})
	t.Run("org: another org's source with the prefix is not counted", func(t *testing.T) {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.integration_sources WHERE id::text LIKE '0c0c0c0c%'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Fatalf("fixture: %d sources with the prefix in all orgs, want 2", n)
		}
		wantPass(t.Name(), run(t.Name(), formExecSrc2x14544SQL), formExecSrc2x14544Name)
	})
	t.Run("org mutant: a second org with the prefix stops G0 (orgs=2)", func(t *testing.T) {
		exec(`INSERT INTO public.organizations (id, slug, name) VALUES ($1, 'form-14544-two', 'form-14544-two')`, formExecSrc2x14544Org2)
		_, err := formExecSrc2x14544Run(ctx, pool, formExecSrc2x14544SQL)
		if err == nil || !strings.Contains(err.Error(), "STOP-G0 orgs=2") {
			t.Errorf("err = %v, want STOP-G0 orgs=2", err)
		}
		exec(`DELETE FROM public.organizations WHERE id = $1`, formExecSrc2x14544Org2)
	})
	t.Run("org mutant: an org prefix matching nothing stops G0 (orgs=0)", func(t *testing.T) {
		text := strings.Replace(formExecSrc2x14544SQL, "starts_with(o.id::text, 'f0f0f0f0')", "starts_with(o.id::text, '00000000')", 1)
		_, err := formExecSrc2x14544Run(ctx, pool, text)
		if err == nil || !strings.Contains(err.Error(), "STOP-G0 orgs=0") {
			t.Errorf("err = %v, want STOP-G0 orgs=0", err)
		}
	})
	for _, mutant := range []struct{ name, from, to string }{
		{"integration_sources bytes cap 0", "'integration_sources') <= 268435456", "'integration_sources') <= 0"},
		{"organizations bytes cap 0", "'organizations') <= 67108864", "'organizations') <= 0"},
	} {
		t.Run("G0 fires: "+mutant.name, func(t *testing.T) {
			if got := strings.Count(formExecSrc2x14544SQL, mutant.from); got != 1 {
				t.Fatalf("cap literal %q %d time(s), want 1", mutant.from, got)
			}
			_, err := formExecSrc2x14544Run(ctx, pool, strings.Replace(formExecSrc2x14544SQL, mutant.from, mutant.to, 1))
			if err == nil || !strings.Contains(err.Error(), "STOP-G0 orgs=1") || !strings.Contains(err.Error(), "bytes=") {
				t.Errorf("err = %v, want STOP-G0 with the bytes text", err)
			}
		})
	}
	t.Run("G0 counts TOAST: a cap at the heap size stops once TOAST holds bytes", func(t *testing.T) {
		exec(`INSERT INTO public.integration_sources
			  (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
			  VALUES ($1, $2, $3, 'github', 'repository', 'fxorg/big-14544', 'big-14544', 'fxorg/big-14544',
			  json_build_object('blob', (SELECT string_agg(md5(i::text), '') FROM generate_series(1, 30000) AS i)), TRUE, NOW(), NOW())`,
			formExecSrc2x14544Big, formExecSrc2x14544Org, firstIntegrationID)
		var heap, toast int64
		if err := pool.QueryRow(ctx, `SELECT pg_relation_size(c.oid), CASE WHEN c.reltoastrelid = 0 THEN 0 ELSE pg_relation_size(c.reltoastrelid) END
			FROM pg_class c WHERE c.oid = 'public.integration_sources'::regclass`).Scan(&heap, &toast); err != nil {
			t.Fatal(err)
		}
		t.Logf("integration_sources heap %d bytes, TOAST %d bytes", heap, toast)
		if toast <= 0 {
			t.Fatalf("TOAST holds %d bytes, want > 0 for this case", toast)
		}
		at := strconv.FormatInt(heap, 10)
		passText := strings.Replace(formExecSrc2x14544SQL, "'integration_sources') <= 268435456", "'integration_sources') <= "+strconv.FormatInt(heap+toast, 10), 1)
		results := run(t.Name()+" cap heap+TOAST", passText)
		formExecSrc2x14544Want(t, t.Name()+" G0", formExecSrc2x14544Cells(t, results, 1, t.Name()+" G0"), map[string]string{
			"cost_gate_ok": "0", "sources_heap_bytes": at, "sources_toast_bytes": strconv.FormatInt(toast, 10)})
		_, err := formExecSrc2x14544Run(ctx, pool, strings.Replace(formExecSrc2x14544SQL, "'integration_sources') <= 268435456", "'integration_sources') <= "+at, 1))
		if err == nil || !strings.Contains(err.Error(), "STOP-G0") || !strings.Contains(err.Error(), "integration_sources:"+at+"+") {
			t.Errorf("err = %v, want STOP-G0 naming integration_sources:%s+<toast>", err, at)
		}
		exec(`DELETE FROM public.integration_sources WHERE id = $1`, formExecSrc2x14544Big)
	})
	t.Run("control mutant: no github source reads github_sources 0", func(t *testing.T) {
		text := strings.Replace(formExecSrc2x14544SQL, "AND s.provider = 'github';", "AND s.provider = 'none';", 1)
		if text == formExecSrc2x14544SQL {
			t.Fatal("control literal not found")
		}
		formExecSrc2x14544Want(t, t.Name(), formExecSrc2x14544Cells(t, run(t.Name(), text), 4, t.Name()+" stmt2"), map[string]string{"github_sources": "0"})
	})
	t.Run("read only: a write inside the transaction is refused", func(t *testing.T) {
		text := strings.Replace(formExecSrc2x14544SQL, "ROLLBACK;", "DELETE FROM public.integration_sources;\nROLLBACK;", 1)
		_, err := formExecSrc2x14544Run(ctx, pool, text)
		if err == nil || !strings.Contains(err.Error(), "read-only") {
			t.Errorf("err = %v, want a read-only transaction error", err)
		}
	})
}

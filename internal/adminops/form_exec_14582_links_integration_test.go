//go:build integration

package adminops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DO NOT MERGE. CHAOS-14582 FORM exec leg: FORM-14582-LINKS-1 executed as pm runs it, on a migrated PostgreSQL with
// synthetic rows. The embedded text is the CI copy: FORM-14582-LINKS-1.sql (sha256 24e1f0e191867a069000540938263fea04c5b8ab608dcf90ab1ce82057406f5b) with its org prefix
// swapped to f0f0f0f0 and nothing else; its own sha256 is checked below. Synthetic org ids: f0f0f0f0 (the org of the
// FORM's org filters) and e1e1e1e1 (another org). No prod id is in this file.
const (
	links14582FormSHA256 = "24e1f0e191867a069000540938263fea04c5b8ab608dcf90ab1ce82057406f5b"
	links14582CopySHA256 = "2da5f75b79b18e6069ad74d578fc3e2f6424ea9030c712b9db44bb53175542d8"
	links14582SQL        = `-- FORM-14582-LINKS-1 (lane-14559, 2026-11-24, ops main ede68fe0f296). PostgreSQL prod, READ ONLY, CHAOS-14582.
-- Cross-org links: sync configurations and integration datasets whose integration belongs to another org. The whole-PATCH
-- 422 of CHAOS-14582 changes a live 200 only for such a configuration; WANT 0 of each before the merge.
-- NAMED EXCEPTION (acp FORM scope ruling): S1 and S2 run over ALL orgs (org-free). A cross-org link of any org pair breaks
-- the rule, and an org-scoped count would be a false 0. They print counts only (no id, no cut). ORG-1 (in G0 and S0) and
-- C1 keep the org prefix f0f0f0f0.
-- Shape: pm-guard/FORM-TEMPLATE-3.md (sha12 6b9e73a3c45a), PostgreSQL form (b); run line in FORM-14582-LINKS-1.md.
-- No SET, EXPLAIN, write or cast of stored text. Every statement holds the same catalog gate as a one-time filter.
-- WANT text and work math: FORM-14582-LINKS-1.md.

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;

-- G0 catalog gate (pg_catalog) and ORG-1. WANT cost_gate_ok = 0. Caps (reltuples / heap + TOAST bytes):
-- organizations 100000 / 268435456, integrations 150000 / 402653184, sync_configurations 200000 / 536870912,
-- integration_datasets 1000000 / 1073741824. A table missing, never analyzed or over a cap, or orgs <> 1 = the named
-- cast error STOP-G0 (sizes and counts only). Every later statement holds the same gate as a one-time filter.
WITH gate AS MATERIALIZED (
    WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
    rel AS (
        SELECT c.relname::text AS relname, c.reltuples::bigint AS reltuples,
               c.relpages::bigint * bs.block_size + COALESCE(t.relpages::bigint, 0) * bs.block_size AS bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN bs
        LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname IN ('organizations', 'integrations', 'sync_configurations', 'integration_datasets')
    ),
    caps (relname, max_rows, max_bytes) AS (
        VALUES ('organizations', 100000::bigint, 268435456::bigint),
               ('integrations', 150000::bigint, 402653184::bigint),
               ('sync_configurations', 200000::bigint, 536870912::bigint),
               ('integration_datasets', 1000000::bigint, 1073741824::bigint)
    ),
    org AS (
        SELECT count(*) AS orgs FROM public.organizations o WHERE starts_with(o.id::text, 'f0f0f0f0')
    )
    SELECT CASE WHEN count(rel.relname) = 4
                 AND bool_and(rel.reltuples >= 0 AND rel.reltuples <= caps.max_rows AND rel.bytes <= caps.max_bytes)
                 AND max(org.orgs) = 1
                THEN 0
                ELSE ('STOP-G0 ' || string_agg(caps.relname || '=' || COALESCE(rel.reltuples::text, 'missing') || '/'
                                               || COALESCE(rel.bytes::text, 'missing'), ' ' ORDER BY caps.relname)
                      || ' orgs=' || max(org.orgs)::text)::int
           END AS cost_gate_ok
    FROM caps LEFT JOIN rel ON rel.relname = caps.relname CROSS JOIN org
)
SELECT cost_gate_ok FROM gate;

-- S0 ORG-1 (G0 stops unless it is 1). WANT orgs = 1.
WITH gate AS MATERIALIZED (
    WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
    rel AS (
        SELECT c.relname::text AS relname, c.reltuples::bigint AS reltuples,
               c.relpages::bigint * bs.block_size + COALESCE(t.relpages::bigint, 0) * bs.block_size AS bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN bs
        LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname IN ('organizations', 'integrations', 'sync_configurations', 'integration_datasets')
    ),
    caps (relname, max_rows, max_bytes) AS (
        VALUES ('organizations', 100000::bigint, 268435456::bigint),
               ('integrations', 150000::bigint, 402653184::bigint),
               ('sync_configurations', 200000::bigint, 536870912::bigint),
               ('integration_datasets', 1000000::bigint, 1073741824::bigint)
    ),
    org AS (
        SELECT count(*) AS orgs FROM public.organizations o WHERE starts_with(o.id::text, 'f0f0f0f0')
    )
    SELECT CASE WHEN count(rel.relname) = 4
                 AND bool_and(rel.reltuples >= 0 AND rel.reltuples <= caps.max_rows AND rel.bytes <= caps.max_bytes)
                 AND max(org.orgs) = 1
                THEN 0
                ELSE ('STOP-G0 ' || string_agg(caps.relname || '=' || COALESCE(rel.reltuples::text, 'missing') || '/'
                                               || COALESCE(rel.bytes::text, 'missing'), ' ' ORDER BY caps.relname)
                      || ' orgs=' || max(org.orgs)::text)::int
           END AS cost_gate_ok
    FROM caps LEFT JOIN rel ON rel.relname = caps.relname CROSS JOIN org
)
SELECT count(*) AS orgs
FROM public.organizations o
WHERE (SELECT g.cost_gate_ok FROM gate g) = 0
  AND starts_with(o.id::text, 'f0f0f0f0');

-- S1 org-free (named exception): sync configurations whose integration belongs to a different org. WANT cross_org_configs = 0.
WITH gate AS MATERIALIZED (
    WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
    rel AS (
        SELECT c.relname::text AS relname, c.reltuples::bigint AS reltuples,
               c.relpages::bigint * bs.block_size + COALESCE(t.relpages::bigint, 0) * bs.block_size AS bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN bs
        LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname IN ('organizations', 'integrations', 'sync_configurations', 'integration_datasets')
    ),
    caps (relname, max_rows, max_bytes) AS (
        VALUES ('organizations', 100000::bigint, 268435456::bigint),
               ('integrations', 150000::bigint, 402653184::bigint),
               ('sync_configurations', 200000::bigint, 536870912::bigint),
               ('integration_datasets', 1000000::bigint, 1073741824::bigint)
    ),
    org AS (
        SELECT count(*) AS orgs FROM public.organizations o WHERE starts_with(o.id::text, 'f0f0f0f0')
    )
    SELECT CASE WHEN count(rel.relname) = 4
                 AND bool_and(rel.reltuples >= 0 AND rel.reltuples <= caps.max_rows AND rel.bytes <= caps.max_bytes)
                 AND max(org.orgs) = 1
                THEN 0
                ELSE ('STOP-G0 ' || string_agg(caps.relname || '=' || COALESCE(rel.reltuples::text, 'missing') || '/'
                                               || COALESCE(rel.bytes::text, 'missing'), ' ' ORDER BY caps.relname)
                      || ' orgs=' || max(org.orgs)::text)::int
           END AS cost_gate_ok
    FROM caps LEFT JOIN rel ON rel.relname = caps.relname CROSS JOIN org
)
SELECT count(*) AS cross_org_configs
FROM public.sync_configurations c
JOIN public.integrations i ON i.id = c.integration_id
WHERE (SELECT g.cost_gate_ok FROM gate g) = 0
  AND c.org_id::text <> i.org_id::text;

-- S2 org-free (named exception): integration dataset rows whose integration belongs to a different org. WANT cross_org_datasets = 0.
WITH gate AS MATERIALIZED (
    WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
    rel AS (
        SELECT c.relname::text AS relname, c.reltuples::bigint AS reltuples,
               c.relpages::bigint * bs.block_size + COALESCE(t.relpages::bigint, 0) * bs.block_size AS bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN bs
        LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname IN ('organizations', 'integrations', 'sync_configurations', 'integration_datasets')
    ),
    caps (relname, max_rows, max_bytes) AS (
        VALUES ('organizations', 100000::bigint, 268435456::bigint),
               ('integrations', 150000::bigint, 402653184::bigint),
               ('sync_configurations', 200000::bigint, 536870912::bigint),
               ('integration_datasets', 1000000::bigint, 1073741824::bigint)
    ),
    org AS (
        SELECT count(*) AS orgs FROM public.organizations o WHERE starts_with(o.id::text, 'f0f0f0f0')
    )
    SELECT CASE WHEN count(rel.relname) = 4
                 AND bool_and(rel.reltuples >= 0 AND rel.reltuples <= caps.max_rows AND rel.bytes <= caps.max_bytes)
                 AND max(org.orgs) = 1
                THEN 0
                ELSE ('STOP-G0 ' || string_agg(caps.relname || '=' || COALESCE(rel.reltuples::text, 'missing') || '/'
                                               || COALESCE(rel.bytes::text, 'missing'), ' ' ORDER BY caps.relname)
                      || ' orgs=' || max(org.orgs)::text)::int
           END AS cost_gate_ok
    FROM caps LEFT JOIN rel ON rel.relname = caps.relname CROSS JOIN org
)
SELECT count(*) AS cross_org_datasets
FROM public.integration_datasets d
JOIN public.integrations i ON i.id = d.integration_id
WHERE (SELECT g.cost_gate_ok FROM gate g) = 0
  AND d.org_id::text <> i.org_id::text;

-- C1 control: same-org links of the org (its sync configurations joined to its own integrations). WANT same_org_configs > 0.
WITH gate AS MATERIALIZED (
    WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
    rel AS (
        SELECT c.relname::text AS relname, c.reltuples::bigint AS reltuples,
               c.relpages::bigint * bs.block_size + COALESCE(t.relpages::bigint, 0) * bs.block_size AS bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN bs
        LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname IN ('organizations', 'integrations', 'sync_configurations', 'integration_datasets')
    ),
    caps (relname, max_rows, max_bytes) AS (
        VALUES ('organizations', 100000::bigint, 268435456::bigint),
               ('integrations', 150000::bigint, 402653184::bigint),
               ('sync_configurations', 200000::bigint, 536870912::bigint),
               ('integration_datasets', 1000000::bigint, 1073741824::bigint)
    ),
    org AS (
        SELECT count(*) AS orgs FROM public.organizations o WHERE starts_with(o.id::text, 'f0f0f0f0')
    )
    SELECT CASE WHEN count(rel.relname) = 4
                 AND bool_and(rel.reltuples >= 0 AND rel.reltuples <= caps.max_rows AND rel.bytes <= caps.max_bytes)
                 AND max(org.orgs) = 1
                THEN 0
                ELSE ('STOP-G0 ' || string_agg(caps.relname || '=' || COALESCE(rel.reltuples::text, 'missing') || '/'
                                               || COALESCE(rel.bytes::text, 'missing'), ' ' ORDER BY caps.relname)
                      || ' orgs=' || max(org.orgs)::text)::int
           END AS cost_gate_ok
    FROM caps LEFT JOIN rel ON rel.relname = caps.relname CROSS JOIN org
)
SELECT count(*) AS same_org_configs
FROM public.sync_configurations c
JOIN public.integrations i ON i.id = c.integration_id
WHERE (SELECT g.cost_gate_ok FROM gate g) = 0
  AND starts_with(c.org_id::text, 'f0f0f0f0') AND starts_with(i.org_id::text, 'f0f0f0f0')
  AND c.org_id::text = i.org_id::text;

ROLLBACK;
`
)

const (
	links14582Org         = "f0f0f0f0-1458-4000-8000-0000000000a1"
	links14582OtherOrg    = "e1e1e1e1-1458-4000-8000-0000000000b1"
	links14582Integration = "f0f0f0f0-1458-4000-8000-0000000001a1"
	links14582OtherInt    = "e1e1e1e1-1458-4000-8000-0000000001b1"
)

type links14582Piece struct{ id, text string }

func links14582SQLLines(text string) string {
	var out strings.Builder
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, "--") {
			out.WriteString(line + "\n")
		}
	}
	return out.String()
}

// links14582Pieces splits the FORM as psql runs it (a statement ends at a SQL line ending ";"), names each piece by the
// first word of its leading comment, and checks the text: its sha256, the org slots (each f0f0f0f0 is a starts_with
// filter), and the banned words.
func links14582Pieces(t *testing.T) []links14582Piece {
	t.Helper()
	sum := sha256.Sum256([]byte(links14582SQL))
	if got := hex.EncodeToString(sum[:]); got != links14582CopySHA256 {
		t.Fatalf("sha256 of the embedded CI copy = %s, want %s", got, links14582CopySHA256)
	}
	ids := regexp.MustCompile(`^(G0|S[012]|C1)$`)
	orgFilter := regexp.MustCompile(`starts_with\([a-z]+\.(org_id|id)::text, 'f0f0f0f0'\)`)
	var pieces []links14582Piece
	var current []string
	for _, line := range strings.Split(links14582SQL, "\n") {
		if strings.HasPrefix(line, "--") || !strings.HasSuffix(strings.TrimRight(line, " "), ";") {
			current = append(current, line)
			continue
		}
		current = append(current, strings.TrimSuffix(strings.TrimRight(line, " "), ";"))
		statement := strings.TrimSpace(strings.Join(current, "\n"))
		current = nil
		id := ""
		for _, l := range strings.Split(statement, "\n") {
			if fields := strings.Fields(l); strings.HasPrefix(l, "--") && id == "" && len(fields) > 1 && ids.MatchString(fields[1]) {
				id = fields[1]
			}
		}
		sqlText := strings.TrimSpace(links14582SQLLines(statement))
		switch {
		case strings.HasPrefix(sqlText, "BEGIN "):
			id = "BEGIN"
		case sqlText == "ROLLBACK":
			id = "ROLLBACK"
		}
		if id == "" {
			t.Fatalf("a piece with no id:\n%s", statement)
		}
		upper := strings.ToUpper(sqlText)
		for _, banned := range []string{"SET ", "INSERT ", "UPDATE ", "DELETE ", "EXPLAIN", "COPY ", "CREATE ", "DROP ", "ALTER ", "::JSON"} {
			if strings.Contains(upper, banned) {
				t.Errorf("%s holds %q", id, banned)
			}
		}
		if slots, filters := strings.Count(sqlText, "f0f0f0f0"), len(orgFilter.FindAllString(sqlText, -1)); slots != filters {
			t.Errorf("%s: %d org slot(s), %d org filter(s)", id, slots, filters)
		}
		pieces = append(pieces, links14582Piece{id: id, text: statement})
	}
	if strings.TrimSpace(links14582SQLLines(strings.Join(current, "\n"))) != "" {
		t.Fatalf("SQL after the last statement")
	}
	var names []string
	for _, piece := range pieces {
		names = append(names, piece.id)
	}
	if got := strings.Join(names, ","); got != "BEGIN,G0,S0,S1,S2,C1,ROLLBACK" {
		t.Fatalf("pieces = %s", got)
	}
	return pieces
}

// links14582Run runs the pieces in one transaction, replacing from with to in piece replaceID; it returns each data
// statement's one row as "column=value".
func links14582Run(ctx context.Context, t *testing.T, conn *pgx.Conn, pieces []links14582Piece, replaceID, from, to string) (map[string]string, error) {
	t.Helper()
	results := map[string]string{}
	for _, piece := range pieces {
		text := piece.text
		if piece.id == replaceID {
			if !strings.Contains(text, from) {
				t.Fatalf("%s does not hold %q", piece.id, from)
			}
			text = strings.ReplaceAll(text, from, to)
		}
		if piece.id == "BEGIN" || piece.id == "ROLLBACK" {
			if _, err := conn.Exec(ctx, text); err != nil {
				t.Fatalf("%s: %v", piece.id, err)
			}
			continue
		}
		rows, err := conn.Query(ctx, text)
		if err != nil {
			_, _ = conn.Exec(ctx, "ROLLBACK")
			return results, fmt.Errorf("%s: %w", piece.id, err)
		}
		var cells []string
		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				rows.Close()
				_, _ = conn.Exec(ctx, "ROLLBACK")
				return results, fmt.Errorf("%s: %w", piece.id, err)
			}
			for i, field := range rows.FieldDescriptions() {
				cells = append(cells, field.Name+"="+fmt.Sprint(values[i]))
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			_, _ = conn.Exec(ctx, "ROLLBACK")
			return results, fmt.Errorf("%s: %w", piece.id, err)
		}
		results[piece.id] = strings.Join(cells, " ")
	}
	return results, nil
}

// links14582Seed: two orgs, each with one integration, one sync configuration and one dataset row of its own (same-org
// links only).
func links14582Seed(ctx context.Context, t *testing.T, conn *pgx.Conn) {
	t.Helper()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	for _, org := range []string{links14582Org, links14582OtherOrg} {
		exec(`INSERT INTO organizations (id, slug, name, tier, is_active, created_at, updated_at)
VALUES ($1::uuid, 'org-' || $1::text, 'org', 'enterprise', true, now(), now())`, org)
	}
	for _, row := range [][2]string{{links14582Integration, links14582Org}, {links14582OtherInt, links14582OtherOrg}} {
		exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1::uuid, $2, 'github', 'fx', '{}'::json, true, now(), now())`, row[0], row[1])
		links14582Config(ctx, t, conn, row[1], row[0])
		links14582Dataset(ctx, t, conn, row[1], row[0], "tests")
	}
	links14582Analyze(ctx, t, conn)
}

func links14582Config(ctx context.Context, t *testing.T, conn *pgx.Conn, org, integration string) {
	t.Helper()
	if _, err := conn.Exec(ctx, `INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed, integration_id, created_at, updated_at)
VALUES (gen_random_uuid(), $1, 'fx-' || gen_random_uuid()::text, 'github', '["tests"]'::json, '{}'::json, true, true, $2::uuid, now(), now())`, org, integration); err != nil {
		t.Fatalf("config: %v", err)
	}
}

func links14582Dataset(ctx context.Context, t *testing.T, conn *pgx.Conn, org, integration, key string) {
	t.Helper()
	if _, err := conn.Exec(ctx, `INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options)
VALUES (gen_random_uuid(), $1, $2::uuid, $3, true, '{}'::json)`, org, integration, key); err != nil {
		t.Fatalf("dataset: %v", err)
	}
}

func links14582Analyze(ctx context.Context, t *testing.T, conn *pgx.Conn) {
	t.Helper()
	for _, table := range []string{"organizations", "integrations", "sync_configurations", "integration_datasets"} {
		if _, err := conn.Exec(ctx, `ANALYZE `+table); err != nil {
			t.Fatal(err)
		}
	}
}

// FORM-14582-LINKS-1 as pm runs it. Same-org links only: S1 = S2 = 0 and the control C1 > 0. A planted cross-org
// configuration and dataset row (the org's configuration and dataset pointed at the other org's integration): S1 = 1 and
// S2 = 1, so the FORM can fail. Mutants: S1/S2 with a same-org join change; C1 on another prefix or with the cross-org
// rule changes. The gate: every statement plans it as a one-time filter and stops itself with STOP-G0 on its own copy;
// G0 fires on each cap literal and on orgs=0.
func TestFormExec14582LinksCountsCrossOrgLinks(t *testing.T) {
	ctx := context.Background()
	db := startDatabase(t)
	if len(links14582FormSHA256) != 64 {
		t.Fatalf("the FORM file's sha256 is not named")
	}
	links14582Seed(ctx, t, db.conn)
	pieces := links14582Pieces(t)
	run := func(label, replaceID, from, to string) map[string]string {
		t.Helper()
		results, err := links14582Run(ctx, t, db.conn, pieces, replaceID, from, to)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		return results
	}
	expect := func(label string, got map[string]string, want map[string]string) {
		t.Helper()
		for id, value := range want {
			if got[id] != value {
				t.Errorf("%s: %s = %q, want %q", label, id, got[id], value)
			}
		}
	}
	clean := run("same-org only", "", "", "")
	expect("same-org only", clean, map[string]string{"G0": "cost_gate_ok=0", "S0": "orgs=1", "S1": "cross_org_configs=0",
		"S2": "cross_org_datasets=0", "C1": "same_org_configs=1"})
	mutants := []struct{ name, id, from, to string }{
		{"s1-same-org-join", "S1", "c.org_id::text <> i.org_id::text", "c.org_id::text = i.org_id::text"},
		{"s2-same-org-join", "S2", "d.org_id::text <> i.org_id::text", "d.org_id::text = i.org_id::text"},
		{"c1-other-prefix", "C1", "starts_with(c.org_id::text, 'f0f0f0f0')", "starts_with(c.org_id::text, 'd0d0d0d0')"},
	}
	for _, m := range mutants {
		if got := run("MUTANT "+m.name, m.id, m.from, m.to); got[m.id] == clean[m.id] {
			t.Errorf("MUTANT %s SURVIVED (same-org only): %s", m.name, got[m.id])
		}
	}

	// The planted case: the org's second configuration and a dataset row point at the other org's integration.
	links14582Config(ctx, t, db.conn, links14582Org, links14582OtherInt)
	links14582Dataset(ctx, t, db.conn, links14582Org, links14582OtherInt, "work-items")
	links14582Analyze(ctx, t, db.conn)
	planted := run("planted", "", "", "")
	expect("planted", planted, map[string]string{"G0": "cost_gate_ok=0", "S0": "orgs=1", "S1": "cross_org_configs=1",
		"S2": "cross_org_datasets=1", "C1": "same_org_configs=1"})
	t.Logf("PLANTED S1 %s, S2 %s (WANT 0 on prod: this case is the FORM's FAIL)", planted["S1"], planted["S2"])
	for _, m := range append(mutants, struct{ name, id, from, to string }{"c1-cross-org-rule", "C1", "c.org_id::text = i.org_id::text", "c.org_id::text <> i.org_id::text"}) {
		if got := run("MUTANT "+m.name, m.id, m.from, m.to); got[m.id] == planted[m.id] {
			t.Errorf("MUTANT %s SURVIVED (planted): %s", m.name, got[m.id])
		}
	}

	// The gate: one-time filter in each statement after G0, each statement stops itself, G0 fires on each cap and orgs=0.
	for index, piece := range pieces {
		if index < 2 || piece.id == "ROLLBACK" {
			continue
		}
		var plan []string
		rows, err := db.conn.Query(ctx, "EXPLAIN (COSTS OFF) "+piece.text)
		if err != nil {
			t.Fatalf("EXPLAIN %s: %v", piece.id, err)
		}
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			plan = append(plan, line)
		}
		rows.Close()
		if !strings.Contains(strings.Join(plan, "\n"), "One-Time Filter: ((InitPlan") {
			t.Errorf("%s: the gate is not a one-time filter:\n%s", piece.id, strings.Join(plan, "\n"))
		}
		results, err := links14582Run(ctx, t, db.conn, pieces, piece.id, "100000::bigint", "0::bigint")
		var pgErr *pgconn.PgError
		if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "22P02" || !strings.Contains(pgErr.Message, "STOP-G0") {
			t.Errorf("GATE in %s: err = %v, want 22P02 STOP-G0", piece.id, err)
			continue
		}
		if _, ran := results[piece.id]; ran || len(results) != index-1 {
			t.Errorf("GATE in %s: %d statement(s) ran, want %d before it", piece.id, len(results), index-1)
		}
	}
	for _, f := range []struct{ from, to, want string }{
		{"100000::bigint", "0::bigint", "STOP-G0"}, {"268435456::bigint", "0::bigint", "STOP-G0"},
		{"150000::bigint", "0::bigint", "STOP-G0"}, {"402653184::bigint", "0::bigint", "STOP-G0"},
		{"200000::bigint", "0::bigint", "STOP-G0"}, {"536870912::bigint", "0::bigint", "STOP-G0"},
		{"1000000::bigint", "0::bigint", "STOP-G0"}, {"1073741824::bigint", "0::bigint", "STOP-G0"},
		{"starts_with(o.id::text, 'f0f0f0f0')", "starts_with(o.id::text, 'f0f0f0fz')", "orgs=0"},
	} {
		if strings.Count(pieces[1].text, f.from) != 1 {
			t.Fatalf("G0 holds %q %d time(s), want 1", f.from, strings.Count(pieces[1].text, f.from))
		}
		results, err := links14582Run(ctx, t, db.conn, pieces, "G0", f.from, f.to)
		var pgErr *pgconn.PgError
		if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "22P02" || !strings.Contains(pgErr.Message, f.want) {
			t.Errorf("GATE %s -> %s: err = %v, want 22P02 with %q", f.from, f.to, err, f.want)
			continue
		}
		if len(results) != 0 {
			t.Errorf("GATE %s: statements ran before the gate", f.from)
		}
	}
}

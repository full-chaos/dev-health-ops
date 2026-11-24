//go:build integration

package adminops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DO NOT MERGE. CHAOS-14582 FORM exec leg: FORM-14582-LINKS-2 executed as pm runs it, on a migrated PostgreSQL with
// synthetic rows. The embedded text is the CI copy: FORM-14582-LINKS-2.sql (sha256 f7dd36051e2c535b5bbbb7ed29b4b675e2948bbbc58d1fb149d99f28c8d05234) with its org prefix
// swapped to f0f0f0f0 and nothing else; its own sha256 is checked below. Synthetic orgs: f0f0f0f0 (the FORM's org
// filters), e1e1e1e1 and d2d2d2d2 (two other orgs). No prod id is in this file.
const (
	links2FormSHA256 = "f7dd36051e2c535b5bbbb7ed29b4b675e2948bbbc58d1fb149d99f28c8d05234"
	links2CopySHA256 = "adea8164c0eedb92cf0fb2c069f2100f1a5b8fc0f7894b8fc55e9643ad843096"
	links2SQL        = `-- FORM-14582-LINKS-2 (lane-14559, 2026-11-24, ops main ae65d4a36948). PostgreSQL prod, READ ONLY, CHAOS-14582.
-- Cross-org links: sync configurations, integration dataset rows and child configurations whose integration (or parent)
-- belongs to another org. The whole-PATCH 422 of CHAOS-14582 changes a live 200 for such a configuration, and its cascade
-- org filter stops writes to such a child; WANT 0 of each before the merge.
-- NAMED EXCEPTION (acp FORM scope ruling): S1, S2 and S3 run over ALL orgs (org-free). A cross-org link of any org pair
-- breaks the rule, and an org-scoped count would be a false 0. They print counts only (no id, no cut). ORG-1 (in the
-- gate and in S0), C1 and C2 keep the org prefix f0f0f0f0.
-- Changed vs FORM-14582-LINKS-1 (READ-FORM-14582-LINKS-1-1): the gate sizes tables by file (pg_relation_size of the heap
-- and its TOAST; reltuples is INFO only) per FORM-TEMPLATE-5 (sha12 7fb32b16d35d); S3 (cross-org parent links); C2 (the
-- control of S2). Run line, M0 step and WANT text: FORM-14582-LINKS-2.md.
-- No SET, EXPLAIN, write or cast of stored text. Every statement holds the same gate as a one-time filter.

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;

-- G0 size gate (pg_catalog + pg_relation_size: file sizes, no data row) and ORG-1. WANT cost_gate_ok = 0. Byte caps
-- (heap + TOAST): organizations 67108864, integrations 134217728, sync_configurations 268435456, integration_datasets
-- 201326592. A table missing or over its cap, or orgs <> 1 = the named cast error STOP-G0 (bytes, reltuples, orgs only).
WITH gate AS MATERIALIZED (
    WITH rel AS (
        SELECT c.relname::text AS relname, c.reltuples::bigint AS reltuples,
               pg_catalog.pg_relation_size(c.oid)
               + CASE WHEN c.reltoastrelid = 0 THEN 0 ELSE pg_catalog.pg_relation_size(c.reltoastrelid) END AS bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname IN ('organizations', 'integrations', 'sync_configurations', 'integration_datasets')
    ),
    caps (relname, max_bytes) AS (
        VALUES ('organizations', 67108864::bigint),
               ('integrations', 134217728::bigint),
               ('sync_configurations', 268435456::bigint),
               ('integration_datasets', 201326592::bigint)
    ),
    org AS (
        SELECT count(*) AS orgs FROM public.organizations o WHERE starts_with(o.id::text, 'f0f0f0f0')
    )
    SELECT CASE WHEN count(rel.relname) = 4
                 AND bool_and(rel.bytes <= caps.max_bytes)
                 AND max(org.orgs) = 1
                THEN 0
                ELSE ('STOP-G0 ' || string_agg(caps.relname || '=' || COALESCE(rel.bytes::text, 'missing') || 'B/'
                                               || COALESCE(rel.reltuples::text, 'missing') || 'r', ' ' ORDER BY caps.relname)
                      || ' orgs=' || max(org.orgs)::text)::int
           END AS cost_gate_ok
    FROM caps LEFT JOIN rel ON rel.relname = caps.relname CROSS JOIN org
)
SELECT cost_gate_ok FROM gate;

-- S0 ORG-1 (G0 stops unless it is 1). WANT orgs = 1.
WITH gate AS MATERIALIZED (
    WITH rel AS (
        SELECT c.relname::text AS relname, c.reltuples::bigint AS reltuples,
               pg_catalog.pg_relation_size(c.oid)
               + CASE WHEN c.reltoastrelid = 0 THEN 0 ELSE pg_catalog.pg_relation_size(c.reltoastrelid) END AS bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname IN ('organizations', 'integrations', 'sync_configurations', 'integration_datasets')
    ),
    caps (relname, max_bytes) AS (
        VALUES ('organizations', 67108864::bigint),
               ('integrations', 134217728::bigint),
               ('sync_configurations', 268435456::bigint),
               ('integration_datasets', 201326592::bigint)
    ),
    org AS (
        SELECT count(*) AS orgs FROM public.organizations o WHERE starts_with(o.id::text, 'f0f0f0f0')
    )
    SELECT CASE WHEN count(rel.relname) = 4
                 AND bool_and(rel.bytes <= caps.max_bytes)
                 AND max(org.orgs) = 1
                THEN 0
                ELSE ('STOP-G0 ' || string_agg(caps.relname || '=' || COALESCE(rel.bytes::text, 'missing') || 'B/'
                                               || COALESCE(rel.reltuples::text, 'missing') || 'r', ' ' ORDER BY caps.relname)
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
    WITH rel AS (
        SELECT c.relname::text AS relname, c.reltuples::bigint AS reltuples,
               pg_catalog.pg_relation_size(c.oid)
               + CASE WHEN c.reltoastrelid = 0 THEN 0 ELSE pg_catalog.pg_relation_size(c.reltoastrelid) END AS bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname IN ('organizations', 'integrations', 'sync_configurations', 'integration_datasets')
    ),
    caps (relname, max_bytes) AS (
        VALUES ('organizations', 67108864::bigint),
               ('integrations', 134217728::bigint),
               ('sync_configurations', 268435456::bigint),
               ('integration_datasets', 201326592::bigint)
    ),
    org AS (
        SELECT count(*) AS orgs FROM public.organizations o WHERE starts_with(o.id::text, 'f0f0f0f0')
    )
    SELECT CASE WHEN count(rel.relname) = 4
                 AND bool_and(rel.bytes <= caps.max_bytes)
                 AND max(org.orgs) = 1
                THEN 0
                ELSE ('STOP-G0 ' || string_agg(caps.relname || '=' || COALESCE(rel.bytes::text, 'missing') || 'B/'
                                               || COALESCE(rel.reltuples::text, 'missing') || 'r', ' ' ORDER BY caps.relname)
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
    WITH rel AS (
        SELECT c.relname::text AS relname, c.reltuples::bigint AS reltuples,
               pg_catalog.pg_relation_size(c.oid)
               + CASE WHEN c.reltoastrelid = 0 THEN 0 ELSE pg_catalog.pg_relation_size(c.reltoastrelid) END AS bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname IN ('organizations', 'integrations', 'sync_configurations', 'integration_datasets')
    ),
    caps (relname, max_bytes) AS (
        VALUES ('organizations', 67108864::bigint),
               ('integrations', 134217728::bigint),
               ('sync_configurations', 268435456::bigint),
               ('integration_datasets', 201326592::bigint)
    ),
    org AS (
        SELECT count(*) AS orgs FROM public.organizations o WHERE starts_with(o.id::text, 'f0f0f0f0')
    )
    SELECT CASE WHEN count(rel.relname) = 4
                 AND bool_and(rel.bytes <= caps.max_bytes)
                 AND max(org.orgs) = 1
                THEN 0
                ELSE ('STOP-G0 ' || string_agg(caps.relname || '=' || COALESCE(rel.bytes::text, 'missing') || 'B/'
                                               || COALESCE(rel.reltuples::text, 'missing') || 'r', ' ' ORDER BY caps.relname)
                      || ' orgs=' || max(org.orgs)::text)::int
           END AS cost_gate_ok
    FROM caps LEFT JOIN rel ON rel.relname = caps.relname CROSS JOIN org
)
SELECT count(*) AS cross_org_datasets
FROM public.integration_datasets d
JOIN public.integrations i ON i.id = d.integration_id
WHERE (SELECT g.cost_gate_ok FROM gate g) = 0
  AND d.org_id::text <> i.org_id::text;

-- S3 org-free (named exception): child configurations whose parent configuration belongs to a different org. WANT cross_org_children = 0.
WITH gate AS MATERIALIZED (
    WITH rel AS (
        SELECT c.relname::text AS relname, c.reltuples::bigint AS reltuples,
               pg_catalog.pg_relation_size(c.oid)
               + CASE WHEN c.reltoastrelid = 0 THEN 0 ELSE pg_catalog.pg_relation_size(c.reltoastrelid) END AS bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname IN ('organizations', 'integrations', 'sync_configurations', 'integration_datasets')
    ),
    caps (relname, max_bytes) AS (
        VALUES ('organizations', 67108864::bigint),
               ('integrations', 134217728::bigint),
               ('sync_configurations', 268435456::bigint),
               ('integration_datasets', 201326592::bigint)
    ),
    org AS (
        SELECT count(*) AS orgs FROM public.organizations o WHERE starts_with(o.id::text, 'f0f0f0f0')
    )
    SELECT CASE WHEN count(rel.relname) = 4
                 AND bool_and(rel.bytes <= caps.max_bytes)
                 AND max(org.orgs) = 1
                THEN 0
                ELSE ('STOP-G0 ' || string_agg(caps.relname || '=' || COALESCE(rel.bytes::text, 'missing') || 'B/'
                                               || COALESCE(rel.reltuples::text, 'missing') || 'r', ' ' ORDER BY caps.relname)
                      || ' orgs=' || max(org.orgs)::text)::int
           END AS cost_gate_ok
    FROM caps LEFT JOIN rel ON rel.relname = caps.relname CROSS JOIN org
)
SELECT count(*) AS cross_org_children
FROM public.sync_configurations ch
JOIN public.sync_configurations p ON p.id = ch.parent_id
WHERE (SELECT g.cost_gate_ok FROM gate g) = 0
  AND ch.org_id::text <> p.org_id::text;

-- C1 control of S1: same-org links of the org (its sync configurations joined to its own integrations). WANT same_org_configs > 0.
WITH gate AS MATERIALIZED (
    WITH rel AS (
        SELECT c.relname::text AS relname, c.reltuples::bigint AS reltuples,
               pg_catalog.pg_relation_size(c.oid)
               + CASE WHEN c.reltoastrelid = 0 THEN 0 ELSE pg_catalog.pg_relation_size(c.reltoastrelid) END AS bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname IN ('organizations', 'integrations', 'sync_configurations', 'integration_datasets')
    ),
    caps (relname, max_bytes) AS (
        VALUES ('organizations', 67108864::bigint),
               ('integrations', 134217728::bigint),
               ('sync_configurations', 268435456::bigint),
               ('integration_datasets', 201326592::bigint)
    ),
    org AS (
        SELECT count(*) AS orgs FROM public.organizations o WHERE starts_with(o.id::text, 'f0f0f0f0')
    )
    SELECT CASE WHEN count(rel.relname) = 4
                 AND bool_and(rel.bytes <= caps.max_bytes)
                 AND max(org.orgs) = 1
                THEN 0
                ELSE ('STOP-G0 ' || string_agg(caps.relname || '=' || COALESCE(rel.bytes::text, 'missing') || 'B/'
                                               || COALESCE(rel.reltuples::text, 'missing') || 'r', ' ' ORDER BY caps.relname)
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

-- C2 control of S2: same-org dataset rows of the org (joined to its own integrations). WANT same_org_datasets > 0.
WITH gate AS MATERIALIZED (
    WITH rel AS (
        SELECT c.relname::text AS relname, c.reltuples::bigint AS reltuples,
               pg_catalog.pg_relation_size(c.oid)
               + CASE WHEN c.reltoastrelid = 0 THEN 0 ELSE pg_catalog.pg_relation_size(c.reltoastrelid) END AS bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind = 'r'
          AND c.relname IN ('organizations', 'integrations', 'sync_configurations', 'integration_datasets')
    ),
    caps (relname, max_bytes) AS (
        VALUES ('organizations', 67108864::bigint),
               ('integrations', 134217728::bigint),
               ('sync_configurations', 268435456::bigint),
               ('integration_datasets', 201326592::bigint)
    ),
    org AS (
        SELECT count(*) AS orgs FROM public.organizations o WHERE starts_with(o.id::text, 'f0f0f0f0')
    )
    SELECT CASE WHEN count(rel.relname) = 4
                 AND bool_and(rel.bytes <= caps.max_bytes)
                 AND max(org.orgs) = 1
                THEN 0
                ELSE ('STOP-G0 ' || string_agg(caps.relname || '=' || COALESCE(rel.bytes::text, 'missing') || 'B/'
                                               || COALESCE(rel.reltuples::text, 'missing') || 'r', ' ' ORDER BY caps.relname)
                      || ' orgs=' || max(org.orgs)::text)::int
           END AS cost_gate_ok
    FROM caps LEFT JOIN rel ON rel.relname = caps.relname CROSS JOIN org
)
SELECT count(*) AS same_org_datasets
FROM public.integration_datasets d
JOIN public.integrations i ON i.id = d.integration_id
WHERE (SELECT g.cost_gate_ok FROM gate g) = 0
  AND starts_with(d.org_id::text, 'f0f0f0f0') AND starts_with(i.org_id::text, 'f0f0f0f0')
  AND d.org_id::text = i.org_id::text;

ROLLBACK;
`
)

var links2Orgs = [3]string{
	"f0f0f0f0-1458-4000-8000-0000000000a1",
	"e1e1e1e1-1458-4000-8000-0000000000b1",
	"d2d2d2d2-1458-4000-8000-0000000000c1",
}

var links2Integrations = [3]string{
	"f0f0f0f0-1458-4000-8000-0000000001a1",
	"e1e1e1e1-1458-4000-8000-0000000001b1",
	"d2d2d2d2-1458-4000-8000-0000000001c1",
}

type links2Piece struct{ id, text string }

func links2SQLLines(text string) string {
	var out strings.Builder
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, "--") {
			out.WriteString(line + "\n")
		}
	}
	return out.String()
}

// links2Pieces splits the FORM as psql runs it, names each piece by the first word of its leading comment, and checks
// the text: its sha256, the org slots (each f0f0f0f0 is a starts_with filter) and the banned words.
func links2Pieces(t *testing.T) []links2Piece {
	t.Helper()
	sum := sha256.Sum256([]byte(links2SQL))
	if got := hex.EncodeToString(sum[:]); got != links2CopySHA256 {
		t.Fatalf("sha256 of the embedded CI copy = %s, want %s", got, links2CopySHA256)
	}
	ids := regexp.MustCompile(`^(G0|S[0-3]|C[12])$`)
	orgFilter := regexp.MustCompile(`starts_with\([a-z]+\.(org_id|id)::text, 'f0f0f0f0'\)`)
	var pieces []links2Piece
	var current []string
	for _, line := range strings.Split(links2SQL, "\n") {
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
		sqlText := strings.TrimSpace(links2SQLLines(statement))
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
		for _, banned := range []string{"SET ", "INSERT ", "UPDATE ", "DELETE ", "EXPLAIN", "COPY ", "CREATE ", "DROP ", "ALTER ", "::JSON", "RELPAGES"} {
			if strings.Contains(upper, banned) {
				t.Errorf("%s holds %q", id, banned)
			}
		}
		if slots, filters := strings.Count(sqlText, "f0f0f0f0"), len(orgFilter.FindAllString(sqlText, -1)); slots != filters {
			t.Errorf("%s: %d org slot(s), %d org filter(s)", id, slots, filters)
		}
		pieces = append(pieces, links2Piece{id: id, text: statement})
	}
	if strings.TrimSpace(links2SQLLines(strings.Join(current, "\n"))) != "" {
		t.Fatalf("SQL after the last statement")
	}
	var names []string
	for _, piece := range pieces {
		names = append(names, piece.id)
	}
	if got := strings.Join(names, ","); got != "BEGIN,G0,S0,S1,S2,S3,C1,C2,ROLLBACK" {
		t.Fatalf("pieces = %s", got)
	}
	return pieces
}

// links2Run runs the pieces in one transaction, replacing from with to in piece replaceID; it returns each data
// statement's one row as "column=value".
func links2Run(ctx context.Context, t *testing.T, conn *pgx.Conn, pieces []links2Piece, replaceID, from, to string) (map[string]string, error) {
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

func links2Exec(ctx context.Context, t *testing.T, conn *pgx.Conn, sql string, args ...any) {
	t.Helper()
	if _, err := conn.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// links2Config writes a sync configuration (integration and parent may be empty = NULL) and returns its id.
func links2Config(ctx context.Context, t *testing.T, conn *pgx.Conn, org, integration, parent string) string {
	t.Helper()
	var id string
	if err := conn.QueryRow(ctx, `INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed, integration_id, parent_id, created_at, updated_at)
VALUES (gen_random_uuid(), $1, 'fx-' || gen_random_uuid()::text, 'github', '["tests"]'::json, '{}'::json, true, true, NULLIF($2, '')::uuid, NULLIF($3, '')::uuid, now(), now())
RETURNING id::text`, org, integration, parent).Scan(&id); err != nil {
		t.Fatalf("config: %v", err)
	}
	return id
}

func links2Dataset(ctx context.Context, t *testing.T, conn *pgx.Conn, org, integration, key string) {
	t.Helper()
	links2Exec(ctx, t, conn, `INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options)
VALUES (gen_random_uuid(), $1, $2::uuid, $3, true, '{}'::json)`, org, integration, key)
}

func links2Analyze(ctx context.Context, t *testing.T, conn *pgx.Conn) {
	t.Helper()
	for _, table := range []string{"organizations", "integrations", "sync_configurations", "integration_datasets"} {
		links2Exec(ctx, t, conn, `ANALYZE `+table)
	}
}

// links2Fire runs the FORM with from replaced by to in piece id and wants the named 22P02 STOP-G0 holding want, with no
// statement of the piece run.
func links2Fire(ctx context.Context, t *testing.T, conn *pgx.Conn, pieces []links2Piece, id, from, to, want string) {
	t.Helper()
	results, err := links2Run(ctx, t, conn, pieces, id, from, to)
	var pgErr *pgconn.PgError
	if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "22P02" || !strings.Contains(pgErr.Message, "STOP-G0") || !strings.Contains(pgErr.Message, want) {
		t.Errorf("GATE %s %s -> %s: err = %v, want 22P02 STOP-G0 with %q", id, from, to, err, want)
		return
	}
	if _, ran := results[id]; ran {
		t.Errorf("GATE %s %s: the statement ran", id, from)
	}
}

// FORM-14582-LINKS-2 as pm runs it. Same-org links only (each org has an integration, a configuration and a dataset
// row of its own; f0f0f0f0 also has a parent and a child configuration): S1 = S2 = S3 = 0, C1 = C2 = 1. A planted
// cross-org link touching f0f0f0f0 reads 1 in each of S1, S2, S3; a planted link between the two other orgs reads 1
// more (the counts are org-free). Mutants change their rows. The gate sizes tables by file: it passes on never-analyzed
// tables (reltuples -1), fires on a never-analyzed table over its byte cap, and fires on a table that grew after its
// last ANALYZE although the stats size (relpages) is under the cap. Every statement plans the gate as a one-time filter
// and stops itself; G0 fires on each byte cap and on orgs=0.
func TestFormExec14582LinksV2CountsCrossOrgLinks(t *testing.T) {
	ctx := context.Background()
	db := startDatabase(t)
	conn := db.conn
	if len(links2FormSHA256) != 64 {
		t.Fatalf("the FORM file's sha256 is not named")
	}
	pieces := links2Pieces(t)
	for i, org := range links2Orgs {
		links2Exec(ctx, t, conn, `INSERT INTO organizations (id, slug, name, tier, is_active, created_at, updated_at)
VALUES ($1::uuid, 'org-' || $1::text, 'org', 'enterprise', true, now(), now())`, org)
		links2Exec(ctx, t, conn, `INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1::uuid, $2, 'github', 'fx', '{}'::json, true, now(), now())`, links2Integrations[i], org)
		links2Config(ctx, t, conn, org, links2Integrations[i], "")
		links2Dataset(ctx, t, conn, org, links2Integrations[i], "tests")
	}
	parent := links2Config(ctx, t, conn, links2Orgs[0], "", "")
	links2Config(ctx, t, conn, links2Orgs[0], "", parent)
	run := func(label, replaceID, from, to string) map[string]string {
		t.Helper()
		results, err := links2Run(ctx, t, conn, pieces, replaceID, from, to)
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
	world := func(s1, s2, s3 int) map[string]string {
		return map[string]string{"G0": "cost_gate_ok=0", "S0": "orgs=1", "S1": "cross_org_configs=" + strconv.Itoa(s1),
			"S2": "cross_org_datasets=" + strconv.Itoa(s2), "S3": "cross_org_children=" + strconv.Itoa(s3),
			"C1": "same_org_configs=1", "C2": "same_org_datasets=1"}
	}

	// Never analyzed: the gate reads file sizes, so reltuples -1 does not stop it; a byte cap under the file size does.
	var reltuples float64
	if err := conn.QueryRow(ctx, `SELECT reltuples FROM pg_catalog.pg_class WHERE oid = 'public.integration_datasets'::regclass`).Scan(&reltuples); err != nil {
		t.Fatal(err)
	}
	t.Logf("NEVER-ANALYZED integration_datasets reltuples=%v", reltuples)
	expect("never analyzed", run("never analyzed", "", "", ""), world(0, 0, 0))
	links2Fire(ctx, t, conn, pieces, "G0", "201326592::bigint", "1::bigint", "integration_datasets=")

	links2Analyze(ctx, t, conn)
	clean := run("same-org only", "", "", "")
	expect("same-org only", clean, world(0, 0, 0))
	mutants := []struct{ name, id, from, to string }{
		{"s1-same-org-join", "S1", "c.org_id::text <> i.org_id::text", "c.org_id::text = i.org_id::text"},
		{"s2-same-org-join", "S2", "d.org_id::text <> i.org_id::text", "d.org_id::text = i.org_id::text"},
		{"s3-same-org-join", "S3", "ch.org_id::text <> p.org_id::text", "ch.org_id::text = p.org_id::text"},
		{"c1-other-prefix", "C1", "starts_with(c.org_id::text, 'f0f0f0f0')", "starts_with(c.org_id::text, 'd0d0d0d0')"},
		{"c2-other-prefix", "C2", "starts_with(d.org_id::text, 'f0f0f0f0')", "starts_with(d.org_id::text, 'd0d0d0d0')"},
	}
	for _, m := range mutants {
		if got := run("MUTANT "+m.name, m.id, m.from, m.to); got[m.id] == clean[m.id] {
			t.Errorf("MUTANT %s SURVIVED (same-org only): %s", m.name, got[m.id])
		}
	}

	// Planted, touching the prefix org: f0's configuration and dataset row on e1's integration, an e1 child under f0's parent.
	links2Config(ctx, t, conn, links2Orgs[0], links2Integrations[1], "")
	links2Dataset(ctx, t, conn, links2Orgs[0], links2Integrations[1], "work-items")
	links2Config(ctx, t, conn, links2Orgs[1], "", parent)
	links2Analyze(ctx, t, conn)
	planted := run("planted", "", "", "")
	expect("planted", planted, world(1, 1, 1))
	t.Logf("PLANTED S1 %s, S2 %s, S3 %s (WANT 0 on prod: this case is the FORM's FAIL)", planted["S1"], planted["S2"], planted["S3"])

	// Planted between the two other orgs: d2's configuration and dataset row on e1's integration, a d2 child under an e1 parent.
	links2Config(ctx, t, conn, links2Orgs[2], links2Integrations[1], "")
	links2Dataset(ctx, t, conn, links2Orgs[2], links2Integrations[1], "work-items")
	otherParent := links2Config(ctx, t, conn, links2Orgs[1], "", "")
	links2Config(ctx, t, conn, links2Orgs[2], "", otherParent)
	links2Analyze(ctx, t, conn)
	both := run("planted, other orgs too", "", "", "")
	expect("planted, other orgs too", both, world(2, 2, 2))
	t.Logf("PLANTED-OTHER-ORGS S1 %s, S2 %s, S3 %s", both["S1"], both["S2"], both["S3"])
	for _, m := range append(mutants,
		struct{ name, id, from, to string }{"c1-cross-org-rule", "C1", "c.org_id::text = i.org_id::text", "c.org_id::text <> i.org_id::text"},
		struct{ name, id, from, to string }{"c2-cross-org-rule", "C2", "d.org_id::text = i.org_id::text", "d.org_id::text <> i.org_id::text"}) {
		if got := run("MUTANT "+m.name, m.id, m.from, m.to); got[m.id] == both[m.id] {
			t.Errorf("MUTANT %s SURVIVED (planted): %s", m.name, got[m.id])
		}
	}

	// The gate: one-time filter in each statement after G0; each statement stops itself; G0 fires on each byte cap and orgs=0.
	for index, piece := range pieces {
		if index < 2 || piece.id == "ROLLBACK" {
			continue
		}
		var plan []string
		rows, err := conn.Query(ctx, "EXPLAIN (COSTS OFF) "+piece.text)
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
		results, err := links2Run(ctx, t, conn, pieces, piece.id, "67108864::bigint", "0::bigint")
		var pgErr *pgconn.PgError
		if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "22P02" || !strings.Contains(pgErr.Message, "STOP-G0") {
			t.Errorf("GATE in %s: err = %v, want 22P02 STOP-G0", piece.id, err)
			continue
		}
		if _, ran := results[piece.id]; ran || len(results) != index-1 {
			t.Errorf("GATE in %s: %d statement(s) ran, want %d before it", piece.id, len(results), index-1)
		}
	}
	for _, c := range []struct{ literal, table string }{
		{"67108864::bigint", "organizations="}, {"134217728::bigint", "integrations="},
		{"268435456::bigint", "sync_configurations="}, {"201326592::bigint", "integration_datasets="},
	} {
		if strings.Count(pieces[1].text, c.literal) != 1 {
			t.Fatalf("G0 holds %q %d time(s), want 1", c.literal, strings.Count(pieces[1].text, c.literal))
		}
		links2Fire(ctx, t, conn, pieces, "G0", c.literal, "0::bigint", c.table)
	}
	links2Fire(ctx, t, conn, pieces, "G0", "starts_with(o.id::text, 'f0f0f0f0')", "starts_with(o.id::text, 'f0f0f0fz')", "orgs=0")

	// Stale stats: integration_datasets grows after its last ANALYZE. The stats size (relpages x block size) stays under a
	// cap that the file size is over: the gate reads the file and fires.
	links2Exec(ctx, t, conn, `INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options)
SELECT gen_random_uuid(), $1, $2::uuid, 'grown-' || g::text, true, '{"padding": "`+strings.Repeat("x", 200)+`"}'::json
FROM generate_series(1, 4000) AS g`, links2Orgs[1], links2Integrations[1])
	var statsBytes, fileBytes int64
	if err := conn.QueryRow(ctx, `SELECT c.relpages::bigint * current_setting('block_size')::bigint,
       pg_catalog.pg_relation_size(c.oid) + CASE WHEN c.reltoastrelid = 0 THEN 0 ELSE pg_catalog.pg_relation_size(c.reltoastrelid) END
FROM pg_catalog.pg_class c WHERE c.oid = 'public.integration_datasets'::regclass`).Scan(&statsBytes, &fileBytes); err != nil {
		t.Fatal(err)
	}
	limit := statsBytes + 8192
	if limit >= fileBytes {
		t.Fatalf("premise: the table did not grow past its stats size (stats %d, file %d)", statsBytes, fileBytes)
	}
	t.Logf("STALE-STATS integration_datasets stats=%dB file=%dB cap=%dB", statsBytes, fileBytes, limit)
	links2Fire(ctx, t, conn, pieces, "G0", "201326592::bigint", strconv.FormatInt(limit, 10)+"::bigint", "integration_datasets="+strconv.FormatInt(fileBytes, 10)+"B")
}

package containers

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// PostgresTemplateEnv set to "off" turns the template database off:
// StartPostgresFromTemplate then builds the schema in every caller's own
// database, as a plain StartPostgres plus the build does. Unset or "on" keeps
// the template.
const PostgresTemplateEnv = "DEV_HEALTH_TEST_PG_TEMPLATE"

const templateNamePrefix = "dh_tpl_"

// templateLog receives the named line for every call that falls back to the
// build in its own database. A variable so tests can read it.
var templateLog io.Writer = os.Stderr

var templateKeyPattern = regexp.MustCompile(`^[0-9a-f]{24,}$`)

// templateAlterDDL returns the statement that closes a template to connections
// and marks it a template, and the one that opens it again for the end-of-run
// check. The name is quoted with pgx.Identifier and is a template name from
// templateDatabaseName (lowercase hex after a fixed prefix).
func templateAlterDDL(name string) (closeStatement string, openStatement string) {
	quoted := pgx.Identifier{name}.Sanitize()
	return "ALTER DATABASE " + quoted + " WITH IS_TEMPLATE true ALLOW_CONNECTIONS false",
		"ALTER DATABASE " + quoted + " WITH ALLOW_CONNECTIONS true"
}

// templateDatabaseName is the name of the template database for key: the
// prefix and the first 24 hex digits of the key.
func templateDatabaseName(key string) (string, error) {
	if !templateKeyPattern.MatchString(key) {
		return "", fmt.Errorf("template key %q: want at least 24 lowercase hex digits", key)
	}
	return templateNamePrefix + key[:24], nil
}

// StartPostgresFromTemplate gives the caller a private, migrated database.
// In shared mode the shared server builds ONE template database per key, by
// running build on it once; each call is then a CREATE DATABASE from that
// template. key names the schema (see pgschema.Key): a new key is a new
// template, and a template is never reused for another key.
//
// Everywhere else (no shared mode, a remote DSN, a server this process did
// not start, PostgresTemplateEnv=off) the call is StartPostgres plus build on
// the caller's database, and a "template fallback" line names the reason.
func StartPostgresFromTemplate(ctx context.Context, key string, build func(ctx context.Context, uri string) error) (*Instance, error) {
	switch value := strings.TrimSpace(os.Getenv(PostgresTemplateEnv)); value {
	case "", "on", "off":
	default:
		return nil, fmt.Errorf("%s=%q: want unset, \"on\" or \"off\"", PostgresTemplateEnv, value)
	}
	name, err := templateDatabaseName(key)
	if err != nil {
		return nil, err
	}
	servers := activeSharedServers()
	if reason := templateFallbackReason(servers); reason != "" {
		return buildInOwnDatabase(ctx, reason, build)
	}
	server, err := servers.postgres()
	if err != nil {
		return nil, err
	}
	if reason := templateFallbackReason(servers); reason != "" {
		return buildInOwnDatabase(ctx, reason, build)
	}
	entry := servers.templates.entry(key)
	err = servers.templates.ensure(key, func() error {
		// The template outlives the call that triggered it, so the caller's
		// context does not bound or cancel the build.
		buildCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sharedBootTimeout)
		defer cancel()
		fingerprint, err := buildTemplate(buildCtx, server, name, build)
		entry.fingerprint = fingerprint
		return err
	})
	if err != nil {
		return nil, err
	}
	return timedStart(ctx, "postgres", PostgresDSNEnv, func(ctx context.Context) (*Instance, error) {
		return startPostgresRemoteFrom(ctx, server.adminDSN, name)
	})
}

// templateFallbackReason names why a call cannot use a template, or "" when it can.
func templateFallbackReason(servers *sharedServers) string {
	switch {
	case strings.TrimSpace(os.Getenv(PostgresTemplateEnv)) == "off":
		return PostgresTemplateEnv + "=off"
	case remoteDSN(PostgresDSNEnv) != "":
		return "remote DSN"
	case servers == nil:
		return "not in shared mode"
	case servers.pg != nil && !servers.pg.startedHere:
		return "server not started by this binary"
	}
	return ""
}

func buildInOwnDatabase(ctx context.Context, reason string, build func(ctx context.Context, uri string) error) (*Instance, error) {
	fmt.Fprintf(templateLog, "containers: postgres template fallback: %s\n", reason)
	instance, err := StartPostgres(ctx)
	if err != nil {
		return nil, err
	}
	if err := build(ctx, instance.URI); err != nil {
		_ = instance.Close(context.WithoutCancel(ctx))
		return nil, err
	}
	return instance, nil
}

type templateEntry struct {
	once        sync.Once
	done        bool
	err         error
	fingerprint []string
}

// templateRegistry holds the templates of one shared server, by key.
type templateRegistry struct {
	mu      sync.Mutex
	entries map[string]*templateEntry
}

func (r *templateRegistry) entry(key string) *templateEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entries == nil {
		r.entries = map[string]*templateEntry{}
	}
	if r.entries[key] == nil {
		r.entries[key] = &templateEntry{}
	}
	return r.entries[key]
}

// ensure runs build once per key. Every caller of a key gets that one result,
// a build error included; a failed key is never retried.
func (r *templateRegistry) ensure(key string, build func() error) error {
	entry := r.entry(key)
	entry.once.Do(func() {
		entry.err = build()
		r.mu.Lock()
		entry.done = true
		r.mu.Unlock()
	})
	return entry.err
}

// used reports whether any caller asked this server for a template.
func (r *templateRegistry) used() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries) > 0
}

// names lists the template databases that were built, sorted.
func (r *templateRegistry) names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for key, entry := range r.entries {
		if entry.done && entry.err == nil {
			if name, err := templateDatabaseName(key); err == nil {
				out = append(out, name)
			}
		}
	}
	sort.Strings(out)
	return out
}

func (r *templateRegistry) built() map[string]*templateEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[string]*templateEntry{}
	for key, entry := range r.entries {
		if entry.done && entry.err == nil {
			if name, err := templateDatabaseName(key); err == nil {
				out[name] = entry
			}
		}
	}
	return out
}

// clientSessionCount counts the sessions a test could have opened. An
// autovacuum worker or another background process may sit in a fresh database
// for a moment and is not one.
func clientSessionCount(backendTypes []string) int {
	count := 0
	for _, kind := range backendTypes {
		if kind == "client backend" {
			count++
		}
	}
	return count
}

func clientSessions(ctx context.Context, conn *pgx.Conn, database string) (int, error) {
	rows, err := conn.Query(ctx, "SELECT backend_type FROM pg_stat_activity WHERE datname = $1", database)
	if err != nil {
		return 0, err
	}
	types, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, err
	}
	return clientSessionCount(types), nil
}

// waitNoClientSessions waits for the server to reap the backends of closed
// connections: a client's Close returns before its backend has exited.
func waitNoClientSessions(ctx context.Context, conn *pgx.Conn, database string) (int, error) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		count, err := clientSessions(ctx, conn, database)
		if err != nil || count == 0 || time.Now().After(deadline) {
			return count, err
		}
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// buildTemplate creates the template database, runs build on it, checks that
// no client session is left on it, records its fingerprint, and closes it to
// connections. On any failure the database is dropped.
func buildTemplate(ctx context.Context, server *sharedServer, name string, build func(ctx context.Context, uri string) error) (fingerprint []string, err error) {
	defer TimePhase("template-build", "postgres", "shared-server")()
	if err := assertSafeIdentifier(name); err != nil {
		return nil, err
	}
	admin, err := pgx.Connect(ctx, server.adminDSN)
	if err != nil {
		return nil, fmt.Errorf("template %s: connect: %w", name, err)
	}
	defer func() { _ = admin.Close(context.WithoutCancel(ctx)) }()
	createStatement, dropStatement := postgresScratchDDL(name)
	closeStatement, _ := templateAlterDDL(name)
	uri, err := withPostgresDatabase(server.adminDSN, name)
	if err != nil {
		return nil, err
	}
	if _, err := admin.Exec(ctx, createStatement); err != nil {
		return nil, fmt.Errorf("template %s: create: %w", name, err)
	}
	defer func() {
		if err != nil {
			_, _ = admin.Exec(context.WithoutCancel(ctx), dropStatement)
		}
	}()
	if err := build(ctx, uri); err != nil {
		return nil, fmt.Errorf("template %s: build: %w", name, err)
	}
	if count, err := waitNoClientSessions(ctx, admin, name); err != nil {
		return nil, fmt.Errorf("template %s: read sessions: %w", name, err)
	} else if count != 0 {
		return nil, fmt.Errorf("template %s: the build left %d client session(s) on the template", name, count)
	}
	if fingerprint, err = pgTemplateFingerprint(ctx, uri); err != nil {
		return nil, fmt.Errorf("template %s: fingerprint: %w", name, err)
	}
	if _, err := admin.Exec(ctx, closeStatement); err != nil {
		return nil, fmt.Errorf("template %s: close to connections: %w", name, err)
	}
	if count, err := waitNoClientSessions(ctx, admin, name); err != nil {
		return nil, fmt.Errorf("template %s: read sessions: %w", name, err)
	} else if count != 0 {
		return nil, fmt.Errorf("template %s: %d client session(s) on the template after the fingerprint", name, count)
	}
	return fingerprint, nil
}

// pgTemplateFingerprintQuery returns one text row per object and per piece of
// state a test could change in a database: relations, columns (type, null
// flag, default), indexes, constraints, functions, triggers, sequences (last
// value and is_called), extensions with their versions, and the exact row count
// of every table. Definitions enter as md5 so a row stays short.
const pgTemplateFingerprintQuery = `
WITH rel AS (
  SELECT c.oid, c.relkind, n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%'
)
SELECT 'relation ' || nspname || '.' || relname || ' kind=' || relkind::text FROM rel
UNION ALL SELECT 'column ' || r.nspname || '.' || r.relname || '.' || a.attname || ' ' || format_type(a.atttypid, a.atttypmod) ||
         ' notnull=' || a.attnotnull || ' default=' || coalesce(md5(pg_get_expr(d.adbin, d.adrelid)), '')
  FROM rel r JOIN pg_attribute a ON a.attrelid = r.oid AND a.attnum > 0 AND NOT a.attisdropped
  LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum WHERE r.relkind IN ('r', 'p', 'v', 'm', 'f')
UNION ALL SELECT 'index ' || r.nspname || '.' || r.relname || ' ' || md5(pg_get_indexdef(r.oid)) FROM rel r WHERE r.relkind IN ('i', 'I')
UNION ALL SELECT 'constraint ' || r.nspname || '.' || r.relname || '.' || k.conname || ' ' || md5(pg_get_constraintdef(k.oid))
  FROM pg_constraint k JOIN rel r ON r.oid = k.conrelid
UNION ALL SELECT 'trigger ' || r.nspname || '.' || r.relname || '.' || t.tgname || ' ' || md5(pg_get_triggerdef(t.oid))
  FROM pg_trigger t JOIN rel r ON r.oid = t.tgrelid WHERE NOT t.tgisinternal
UNION ALL SELECT 'function ' || n.nspname || '.' || p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ') ' || md5(coalesce(p.prosrc, ''))
  FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg_toast%' AND p.prokind <> 'a'
UNION ALL SELECT 'extension ' || extname || ' ' || extversion FROM pg_extension
UNION ALL SELECT 'sequence ' || schemaname || '.' || sequencename || ' last=' ||
    (xpath('/row/l/text()', query_to_xml(format('SELECT last_value AS l FROM %I.%I', schemaname, sequencename), false, true, '')))[1]::text || ' called=' ||
    (xpath('/row/c/text()', query_to_xml(format('SELECT is_called AS c FROM %I.%I', schemaname, sequencename), false, true, '')))[1]::text
  FROM pg_sequences
UNION ALL SELECT 'rows ' || nspname || '.' || relname || '=' ||
    (xpath('/row/c/text()', query_to_xml(format('SELECT count(*) AS c FROM %I.%I', nspname, relname), false, true, '')))[1]::text
  FROM rel WHERE relkind IN ('r', 'p')`

// pgTemplateFingerprint reads the state a template must keep unchanged.
func pgTemplateFingerprint(ctx context.Context, dsn string) ([]string, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	rows, err := conn.Query(ctx, pgTemplateFingerprintQuery)
	if err != nil {
		return nil, err
	}
	values, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	sort.Strings(values)
	return values, nil
}

// templateDiff names every change between two fingerprints of one template.
func templateDiff(name string, before, after []string) []string {
	var out []string
	for _, finding := range diffSnapshots(before, after) {
		out = append(out, "template "+name+": "+finding)
	}
	return out
}

func withoutTemplateRows(rows []string) []string {
	out := rows[:0:0]
	for _, row := range rows {
		if !strings.HasPrefix(row, "database "+templateNamePrefix) {
			out = append(out, row)
		}
	}
	return out
}

// checkTemplates compares every template with the state it had when it was
// built, and names any template database nobody registered. A template that
// changed, is open to connections, or lost its template flag is a finding.
func (s *sharedServers) checkTemplates(ctx context.Context, server *sharedServer) []string {
	if !s.templates.used() {
		return nil
	}
	var findings []string
	admin, err := pgx.Connect(ctx, server.adminDSN)
	if err != nil {
		return []string{"template check: connect: " + err.Error()}
	}
	defer func() { _ = admin.Close(context.WithoutCancel(ctx)) }()
	rows, err := admin.Query(ctx, `SELECT datname, datistemplate, datallowconn FROM pg_database WHERE datname LIKE 'dh\_tpl\_%'`)
	if err != nil {
		return []string{"template check: list: " + err.Error()}
	}
	type flags struct{ isTemplate, allowConn bool }
	present := map[string]flags{}
	for rows.Next() {
		var name string
		var f flags
		if err := rows.Scan(&name, &f.isTemplate, &f.allowConn); err != nil {
			rows.Close()
			return []string{"template check: list: " + err.Error()}
		}
		present[name] = f
	}
	rows.Close()
	registered := s.templates.built()
	for name := range present {
		if _, ok := registered[name]; !ok {
			findings = append(findings, "template "+name+": unregistered template database")
		}
	}
	for name, entry := range registered {
		f, ok := present[name]
		if !ok {
			findings = append(findings, "template "+name+": gone")
			continue
		}
		if !f.isTemplate || f.allowConn {
			findings = append(findings, fmt.Sprintf("template %s: istemplate=%v allowconn=%v, want true false", name, f.isTemplate, f.allowConn))
		}
		_, openStatement := templateAlterDDL(name)
		if !f.allowConn {
			if _, err := admin.Exec(ctx, openStatement); err != nil {
				findings = append(findings, "template "+name+": open for the check: "+err.Error())
				continue
			}
		}
		uri, err := withPostgresDatabase(server.adminDSN, name)
		if err != nil {
			findings = append(findings, "template "+name+": "+err.Error())
			continue
		}
		after, err := pgTemplateFingerprint(ctx, uri)
		if err != nil {
			findings = append(findings, "template "+name+": fingerprint: "+err.Error())
			continue
		}
		findings = append(findings, templateDiff(name, entry.fingerprint, after)...)
	}
	sort.Strings(findings)
	return findings
}

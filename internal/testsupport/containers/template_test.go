package containers

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

var templateFixtureHex = strings.Repeat("ab", 32)

func captureTemplateLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buffer bytes.Buffer
	original := templateLog
	templateLog = &buffer
	t.Cleanup(func() { templateLog = original })
	return &buffer
}

func TestTemplateDatabaseNameHoldsTheKey(t *testing.T) {
	name, err := templateDatabaseName(templateFixtureHex)
	if err != nil {
		t.Fatal(err)
	}
	if want := "dh_tpl_" + templateFixtureHex[:24]; name != want {
		t.Errorf("name = %q, want %q", name, want)
	}
	if err := assertSafeIdentifier(name); err != nil {
		t.Errorf("name is not a safe identifier: %v", err)
	}
	other, err := templateDatabaseName("f" + templateFixtureHex[1:])
	if err != nil {
		t.Fatal(err)
	}
	if other == name {
		t.Error("two keys that differ in the first 24 hex digits gave one name")
	}
	for _, bad := range []string{"", "abc", strings.Repeat("g", 64), strings.Repeat("A", 64), "dh_tpl_" + templateFixtureHex} {
		if _, err := templateDatabaseName(bad); err == nil {
			t.Errorf("key %q was accepted", bad)
		}
	}
}

func TestTemplateFallbackReason(t *testing.T) {
	owned := &sharedServers{pg: &sharedServer{engine: "postgres", startedHere: true}}
	attached := &sharedServers{pg: &sharedServer{engine: "postgres", startedHere: false}}
	cases := []struct {
		name    string
		servers *sharedServers
		env     map[string]string
		want    string
	}{
		{"not in shared mode", nil, nil, "not in shared mode"},
		{"remote DSN", owned, map[string]string{PostgresDSNEnv: unreachablePostgresDSN}, "remote DSN"},
		{"escape hatch off", owned, map[string]string{PostgresTemplateEnv: "off"}, PostgresTemplateEnv + "=off"},
		{"server not started by this binary", attached, nil, "server not started by this binary"},
		{"shared server not booted yet", &sharedServers{}, nil, ""},
		{"server started by this binary", owned, nil, ""},
		{"explicit on", owned, map[string]string{PostgresTemplateEnv: "on"}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for key, value := range tc.env {
				t.Setenv(key, value)
			}
			if got := templateFallbackReason(tc.servers); got != tc.want {
				t.Errorf("reason = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTemplateEnvRefusesAnUnknownValue(t *testing.T) {
	recordDockerCalls(t)
	t.Setenv(PostgresTemplateEnv, "maybe")
	log := captureTemplateLog(t)
	_, err := StartPostgresFromTemplate(context.Background(), templateFixtureHex, func(context.Context, string) error { return nil })
	if err == nil || !strings.Contains(err.Error(), PostgresTemplateEnv) {
		t.Errorf("want an error naming %s, got %v (log %q)", PostgresTemplateEnv, err, log)
	}
}

func TestTemplateFallbackLogsANamedLineAndAsksDockerOnce(t *testing.T) {
	calls := recordDockerCalls(t)
	log := captureTemplateLog(t)
	built := false
	_, err := StartPostgresFromTemplate(context.Background(), templateFixtureHex, func(context.Context, string) error {
		built = true
		return nil
	})
	if err == nil {
		t.Fatal("the stub Docker cannot start a container; want an error")
	}
	if want := "containers: postgres template fallback: not in shared mode"; !strings.Contains(log.String(), want) {
		t.Errorf("log %q lacks %q", log, want)
	}
	if built {
		t.Error("the build ran although no instance started")
	}
	if got := calls(); got != 1 {
		t.Errorf("Docker asked %d times, want 1 (one plain StartPostgres)", got)
	}
}

func TestTemplateEscapeHatchOffNeverBuildsATemplate(t *testing.T) {
	recordDockerCalls(t)
	t.Setenv(PostgresTemplateEnv, "off")
	log := captureTemplateLog(t)
	RunWithSharedServers(func() int {
		_, _ = StartPostgresFromTemplate(context.Background(), templateFixtureHex, func(context.Context, string) error { return nil })
		if servers := activeSharedServers(); servers != nil && len(servers.templates.names()) != 0 {
			t.Errorf("templates registered with %s=off: %v", PostgresTemplateEnv, servers.templates.names())
		}
		return 0
	})
	if want := "containers: postgres template fallback: " + PostgresTemplateEnv + "=off"; !strings.Contains(log.String(), want) {
		t.Errorf("log %q lacks %q", log, want)
	}
}

func TestTemplateBuildErrorIsCachedAndLoud(t *testing.T) {
	var registry templateRegistry
	var builds atomic.Int32
	boom := errors.New("migration 0099 failed")
	build := func() error { builds.Add(1); return boom }
	var wait sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wait.Add(1)
		go func() {
			defer wait.Done()
			errs[i] = registry.ensure(templateFixtureHex, build)
		}()
	}
	wait.Wait()
	if got := builds.Load(); got != 1 {
		t.Errorf("build ran %d times, want 1", got)
	}
	for i, err := range errs {
		if !errors.Is(err, boom) {
			t.Errorf("caller %d got %v, want the build error", i, err)
		}
	}
	if err := registry.ensure(templateFixtureHex, func() error { builds.Add(1); return nil }); !errors.Is(err, boom) {
		t.Errorf("a later caller got %v, want the cached build error (no half template)", err)
	}
	if got := builds.Load(); got != 1 {
		t.Errorf("a later caller re-ran the build (%d)", got)
	}
}

func TestTemplateRegistryBuildsEachKeyOnce(t *testing.T) {
	var registry templateRegistry
	var builds atomic.Int32
	other := "f" + templateFixtureHex[1:]
	var wait sync.WaitGroup
	for range 16 {
		wait.Add(2)
		go func() {
			defer wait.Done()
			_ = registry.ensure(templateFixtureHex, func() error { builds.Add(1); return nil })
		}()
		go func() { defer wait.Done(); _ = registry.ensure(other, func() error { builds.Add(1); return nil }) }()
	}
	wait.Wait()
	if got := builds.Load(); got != 2 {
		t.Errorf("builds = %d, want 2 (one per key)", got)
	}
	if got := len(registry.names()); got != 2 {
		t.Errorf("registered %d templates, want 2", got)
	}
}

// An autovacuum worker may sit in a fresh template for a moment. Only a
// client backend is a session a test could have opened.
func TestClientSessionCountIgnoresBackgroundBackends(t *testing.T) {
	cases := []struct {
		name  string
		types []string
		want  int
	}{
		{"none", nil, 0},
		{"autovacuum worker only", []string{"autovacuum worker"}, 0},
		{"every background kind", []string{"autovacuum worker", "autovacuum launcher", "background worker", "walsender", "logical replication worker", "parallel worker", "checkpointer"}, 0},
		{"one client", []string{"client backend"}, 1},
		{"client and autovacuum", []string{"autovacuum worker", "client backend", "client backend"}, 2},
	}
	for _, tc := range cases {
		if got := clientSessionCount(tc.types); got != tc.want {
			t.Errorf("%s: count = %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestTemplateFingerprintDiffNamesTheChange(t *testing.T) {
	before := []string{"extension pg_trgm 1.6", "rows public.t=1", "sequence public.s last=1 called=true", "tables 3"}
	after := []string{"extension pg_trgm 1.6", "rows public.t=2", "sequence public.s last=2 called=true", "tables 3"}
	got := strings.Join(templateDiff("dh_tpl_x", before, after), "\n")
	for _, want := range []string{"template dh_tpl_x", "rows public.t=2", "sequence public.s last=2 called=true", "rows public.t=1"} {
		if !strings.Contains(got, want) {
			t.Errorf("diff lacks %q:\n%s", want, got)
		}
	}
	if same := templateDiff("dh_tpl_x", before, before); len(same) != 0 {
		t.Errorf("equal fingerprints gave a diff: %v", same)
	}
}

// PostgreSQL has no single || for text and the "char" type of the catalog
// columns relkind, contype, prokind and attalign (SQLSTATE 42725), so each
// such column enters the fingerprint text through ::text.
func TestFingerprintQueryCastsCatalogCharColumnsToText(t *testing.T) {
	pattern := regexp.MustCompile(`\|\|\s*(?:\w+\.)?(relkind|contype|prokind|attalign|typtype|tgtype|polcmd|defaclobjtype)\b(::text)?`)
	matches := pattern.FindAllStringSubmatch(pgTemplateFingerprintQuery, -1)
	if len(matches) == 0 {
		t.Fatal("the query concatenates no catalog char column; the check proves nothing, update it")
	}
	for _, match := range matches {
		if match[2] != "::text" {
			t.Errorf("%q is concatenated without ::text", strings.TrimSpace(match[0]))
		}
	}
}

// A database named like a template that no caller asked for is a leak even when no
// template was built in the run: nothing registered, one finding.
func TestFinishReportsAnUnregisteredTemplateDatabaseWhenNoTemplateWasRegistered(t *testing.T) {
	servers := &sharedServers{
		pg: &sharedServer{engine: "postgres", baseline: []string{"database worker_test allowconn=true"},
			snapshot: func(context.Context, string) ([]string, error) {
				return []string{"database worker_test allowconn=true", "database dh_tpl_0123456789abcdef01234567 allowconn=true"}, nil
			}},
	}

	findings, err := servers.finish(context.Background())

	if err != nil {
		t.Fatal(err)
	}
	want := []string{"postgres template dh_tpl_0123456789abcdef01234567: unregistered template database"}
	if !reflect.DeepEqual(findings, want) {
		t.Errorf("findings = %q, want %q", findings, want)
	}
}

// The fingerprint reads what a test could change besides the rows' count: owners, grants,
// storage options, comments, default privileges and the content of every table.
func TestFingerprintQueryReadsOwnersGrantsOptionsCommentsDefaultsAndContent(t *testing.T) {
	for _, want := range []string{
		"pg_get_userbyid(", "relacl", "reloptions", "obj_description(", "col_description(",
		"pg_default_acl", "string_agg(", "proacl", "nspacl", "'content '",
	} {
		if !strings.Contains(pgTemplateFingerprintQuery, want) {
			t.Errorf("the fingerprint query lacks %s", want)
		}
	}
}

func TestPostgresSessionStateIsEmptyOutsideSharedMode(t *testing.T) {
	recordDockerCalls(t)
	user, searchPath, err := PostgresSessionState(context.Background())
	if err != nil || user != "" || searchPath != "" {
		t.Errorf("PostgresSessionState outside shared mode = (%q, %q, %v), want empty and no error", user, searchPath, err)
	}
}

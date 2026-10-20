package bigboy_test

import (
	"regexp"
	"strings"
	"testing"
)

const composeReadersSQL = "ci/bigboy/prepare-clickhouse-readonly-user.sql"

var composeSQLCommentRe = regexp.MustCompile(`--[^\n]*`)

// composeSQL is the statement text of a SQL file: a comment that names a setting must not satisfy a guard.
func composeSQL(g *guard) string {
	return composeSQLCommentRe.ReplaceAllString(g.read(composeReadersSQL), "")
}

var composeHostIPRe = regexp.MustCompile(`HOST IP '([^']+)'`)

func composeReadersHostIP(g *guard) {
	subnet := str(at(g.yamlFile(composeFile), "networks", "dho-clickhouse-readers", "ipam", "config", 0, "subnet"))
	m := composeHostIPRe.FindStringSubmatch(composeSQL(g))
	if m == nil {
		g.failf("prepare-clickhouse-readonly-user.sql has no HOST IP clause")
		return
	}
	g.expect(m[1] == subnet, "readers user HOST IP %q is not the dho-clickhouse-readers subnet %q", m[1], subnet)
}

func composeClickHouseAccessManagement(g *guard) {
	r := composeMust(g, composeIn{})
	got := mp(r.services["clickhouse"]["environment"])["CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT"]
	g.expect(got == "1", "clickhouse CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=%v, want 1 (CREATE USER needs it)", got)
}

var (
	composeReadonly2Re = regexp.MustCompile(`readonly\s*=\s*2\b`)
	composeExecTimeRe  = regexp.MustCompile(`max_execution_time\s*=\s*30\s+MAX\s+30\b`)
	composeMemoryRe    = regexp.MustCompile(`max_memory_usage\s*=\s*4000000000\s+MAX\s+4000000000\b`)
	composeGrantRe     = regexp.MustCompile(`(?m)^GRANT (.+?) TO (\w+);`)
	composeAnyGrantRe  = regexp.MustCompile(`(?m)^GRANT (.*)$`)
	composeTableGrant  = regexp.MustCompile(`(?m)^GRANT SELECT ON default\.\w+ `)
)

func composeReadersProfile(g *guard) {
	sql := composeSQL(g)
	g.expect(composeReadonly2Re.MatchString(sql), "the readers profile is not readonly = 2")
	g.expect(composeExecTimeRe.MatchString(sql), "the readers profile does not pin max_execution_time = 30 MAX 30")
	g.expect(composeMemoryRe.MatchString(sql), "the readers profile does not pin max_memory_usage = 4000000000 MAX 4000000000")
}

func composeReadersGrants(g *guard) {
	sql := composeSQL(g)
	var got [][2]string
	for _, m := range composeGrantRe.FindAllStringSubmatch(sql, -1) {
		got = append(got, [2]string{m[1], m[2]})
	}
	want := [][2]string{{"SELECT ON default.*", "mcp_readonly"}, {"SELECT ON system.tables", "mcp_readonly"}, {"SELECT ON system.columns", "mcp_readonly"}}
	g.expect(equal(got, want), "readers grants are %v, want %v", got, want)
	for _, m := range composeAnyGrantRe.FindAllStringSubmatch(sql, -1) {
		g.expect(strings.HasPrefix(m[1], "SELECT ON "), "the readers file grants more than SELECT: GRANT %s", m[1])
	}
	g.expect(!composeTableGrant.MatchString(sql), "the readers file is back to a per-table grant list")
	g.expect(composeReadonly2Re.MatchString(sql), "the readers profile is not readonly = 2")
}

var composeReadersChecks = []check{
	{"readers_user_host_ip_is_the_readers_network_subnet", composeReadersHostIP},
	{"clickhouse_enables_access_management_so_the_readers_sql_can_apply", composeClickHouseAccessManagement},
	{"readers_profile_forbids_writes_but_allows_per_query_settings_within_pinned_ceilings", composeReadersProfile},
	{"readers_user_is_granted_select_on_the_default_database_and_two_system_tables_only", composeReadersGrants},
}

func TestComposeCHReadersReal(t *testing.T) { runReal(t, composeReadersChecks...) }

func TestComposeCHReadersPlanted(t *testing.T) {
	files := []string{composeFile, composePins, composeReadersSQL}
	runPlanted(t, []planted{
		{name: "subnet drifts", copy: files, mutate: mut(composeFile, "- subnet: 10.199.80.0/28", "- subnet: 10.199.81.0/28"),
			check: composeReadersHostIP, want: "not the dho-clickhouse-readers subnet"},
		{name: "host ip clause gone", copy: files, mutate: mut(composeReadersSQL, "    HOST IP '10.199.80.0/28'\n", ""),
			check: composeReadersHostIP, want: "no HOST IP clause"},
		{name: "access management off", copy: files, mutate: mut(composeFile, "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=0"),
			check: composeClickHouseAccessManagement, want: "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT"},
		{name: "readonly 1 with the comment kept", copy: files, mutate: mut(composeReadersSQL, "SETTINGS readonly = 2,", "SETTINGS readonly = 1,"),
			check: composeReadersProfile, want: "readonly = 2"},
		{name: "execution ceiling raisable", copy: files, mutate: mut(composeReadersSQL, "max_execution_time = 30 MAX 30", "max_execution_time = 30"),
			check: composeReadersProfile, want: "max_execution_time"},
		{name: "memory ceiling raised", copy: files, mutate: mut(composeReadersSQL, "4000000000 MAX 4000000000", "4000000000 MAX 8000000000"),
			check: composeReadersProfile, want: "max_memory_usage"},
		{name: "insert granted", copy: files, mutate: mut(composeReadersSQL, "GRANT SELECT ON system.columns TO mcp_readonly;", "GRANT SELECT ON system.columns TO mcp_readonly;\nGRANT INSERT ON default.* TO mcp_readonly;"),
			check: composeReadersGrants, want: "more than SELECT"},
		{name: "per-table grant", copy: files, mutate: mut(composeReadersSQL, "GRANT SELECT ON default.* TO mcp_readonly;", "GRANT SELECT ON default.teams TO mcp_readonly;"),
			check: composeReadersGrants, want: "per-table"},
	})
}

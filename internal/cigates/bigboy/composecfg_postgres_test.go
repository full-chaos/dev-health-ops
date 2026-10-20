package bigboy_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var ccfgRiverRoleEnv = []string{
	"RIVER_DOMAIN_DATABASE_ROLE", "RIVER_QUEUE_DATABASE_ROLE", "RIVER_COORDINATOR_DATABASE_ROLE",
	"RIVER_DOMAIN_DATABASE_PASSWORD", "RIVER_QUEUE_DATABASE_PASSWORD", "RIVER_COORDINATOR_DATABASE_PASSWORD",
}

type ccfgPsqlRun struct {
	argv []string
	sql  string
}

func (r ccfgPsqlRun) variable(name string) (string, bool) {
	for i := 0; i+1 < len(r.argv); i++ {
		if r.argv[i] == "-v" {
			if v, ok := strings.CutPrefix(r.argv[i+1], name+"="); ok {
				return v, true
			}
		}
	}
	return "", false
}

// Executed with psql stubbed so guards read the argv and SQL the script really sends, not its source text.
func ccfgRunInitDBs(g *guard, label string, env map[string]string) ccfgPsqlRun {
	out := filepath.Join(g.tmp, label)
	ccfgWrite(g, filepath.Join(label, "bin", "psql"), "#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" > \"$CCFG_OUT/argv\"\ncat > \"$CCFG_OUT/sql\"\n", 0o755)
	args := []string{}
	for _, k := range ccfgRiverRoleEnv {
		args = append(args, "-u", k)
	}
	args = append(args, "PATH="+filepath.Join(out, "bin")+":"+os.Getenv("PATH"), "CCFG_OUT="+out,
		"POSTGRES_DB=ccfg_app_db", "POSTGRES_USER=ccfg_superuser")
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		args = append(args, k+"="+env[k])
	}
	args = append(args, "bash", g.path(ccfgInitDBs))
	cmd := exec.Command("env", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		g.failf("%s (%s run) failed: %v: %s", ccfgInitDBs, label, err, stderr.String())
		return ccfgPsqlRun{}
	}
	argv, _ := os.ReadFile(filepath.Join(out, "argv"))
	sql, _ := os.ReadFile(filepath.Join(out, "sql"))
	return ccfgPsqlRun{argv: strings.Split(strings.TrimRight(string(argv), "\n"), "\n"), sql: string(sql)}
}

func ccfgLocalPostgresRoles(g *guard) {
	env := ccfgEnv(ccfgService(g, ccfgServices(g), "postgres"))
	for k, want := range map[string]string{
		"RIVER_DOMAIN_DATABASE_ROLE": "${RIVER_DOMAIN_DATABASE_ROLE:-devhealth_domain}",
		"RIVER_QUEUE_DATABASE_ROLE":  "${RIVER_QUEUE_DATABASE_ROLE:-devhealth_queue}",
	} {
		g.expect(str(env[k]) == want, "postgres %s = %q, want %q", k, env[k], want)
	}

	defaults := ccfgRunInitDBs(g, "defaults", nil)
	custom := ccfgRunInitDBs(g, "custom", map[string]string{
		"RIVER_DOMAIN_DATABASE_ROLE": "ccfg_domain", "RIVER_QUEUE_DATABASE_ROLE": "ccfg_queue", "RIVER_COORDINATOR_DATABASE_ROLE": "ccfg_coordinator",
	})
	for _, c := range []struct {
		run        ccfgPsqlRun
		name, want string
	}{
		{defaults, "app_database", "ccfg_app_db"},
		{defaults, "domain_role", "devhealth_domain"},
		{defaults, "queue_role", "devhealth_queue"},
		{defaults, "coordinator_role", "devhealth_coordinator"},
		{custom, "domain_role", "ccfg_domain"},
		{custom, "queue_role", "ccfg_queue"},
		{custom, "coordinator_role", "ccfg_coordinator"},
	} {
		got, ok := c.run.variable(c.name)
		g.expect(ok && got == c.want, "init-extra-dbs.sh passes psql -v %s=%q, want %q", c.name, got, c.want)
	}

	sql := defaults.sql
	for _, frag := range []string{
		"CREATE ROLE %I LOGIN NOSUPERUSER",
		"NOCREATEDB", "NOCREATEROLE", "NOREPLICATION", "NOBYPASSRLS",
		`:"app_database"`,
		"GRANT CONNECT ON DATABASE",
		`REVOKE TEMPORARY ON DATABASE :"app_database" FROM PUBLIC, :"domain_role", :"queue_role", :"coordinator_role";`,
		`:'coordinator_role', :'coordinator_password'`,
		`GRANT CONNECT ON DATABASE :"app_database" TO :"coordinator_role";`,
		`GRANT USAGE ON SCHEMA public TO :"coordinator_role";`,
		`REVOKE CREATE ON SCHEMA public FROM :"coordinator_role";`,
		"GRANT USAGE ON SCHEMA public",
		"REVOKE CREATE ON SCHEMA public FROM",
		"public.alembic_version",
		"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public",
		"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public",
		"GRANT SELECT, UPDATE, DELETE ON TABLE public.worker_job_outbox",
		"GRANT SELECT, INSERT ON TABLE public.worker_job_delivery_abandonments",
		"GRANT SELECT, UPDATE, DELETE ON TABLE public.worker_job_completion_fences",
		`REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC, :"domain_role", :"queue_role";`,
	} {
		g.expect(strings.Contains(sql, frag), "init-extra-dbs.sh no longer sends %q to psql", frag)
	}
	for _, frag := range []string{
		"worker_job_routes",
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
		"GRANT INSERT ON TABLE public.worker_job_completion_fences",
	} {
		g.expect(!strings.Contains(sql, frag) && !strings.Contains(strings.Join(defaults.argv, "\n"), frag), "init-extra-dbs.sh sends forbidden %q to psql", frag)
	}
	ccfgLeastPrivilege(g, ccfgDomainSection(g, sql))
}

const ccfgDomainMarker = `GRANT USAGE ON SCHEMA public TO :"queue_role";`

func ccfgDomainSection(g *guard, sql string) string {
	before, _, found := strings.Cut(sql, ccfgDomainMarker)
	if !found {
		g.failf("init-extra-dbs.sh does not contain its domain-section marker %q; every domain assertion would widen to the queue role", ccfgDomainMarker)
	}
	return before
}

const (
	ccfgReadOnlyGrant  = "GRANT SELECT ON TABLE public.%I TO %I"
	ccfgReadWriteGrant = "GRANT SELECT, INSERT, UPDATE ON TABLE public.%I TO %I"
)

var (
	ccfgValuesRowRe   = regexp.MustCompile(`\('([^']+)'\)`)
	ccfgDeleteGrantRe = regexp.MustCompile(`GRANT\s+([A-Z,\s()a-z_]+?)\s+ON TABLE public\.([A-Za-z0-9_%]+)`)
)

func ccfgFormattedGrantTables(g *guard, domain, grant string) []string {
	re := regexp.MustCompile(`(?s)SELECT\s+format\(\s*'` + regexp.QuoteMeta(grant) + `'.*?FROM\s+\((.*?)\)\s+AS\s+required\(table_name\)`)
	matches := re.FindAllStringSubmatch(domain, -1)
	if len(matches) != 1 {
		g.failf("expected exactly one VALUES block for %q, found %d", grant, len(matches))
		return nil
	}
	var out []string
	for _, m := range ccfgValuesRowRe.FindAllStringSubmatch(matches[0][1], -1) {
		out = append(out, m[1])
	}
	sort.Strings(out)
	return out
}

func ccfgDeleteGrantTables(domain string) []string {
	set := map[string]bool{}
	for _, m := range ccfgDeleteGrantRe.FindAllStringSubmatch(domain, -1) {
		for _, p := range strings.Split(m[1], ",") {
			name, _, _ := strings.Cut(p, "(")
			if strings.ToUpper(strings.TrimSpace(name)) == "DELETE" {
				set[m[2]] = true
			}
		}
	}
	return ccfgKeys(set)
}

func ccfgLeastPrivilege(g *guard, domain string) {
	ro := ccfgFormattedGrantTables(g, domain, ccfgReadOnlyGrant)
	g.expect(equal(ro, []string{"integration_credentials", "integrations", "sync_dispatch_transport_routes"}),
		"read-only grant block covers the wrong tables: %v", ro)
	rw := ccfgFormattedGrantTables(g, domain, ccfgReadWriteGrant)
	g.expect(equal(rw, []string{"integration_datasets", "integration_sources", "sync_run_units", "sync_runs"}),
		"read-write grant block covers the wrong tables: %v", rw)
	for _, frag := range []string{
		ccfgReadOnlyGrant, ccfgReadWriteGrant,
		"GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_watermarks",
		"GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_dispatch_outbox",
		"GRANT SELECT, INSERT ON TABLE public.worker_job_outbox",
		"GRANT SELECT, INSERT, DELETE ON TABLE public.sync_run_unit_effect_snapshots",
	} {
		g.expect(strings.Contains(domain, frag), "domain section is missing %q", frag)
	}
	for _, frag := range []string{
		"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES",
		"GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES",
		"DELETE ON ALL TABLES",
	} {
		g.expect(!strings.Contains(domain, frag), "domain section grants forbidden %q", frag)
	}
	del := ccfgDeleteGrantTables(domain)
	g.expect(equal(del, []string{"sync_run_unit_effect_snapshots", "worker_concurrency_leases", "worker_instances"}),
		"domain section grants DELETE on the wrong tables: %v", del)
}

func ccfgSwappedPrivilegeBlocks(g *guard) {
	ccfgLeastPrivilege(g, ccfgDomainSection(g, ccfgRunInitDBs(g, "domain", nil).sql))
}

var (
	ccfgTableGrantRe    = regexp.MustCompile(`GRANT\s+\S[^;]*ON TABLE`)
	ccfgSequenceGrantRe = regexp.MustCompile(`GRANT\s+\S[^;]*ON SEQUENCE`)
)

func ccfgProvisionNotGrantAuthority(g *guard) {
	script := g.read(ccfgProvisionSQL)
	for _, frag := range []string{
		"NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION",
		"WHERE NOT EXISTS",
		"domain_role, queue_role, and coordinator_role must be distinct",
		`REVOKE TEMPORARY ON DATABASE :"app_database" FROM PUBLIC, :"domain_role", :"queue_role", :"coordinator_role";`,
	} {
		g.expect(strings.Contains(script, frag), "%s is missing %q", ccfgProvisionSQL, frag)
	}
	for _, role := range []string{"domain_role", "queue_role", "coordinator_role"} {
		for _, frag := range []string{
			`GRANT CONNECT ON DATABASE :"app_database" TO :"` + role + `"`,
			`GRANT USAGE ON SCHEMA public TO :"` + role + `"`,
			`REVOKE CREATE ON SCHEMA public FROM :"` + role + `"`,
		} {
			g.expect(strings.Contains(script, frag), "%s is missing bootstrap %q", ccfgProvisionSQL, frag)
		}
	}
	var code []string
	for _, line := range strings.Split(script, "\n") {
		before, _, _ := strings.Cut(line, "--")
		code = append(code, before)
	}
	codeOnly := strings.Join(code, "\n")
	g.expect(!strings.Contains(codeOnly, "REVOKE ALL"), "%s carries REVOKE ALL again: it must not be a grant authority (CHAOS-4261)", ccfgProvisionSQL)
	g.expect(!ccfgTableGrantRe.MatchString(codeOnly), "%s grants a table-level privilege again: it must not be a grant authority (CHAOS-4261)", ccfgProvisionSQL)
	g.expect(!ccfgSequenceGrantRe.MatchString(codeOnly), "%s grants a sequence privilege again: it must not be a grant authority (CHAOS-4261)", ccfgProvisionSQL)
}

func TestComposeCfgPostgresRealTree(t *testing.T) {
	runReal(t,
		check{"local_postgres_bootstraps_distinct_go_runtime_roles", ccfgLocalPostgresRoles},
		check{"least_privilege_domain_grants_reject_swapped_privilege_blocks", ccfgSwappedPrivilegeBlocks},
		check{"provision_river_roles_sql_is_not_a_grant_authority", ccfgProvisionNotGrantAuthority},
	)
}

func TestComposeCfgPostgresPlanted(t *testing.T) {
	files := []string{ccfgCompose, ccfgInitDBs}
	s := ccfgInitDBs
	sqlCase := func(name, want string, m func(*testing.T, string)) planted {
		return planted{name: name, copy: []string{ccfgProvisionSQL}, mutate: m, check: ccfgProvisionNotGrantAuthority, want: want}
	}
	initCase := func(name, want string, m func(*testing.T, string)) planted {
		return planted{name: name, copy: files, mutate: m, check: ccfgLocalPostgresRoles, want: want}
	}
	runPlanted(t, []planted{
		initCase("postgres_domain_role_hardcoded", "postgres RIVER_DOMAIN_DATABASE_ROLE",
			mut(ccfgCompose, "      RIVER_DOMAIN_DATABASE_ROLE: ${RIVER_DOMAIN_DATABASE_ROLE:-devhealth_domain}\n      RIVER_QUEUE_DATABASE_ROLE", "      RIVER_DOMAIN_DATABASE_ROLE: devhealth_domain\n      RIVER_QUEUE_DATABASE_ROLE")),
		initCase("coordinator_override_ignored", `-v coordinator_role="devhealth_coordinator", want "ccfg_coordinator"`,
			mut(s, `-v coordinator_role="$river_coordinator_role"`, `-v coordinator_role="devhealth_coordinator"`)),
		initCase("coordinator_default_drifts", `-v coordinator_role="devhealth_coord", want "devhealth_coordinator"`,
			mut(s, "${RIVER_COORDINATOR_DATABASE_ROLE:-devhealth_coordinator}", "${RIVER_COORDINATOR_DATABASE_ROLE:-devhealth_coord}")),
		initCase("app_database_not_passed", "-v app_database=",
			mut(s, `  -v app_database="$POSTGRES_DB" \`+"\n", "")),
		initCase("worker_job_routes_regranted", `forbidden "worker_job_routes"`,
			mut(s, "    GRANT USAGE ON SCHEMA public TO :\"queue_role\";\n", "    GRANT SELECT ON TABLE public.worker_job_routes TO :\"domain_role\";\n    GRANT USAGE ON SCHEMA public TO :\"queue_role\";\n")),
		initCase("public_create_revoked", `forbidden "REVOKE CREATE ON SCHEMA public FROM PUBLIC"`,
			mut(s, `REVOKE CREATE ON SCHEMA public FROM :"queue_role";`, `REVOKE CREATE ON SCHEMA public FROM PUBLIC, :"queue_role";`)),
		initCase("temporary_not_revoked_from_coordinator", "REVOKE TEMPORARY ON DATABASE",
			mut(s, `:"queue_role", :"coordinator_role";`, `:"queue_role";`)),
		initCase("function_execute_kept", "REVOKE EXECUTE ON ALL FUNCTIONS",
			mut(s, `    REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC, :"domain_role", :"queue_role";`+"\n", "")),
		initCase("read_only_block_widened", "read-only grant block covers the wrong tables",
			mut(s, "          ('sync_dispatch_transport_routes')\n", "          ('sync_dispatch_transport_routes'),\n          ('worker_job_routes_shadow')\n")),
		initCase("delete_written_mid_list", "domain section grants DELETE on the wrong tables",
			mut(s, "'GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_watermarks TO %I'", "'GRANT SELECT, DELETE, UPDATE ON TABLE public.sync_watermarks TO %I'")),
		initCase("domain_all_tables_delete", `forbidden "DELETE ON ALL TABLES"`,
			mut(s, `    REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM :"domain_role";`+"\n", `    REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM :"domain_role";`+"\n    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO :\"domain_role\";\n")),
		{
			name:  "swapped_privilege_blocks",
			copy:  files,
			check: ccfgSwappedPrivilegeBlocks,
			want:  "grant block covers the wrong tables",
			mutate: all(
				mut(s, ccfgReadOnlyGrant, "__CCFG_READ_ONLY__"),
				mut(s, ccfgReadWriteGrant, ccfgReadOnlyGrant),
				mut(s, "__CCFG_READ_ONLY__", ccfgReadWriteGrant),
			),
		},
		{
			name:   "domain_marker_missing",
			copy:   files,
			check:  ccfgSwappedPrivilegeBlocks,
			want:   "domain-section marker",
			mutate: mut(s, ccfgDomainMarker, `GRANT USAGE ON SCHEMA public TO :"queue_role" ;`),
		},
		sqlCase("table_grant_returns", "grants a table-level privilege again",
			mut(ccfgProvisionSQL, `REVOKE CREATE ON SCHEMA public FROM :"queue_role";`, `REVOKE CREATE ON SCHEMA public FROM :"queue_role";`+"\nGRANT REFERENCES ON TABLE public.sync_runs TO :\"domain_role\";")),
		sqlCase("sequence_grant_returns", "grants a sequence privilege again",
			mut(ccfgProvisionSQL, `REVOKE CREATE ON SCHEMA public FROM :"queue_role";`, `REVOKE CREATE ON SCHEMA public FROM :"queue_role";`+"\nGRANT USAGE ON SEQUENCE public.sync_runs_id_seq TO :\"coordinator_role\";")),
		sqlCase("revoke_all_returns", "carries REVOKE ALL again",
			mut(ccfgProvisionSQL, `REVOKE CREATE ON SCHEMA public FROM :"queue_role";`, `REVOKE CREATE ON SCHEMA public FROM :"queue_role";`+"\nREVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM :\"queue_role\";")),
		sqlCase("coordinator_bootstrap_dropped", `GRANT USAGE ON SCHEMA public TO :\"coordinator_role\"`,
			mut(ccfgProvisionSQL, `GRANT USAGE ON SCHEMA public TO :"coordinator_role";`, "")),
		sqlCase("distinct_role_guard_dropped", "must be distinct",
			mut(ccfgProvisionSQL, "domain_role, queue_role, and coordinator_role must be distinct", "roles collide")),
	})
}

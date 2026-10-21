package bigboy_test

import (
	"regexp"
	"strings"
	"testing"
)

var (
	composeACRAll   = []string{"acr-api", "acr-db-acl", "acr-db-init", "acr-entitlement-standin", "acr-falkordb", "acr-mcp", "acr-migrate", "acr-postgres", "acr-projector"}
	composeACRVenue = []string{"acr-db-acl", "acr-db-init", "acr-entitlement-standin", "acr-migrate", "acr-postgres"}
	composeACRUp    = []string{"acr-postgres", "acr-db-init", "acr-migrate", "acr-db-acl", "acr-entitlement-standin", "acr-falkordb", "acr-api", "acr-projector", "acr-mcp"}
	composeACRPins  = []string{"BIGBOY_ACR_API_IMAGE", "BIGBOY_ACR_MCP_IMAGE", "BIGBOY_ACR_POSTGRES_IMAGE", "BIGBOY_ACR_NGINX_IMAGE", "BIGBOY_ACR_FALKORDB_IMAGE"}
)

const composeSecretsDir = "/var/lib/oci-cache/mcp-venue/secrets/"

func composeACR(g *guard) composeOut { return composeMust(g, composeIn{profiles: []string{"acr"}}) }

func composeNetworks(s doc) []string { return composeSorted(mp(s["networks"])) }

func composeACRDefaultStack(g *guard) {
	got := composeSorted(composeMust(g, composeIn{}).services)
	g.expect(equal(got, composeSortedStrs(composeStackServices)), "the default stack renders %v, want %v (no acr service)", got, composeStackServices)
}

func composeACRProfilesAddOnlyACR(g *guard) {
	base := composeMust(g, composeIn{})
	for _, c := range []struct {
		profiles []string
		want     []string
	}{
		{[]string{"acr"}, composeACRAll},
		{[]string{"acr-venue"}, composeACRVenue},
		{[]string{"acr", "acr-venue"}, composeACRAll},
	} {
		got := composeAdded(base, composeMust(g, composeIn{profiles: c.profiles}))
		g.expect(equal(got, c.want), "profiles %v add %v, want %v", c.profiles, got, c.want)
	}
}

func composeACRDataHostPaths(g *guard) {
	r := composeACR(g)
	binds := map[string]string{}
	for name, s := range r.services {
		if !strings.HasPrefix(name, "acr-") {
			continue
		}
		for _, v := range list(s["volumes"]) {
			if str(at(v, "type")) == "bind" {
				binds[name+" "+str(at(v, "target"))] = str(at(v, "source"))
			}
		}
	}
	for key, want := range map[string]string{
		"acr-postgres /var/lib/postgresql":    "/var/lib/oci-cache/mcp-venue/postgres-data",
		"acr-falkordb /var/lib/falkordb/data": "/var/lib/oci-cache/mcp-venue/falkordb-data",
	} {
		g.expect(binds[key] == want, "bind %s has source %q, want the old project's host path %q", key, binds[key], want)
	}
	name := str(at(r.top, "volumes", "acr_entitlement_tmp", "name"))
	g.expect(name == "dev-health-acr-entitlement-tmp", "volume acr_entitlement_tmp is named %q, want dev-health-acr-entitlement-tmp", name)
}

func composeACRContainerNames(g *guard) {
	r := composeACR(g)
	want := map[string]string{
		"acr-api": "dev-health-acr-mcp-acr-api-1", "acr-db-acl": "dev-health-acr-mcp-acr-db-acl-1",
		"acr-db-init": "dev-health-acr-mcp-acr-db-init-1", "acr-entitlement-standin": "dev-health-acr-mcp-acr-entitlement-standin-1",
		"acr-falkordb": "dev-health-acr-mcp-falkordb-1", "acr-mcp": "dev-health-acr-mcp-acr-mcp-1",
		"acr-migrate": "dev-health-acr-mcp-acr-migrate-1", "acr-postgres": "dev-health-acr-mcp-postgres-1",
		"acr-projector": "dev-health-acr-mcp-acr-projector-1",
	}
	for _, n := range composeACRAll {
		got := str(r.services[n]["container_name"])
		g.expect(got == want[n], "%s container_name is %q, want %q", n, got, want[n])
	}
	for svc, alias := range map[string]string{"acr-postgres": "postgres", "acr-falkordb": "falkordb"} {
		got := strs(at(r.services[svc], "networks", "acr", "aliases"))
		g.expect(equal(got, []string{alias}), "%s answers on the acr network as %v, want [%s]", svc, got, alias)
	}
	_, onACR := mp(r.services["postgres"]["networks"])["acr"]
	g.expect(!onACR, "the dev-health postgres is attached to the acr network")
}

var composeQueryURLRe = regexp.MustCompile(`^http://query-api:\d+$`)

func composeACRNetworks(g *guard) {
	r := composeACR(g)
	for _, n := range composeACRAll {
		want := []string{"acr"}
		switch n {
		case "acr-api":
			want = []string{"acr", "dho-clickhouse-readers", "dho-query-internal"}
		case "acr-projector":
			want = []string{"acr", "dho-clickhouse-readers"}
		}
		got := composeNetworks(r.services[n])
		g.expect(equal(got, want), "%s is on networks %v, want %v", n, got, want)
		for _, net := range got {
			g.expect(!strings.Contains(net, "dev-health"), "%s is attached to the ops network %s", n, net)
		}
	}
	env := mp(r.services["acr-api"]["environment"])
	for _, k := range []string{"ACR_DATA_QUERY_URL", "ACR_DATA_GRAPHQL_URL"} {
		g.expect(composeQueryURLRe.MatchString(str(env[k])), "acr-api %s=%q is not http://query-api:<port>", k, str(env[k]))
	}
}

func composeACRPorts(g *guard) {
	r := composeACR(g)
	var published []string
	for _, n := range composeACRAll {
		if len(list(r.services[n]["ports"])) > 0 {
			published = append(published, n)
		}
	}
	g.expect(equal(published, []string{"acr-mcp"}), "acr services publishing a port: %v, want only acr-mcp", published)
	ports := list(r.services["acr-mcp"]["ports"])
	if len(ports) != 1 {
		g.failf("acr-mcp publishes %d ports, want 1", len(ports))
		return
	}
	got := []any{at(ports[0], "host_ip"), at(ports[0], "published"), at(ports[0], "target")}
	g.expect(equal(got, []any{"127.0.0.1", "8081", 8081}), "acr-mcp publishes %v, want [127.0.0.1 8081 8081] (loopback only)", got)
}

var (
	composeURLCredRe   = regexp.MustCompile(`://[^/\s]*:[^/\s@]*@`)
	composeSecretKeyRe = regexp.MustCompile(`(PASSWORD|KEY|TOKEN|DSN|KEYS)$`)
)

func composeACRSecretsAreFiles(g *guard) {
	r := composeACR(g)
	for name, sec := range mp(r.top["secrets"]) {
		if strings.HasPrefix(name, "acr_") {
			file := str(at(sec, "file"))
			g.expect(strings.HasPrefix(file, composeSecretsDir), "secret %s reads %q, outside %s", name, file, composeSecretsDir)
		}
	}
	for _, n := range composeACRAll {
		for k, v := range mp(r.services[n]["environment"]) {
			g.expect(!composeURLCredRe.MatchString(str(v)), "%s %s carries a credential in a URL", n, k)
			if composeSecretKeyRe.MatchString(k) {
				g.expect(strings.HasPrefix(str(v), "/run/secrets/"), "%s %s is a secret value, not a /run/secrets/ file path", n, k)
			}
		}
	}
}

var (
	composeACROwnPinRe = regexp.MustCompile(`^ghcr\.io/full-chaos/dev-health-acr/acr-(api|mcp)@sha256:[0-9a-f]{64}$`)
	composeDigestEndRe = regexp.MustCompile(`@sha256:[0-9a-f]{64}$`)
)

func composeACRImagesPinned(g *guard) {
	r := composeACR(g)
	pins := composePinsMap(g.read(composePins))
	for _, c := range []struct {
		pin   string
		own   bool
		names []string
	}{
		{"BIGBOY_ACR_API_IMAGE", true, []string{"acr-api", "acr-migrate", "acr-projector"}},
		{"BIGBOY_ACR_MCP_IMAGE", true, []string{"acr-mcp"}},
		{"BIGBOY_ACR_POSTGRES_IMAGE", false, []string{"acr-postgres", "acr-db-init", "acr-db-acl"}},
		{"BIGBOY_ACR_NGINX_IMAGE", false, []string{"acr-entitlement-standin"}},
		{"BIGBOY_ACR_FALKORDB_IMAGE", false, []string{"acr-falkordb"}},
	} {
		re := composeDigestEndRe
		if c.own {
			re = composeACROwnPinRe
		}
		g.expect(re.MatchString(pins[c.pin]), "pin %s=%q is not pinned by registry digest", c.pin, pins[c.pin])
		for _, n := range c.names {
			got := str(r.services[n]["image"])
			g.expect(got == pins[c.pin], "%s renders image %q, want the %s pin", n, got, c.pin)
		}
	}
	for _, pin := range composeACRPins {
		for _, p := range [][]string{nil, {"acr"}} {
			out := composeRender(g, composeIn{profiles: p, pins: composeDropLine(pin + "=")})
			g.expect(out.err != "" && strings.Contains(out.err, pin) && strings.Contains(out.err, "is required"),
				"without the %s pin the render (profiles %v) does not fail naming it as required: %q", pin, p, out.err)
		}
	}
}

func composeACRStartOrder(g *guard) {
	r := composeACR(g)
	cond := func(n string) map[string]string {
		out := map[string]string{}
		for d, v := range mp(r.services[n]["depends_on"]) {
			out[d] = str(at(v, "condition"))
		}
		return out
	}
	healthy, done := "service_healthy", "service_completed_successfully"
	for n, want := range map[string]map[string]string{
		"acr-db-init":   {"acr-postgres": healthy},
		"acr-migrate":   {"acr-postgres": healthy, "acr-db-init": done},
		"acr-db-acl":    {"acr-postgres": healthy, "acr-migrate": done},
		"acr-api":       {"acr-postgres": healthy, "acr-db-acl": done},
		"acr-projector": {"acr-postgres": healthy, "acr-db-acl": done},
		"acr-mcp":       {"acr-api": "service_started"},
	} {
		got := cond(n)
		g.expect(equal(got, want), "%s depends_on %v, want %v", n, got, want)
	}
}

func composeACRTalksToRealOps(g *guard) {
	r := composeACR(g)
	api := r.services["acr-api"]
	env := mp(api["environment"])
	g.expect(str(env["ACR_DEV_HEALTH_ENTITLEMENT_URL"]) == "http://go-api:8091", "acr-api ACR_DEV_HEALTH_ENTITLEMENT_URL=%q, want http://go-api:8091", str(env["ACR_DEV_HEALTH_ENTITLEMENT_URL"]))
	g.expect(str(env["ACR_DEV_HEALTH_ENTITLEMENT_TOKEN_FILE"]) == "/run/secrets/acr_ops_svc_token", "acr-api entitlement token is not the acr_ops_svc_token secret file")
	var secrets []string
	for _, s := range list(api["secrets"]) {
		secrets = append(secrets, str(at(s, "source")))
	}
	g.expect(composeSet(secrets...)["acr_ops_svc_token"], "acr-api does not mount the secret acr_ops_svc_token (mounts %v)", secrets)
	_, standin := mp(api["depends_on"])["acr-entitlement-standin"]
	g.expect(!standin, "acr-api depends on the entitlement stand-in")
	_, retries := env["ACR_CONTEXT_FABRIC_MODEL_MAX_TRANSPORT_RETRIES"]
	g.expect(!retries, "acr-api sets the removed ACR_CONTEXT_FABRIC_MODEL_MAX_TRANSPORT_RETRIES")
	file := str(at(r.top, "secrets", "acr_ops_svc_token", "file"))
	g.expect(file == composeSecretsDir+"ops_svc_acr_token", "secret acr_ops_svc_token reads %q, want %sops_svc_acr_token", file, composeSecretsDir)
	_, apiNet := mp(api["networks"])["dho-query-internal"]
	_, goNet := mp(composeMust(g, composeIn{}).services["go-api"]["networks"])["dho-query-internal"]
	g.expect(apiNet && goNet, "acr-api and go-api do not share the network dho-query-internal (acr-api %v, go-api %v)", apiNet, goNet)
}

const composeLLMSource = "ACR_CONTEXT_FABRIC_ORG_LLM_CONFIG_SOURCE"

func composeACROrgLLMSourcePassedThrough(g *guard) {
	for _, line := range strings.Split(g.read(composePins), "\n") {
		g.expect(!strings.HasPrefix(line, composeLLMSource), "images.env sets %s (a persistent BYO setting bills chris's key)", composeLLMSource)
	}
	off := composeACR(g)
	got := mp(off.services["acr-api"]["environment"])[composeLLMSource]
	g.expect(got == "", "acr-api %s renders %q with nothing set, want empty", composeLLMSource, got)
	on := composeMust(g, composeIn{profiles: []string{"acr"}, env: composeEnv(map[string]string{composeLLMSource: "ops"})})
	got = mp(on.services["acr-api"]["environment"])[composeLLMSource]
	g.expect(got == "ops", "acr-api %s renders %q with the environment set to ops, want ops", composeLLMSource, got)
	_, proj := mp(off.services["acr-projector"]["environment"])[composeLLMSource]
	g.expect(!proj, "acr-projector reads %s; only acr-api does", composeLLMSource)
}

var (
	composeReadmeCmdRe = regexp.MustCompile("`([^`]*docker [^`]+)`")
	composeDownRe      = regexp.MustCompile(`\bdown\b`)
	composeStopRe      = regexp.MustCompile(`\bstop\b|\bkill\b`)
	composeDownBadRe   = regexp.MustCompile(`\s-v\b|--volumes|--rmi`)
	composeRepoCountRe = regexp.MustCompile(`GRAPH\.QUERY <graph> "MATCH \(r:Repository\) RETURN count\(r\)"`)
)

func composeACRSwitchProcedure(g *guard) {
	readme := g.read("ci/bigboy/README.md")
	start := strings.Index(readme, "The one-time move from the old project")
	if start < 0 {
		g.failf("README.md has no section on the one-time move from the old project")
		return
	}
	section := readme[start:]
	if end := strings.Index(section, "\n## "); end >= 0 {
		section = section[:end]
	}
	idx := func(s string) int { return strings.Index(section, s) }
	var commands, downs, ups []string
	for _, m := range composeReadmeCmdRe.FindAllStringSubmatch(section, -1) {
		commands = append(commands, m[1])
		if composeDownRe.MatchString(m[1]) {
			downs = append(downs, m[1])
		}
		if strings.Contains(m[1], "--profile acr up") {
			ups = append(ups, m[1])
		}
		g.expect(!composeStopRe.MatchString(m[1]), "the switch procedure stops or kills instead of removing: %s", m[1])
	}
	if len(downs) != 1 || len(ups) != 1 {
		g.failf("the switch procedure has %d down and %d acr up commands, want one each: %v", len(downs), len(ups), commands)
		return
	}
	down, up := downs[0], ups[0]
	g.expect(strings.Contains(down, "-p dev-health-acr-mcp ") && strings.Contains(down, "-f compose.yml"), "the down is not of the old project dev-health-acr-mcp compose.yml: %s", down)
	g.expect(!composeDownBadRe.MatchString(down), "the down removes data or images (-v, --volumes or --rmi): %s", down)
	g.expect(idx(down) < idx("--profile acr up -d"), "the down of the old project does not come before the new up")
	g.expect(strings.Contains(section, "REMOVED, not only stopped"), "the switch procedure lost 'REMOVED, not only stopped'")
	_, names, _ := strings.Cut(up, " up -d --no-build ")
	g.expect(equal(strings.Fields(names), composeACRUp), "the up starts %v, want exactly the nine acr services in start order %v", strings.Fields(names), composeACRUp)
	g.expect(idx("ACR_CONTEXT_FABRIC_PROJECTION_ENABLED=true") >= 0 && idx("ACR_CONTEXT_FABRIC_PROJECTION_ENABLED=true") < idx(up), "the projection switch is not named before the up")
	g.expect(idx("BGSAVE") >= 0 && idx("BGSAVE") < idx(down), "the graph is not saved (BGSAVE) before the down")
	g.expect(strings.Contains(section, "falkordb-data.backup-"), "the switch procedure lost the falkordb-data.backup- copy")
	g.expect(idx("--profile acr up -d") < idx("docker inspect --format"), "the images are not checked (docker inspect) after the up")
	counts := composeRepoCountRe.FindAllStringIndex(section, -1)
	g.expect(len(counts) == 2 && counts[0][0] < idx(down) && idx(down) < idx("--profile acr up -d") && idx("--profile acr up -d") < counts[1][0],
		"the repository count is not read before the down and again after the up: %v", counts)
	g.expect(strings.Contains(section, "docker volume rm dev-health-acr-mcp_acr_entitlement_tmp"), "the switch procedure lost the removal of the old entitlement volume")
}

func composeACROldDefaults(g *guard) {
	ops := "POSTGRES_USER=devhealth\n"
	r := composeMust(g, composeIn{profiles: []string{"acr"}, opsEnv: ops})
	for _, n := range []string{"acr-postgres", "acr-db-init"} {
		got := mp(r.services[n]["environment"])["POSTGRES_USER"]
		g.expect(got == "acr_admin", "%s POSTGRES_USER=%q with ops/.env setting the ops user, want acr_admin", n, got)
	}
	const key = "ACR_CONTEXT_FABRIC_PROJECTION_ENABLED"
	got := mp(r.services["acr-projector"]["environment"])[key]
	g.expect(got == "true", "acr-projector %s=%q from the tracked env file, want true", key, got)
	off := composeMust(g, composeIn{profiles: []string{"acr"}, opsEnv: ops, pins: composeDropLine(key)})
	got = mp(off.services["acr-projector"]["environment"])[key]
	g.expect(got == "false", "acr-projector %s=%q without the env-file line, want the old default false", key, got)
}

var composeACRChecks = []check{
	{"the_default_stack_does_not_start_an_acr_service", composeACRDefaultStack},
	{"each_acr_profile_renders_alone_and_adds_only_acr_services", composeACRProfilesAddOnlyACR},
	{"the_data_stays_on_the_host_paths_of_the_old_project", composeACRDataHostPaths},
	{"container_names_and_aliases_keep_every_acr_hostname", composeACRContainerNames},
	{"the_acr_services_reach_the_networks_they_always_did", composeACRNetworks},
	{"only_the_mcp_server_publishes_a_port_and_only_on_loopback", composeACRPorts},
	{"no_secret_value_is_in_the_file_only_paths_of_secret_files", composeACRSecretsAreFiles},
	{"the_acr_images_are_pinned_by_registry_digest_and_a_missing_pin_fails_the_render", composeACRImagesPinned},
	{"the_start_order_of_the_acr_stack_is_kept", composeACRStartOrder},
	{"acr_api_talks_to_the_real_ops_and_the_stand_in_is_off_its_path", composeACRTalksToRealOps},
	{"the_org_llm_config_source_is_passed_through_and_no_file_sets_it", composeACROrgLLMSourcePassedThrough},
	{"the_switch_procedure_removes_the_old_containers_and_keeps_the_data", composeACRSwitchProcedure},
	{"the_acr_settings_keep_the_old_defaults_where_the_cutover_changes_the_env", composeACROldDefaults},
}

func TestComposeACRVenueReal(t *testing.T) { runReal(t, composeACRChecks...) }

func TestComposeACRVenuePlanted(t *testing.T) {
	readme := []string{"ci/bigboy/README.md"}
	cases := []planted{
		{name: "acr-mcp in the default stack", mutate: mut(composeFile, "      protocol: tcp\n    profiles: [acr]\n", "      protocol: tcp\n"),
			check: composeACRDefaultStack, want: "acr-mcp"},
		{name: "falkordb joins acr-venue", mutate: mut(composeFile, "        aliases: [falkordb]\n    profiles: [acr]\n", "        aliases: [falkordb]\n    profiles: [acr, acr-venue]\n"),
			check: composeACRProfilesAddOnlyACR, want: "profiles [acr-venue] add"},
		{name: "falkordb data moves", mutate: mut(composeFile, "source: /var/lib/oci-cache/mcp-venue/falkordb-data", "source: /var/lib/oci-cache/falkordb-new"),
			check: composeACRDataHostPaths, want: "acr-falkordb /var/lib/falkordb/data"},
		{name: "entitlement volume renamed", mutate: mut(composeFile, "name: dev-health-acr-entitlement-tmp", "name: acr-entitlement-tmp"),
			check: composeACRDataHostPaths, want: "acr_entitlement_tmp"},
		{name: "postgres container renamed", mutate: mut(composeFile, "container_name: dev-health-acr-mcp-postgres-1", "container_name: dev-health-acr-postgres-1"),
			check: composeACRContainerNames, want: "acr-postgres container_name"},
		{name: "falkordb alias lost", mutate: mut(composeFile, "aliases: [falkordb]", "aliases: [graph]"),
			check: composeACRContainerNames, want: "acr-falkordb answers"},
		{name: "ops postgres on the acr network", mutate: mut(composeFile, "    image: postgres:18-alpine\n    networks:\n      dev-health: null\n    ports:\n    - ${POSTGRES_PORT", "    image: postgres:18-alpine\n    networks:\n      dev-health: null\n      acr: null\n    ports:\n    - ${POSTGRES_PORT"),
			check: composeACRContainerNames, want: "dev-health postgres"},
		{name: "acr-api on the ops network", mutate: mut(composeFile, "      dho-clickhouse-readers: null\n      dho-query-internal: null\n    profiles: [acr]", "      dho-clickhouse-readers: null\n      dho-query-internal: null\n      dev-health: null\n    profiles: [acr]"),
			check: composeACRNetworks, want: "acr-api is on networks"},
		{name: "acr-api query url by address", mutate: mut(composeFile, "ACR_DATA_QUERY_URL: http://query-api:8095", "ACR_DATA_QUERY_URL: http://10.199.79.2:8095"),
			check: composeACRNetworks, want: "ACR_DATA_QUERY_URL"},
		{name: "mcp port on every address", mutate: mut(composeFile, "      host_ip: 127.0.0.1\n      target: 8081", "      host_ip: 0.0.0.0\n      target: 8081"),
			check: composeACRPorts, want: "loopback only"},
		{name: "secret file outside the secrets dir", mutate: mut(composeFile, "${ACR_RUNTIME_DSN_FILE:-/var/lib/oci-cache/mcp-venue/secrets/runtime_dsn}", "${ACR_RUNTIME_DSN_FILE:-/home/ubuntu/runtime_dsn}"),
			check: composeACRSecretsAreFiles, want: "secret acr_runtime_dsn"},
		{name: "dsn value inline", mutate: mut(composeFile, "ACR_POSTGRES_MIGRATION_DSN_FILE: /run/secrets/acr_migration_dsn", "ACR_POSTGRES_MIGRATION_DSN: postgres://acr:pw@postgres/acr"),
			check: composeACRSecretsAreFiles, want: "credential in a URL"},
		{name: "migrate runs the mcp image", mutate: mut(composeFile, "    image: ${BIGBOY_ACR_API_IMAGE:?", "    image: ${BIGBOY_ACR_MCP_IMAGE:?"),
			check: composeACRImagesPinned, want: "acr-migrate renders image"},
		{name: "falkordb pin has a default", mutate: mut(composeFile, "${BIGBOY_ACR_FALKORDB_IMAGE:?BIGBOY_ACR_FALKORDB_IMAGE is required", "${BIGBOY_ACR_FALKORDB_IMAGE:-falkordb/falkordb:latest"),
			check: composeACRImagesPinned, want: "without the BIGBOY_ACR_FALKORDB_IMAGE pin"},
		{name: "nginx pin is a tag", mutate: mut(composePins, "BIGBOY_ACR_NGINX_IMAGE=nginx:1.27-alpine@sha256:", "BIGBOY_ACR_NGINX_IMAGE=nginx:1.27-alpine#"),
			check: composeACRImagesPinned, want: "pin BIGBOY_ACR_NGINX_IMAGE"},
		{name: "mcp starts before api is up", mutate: mut(composeFile, "      acr-api:\n        condition: service_started", "      acr-api:\n        condition: service_healthy"),
			check: composeACRStartOrder, want: "acr-mcp depends_on"},
		{name: "acr-api back on the stand-in", mutate: mut(composeFile, "${ACR_DEV_HEALTH_ENTITLEMENT_URL:-http://go-api:8091}", "${ACR_DEV_HEALTH_ENTITLEMENT_URL:-http://acr-entitlement-standin:8088}"),
			check: composeACRTalksToRealOps, want: "ACR_DEV_HEALTH_ENTITLEMENT_URL"},
		{name: "retries setting back", mutate: mut(composeFile, "      ACR_CONTEXT_FABRIC_MODEL_MAX_ATTEMPTS: '1'\n", "      ACR_CONTEXT_FABRIC_MODEL_MAX_ATTEMPTS: '1'\n      ACR_CONTEXT_FABRIC_MODEL_MAX_TRANSPORT_RETRIES: '2'\n"),
			check: composeACRTalksToRealOps, want: "MAX_TRANSPORT_RETRIES"},
		{name: "llm source defaults to ops", mutate: mut(composeFile, "${ACR_CONTEXT_FABRIC_ORG_LLM_CONFIG_SOURCE:-}", "${ACR_CONTEXT_FABRIC_ORG_LLM_CONFIG_SOURCE:-ops}"),
			check: composeACROrgLLMSourcePassedThrough, want: "with nothing set"},
		{name: "env file sets the llm source", mutate: mut(composePins, "ACR_CONTEXT_FABRIC_PROJECTION_ENABLED=true", "ACR_CONTEXT_FABRIC_PROJECTION_ENABLED=true\nACR_CONTEXT_FABRIC_ORG_LLM_CONFIG_SOURCE=ops"),
			check: composeACROrgLLMSourcePassedThrough, want: "images.env sets"},
		{name: "down removes the volumes", copy: readme, mutate: mut("ci/bigboy/README.md", "-f compose.yml down`", "-f compose.yml down -v`"),
			check: composeACRSwitchProcedure, want: "removes data"},
		{name: "up starts the whole profile", copy: readme, mutate: mut("ci/bigboy/README.md", " acr-projector acr-mcp`", " acr-projector`"),
			check: composeACRSwitchProcedure, want: "nine acr services"},
		{name: "no graph save", copy: readme, mutate: mut("ci/bigboy/README.md", "redis-cli BGSAVE", "redis-cli SAVE"),
			check: composeACRSwitchProcedure, want: "BGSAVE"},
		{name: "acr admin is the ops user", mutate: mut(composeFile, "      POSTGRES_DB: ${ACR_DB_NAME:-acr}\n      POSTGRES_PASSWORD_FILE: /run/secrets/acr_postgres_password\n      POSTGRES_USER: ${ACR_POSTGRES_ADMIN_USER:-acr_admin}",
			"      POSTGRES_DB: ${ACR_DB_NAME:-acr}\n      POSTGRES_PASSWORD_FILE: /run/secrets/acr_postgres_password\n      POSTGRES_USER: ${POSTGRES_USER:-acr_admin}"),
			check: composeACROldDefaults, want: "acr-postgres POSTGRES_USER"},
		{name: "projector defaults on", mutate: mut(composeFile, "${ACR_CONTEXT_FABRIC_PROJECTION_ENABLED:-false}", "${ACR_CONTEXT_FABRIC_PROJECTION_ENABLED:-true}"),
			check: composeACROldDefaults, want: "old default false"},
	}
	for i := range cases {
		if cases[i].copy == nil {
			cases[i].copy = composeFiles
		}
	}
	runPlanted(t, cases)
}
